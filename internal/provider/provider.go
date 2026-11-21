// Package provider implements Cluster Autoscaler's externalgrpc CloudProvider service for node
// groups made of existing machines: growing a group wakes a powered-off machine, shrinking it powers
// one off.
package provider

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/config"
	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/kube"
	pb "github.com/oldsj/cluster-autoscaler-wol-provider/proto/externalgrpc"
)

// Power switches machines on and off.
type Power interface {
	PowerOn(ctx context.Context, m config.Machine) error
	PowerOff(ctx context.Context, m config.Machine) error
}

// staleAfter is how old the node list may be before a call re-reads it. Cluster Autoscaler calls
// Refresh at the start of every loop, so this only matters between loops.
const staleAfter = 10 * time.Second

// errDryRun is what every call that would change a machine returns in dry-run mode, so Cluster
// Autoscaler treats the scale-up or scale-down as failed and rolls back its own bookkeeping
// (taints, target sizes) instead of waiting for a change that won't come.
var errDryRun = status.Error(codes.FailedPrecondition, "dry-run: no machine was changed")

type phase int

const (
	off      phase = iota // powered off, or never seen
	creating              // woken, not registered yet
	running               // a Node object exists
	stopping              // power-off requested, Node object not removed yet
)

func (p phase) String() string {
	return [...]string{"off", "creating", "running", "stopping"}[p]
}

type machine struct {
	group  *config.NodeGroup
	spec   config.Machine
	wokeAt time.Time
	offAt  time.Time
}

// Provider is the CloudProvider gRPC service.
type Provider struct {
	pb.UnimplementedCloudProviderServer

	cfg    *config.Config
	power  Power
	nodes  kube.Nodes
	dryRun bool
	log    *slog.Logger
	now    func() time.Time

	mu        sync.Mutex
	machines  map[string]*machine // by node name
	groups    map[string]*config.NodeGroup
	snapshot  map[string]kube.Node
	refreshed time.Time
}

// Options are the provider's dependencies.
type Options struct {
	Config *config.Config
	Power  Power
	Nodes  kube.Nodes
	DryRun bool
	Log    *slog.Logger
	Now    func() time.Time
}

// New builds a provider. It reads nodes on the first call, not here.
func New(o Options) *Provider {
	p := &Provider{cfg: o.Config, power: o.Power, nodes: o.Nodes, dryRun: o.DryRun, log: o.Log, now: o.Now,
		machines: map[string]*machine{}, groups: map[string]*config.NodeGroup{}}
	if p.log == nil {
		p.log = slog.Default()
	}
	if p.now == nil {
		p.now = time.Now
	}
	for i := range o.Config.NodeGroups {
		g := &o.Config.NodeGroups[i]
		p.groups[g.ID] = g
		for _, m := range g.Machines {
			p.machines[m.Node] = &machine{group: g, spec: m}
		}
	}
	return p
}

// ---- state, under p.mu ----

func (p *Provider) phase(m *machine) phase {
	if !m.offAt.IsZero() {
		return stopping
	}
	if _, ok := p.snapshot[m.spec.Node]; ok {
		return running
	}
	if !m.wokeAt.IsZero() && p.now().Sub(m.wokeAt) < p.cfg.BootTimeout.Duration {
		return creating
	}
	return off
}

// refresh re-reads the nodes and moves machines along: woken ones that registered, powered-off ones
// whose Node object can go.
func (p *Provider) refresh(ctx context.Context) error {
	nodes, err := p.nodes.List(ctx)
	if err != nil {
		return status.Errorf(codes.Unavailable, "listing nodes: %v", err)
	}
	p.snapshot = nodes
	p.refreshed = p.now()
	for _, name := range p.order() {
		m := p.machines[name]
		node, exists := nodes[name]
		if !m.wokeAt.IsZero() {
			switch {
			case exists && node.Ready:
				p.log.Info("node registered and Ready", "node", name, "group", m.group.ID,
					"afterWake", p.now().Sub(m.wokeAt).Round(time.Second).String())
				m.wokeAt = time.Time{}
			case !exists && p.now().Sub(m.wokeAt) >= p.cfg.BootTimeout.Duration:
				p.log.Warn("woken machine didn't register in time; counting it as off", "node", name,
					"group", m.group.ID, "bootTimeout", p.cfg.BootTimeout.String())
				m.wokeAt = time.Time{}
			}
		}
		if m.offAt.IsZero() {
			continue
		}
		switch {
		case !exists:
			p.log.Info("powered off, Node object gone", "node", name, "group", m.group.ID)
			m.offAt = time.Time{}
		case !node.Ready:
			// The kubelet has stopped. The Node object would otherwise stay NotReady and count against
			// the group; the kubelet registers it again when the machine next boots.
			if err := p.nodes.Delete(ctx, name); err != nil {
				p.log.Error("deleting Node object of powered-off machine", "node", name, "err", err)
				continue
			}
			p.log.Info("powered off; deleted its Node object", "node", name, "group", m.group.ID,
				"afterPowerOff", p.now().Sub(m.offAt).Round(time.Second).String())
			delete(p.snapshot, name)
			m.offAt = time.Time{}
		case p.now().Sub(m.offAt) >= p.cfg.ShutdownTimeout.Duration:
			p.log.Error("machine still Ready after power-off; counting it as running", "node", name,
				"group", m.group.ID, "shutdownTimeout", p.cfg.ShutdownTimeout.String())
			m.offAt = time.Time{}
		}
	}
	return nil
}

func (p *Provider) refreshIfStale(ctx context.Context) error {
	if p.snapshot != nil && p.now().Sub(p.refreshed) < staleAfter {
		return nil
	}
	return p.refresh(ctx)
}

// order is every machine's node name, in config order.
func (p *Provider) order() []string {
	var names []string
	for _, g := range p.cfg.NodeGroups {
		for _, m := range g.Machines {
			names = append(names, m.Node)
		}
	}
	return names
}

func (p *Provider) inGroup(g *config.NodeGroup, want ...phase) []*machine {
	var out []*machine
	for _, spec := range g.Machines {
		m := p.machines[spec.Node]
		ph := p.phase(m)
		for _, w := range want {
			if ph == w {
				out = append(out, m)
				break
			}
		}
	}
	return out
}

func (p *Provider) targetSize(g *config.NodeGroup) int {
	return len(p.inGroup(g, running, creating))
}

func (p *Provider) group(id string) (*config.NodeGroup, error) {
	g, ok := p.groups[id]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no node group %q", id)
	}
	return g, nil
}

// machineFor finds the machine behind a node Cluster Autoscaler names, by provider ID or by name.
func (p *Provider) machineFor(n *pb.ExternalGrpcNode) *machine {
	if n == nil {
		return nil
	}
	if id, ok := strings.CutPrefix(n.GetProviderID(), p.cfg.ProviderIDPrefix); ok && id != "" {
		if m, ok := p.machines[id]; ok {
			return m
		}
	}
	return p.machines[n.GetName()]
}

func (p *Provider) pbGroup(g *config.NodeGroup) *pb.NodeGroup {
	return &pb.NodeGroup{Id: g.ID, MinSize: int32(g.MinSize), MaxSize: int32(g.MaxSize),
		Debug: fmt.Sprintf("%s: %d machine(s), min %d, max %d", g.ID, len(g.Machines), g.MinSize, g.MaxSize)}
}

// ---- CloudProvider ----

// NodeGroups lists every configured group.
func (p *Provider) NodeGroups(context.Context, *pb.NodeGroupsRequest) (*pb.NodeGroupsResponse, error) {
	out := &pb.NodeGroupsResponse{}
	for i := range p.cfg.NodeGroups {
		out.NodeGroups = append(out.NodeGroups, p.pbGroup(&p.cfg.NodeGroups[i]))
	}
	return out, nil
}

// NodeGroupForNode returns the node's group, or an empty one for nodes this provider doesn't manage.
func (p *Provider) NodeGroupForNode(_ context.Context, req *pb.NodeGroupForNodeRequest) (*pb.NodeGroupForNodeResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if m := p.machineFor(req.GetNode()); m != nil {
		return &pb.NodeGroupForNodeResponse{NodeGroup: p.pbGroup(m.group)}, nil
	}
	return &pb.NodeGroupForNodeResponse{NodeGroup: &pb.NodeGroup{}}, nil
}

// GPULabel returns the configured GPU node label.
func (p *Provider) GPULabel(context.Context, *pb.GPULabelRequest) (*pb.GPULabelResponse, error) {
	return &pb.GPULabelResponse{Label: p.cfg.GPULabel}, nil
}

// GetAvailableGPUTypes returns none: groups are fixed machines, not machine types.
func (p *Provider) GetAvailableGPUTypes(context.Context, *pb.GetAvailableGPUTypesRequest) (*pb.GetAvailableGPUTypesResponse, error) {
	return &pb.GetAvailableGPUTypesResponse{}, nil
}

// Cleanup has nothing to release.
func (p *Provider) Cleanup(context.Context, *pb.CleanupRequest) (*pb.CleanupResponse, error) {
	return &pb.CleanupResponse{}, nil
}

// Refresh re-reads the nodes; Cluster Autoscaler calls it at the start of every loop.
func (p *Provider) Refresh(ctx context.Context, _ *pb.RefreshRequest) (*pb.RefreshResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.refresh(ctx); err != nil {
		return nil, err
	}
	return &pb.RefreshResponse{}, nil
}

// NodeGroupTargetSize is how many of the group's machines are running or starting.
func (p *Provider) NodeGroupTargetSize(ctx context.Context, req *pb.NodeGroupTargetSizeRequest) (*pb.NodeGroupTargetSizeResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	g, err := p.group(req.GetId())
	if err != nil {
		return nil, err
	}
	if err := p.refreshIfStale(ctx); err != nil {
		return nil, err
	}
	return &pb.NodeGroupTargetSizeResponse{TargetSize: int32(p.targetSize(g))}, nil
}

// NodeGroupIncreaseSize wakes the next delta powered-off machines, in config order.
func (p *Provider) NodeGroupIncreaseSize(ctx context.Context, req *pb.NodeGroupIncreaseSizeRequest) (*pb.NodeGroupIncreaseSizeResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	g, err := p.group(req.GetId())
	if err != nil {
		return nil, err
	}
	delta := int(req.GetDelta())
	if delta <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "delta must be positive, got %d", delta)
	}
	if err := p.refreshIfStale(ctx); err != nil {
		return nil, err
	}
	if size := p.targetSize(g); size+delta > g.MaxSize {
		return nil, status.Errorf(codes.InvalidArgument, "%s: size %d + %d is over max %d", g.ID, size, delta, g.MaxSize)
	}
	candidates := p.inGroup(g, off)
	if len(candidates) < delta {
		return nil, status.Errorf(codes.FailedPrecondition, "%s: %d machine(s) off, %d wanted (a machine still powering off can't be woken yet)",
			g.ID, len(candidates), delta)
	}
	for _, m := range candidates[:delta] {
		if p.dryRun {
			p.log.Info("dry-run: would wake machine", "node", m.spec.Node, "group", g.ID, "method", m.spec.PowerOn,
				"broadcast", m.spec.Broadcast)
			continue
		}
		if err := p.power.PowerOn(ctx, m.spec); err != nil {
			p.log.Error("wake failed", "node", m.spec.Node, "group", g.ID, "err", err)
			return nil, status.Errorf(codes.Internal, "waking %s: %v", m.spec.Node, err)
		}
		m.wokeAt = p.now()
		p.log.Info("woke machine", "node", m.spec.Node, "group", g.ID, "method", m.spec.PowerOn)
	}
	if p.dryRun {
		return nil, errDryRun
	}
	return &pb.NodeGroupIncreaseSizeResponse{}, nil
}

// NodeGroupDeleteNodes powers off the machines behind the given nodes. Cluster Autoscaler has
// already drained them.
func (p *Provider) NodeGroupDeleteNodes(ctx context.Context, req *pb.NodeGroupDeleteNodesRequest) (*pb.NodeGroupDeleteNodesResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	g, err := p.group(req.GetId())
	if err != nil {
		return nil, err
	}
	if err := p.refreshIfStale(ctx); err != nil {
		return nil, err
	}
	var targets []*machine
	for _, n := range req.GetNodes() {
		m := p.machineFor(n)
		if m == nil || m.group != g {
			return nil, status.Errorf(codes.InvalidArgument, "node %q (%s) isn't in group %s", n.GetName(), n.GetProviderID(), g.ID)
		}
		targets = append(targets, m)
	}
	if size := p.targetSize(g); size-len(targets) < g.MinSize {
		return nil, status.Errorf(codes.FailedPrecondition, "%s: size %d - %d is under min %d", g.ID, size, len(targets), g.MinSize)
	}
	for _, m := range targets {
		name := m.spec.Node
		node := p.snapshot[name]
		switch ph := p.phase(m); {
		case ph == off || ph == stopping:
			p.log.Info("already off or powering off", "node", name, "group", g.ID, "phase", ph.String())
		case ph == creating:
			if p.dryRun {
				p.log.Info("dry-run: would stop counting a starting machine", "node", name, "group", g.ID)
				continue
			}
			// It can't be un-woken. If it boots anyway, it registers and is scaled down like any node.
			p.log.Warn("stopped counting a machine that hasn't registered; it may still boot", "node", name, "group", g.ID)
			m.wokeAt = time.Time{}
		case !node.Ready:
			if p.dryRun {
				p.log.Info("dry-run: would delete the Node object of a NotReady machine (no power-off)", "node", name, "group", g.ID)
				continue
			}
			p.log.Info("node NotReady; skipping power-off, removing its Node object", "node", name, "group", g.ID)
			m.offAt = p.now()
		default:
			if p.dryRun {
				p.log.Info("dry-run: would power off machine", "node", name, "group", g.ID, "method", m.spec.PowerOff)
				continue
			}
			if err := p.power.PowerOff(ctx, m.spec); err != nil {
				p.log.Error("power-off failed", "node", name, "group", g.ID, "method", m.spec.PowerOff, "err", err)
				return nil, status.Errorf(codes.Internal, "powering off %s: %v", name, err)
			}
			m.offAt = p.now()
			p.log.Info("powering off machine", "node", name, "group", g.ID, "method", m.spec.PowerOff)
		}
	}
	if p.dryRun {
		return nil, errDryRun
	}
	return &pb.NodeGroupDeleteNodesResponse{}, nil
}

// NodeGroupDecreaseTargetSize stops counting woken machines that haven't registered, latest first.
// It never touches a registered node.
func (p *Provider) NodeGroupDecreaseTargetSize(ctx context.Context, req *pb.NodeGroupDecreaseTargetSizeRequest) (*pb.NodeGroupDecreaseTargetSizeResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	g, err := p.group(req.GetId())
	if err != nil {
		return nil, err
	}
	delta := int(req.GetDelta())
	if delta >= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "delta must be negative, got %d", delta)
	}
	if err := p.refreshIfStale(ctx); err != nil {
		return nil, err
	}
	starting := p.inGroup(g, creating)
	if len(starting) < -delta {
		return nil, status.Errorf(codes.FailedPrecondition, "%s: only %d machine(s) starting, can't decrease by %d",
			g.ID, len(starting), -delta)
	}
	// Latest woken first.
	for i := 0; i < -delta; i++ {
		latest := 0
		for j, m := range starting {
			if m.wokeAt.After(starting[latest].wokeAt) {
				latest = j
			}
		}
		m := starting[latest]
		p.log.Info("stopped counting a starting machine", "node", m.spec.Node, "group", g.ID)
		m.wokeAt = time.Time{}
		starting = append(starting[:latest], starting[latest+1:]...)
	}
	return &pb.NodeGroupDecreaseTargetSizeResponse{}, nil
}

// NodeGroupNodes lists the group's machines that are running, starting or powering off.
func (p *Provider) NodeGroupNodes(ctx context.Context, req *pb.NodeGroupNodesRequest) (*pb.NodeGroupNodesResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	g, err := p.group(req.GetId())
	if err != nil {
		return nil, err
	}
	if err := p.refreshIfStale(ctx); err != nil {
		return nil, err
	}
	out := &pb.NodeGroupNodesResponse{}
	for _, m := range p.inGroup(g, running, creating, stopping) {
		state := map[phase]pb.InstanceStatus_InstanceState{
			running:  pb.InstanceStatus_instanceRunning,
			creating: pb.InstanceStatus_instanceCreating,
			stopping: pb.InstanceStatus_instanceDeleting,
		}[p.phase(m)]
		id := p.cfg.ProviderIDPrefix + m.spec.Node
		if node, ok := p.snapshot[m.spec.Node]; ok && node.ProviderID != "" {
			id = node.ProviderID
		}
		out.Instances = append(out.Instances, &pb.Instance{Id: id, Status: &pb.InstanceStatus{InstanceState: state}})
	}
	return out, nil
}

// NodeGroupTemplateNodeInfo returns the group's template node, for scaling up from zero.
func (p *Provider) NodeGroupTemplateNodeInfo(_ context.Context, req *pb.NodeGroupTemplateNodeInfoRequest) (*pb.NodeGroupTemplateNodeInfoResponse, error) {
	g, err := p.group(req.GetId())
	if err != nil {
		return nil, err
	}
	if g.Template == nil {
		return nil, status.Errorf(codes.Unimplemented, "%s has no template", g.ID)
	}
	node, err := TemplateNode(p.cfg.ProviderIDPrefix, g)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s template: %v", g.ID, err)
	}
	b, err := node.Marshal()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s template: %v", g.ID, err)
	}
	return &pb.NodeGroupTemplateNodeInfoResponse{NodeBytes: b}, nil
}

// TemplateNode builds the Node a machine of g registers as, from the config's template.
func TemplateNode(prefix string, g *config.NodeGroup) (*corev1.Node, error) {
	name := "template-" + g.ID
	labels := map[string]string{"kubernetes.io/hostname": name, "kubernetes.io/os": "linux"}
	maps.Copy(labels, g.Template.Labels)
	capacity := corev1.ResourceList{}
	for k, v := range g.Template.Capacity {
		q, err := resource.ParseQuantity(v)
		if err != nil {
			return nil, fmt.Errorf("capacity %s: %w", k, err)
		}
		capacity[corev1.ResourceName(k)] = q
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Spec:       corev1.NodeSpec{ProviderID: prefix + name, Taints: g.Template.Taints},
		Status: corev1.NodeStatus{
			Capacity:    capacity,
			Allocatable: capacity.DeepCopy(),
			Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}, nil
}
