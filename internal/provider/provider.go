// Package provider implements Cluster Autoscaler's externalgrpc CloudProvider service for node
// groups made of existing machines: growing a group powers a machine on, shrinking it powers one off.
// How a machine is powered on, powered off and probed is up to its plugins (package plugin).
package provider

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/config"
	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/kube"
	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugin"
	pb "github.com/oldsj/cluster-autoscaler-wol-provider/proto/externalgrpc"
)

// staleAfter is how old the node list may be before a call re-reads it. Cluster Autoscaler calls
// Refresh at the start of every loop, so this only matters between loops.
const staleAfter = 10 * time.Second

// fakeNodeAnnotation marks the placeholder node Cluster Autoscaler builds for an instance that never
// registered. It's never a real node, so it must never lead to powering off a machine that has one.
const fakeNodeAnnotation = "k8s.io/cluster-autoscaler/fake-node-reason"

// errDryRun is what every call that would change a machine returns in dry-run mode, so Cluster
// Autoscaler treats the scale-up or scale-down as failed and rolls back its own bookkeeping
// (taints, target sizes) instead of waiting for a change that won't come.
var errDryRun = status.Error(codes.FailedPrecondition, "dry-run: no machine was changed")

type phase int

const (
	off      phase = iota // powered off, or never seen
	creating              // powered on (or seen up by its probe), not registered yet
	running               // a Node object exists for it, and it isn't long NotReady
	stopping              // power-off requested, Node object not removed yet
)

func (p phase) String() string {
	return [...]string{"off", "creating", "running", "stopping"}[p]
}

// Plugins are one machine's built plugins. Probe may be nil.
type Plugins struct {
	On    plugin.PowerOn
	Off   plugin.PowerOff
	Probe plugin.Probe
}

type machine struct {
	group *config.NodeGroup
	spec  config.Machine
	pl    Plugins

	// node is the name of the Node object matched to the machine at the last refresh; empty if none.
	node string
	// warned is the last warning logged about the machine, so a lasting condition is logged once.
	warned string

	wokeAt   time.Time // powered on; counts as starting until it registers or bootTimeout passes
	lastSent time.Time // last power-on sent, for resending
	offAt    time.Time // power-off requested; the machine is stopping
	offSince time.Time // last seen go off; it may be woken again after minOffTime
	// powerOffRunning is set while a power-off plugin call is in flight.
	powerOffRunning bool

	// failedWakes counts wakes in a row that didn't register in time; such machines are woken last.
	failedWakes int

	probed  bool      // a probe result is known
	up      bool      // the last probe said the machine is up
	upSince time.Time // first probe that saw it up while it had no node

	// A pending approval, kept between scale-down attempts.
	approvalTicket string
	approvalAt     time.Time
}

func (m *machine) target() plugin.Target {
	return plugin.Target{Name: m.spec.Name, Node: m.node, Group: m.group.ID}
}

// Provider is the CloudProvider gRPC service.
type Provider struct {
	pb.UnimplementedCloudProviderServer

	cfg      *config.Config
	nodes    kube.Nodes
	approver plugin.Approver
	dryRun   bool
	log      *slog.Logger
	now      func() time.Time
	goAsync  func(func())

	mu        sync.Mutex
	machines  map[string]*machine // by machine name
	groups    map[string]*config.NodeGroup
	snapshot  map[string]kube.Node
	refreshed time.Time
}

// Options are the provider's dependencies.
type Options struct {
	Config *config.Config
	// Plugins are each machine's plugins, by machine name.
	Plugins map[string]Plugins
	// Approver, if set, must approve each power-off.
	Approver plugin.Approver
	Nodes    kube.Nodes
	DryRun   bool
	Log      *slog.Logger
	Now      func() time.Time
	// Go runs background work (resent power-ons, power-offs that outlive a request); nil means a
	// goroutine.
	Go func(func())
}

// New builds a provider. It reads nodes on the first call, not here.
func New(o Options) (*Provider, error) {
	p := &Provider{cfg: o.Config, nodes: o.Nodes, approver: o.Approver, dryRun: o.DryRun, log: o.Log, now: o.Now, goAsync: o.Go,
		machines: map[string]*machine{}, groups: map[string]*config.NodeGroup{}}
	if p.log == nil {
		p.log = slog.Default()
	}
	if p.now == nil {
		p.now = time.Now
	}
	if p.goAsync == nil {
		p.goAsync = func(f func()) { go f() }
	}
	for i := range o.Config.NodeGroups {
		g := &o.Config.NodeGroups[i]
		p.groups[g.ID] = g
		for _, m := range g.Machines {
			pl, ok := o.Plugins[m.Name]
			if !ok || pl.On == nil || pl.Off == nil {
				return nil, fmt.Errorf("machine %q: no power-on or power-off plugin", m.Name)
			}
			p.machines[m.Name] = &machine{group: g, spec: m, pl: pl}
		}
	}
	return p, nil
}

// ---- state, under p.mu ----

func (p *Provider) phase(m *machine) phase {
	now := p.now()
	if !m.offAt.IsZero() {
		return stopping
	}
	if m.node != "" {
		n := p.snapshot[m.node]
		switch {
		case n.Ready, m.probed && m.up:
			return running
		case !m.wokeAt.IsZero() && now.Sub(m.wokeAt) < p.cfg.BootTimeout.Duration:
			return creating
		case now.Sub(n.ReadyChanged) < p.cfg.NotReadyGrace.Duration:
			return running
		}
		// NotReady for long, and no probe saying it's up: crashed or switched off by hand.
		return off
	}
	if !m.wokeAt.IsZero() && now.Sub(m.wokeAt) < p.cfg.BootTimeout.Duration {
		return creating
	}
	if m.probed && m.up && !m.upSince.IsZero() && now.Sub(m.upSince) < p.cfg.BootTimeout.Duration {
		return creating
	}
	return off
}

// wakeable reports whether an off machine may be powered on now.
func (p *Provider) wakeable(m *machine) bool {
	if p.phase(m) != off || (m.probed && m.up) {
		return false
	}
	return m.offSince.IsZero() || p.now().Sub(m.offSince) >= p.cfg.MinOffTime.Duration
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
	p.resolve(ctx)
	now := p.now()
	for _, name := range p.order() {
		m := p.machines[name]
		node, exists := nodes[m.node]
		exists = exists && m.node != ""
		if exists && !node.PowerOffAt.IsZero() && m.offAt.IsZero() {
			// A power-off this provider asked for before it restarted.
			p.log.Info("resuming a power-off recorded on the node", "machine", name, "node", m.node, "requestedAt", node.PowerOffAt)
			m.offAt = node.PowerOffAt
		}
		if !m.wokeAt.IsZero() {
			switch {
			case exists && node.Ready:
				p.log.Info("node registered and Ready", "machine", name, "node", m.node, "group", m.group.ID,
					"afterWake", now.Sub(m.wokeAt).Round(time.Second).String())
				m.wokeAt, m.failedWakes, m.upSince = time.Time{}, 0, time.Time{}
			case now.Sub(m.wokeAt) >= p.cfg.BootTimeout.Duration:
				m.failedWakes++
				p.log.Warn("woken machine didn't become Ready in time; counting it as off and waking it after the others",
					"machine", name, "group", m.group.ID, "bootTimeout", p.cfg.BootTimeout.String(), "failedWakes", m.failedWakes)
				m.wokeAt = time.Time{}
			case !exists && now.Sub(m.lastSent) >= p.cfg.WakeResendInterval.Duration && !p.dryRun:
				p.resendPowerOn(m)
			}
		}
		if m.offAt.IsZero() {
			continue
		}
		down := m.probed && !m.up
		switch {
		case !exists:
			p.log.Info("powered off, Node object gone", "machine", name, "group", m.group.ID)
			m.offAt, m.offSince = time.Time{}, now
		case m.powerOffRunning:
			// Wait for the plugin call before deciding anything.
		case !node.Ready || down:
			// The machine has stopped. The Node object would otherwise stay NotReady and count
			// against the group; the kubelet registers it again when the machine next boots.
			if err := p.nodes.Delete(ctx, m.node); err != nil {
				p.log.Error("deleting Node object of powered-off machine", "machine", name, "node", m.node, "err", err)
				continue
			}
			p.log.Info("powered off; deleted its Node object", "machine", name, "node", m.node, "group", m.group.ID,
				"afterPowerOff", now.Sub(m.offAt).Round(time.Second).String())
			delete(p.snapshot, m.node)
			m.node = ""
			m.offAt, m.offSince = time.Time{}, now
		case now.Sub(m.offAt) >= p.cfg.ShutdownTimeout.Duration:
			p.log.Error("machine still Ready after power-off; giving the node back", "machine", name, "node", m.node,
				"group", m.group.ID, "shutdownTimeout", p.cfg.ShutdownTimeout.String())
			p.abandonPowerOff(ctx, m)
		}
	}
	return nil
}

// abandonPowerOff undoes a power-off that didn't happen: it clears the annotation and Cluster
// Autoscaler's deletion taint, so the node is schedulable again and the machine counts as running.
func (p *Provider) abandonPowerOff(ctx context.Context, m *machine) {
	m.offAt = time.Time{}
	if m.node == "" {
		return
	}
	if err := p.nodes.SetPowerOff(ctx, m.node, time.Time{}); err != nil {
		p.log.Error("removing power-off annotation", "machine", m.spec.Name, "node", m.node, "err", err)
	}
	if err := p.nodes.RemoveTaint(ctx, m.node, kube.ToBeDeletedTaint); err != nil {
		p.log.Error("removing Cluster Autoscaler's deletion taint", "machine", m.spec.Name, "node", m.node, "err", err)
	}
}

// resendPowerOn sends a starting machine's power-on again, in the background.
func (p *Provider) resendPowerOn(m *machine) {
	m.lastSent = p.now()
	on, t := m.pl.On, m.target()
	p.log.Debug("resending power-on", "machine", m.spec.Name)
	p.goAsync(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := on.PowerOn(ctx, t); err != nil {
			p.log.Warn("resending power-on failed", "machine", t.Name, "err", err)
		}
	})
}

func (p *Provider) refreshIfStale(ctx context.Context) error {
	if p.snapshot != nil && p.now().Sub(p.refreshed) < staleAfter {
		return nil
	}
	return p.refresh(ctx)
}

// order is every machine's name, in config order.
func (p *Provider) order() []string {
	var names []string
	for _, g := range p.cfg.NodeGroups {
		for _, m := range g.Machines {
			names = append(names, m.Name)
		}
	}
	return names
}

func (p *Provider) instanceID(m *machine) string {
	return p.cfg.InstanceID(&m.spec)
}

// warn logs a condition about a machine once, until a different one is logged.
func (p *Provider) warn(m *machine, level slog.Level, msg string, args ...any) {
	if m.warned == msg {
		return
	}
	m.warned = msg
	p.log.Log(context.Background(), level, msg, append([]any{"machine", m.spec.Name, "group", m.group.ID}, args...)...)
}

// resolve matches each machine to its Node object, by name or by label selector, and sets the
// node's providerID when it's empty.
func (p *Provider) resolve(ctx context.Context) {
	claimed := map[string]string{} // node name -> machine name
	for _, name := range p.order() {
		m := p.machines[name]
		m.node = ""
		var matches []string
		if m.spec.Node != "" {
			if _, ok := p.snapshot[m.spec.Node]; ok {
				matches = []string{m.spec.Node}
			}
		} else {
			for n, node := range p.snapshot {
				if selects(m.spec.NodeSelector, node.Labels) {
					matches = append(matches, n)
				}
			}
			sort.Strings(matches)
		}
		switch {
		case len(matches) == 0:
			continue
		case len(matches) > 1:
			p.warn(m, slog.LevelWarn, "nodeSelector matches more than one node; treating the machine as unmatched", "nodes", matches)
			continue
		}
		if other, ok := claimed[matches[0]]; ok {
			p.warn(m, slog.LevelWarn, "node already matched to another machine; treating this one as unmatched", "node", matches[0], "other", other)
			continue
		}
		claimed[matches[0]] = name
		m.node = matches[0]
		p.assignProviderID(ctx, m)
	}
}

func selects(selector, labels map[string]string) bool {
	for k, v := range selector {
		if got, ok := labels[k]; !ok || got != v {
			return false
		}
	}
	return true
}

// assignProviderID sets the machine's instance ID as its node's spec.providerID when that's empty.
// The API server allows this once, from empty to a value; it refuses any later change.
func (p *Provider) assignProviderID(ctx context.Context, m *machine) {
	node := p.snapshot[m.node]
	want := p.instanceID(m)
	switch {
	case node.ProviderID == want:
		m.warned = ""
	case node.ProviderID != "":
		p.warn(m, slog.LevelWarn, "node already has another providerID; using it. Set the machine's providerID to it to make that explicit",
			"node", m.node, "providerID", node.ProviderID, "expected", want)
	case !*p.cfg.AssignProviderIDs:
		p.warn(m, slog.LevelWarn, "node has no providerID and assignProviderIDs is false; Cluster Autoscaler can't match it to its instance",
			"node", m.node, "expected", want)
	default:
		if err := p.nodes.SetProviderID(ctx, m.node, want); err != nil {
			p.warn(m, slog.LevelError, "setting providerID failed; retrying on each refresh", "node", m.node, "providerID", want, "err", err)
			return
		}
		node.ProviderID = want
		p.snapshot[m.node] = node
		m.warned = ""
		p.log.Info("set node providerID", "machine", m.spec.Name, "node", m.node, "providerID", want, "dryRun", p.dryRun)
	}
}

// machineFor finds the machine behind a node Cluster Autoscaler names: by provider ID, then by the
// node it was matched to, then by its configured node name.
func (p *Provider) machineFor(n *pb.ExternalGrpcNode) *machine {
	if n == nil {
		return nil
	}
	if id := n.GetProviderID(); id != "" {
		for _, m := range p.machines {
			if id == p.instanceID(m) || (m.node != "" && p.snapshot[m.node].ProviderID == id) {
				return m
			}
		}
	}
	if name := n.GetName(); name != "" {
		for _, m := range p.machines {
			if m.node == name || (m.node == "" && m.spec.Node == name) {
				return m
			}
		}
	}
	return nil
}

func (p *Provider) inGroup(g *config.NodeGroup, want ...phase) []*machine {
	var out []*machine
	for _, spec := range g.Machines {
		m := p.machines[spec.Name]
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

// ---- probes ----

// RunProbes probes every machine that has a probe, every probeInterval, until ctx ends. Probes run
// outside the lock, so a slow one never holds up a Cluster Autoscaler call.
func (p *Provider) RunProbes(ctx context.Context) {
	t := time.NewTicker(p.cfg.ProbeInterval.Duration)
	defer t.Stop()
	for {
		p.ProbeOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// ProbeOnce probes every machine that has a probe, concurrently, and records the results.
func (p *Provider) ProbeOnce(ctx context.Context) {
	type result struct {
		m      *machine
		up, ok bool
	}
	p.mu.Lock()
	var todo []*machine
	targets := map[*machine]plugin.Target{}
	for _, m := range p.machines {
		if m.pl.Probe != nil {
			todo = append(todo, m)
			targets[m] = m.target()
		}
	}
	p.mu.Unlock()
	results := make(chan result, len(todo))
	for _, m := range todo {
		go func() {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			up, err := m.pl.Probe.Probe(ctx, targets[m])
			if err != nil {
				p.log.Warn("probe failed", "machine", m.spec.Name, "err", err)
			}
			results <- result{m, up, err == nil}
		}()
	}
	collected := make([]result, 0, len(todo))
	for range todo {
		collected = append(collected, <-results)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range collected {
		if !r.ok {
			r.m.probed = false
			continue
		}
		if r.up && (!r.m.probed || !r.m.up) && r.m.node == "" {
			r.m.upSince = p.now()
		}
		if !r.up {
			r.m.upSince = time.Time{}
		}
		r.m.probed, r.m.up = true, r.up
	}
}
