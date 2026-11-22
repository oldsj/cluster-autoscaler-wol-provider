package provider

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	corev1 "k8s.io/api/core/v1"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/config"
	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/kube"
	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugin"
	pb "github.com/oldsj/cluster-autoscaler-wol-provider/proto/externalgrpc"
)

var bg = context.Background()

const testConfig = `
bootTimeout: 10m
shutdownTimeout: 5m
nodeGroups:
  - id: gpu
    minSize: 0
    maxSize: 2
    template:
      labels: {node-role.example.com/gpu: ""}
      taints: [{key: example.com/gpu, value: "true", effect: NoSchedule}]
      capacity: {cpu: "8", memory: 32Gi, pods: "110", nvidia.com/gpu: "1"}
    machines:
      - {node: node-a, powerOn: {wol: {mac: "00:00:5e:00:53:01"}}, powerOff: {ssh: {host: 192.0.2.11}}}
      - {node: node-b, powerOn: {wol: {mac: "00:00:5e:00:53:02"}}, powerOff: {ssh: {host: 192.0.2.12}}}
  - id: big
    minSize: 1
    maxSize: 1
    machines:
      - {node: node-c, powerOn: {wol: {mac: "00:00:5e:00:53:03"}}, powerOff: {ssh: {host: 192.0.2.13}}}
`

// fakePower is every machine's power-on and power-off plugin.
type fakePower struct {
	mu            sync.Mutex
	on, off       []string
	onErr, offErr error
}

func (f *fakePower) PowerOn(_ context.Context, t plugin.Target) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onErr != nil {
		return f.onErr
	}
	f.on = append(f.on, t.Name)
	return nil
}

func (f *fakePower) PowerOff(_ context.Context, t plugin.Target) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.off = append(f.off, t.Name)
	return f.offErr
}

type fakeProbe struct{ up map[string]bool }

func (f *fakeProbe) Probe(_ context.Context, t plugin.Target) (bool, error) { return f.up[t.Name], nil }

type fakeNodes struct {
	nodes   map[string]kube.Node
	deleted []string
	set     []string // "node=providerID"
	listErr error
	setErr  error
}

// SetProviderID behaves like the API server: empty to a value once, the same value again is a no-op,
// any other change is refused.
func (f *fakeNodes) SetProviderID(_ context.Context, name, id string) error {
	if f.setErr != nil {
		return f.setErr
	}
	n, ok := f.nodes[name]
	switch {
	case !ok:
		return errors.New("not found")
	case n.ProviderID != "" && n.ProviderID != id:
		return errors.New("spec.providerID: Forbidden")
	}
	n.ProviderID = id
	f.nodes[name] = n
	f.set = append(f.set, name+"="+id)
	return nil
}

func (f *fakeNodes) SetPowerOff(_ context.Context, name string, at time.Time) error {
	n, ok := f.nodes[name]
	if !ok {
		if at.IsZero() {
			return nil
		}
		return errors.New("not found")
	}
	n.PowerOffAt = at
	f.nodes[name] = n
	return nil
}

func (f *fakeNodes) RemoveTaint(_ context.Context, name, key string) error {
	if n, ok := f.nodes[name]; ok && key == kube.ToBeDeletedTaint {
		n.ToBeDeleted = false
		f.nodes[name] = n
	}
	return nil
}

func (f *fakeNodes) List(context.Context) (map[string]kube.Node, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := map[string]kube.Node{}
	for k, v := range f.nodes {
		out[k] = v
	}
	return out, nil
}

func (f *fakeNodes) Delete(_ context.Context, name string) error {
	f.deleted = append(f.deleted, name)
	delete(f.nodes, name)
	return nil
}

// countLog counts log records by message.
type countLog struct {
	mu sync.Mutex
	n  map[string]int
}

func (c *countLog) Enabled(context.Context, slog.Level) bool { return true }
func (c *countLog) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n[r.Message]++
	return nil
}
func (c *countLog) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *countLog) WithGroup(string) slog.Handler      { return c }

func (c *countLog) count(prefix string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for msg, n := range c.n {
		if strings.HasPrefix(msg, prefix) {
			total += n
		}
	}
	return total
}

type harness struct {
	p     *Provider
	power *fakePower
	probe *fakeProbe
	nodes *fakeNodes
	logs  *countLog
	now   time.Time
}

type opt func(*Options, *harness)

func withProbes(o *Options, h *harness) {
	for name, pl := range o.Plugins {
		pl.Probe = h.probe
		o.Plugins[name] = pl
	}
}

func withApprover(a plugin.Approver) opt {
	return func(o *Options, _ *harness) { o.Approver = a }
}

func newHarness(t *testing.T, dryRun bool, nodes map[string]kube.Node, opts ...opt) *harness {
	t.Helper()
	return newHarnessWith(t, testConfig, dryRun, nodes, opts...)
}

func newHarnessWith(t *testing.T, yaml string, dryRun bool, nodes map[string]kube.Node, opts ...opt) *harness {
	t.Helper()
	cfg, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{power: &fakePower{}, probe: &fakeProbe{up: map[string]bool{}}, nodes: &fakeNodes{nodes: nodes},
		logs: &countLog{n: map[string]int{}}, now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	o := Options{Config: cfg, Plugins: map[string]Plugins{}, Nodes: h.nodes, DryRun: dryRun,
		Log: slog.New(h.logs), Now: func() time.Time { return h.now }, Go: func(f func()) { f() }}
	for _, g := range cfg.NodeGroups {
		for _, m := range g.Machines {
			o.Plugins[m.Name] = Plugins{On: h.power, Off: h.power}
		}
	}
	for _, f := range opts {
		f(&o, h)
	}
	if h.p, err = New(o); err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *harness) tick(t *testing.T, d time.Duration) {
	t.Helper()
	h.now = h.now.Add(d)
	if _, err := h.p.Refresh(bg, &pb.RefreshRequest{}); err != nil {
		t.Fatal(err)
	}
}

func (h *harness) target(t *testing.T, id string) int {
	t.Helper()
	r, err := h.p.NodeGroupTargetSize(bg, &pb.NodeGroupTargetSizeRequest{Id: id})
	if err != nil {
		t.Fatal(err)
	}
	return int(r.TargetSize)
}

func (h *harness) instances(t *testing.T, id string) map[string]pb.InstanceStatus_InstanceState {
	t.Helper()
	r, err := h.p.NodeGroupNodes(bg, &pb.NodeGroupNodesRequest{Id: id})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]pb.InstanceStatus_InstanceState{}
	for _, i := range r.Instances {
		out[i.Id] = i.Status.InstanceState
	}
	return out
}

func (h *harness) increase(id string, delta int32) error {
	_, err := h.p.NodeGroupIncreaseSize(bg, &pb.NodeGroupIncreaseSizeRequest{Id: id, Delta: delta})
	return err
}

func (h *harness) remove(id string, nodes ...*pb.ExternalGrpcNode) error {
	_, err := h.p.NodeGroupDeleteNodes(bg, &pb.NodeGroupDeleteNodesRequest{Id: id, Nodes: nodes})
	return err
}

// setNode changes a node the way the cluster would.
func (h *harness) setNode(name string, f func(*kube.Node)) {
	n := h.nodes.nodes[name]
	f(&n)
	h.nodes.nodes[name] = n
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("got %v, want code %v", err, want)
	}
}

func ready() kube.Node { return kube.Node{Ready: true} }

// drained is a Ready node Cluster Autoscaler has tainted for deletion.
func drained() kube.Node { return kube.Node{Ready: true, ToBeDeleted: true} }

func node(name, pid string) *pb.ExternalGrpcNode {
	return &pb.ExternalGrpcNode{Name: name, ProviderID: pid}
}

// placeholder is the fake node Cluster Autoscaler sends for an instance it thinks never registered.
func placeholder(pid string) *pb.ExternalGrpcNode {
	return &pb.ExternalGrpcNode{Name: pid, ProviderID: pid, Annotations: map[string]string{fakeNodeAnnotation: "unregistered"}}
}

func TestNodeGroupsAndLookup(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{})
	r, _ := h.p.NodeGroups(bg, &pb.NodeGroupsRequest{})
	if len(r.NodeGroups) != 2 || r.NodeGroups[0].Id != "gpu" || r.NodeGroups[0].MaxSize != 2 || r.NodeGroups[1].MinSize != 1 {
		t.Fatalf("groups: %v", r.NodeGroups)
	}
	for _, tc := range []struct {
		n    *pb.ExternalGrpcNode
		want string
	}{
		{node("whatever", "wol://node-b"), "gpu"},
		{node("node-c", ""), "big"},
		{node("other", "aws:///i-123"), ""},
	} {
		g, _ := h.p.NodeGroupForNode(bg, &pb.NodeGroupForNodeRequest{Node: tc.n})
		if g.NodeGroup.Id != tc.want {
			t.Errorf("%v: group %q, want %q", tc.n, g.NodeGroup.Id, tc.want)
		}
	}
}

func TestScaleUpWakesInOrderAndRegisters(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{"node-c": ready()})
	if got := h.target(t, "gpu"); got != 0 {
		t.Fatalf("target %d", got)
	}
	if err := h.increase("gpu", 1); err != nil {
		t.Fatal(err)
	}
	if len(h.power.on) != 1 || h.power.on[0] != "node-a" {
		t.Fatalf("woke %v, want node-a", h.power.on)
	}
	if got := h.target(t, "gpu"); got != 1 {
		t.Fatalf("target after wake %d", got)
	}
	if got := h.instances(t, "gpu"); got["wol://node-a"] != pb.InstanceStatus_instanceCreating {
		t.Fatalf("instances %v", got)
	}
	h.nodes.nodes["node-a"] = kube.Node{Ready: true, ProviderID: "wol://node-a"}
	h.tick(t, 20*time.Second)
	if got := h.instances(t, "gpu"); got["wol://node-a"] != pb.InstanceStatus_instanceRunning {
		t.Fatalf("instances after register %v", got)
	}
	if err := h.increase("gpu", 1); err != nil {
		t.Fatal(err)
	}
	if h.power.on[len(h.power.on)-1] != "node-b" {
		t.Fatalf("second wake %v", h.power.on)
	}
}

func TestScaleUpLimits(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{"node-a": ready()})
	wantCode(t, h.increase("gpu", 2), codes.InvalidArgument)
	wantCode(t, h.increase("gpu", 0), codes.InvalidArgument)
	wantCode(t, h.increase("nope", 1), codes.NotFound)
	if len(h.power.on) != 0 {
		t.Fatalf("woke %v", h.power.on)
	}
}

func TestScaleUpPluginFailureFreesTheMachine(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{})
	h.power.onErr = errors.New("no route")
	wantCode(t, h.increase("gpu", 1), codes.Internal)
	if got := h.target(t, "gpu"); got != 0 {
		t.Fatalf("target %d", got)
	}
	h.power.onErr = nil
	if err := h.increase("gpu", 2); err != nil {
		t.Fatal(err)
	}
}

func TestBootTimeoutCountsItOff(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{})
	if err := h.increase("gpu", 1); err != nil {
		t.Fatal(err)
	}
	h.tick(t, 9*time.Minute)
	if got := h.target(t, "gpu"); got != 1 {
		t.Fatalf("target before timeout %d", got)
	}
	h.tick(t, 2*time.Minute)
	if got := h.target(t, "gpu"); got != 0 {
		t.Fatalf("target after timeout %d", got)
	}
}

// Review #5: the power-on is resent while the machine is starting, not sent once.
func TestPowerOnResentWhileStarting(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{})
	if err := h.increase("gpu", 1); err != nil {
		t.Fatal(err)
	}
	for range 8 {
		h.tick(t, 30*time.Second)
	}
	if len(h.power.on) < 8 {
		t.Fatalf("power-on sent %d times in 4 minutes, want a resend every 30 s", len(h.power.on))
	}
	h.nodes.nodes["node-a"] = ready()
	h.tick(t, 30*time.Second)
	sent := len(h.power.on)
	h.tick(t, time.Minute)
	if len(h.power.on) != sent {
		t.Fatalf("resent after it registered: %v", h.power.on)
	}
}

// Review #9: a machine whose wake timed out is woken after the others.
func TestDeadFirstMachineIsDeprioritised(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{})
	var woken []string
	for range 3 {
		before := len(h.power.on)
		if err := h.increase("gpu", 1); err != nil {
			t.Fatal(err)
		}
		woken = append(woken, h.power.on[before])
		for range 66 {
			h.tick(t, 10*time.Second)
		}
	}
	if strings.Join(woken, ",") != "node-a,node-b,node-a" {
		t.Fatalf("woke %v, want node-a, then node-b, then node-a again", woken)
	}
}

func TestScaleDownPowersOffThenRemovesNode(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{"node-a": ready(), "node-b": drained()})
	if err := h.remove("gpu", node("node-b", "wol://node-b")); err != nil {
		t.Fatal(err)
	}
	if len(h.power.off) != 1 || h.power.off[0] != "node-b" {
		t.Fatalf("powered off %v", h.power.off)
	}
	if h.nodes.nodes["node-b"].PowerOffAt.IsZero() {
		t.Fatal("power-off not recorded on the node")
	}
	if got := h.target(t, "gpu"); got != 1 {
		t.Fatalf("target %d", got)
	}
	if got := h.instances(t, "gpu"); got["wol://node-b"] != pb.InstanceStatus_instanceDeleting {
		t.Fatalf("instances %v", got)
	}
	h.tick(t, 30*time.Second) // still Ready: kubelet not stopped yet
	if len(h.nodes.deleted) != 0 {
		t.Fatalf("deleted too early %v", h.nodes.deleted)
	}
	h.setNode("node-b", func(n *kube.Node) { n.Ready = false })
	h.tick(t, 30*time.Second)
	if len(h.nodes.deleted) != 1 || h.nodes.deleted[0] != "node-b" {
		t.Fatalf("deleted %v", h.nodes.deleted)
	}
	if got := h.instances(t, "gpu"); len(got) != 1 {
		t.Fatalf("instances after %v", got)
	}
	// Not woken again before minOffTime, then woken.
	wantCode(t, h.increase("gpu", 1), codes.FailedPrecondition)
	h.tick(t, time.Minute)
	if err := h.increase("gpu", 1); err != nil || h.power.on[0] != "node-b" {
		t.Fatalf("rewake: %v %v", err, h.power.on)
	}
}

// Review #1: a Ready node is powered off only with Cluster Autoscaler's deletion taint.
func TestScaleDownRefusesUntaintedReadyNode(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{"node-a": ready(), "node-b": ready()})
	wantCode(t, h.remove("gpu", node("node-a", "wol://node-a")), codes.FailedPrecondition)
	if len(h.power.off) != 0 {
		t.Fatalf("powered off %v", h.power.off)
	}
}

// Review #1: the providerID patch fails, Cluster Autoscaler thinks the instance never registered and
// sends a placeholder node. The machine has a Ready node, so nothing is powered off.
func TestPlaceholderForRegisteredMachineIsRefused(t *testing.T) {
	for name, tc := range map[string]struct {
		yaml  string
		patch error
	}{
		"patch fails":              {testConfig, errors.New(`nodes "node-a" is forbidden: cannot patch`)},
		"assignment disabled":      {"assignProviderIDs: false\n" + testConfig, nil},
		"even with deletion taint": {testConfig, errors.New("forbidden")},
	} {
		t.Run(name, func(t *testing.T) {
			n := ready()
			if strings.HasPrefix(name, "even") {
				n = drained()
			}
			h := newHarnessWith(t, tc.yaml, false, map[string]kube.Node{"node-a": n})
			h.nodes.setErr = tc.patch
			h.tick(t, time.Second)
			if pid := h.nodes.nodes["node-a"].ProviderID; pid != "" {
				t.Fatalf("providerID %q", pid)
			}
			wantCode(t, h.remove("gpu", placeholder("wol://node-a")), codes.FailedPrecondition)
			if len(h.power.off) != 0 {
				t.Fatalf("powered off %v", h.power.off)
			}
		})
	}
}

// A placeholder for a machine that really has no node is fine: it was woken and never registered.
func TestPlaceholderForUnregisteredMachine(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{})
	if err := h.increase("gpu", 1); err != nil {
		t.Fatal(err)
	}
	if err := h.remove("gpu", placeholder("wol://node-a")); err != nil {
		t.Fatal(err)
	}
	if len(h.power.off) != 0 || h.target(t, "gpu") != 0 {
		t.Fatalf("off %v target %d", h.power.off, h.target(t, "gpu"))
	}
}

// Review #13: a retried scale-down for a machine already stopping succeeds and powers off once.
func TestScaleDownRetryIsIdempotent(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{"node-a": {Ready: true, ToBeDeleted: true, ProviderID: "wol://node-a"}})
	req := node("node-a", "wol://node-a")
	if err := h.remove("gpu", req); err != nil {
		t.Fatal(err)
	}
	if err := h.remove("gpu", req); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if len(h.power.off) != 1 {
		t.Fatalf("powered off %v", h.power.off)
	}
}

// Review #6: a NotReady node is still powered off, and a failure is tolerated.
func TestScaleDownNotReadyStillPowersOff(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{"node-a": {Ready: false, ToBeDeleted: true}})
	h.power.offErr = errors.New("no route to host")
	if err := h.remove("gpu", node("node-a", "")); err != nil {
		t.Fatal(err)
	}
	if len(h.power.off) != 1 {
		t.Fatalf("power-off not attempted: %v", h.power.off)
	}
	h.tick(t, time.Second)
	if len(h.nodes.deleted) != 1 {
		t.Fatalf("deleted %v", h.nodes.deleted)
	}
}

func TestScaleDownLimitsAndErrors(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{"node-a": drained(), "node-c": drained()})
	wantCode(t, h.remove("big", node("node-c", "")), codes.FailedPrecondition) // min 1
	wantCode(t, h.remove("gpu", node("node-c", "")), codes.InvalidArgument)    // other group
	h.power.offErr = errors.New("connection refused")
	wantCode(t, h.remove("gpu", node("node-a", "")), codes.Internal)
	if got := h.instances(t, "gpu"); got["wol://node-a"] != pb.InstanceStatus_instanceRunning {
		t.Fatalf("failed power-off changed state: %v", got)
	}
	if n := h.nodes.nodes["node-a"]; n.ToBeDeleted || !n.PowerOffAt.IsZero() {
		t.Fatalf("failed power-off left taint or annotation: %+v", n)
	}
}

func TestShutdownTimeoutGivesTheNodeBack(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{"node-a": drained()})
	if err := h.remove("gpu", node("node-a", "")); err != nil {
		t.Fatal(err)
	}
	h.tick(t, 6*time.Minute)
	if got := h.instances(t, "gpu"); got["wol://node-a"] != pb.InstanceStatus_instanceRunning {
		t.Fatalf("instances %v", got)
	}
	if len(h.nodes.deleted) != 0 {
		t.Fatalf("deleted a Ready node %v", h.nodes.deleted)
	}
	if n := h.nodes.nodes["node-a"]; n.ToBeDeleted || !n.PowerOffAt.IsZero() {
		t.Fatalf("taint or annotation left: %+v", n)
	}
}

// Review #4: a provider restarted mid-shutdown resumes from the node's annotation.
func TestRestartMidShutdownResumes(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{"node-a": drained()})
	if err := h.remove("gpu", node("node-a", "")); err != nil {
		t.Fatal(err)
	}
	// A new provider process, same cluster.
	h2 := newHarness(t, false, h.nodes.nodes)
	h2.now = h.now
	if got := h2.instances(t, "gpu"); got["wol://node-a"] != pb.InstanceStatus_instanceDeleting {
		t.Fatalf("instances after restart %v", got)
	}
	h2.setNode("node-a", func(n *kube.Node) { n.Ready = false })
	h2.tick(t, 30*time.Second)
	if len(h2.nodes.deleted) != 1 {
		t.Fatalf("deleted %v", h2.nodes.deleted)
	}
}

// Review #4: NotReady leftovers (a restart lost the power-off, or a machine switched off by hand)
// stop counting after notReadyGrace and can be woken.
func TestNotReadyLeftoversBecomeWakeable(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{
		"node-a": {Ready: false, ProviderID: "wol://node-a"},
		"node-b": {Ready: false, ProviderID: "wol://node-b"},
	})
	h.tick(t, time.Hour)
	if got := h.target(t, "gpu"); got != 0 {
		t.Fatalf("target %d", got)
	}
	if err := h.increase("gpu", 1); err != nil {
		t.Fatal(err)
	}
	if got := h.instances(t, "gpu"); got["wol://node-a"] != pb.InstanceStatus_instanceCreating {
		t.Fatalf("instances %v", got)
	}
	// A recent NotReady (a kubelet restart) still counts as running.
	h.setNode("node-b", func(n *kube.Node) { n.ReadyChanged = h.now })
	h.tick(t, 0)
	if got := h.target(t, "gpu"); got != 2 {
		t.Fatalf("target %d", got)
	}
}

func TestProbe(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{"node-a": drained()}, withProbes)
	// Up but not registered: counts as starting and isn't woken again.
	h.probe.up["node-b"] = true
	h.p.ProbeOnce(bg)
	if got := h.target(t, "gpu"); got != 2 {
		t.Fatalf("target %d", got)
	}
	// Powering off a Ready node: the node goes once the probe says it's down, kubelet or not.
	h.probe.up["node-a"] = true
	h.p.ProbeOnce(bg)
	if err := h.remove("gpu", node("node-a", "")); err != nil {
		t.Fatal(err)
	}
	h.probe.up["node-a"] = false
	h.p.ProbeOnce(bg)
	h.tick(t, time.Second)
	if len(h.nodes.deleted) != 1 {
		t.Fatalf("deleted %v", h.nodes.deleted)
	}
}

type fakeApprover struct {
	answers []plugin.Decision
	tickets []string
	dryRun  bool
}

func (f *fakeApprover) Approve(_ context.Context, t plugin.Target, ticket string) (plugin.Decision, string, error) {
	f.tickets = append(f.tickets, ticket)
	f.dryRun = t.DryRun
	d := f.answers[0]
	if len(f.answers) > 1 {
		f.answers = f.answers[1:]
	}
	var err error
	if d == plugin.Denied {
		err = errors.New("no")
	}
	return d, "t1", err
}

func TestApproval(t *testing.T) {
	cfg := "approval: {http: {url: http://192.0.2.1/approve}, wait: 1ms, pollInterval: 5ms}\n" + testConfig
	t.Run("pending then approved", func(t *testing.T) {
		a := &fakeApprover{answers: []plugin.Decision{plugin.Pending}}
		h := newHarnessWith(t, cfg, false, map[string]kube.Node{"node-a": drained()}, withApprover(a))
		wantCode(t, h.remove("gpu", node("node-a", "")), codes.Unavailable)
		if len(h.power.off) != 0 {
			t.Fatal("powered off while pending")
		}
		a.tickets, a.answers = nil, []plugin.Decision{plugin.Approved}
		if err := h.remove("gpu", node("node-a", "")); err != nil {
			t.Fatal(err)
		}
		if len(h.power.off) != 1 || strings.Join(a.tickets, ",") != "t1" {
			t.Fatalf("off %v tickets %q", h.power.off, a.tickets)
		}
	})
	t.Run("denied", func(t *testing.T) {
		a := &fakeApprover{answers: []plugin.Decision{plugin.Denied}}
		h := newHarnessWith(t, cfg, false, map[string]kube.Node{"node-a": drained()}, withApprover(a))
		wantCode(t, h.remove("gpu", node("node-a", "")), codes.PermissionDenied)
		if len(h.power.off) != 0 {
			t.Fatal("powered off when denied")
		}
	})
}

// In dry-run the approval hook is asked (with DryRun set) and its answer logged, but nothing changes.
func TestDryRunAsksApproval(t *testing.T) {
	cfg := "approval: {http: {url: http://192.0.2.1/approve}, wait: 1ms, pollInterval: 5ms}\n" + testConfig
	for _, d := range []plugin.Decision{plugin.Approved, plugin.Denied, plugin.Pending} {
		a := &fakeApprover{answers: []plugin.Decision{d}}
		h := newHarnessWith(t, cfg, true, map[string]kube.Node{"node-a": drained()}, withApprover(a))
		wantCode(t, h.remove("gpu", node("node-a", "")), codes.FailedPrecondition)
		if len(a.tickets) == 0 || !a.dryRun {
			t.Fatalf("decision %v: approver asked %d times, dryRun %v", d, len(a.tickets), a.dryRun)
		}
		if got := h.logs.count("dry-run: approval answered"); got != 1 {
			t.Fatalf("decision %v: %d answer logs", d, got)
		}
		if len(h.power.off)+len(h.nodes.deleted) != 0 || !h.nodes.nodes["node-a"].PowerOffAt.IsZero() {
			t.Fatalf("decision %v: dry-run acted: off %v deleted %v", d, h.power.off, h.nodes.deleted)
		}
	}
}

// Review #14: Cluster Autoscaler's revert of a timed-out scale-up succeeds even when nothing is left.
func TestDecreaseTargetSize(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{})
	if err := h.increase("gpu", 1); err != nil {
		t.Fatal(err)
	}
	h.tick(t, time.Second)
	if err := h.increase("gpu", 1); err != nil {
		t.Fatal(err)
	}
	dec := func(d int32) error {
		_, err := h.p.NodeGroupDecreaseTargetSize(bg, &pb.NodeGroupDecreaseTargetSizeRequest{Id: "gpu", Delta: d})
		return err
	}
	wantCode(t, dec(1), codes.InvalidArgument)
	if err := dec(-1); err != nil {
		t.Fatal(err)
	}
	if got := h.instances(t, "gpu"); len(got) != 1 || got["wol://node-a"] != pb.InstanceStatus_instanceCreating {
		t.Fatalf("latest woken should go first: %v", got)
	}
	// A registered node is never removed this way.
	h.nodes.nodes["node-a"] = ready()
	h.tick(t, time.Second)
	if err := dec(-1); err != nil {
		t.Fatal(err)
	}
	if h.target(t, "gpu") != 1 {
		t.Fatal("decrease removed a registered node")
	}
	// After bootTimeout there's nothing left to decrease; still a success.
	h.tick(t, 20*time.Minute)
	if err := dec(-3); err != nil {
		t.Fatal(err)
	}
}

func TestDryRunChangesNothing(t *testing.T) {
	h := newHarness(t, true, map[string]kube.Node{"node-a": drained()})
	wantCode(t, h.increase("gpu", 1), codes.FailedPrecondition)
	wantCode(t, h.remove("gpu", node("node-a", "")), codes.FailedPrecondition)
	h.tick(t, time.Minute)
	if len(h.power.on)+len(h.power.off)+len(h.nodes.deleted) != 0 || !h.nodes.nodes["node-a"].PowerOffAt.IsZero() {
		t.Fatalf("dry-run acted: on %v off %v deleted %v", h.power.on, h.power.off, h.nodes.deleted)
	}
	if got := h.target(t, "gpu"); got != 1 {
		t.Fatalf("target %d", got)
	}
}

func TestInstanceUsesRegisteredProviderID(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{"node-a": {Ready: true, ProviderID: "talos://metal/node-a"}})
	if got := h.instances(t, "gpu"); got["talos://metal/node-a"] != pb.InstanceStatus_instanceRunning {
		t.Fatalf("instances %v", got)
	}
}

func TestAssignsEmptyProviderID(t *testing.T) {
	for _, dryRun := range []bool{false, true} {
		h := newHarness(t, dryRun, map[string]kube.Node{
			"node-a": {Ready: true},
			"node-b": {Ready: true, ProviderID: "talos://metal/node-b"},
		})
		h.tick(t, 0)
		if len(h.nodes.set) != 1 || h.nodes.set[0] != "node-a=wol://node-a" {
			t.Fatalf("dryRun %v: set %v", dryRun, h.nodes.set)
		}
		got := h.instances(t, "gpu")
		if got["wol://node-a"] != pb.InstanceStatus_instanceRunning || got["talos://metal/node-b"] != pb.InstanceStatus_instanceRunning {
			t.Fatalf("instances %v", got)
		}
		h.tick(t, time.Minute)
		if len(h.nodes.set) != 1 {
			t.Fatalf("set again: %v", h.nodes.set)
		}
	}
}

// PR #4 review: a failing patch is retried on each refresh but logged once.
func TestAssignRetriesAfterErrorLoggingOnce(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{"node-a": {Ready: true}})
	h.nodes.setErr = errors.New("forbidden")
	for range 5 {
		h.tick(t, 10*time.Second)
	}
	if n := h.logs.count("setting providerID failed"); n != 1 {
		t.Fatalf("logged %d times", n)
	}
	h.nodes.setErr = nil
	h.tick(t, time.Minute)
	if len(h.nodes.set) != 1 {
		t.Fatalf("set %v", h.nodes.set)
	}
}

func TestAssignDisabled(t *testing.T) {
	h := newHarnessWith(t, "assignProviderIDs: false\n"+testConfig, false, map[string]kube.Node{"node-a": {Ready: true}})
	h.tick(t, 0)
	if len(h.nodes.set) != 0 {
		t.Fatalf("set %v", h.nodes.set)
	}
}

const selectorConfig = `
nodeGroups:
  - id: g
    maxSize: 2
    machines:
      - {name: box-1, nodeSelector: {example.com/machine: box-1}, powerOn: {wol: {mac: "00:00:5e:00:53:01"}}, powerOff: {ssh: {}}}
      - {name: box-2, node: node-x, providerID: "metal://rack/box-2", powerOn: {wol: {mac: "00:00:5e:00:53:02"}}, powerOff: {ssh: {}}}
`

func TestNodeSelectorAndProviderIDMapping(t *testing.T) {
	h := newHarnessWith(t, selectorConfig, false, map[string]kube.Node{
		"host-17": {Ready: true, ToBeDeleted: true, Labels: map[string]string{"example.com/machine": "box-1"}},
		"node-x":  {Ready: true, ProviderID: "metal://rack/box-2"},
	})
	h.tick(t, 0)
	if len(h.nodes.set) != 1 || h.nodes.set[0] != "host-17=wol://box-1" {
		t.Fatalf("set %v", h.nodes.set)
	}
	got := h.instances(t, "g")
	if len(got) != 2 || got["wol://box-1"] != pb.InstanceStatus_instanceRunning || got["metal://rack/box-2"] != pb.InstanceStatus_instanceRunning {
		t.Fatalf("instances %v", got)
	}
	for _, n := range []*pb.ExternalGrpcNode{node("host-17", ""), node("", "wol://box-1"), node("anything", "metal://rack/box-2")} {
		r, _ := h.p.NodeGroupForNode(bg, &pb.NodeGroupForNodeRequest{Node: n})
		if r.NodeGroup.Id != "g" {
			t.Errorf("%v: group %q", n, r.NodeGroup.Id)
		}
	}
	// A power-off reaches the selector-matched node's Node object.
	if err := h.remove("g", node("host-17", "wol://box-1")); err != nil {
		t.Fatal(err)
	}
	h.setNode("host-17", func(n *kube.Node) { n.Ready = false })
	h.tick(t, time.Minute)
	if len(h.nodes.deleted) != 1 || h.nodes.deleted[0] != "host-17" {
		t.Fatalf("deleted %v", h.nodes.deleted)
	}
}

func TestNodeSelectorAmbiguous(t *testing.T) {
	l := map[string]string{"example.com/machine": "box-1"}
	h := newHarnessWith(t, selectorConfig, false, map[string]kube.Node{
		"host-1": {Ready: true, Labels: l},
		"host-2": {Ready: true, Labels: l},
	})
	h.tick(t, 0)
	if len(h.nodes.set) != 0 || h.target(t, "g") != 0 {
		t.Fatalf("set %v target %d", h.nodes.set, h.target(t, "g"))
	}
}

func TestListErrorIsUnavailable(t *testing.T) {
	h := newHarness(t, false, nil)
	h.nodes.listErr = errors.New("apiserver down")
	_, err := h.p.NodeGroupTargetSize(bg, &pb.NodeGroupTargetSizeRequest{Id: "gpu"})
	wantCode(t, err, codes.Unavailable)
}

func TestNewNeedsPlugins(t *testing.T) {
	cfg, err := config.Parse([]byte(testConfig))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(Options{Config: cfg, Plugins: map[string]Plugins{}}); err == nil {
		t.Fatal("built without plugins")
	}
}

func TestTemplateNode(t *testing.T) {
	h := newHarness(t, false, nil)
	ctx := bg
	r, err := h.p.NodeGroupTemplateNodeInfo(ctx, &pb.NodeGroupTemplateNodeInfoRequest{Id: "gpu"})
	if err != nil {
		t.Fatal(err)
	}
	var n corev1.Node
	if err := n.Unmarshal(r.NodeBytes); err != nil {
		t.Fatal(err)
	}
	gpu := n.Status.Allocatable["nvidia.com/gpu"]
	if n.Labels["node-role.example.com/gpu"] != "" || n.Labels["kubernetes.io/hostname"] != "template-gpu" ||
		len(n.Spec.Taints) != 1 || n.Spec.Taints[0].Effect != corev1.TaintEffectNoSchedule || gpu.Value() != 1 ||
		n.Status.Capacity.Memory().String() != "32Gi" {
		t.Fatalf("template node %+v", n)
	}
	_, err = h.p.NodeGroupTemplateNodeInfo(ctx, &pb.NodeGroupTemplateNodeInfoRequest{Id: "big"})
	wantCode(t, err, codes.Unimplemented)
}

// TestGRPC serves the provider and calls it the way Cluster Autoscaler's externalgrpc client does.
func TestGRPC(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{"node-c": ready()})
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	pb.RegisterCloudProviderServer(s, h.p)
	go func() { _ = s.Serve(lis) }()
	defer s.Stop()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	c := pb.NewCloudProviderClient(conn)
	ctx := bg
	if _, err := c.Refresh(ctx, &pb.RefreshRequest{}); err != nil {
		t.Fatal(err)
	}
	r, err := c.NodeGroups(ctx, &pb.NodeGroupsRequest{})
	if err != nil || len(r.NodeGroups) != 2 {
		t.Fatalf("NodeGroups: %v %v", r, err)
	}
	_, err = c.PricingNodePrice(ctx, &pb.PricingNodePriceRequest{})
	wantCode(t, err, codes.Unimplemented) // the client maps this to "not implemented"
	_, err = c.NodeGroupGetOptions(ctx, &pb.NodeGroupAutoscalingOptionsRequest{Id: "gpu"})
	wantCode(t, err, codes.Unimplemented)
	if _, err := c.NodeGroupIncreaseSize(ctx, &pb.NodeGroupIncreaseSizeRequest{Id: "gpu", Delta: 1}); err != nil {
		t.Fatal(err)
	}
	if len(h.power.on) != 1 {
		t.Fatalf("woke %v", h.power.on)
	}
}
