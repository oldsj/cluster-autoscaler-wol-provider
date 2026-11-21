package provider

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
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
	pb "github.com/oldsj/cluster-autoscaler-wol-provider/proto/externalgrpc"
)

const testConfig = `
bootTimeout: 10m
shutdownTimeout: 5m
talos: {caFile: /c/ca.crt, certFile: /c/tls.crt, keyFile: /c/tls.key}
nodeGroups:
  - id: gpu
    minSize: 0
    maxSize: 2
    template:
      labels: {node-role.example.com/gpu: ""}
      taints: [{key: example.com/gpu, value: "true", effect: NoSchedule}]
      capacity: {cpu: "8", memory: 32Gi, pods: "110", nvidia.com/gpu: "1"}
    machines:
      - {node: node-a, powerOn: wol, mac: "00:00:5e:00:53:01", broadcast: "192.0.2.255:9", powerOff: talos, talosEndpoint: "192.0.2.11:50000"}
      - {node: node-b, powerOn: wol, mac: "00:00:5e:00:53:02", broadcast: "192.0.2.255:9", powerOff: talos, talosEndpoint: "192.0.2.12:50000"}
  - id: big
    minSize: 1
    maxSize: 1
    machines:
      - {node: node-c, powerOn: wol, mac: "00:00:5e:00:53:03", powerOff: ssh}
`

type fakePower struct {
	on, off []string
	offErr  error
}

func (f *fakePower) PowerOn(_ context.Context, m config.Machine) error {
	f.on = append(f.on, m.Node)
	return nil
}

func (f *fakePower) PowerOff(_ context.Context, m config.Machine) error {
	if f.offErr != nil {
		return f.offErr
	}
	f.off = append(f.off, m.Node)
	return nil
}

type fakeNodes struct {
	nodes   map[string]kube.Node
	deleted []string
	listErr error
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

type harness struct {
	p     *Provider
	power *fakePower
	nodes *fakeNodes
	now   time.Time
}

func newHarness(t *testing.T, dryRun bool, nodes map[string]kube.Node) *harness {
	t.Helper()
	cfg, err := config.Parse([]byte(testConfig))
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{power: &fakePower{}, nodes: &fakeNodes{nodes: nodes}, now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	h.p = New(Options{Config: cfg, Power: h.power, Nodes: h.nodes, DryRun: dryRun,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return h.now }})
	return h
}

func (h *harness) tick(t *testing.T, d time.Duration) {
	t.Helper()
	h.now = h.now.Add(d)
	if _, err := h.p.Refresh(context.Background(), &pb.RefreshRequest{}); err != nil {
		t.Fatal(err)
	}
}

func (h *harness) target(t *testing.T, id string) int {
	t.Helper()
	r, err := h.p.NodeGroupTargetSize(context.Background(), &pb.NodeGroupTargetSizeRequest{Id: id})
	if err != nil {
		t.Fatal(err)
	}
	return int(r.TargetSize)
}

func (h *harness) instances(t *testing.T, id string) map[string]pb.InstanceStatus_InstanceState {
	t.Helper()
	r, err := h.p.NodeGroupNodes(context.Background(), &pb.NodeGroupNodesRequest{Id: id})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]pb.InstanceStatus_InstanceState{}
	for _, i := range r.Instances {
		out[i.Id] = i.Status.InstanceState
	}
	return out
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("got %v, want code %v", err, want)
	}
}

func ready() kube.Node { return kube.Node{Ready: true} }

func node(name, pid string) *pb.ExternalGrpcNode {
	return &pb.ExternalGrpcNode{Name: name, ProviderID: pid}
}

func TestNodeGroupsAndLookup(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{})
	ctx := context.Background()
	r, _ := h.p.NodeGroups(ctx, &pb.NodeGroupsRequest{})
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
		g, _ := h.p.NodeGroupForNode(ctx, &pb.NodeGroupForNodeRequest{Node: tc.n})
		if g.NodeGroup.Id != tc.want {
			t.Errorf("%v: group %q, want %q", tc.n, g.NodeGroup.Id, tc.want)
		}
	}
}

func TestScaleUpWakesInOrderAndRegisters(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{"node-c": ready()})
	ctx := context.Background()
	if got := h.target(t, "gpu"); got != 0 {
		t.Fatalf("target %d", got)
	}
	if _, err := h.p.NodeGroupIncreaseSize(ctx, &pb.NodeGroupIncreaseSizeRequest{Id: "gpu", Delta: 1}); err != nil {
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
	h.tick(t, time.Minute)
	if got := h.instances(t, "gpu"); got["wol://node-a"] != pb.InstanceStatus_instanceRunning {
		t.Fatalf("instances after register %v", got)
	}
	if _, err := h.p.NodeGroupIncreaseSize(ctx, &pb.NodeGroupIncreaseSizeRequest{Id: "gpu", Delta: 1}); err != nil {
		t.Fatal(err)
	}
	if h.power.on[1] != "node-b" {
		t.Fatalf("second wake %v", h.power.on)
	}
}

func TestScaleUpLimits(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{"node-a": ready()})
	ctx := context.Background()
	_, err := h.p.NodeGroupIncreaseSize(ctx, &pb.NodeGroupIncreaseSizeRequest{Id: "gpu", Delta: 2})
	wantCode(t, err, codes.InvalidArgument)
	_, err = h.p.NodeGroupIncreaseSize(ctx, &pb.NodeGroupIncreaseSizeRequest{Id: "gpu", Delta: 0})
	wantCode(t, err, codes.InvalidArgument)
	_, err = h.p.NodeGroupIncreaseSize(ctx, &pb.NodeGroupIncreaseSizeRequest{Id: "nope", Delta: 1})
	wantCode(t, err, codes.NotFound)
	if len(h.power.on) != 0 {
		t.Fatalf("woke %v", h.power.on)
	}
}

func TestBootTimeoutCountsItOff(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{})
	if _, err := h.p.NodeGroupIncreaseSize(context.Background(), &pb.NodeGroupIncreaseSizeRequest{Id: "gpu", Delta: 1}); err != nil {
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

func TestScaleDownPowersOffThenRemovesNode(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{"node-a": ready(), "node-b": ready()})
	ctx := context.Background()
	_, err := h.p.NodeGroupDeleteNodes(ctx, &pb.NodeGroupDeleteNodesRequest{Id: "gpu", Nodes: []*pb.ExternalGrpcNode{node("node-b", "wol://node-b")}})
	if err != nil {
		t.Fatal(err)
	}
	if len(h.power.off) != 1 || h.power.off[0] != "node-b" {
		t.Fatalf("powered off %v", h.power.off)
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
	h.nodes.nodes["node-b"] = kube.Node{Ready: false}
	h.tick(t, 30*time.Second)
	if len(h.nodes.deleted) != 1 || h.nodes.deleted[0] != "node-b" {
		t.Fatalf("deleted %v", h.nodes.deleted)
	}
	if got := h.instances(t, "gpu"); len(got) != 1 {
		t.Fatalf("instances after %v", got)
	}
	// It can be woken again.
	if _, err := h.p.NodeGroupIncreaseSize(ctx, &pb.NodeGroupIncreaseSizeRequest{Id: "gpu", Delta: 1}); err != nil || h.power.on[0] != "node-b" {
		t.Fatalf("rewake: %v %v", err, h.power.on)
	}
}

func TestScaleDownNotReadySkipsPowerOff(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{"node-a": {Ready: false}})
	_, err := h.p.NodeGroupDeleteNodes(context.Background(), &pb.NodeGroupDeleteNodesRequest{Id: "gpu", Nodes: []*pb.ExternalGrpcNode{node("node-a", "")}})
	if err != nil {
		t.Fatal(err)
	}
	if len(h.power.off) != 0 {
		t.Fatalf("powered off %v", h.power.off)
	}
	h.tick(t, time.Second)
	if len(h.nodes.deleted) != 1 {
		t.Fatalf("deleted %v", h.nodes.deleted)
	}
}

func TestScaleDownLimitsAndErrors(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{"node-a": ready(), "node-c": ready()})
	ctx := context.Background()
	_, err := h.p.NodeGroupDeleteNodes(ctx, &pb.NodeGroupDeleteNodesRequest{Id: "big", Nodes: []*pb.ExternalGrpcNode{node("node-c", "")}})
	wantCode(t, err, codes.FailedPrecondition) // min 1
	_, err = h.p.NodeGroupDeleteNodes(ctx, &pb.NodeGroupDeleteNodesRequest{Id: "gpu", Nodes: []*pb.ExternalGrpcNode{node("node-c", "")}})
	wantCode(t, err, codes.InvalidArgument) // other group
	h.power.offErr = errors.New("connection refused")
	_, err = h.p.NodeGroupDeleteNodes(ctx, &pb.NodeGroupDeleteNodesRequest{Id: "gpu", Nodes: []*pb.ExternalGrpcNode{node("node-a", "")}})
	wantCode(t, err, codes.Internal)
	if got := h.instances(t, "gpu"); got["wol://node-a"] != pb.InstanceStatus_instanceRunning {
		t.Fatalf("failed power-off changed state: %v", got)
	}
}

func TestShutdownTimeoutCountsItRunning(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{"node-a": ready()})
	if _, err := h.p.NodeGroupDeleteNodes(context.Background(), &pb.NodeGroupDeleteNodesRequest{Id: "gpu", Nodes: []*pb.ExternalGrpcNode{node("node-a", "")}}); err != nil {
		t.Fatal(err)
	}
	h.tick(t, 6*time.Minute)
	if got := h.instances(t, "gpu"); got["wol://node-a"] != pb.InstanceStatus_instanceRunning {
		t.Fatalf("instances %v", got)
	}
	if len(h.nodes.deleted) != 0 {
		t.Fatalf("deleted a Ready node %v", h.nodes.deleted)
	}
}

func TestDecreaseTargetSize(t *testing.T) {
	h := newHarness(t, false, map[string]kube.Node{})
	ctx := context.Background()
	if _, err := h.p.NodeGroupIncreaseSize(ctx, &pb.NodeGroupIncreaseSizeRequest{Id: "gpu", Delta: 1}); err != nil {
		t.Fatal(err)
	}
	h.tick(t, time.Second)
	if _, err := h.p.NodeGroupIncreaseSize(ctx, &pb.NodeGroupIncreaseSizeRequest{Id: "gpu", Delta: 1}); err != nil {
		t.Fatal(err)
	}
	_, err := h.p.NodeGroupDecreaseTargetSize(ctx, &pb.NodeGroupDecreaseTargetSizeRequest{Id: "gpu", Delta: 1})
	wantCode(t, err, codes.InvalidArgument)
	_, err = h.p.NodeGroupDecreaseTargetSize(ctx, &pb.NodeGroupDecreaseTargetSizeRequest{Id: "gpu", Delta: -3})
	wantCode(t, err, codes.FailedPrecondition)
	if _, err := h.p.NodeGroupDecreaseTargetSize(ctx, &pb.NodeGroupDecreaseTargetSizeRequest{Id: "gpu", Delta: -1}); err != nil {
		t.Fatal(err)
	}
	if got := h.instances(t, "gpu"); len(got) != 1 || got["wol://node-a"] != pb.InstanceStatus_instanceCreating {
		t.Fatalf("latest woken should go first: %v", got)
	}
	// A registered node is never removed this way.
	h.nodes.nodes["node-a"] = ready()
	h.tick(t, time.Second)
	_, err = h.p.NodeGroupDecreaseTargetSize(ctx, &pb.NodeGroupDecreaseTargetSizeRequest{Id: "gpu", Delta: -1})
	wantCode(t, err, codes.FailedPrecondition)
}

func TestDryRunChangesNothing(t *testing.T) {
	h := newHarness(t, true, map[string]kube.Node{"node-a": ready(), "node-b": {Ready: false}})
	ctx := context.Background()
	h.nodes.nodes = map[string]kube.Node{"node-a": ready()}
	_, err := h.p.NodeGroupIncreaseSize(ctx, &pb.NodeGroupIncreaseSizeRequest{Id: "gpu", Delta: 1})
	wantCode(t, err, codes.FailedPrecondition)
	_, err = h.p.NodeGroupDeleteNodes(ctx, &pb.NodeGroupDeleteNodesRequest{Id: "gpu", Nodes: []*pb.ExternalGrpcNode{node("node-a", "")}})
	wantCode(t, err, codes.FailedPrecondition)
	h.tick(t, time.Minute)
	if len(h.power.on)+len(h.power.off)+len(h.nodes.deleted) != 0 {
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

func TestListErrorIsUnavailable(t *testing.T) {
	h := newHarness(t, false, nil)
	h.nodes.listErr = errors.New("apiserver down")
	_, err := h.p.NodeGroupTargetSize(context.Background(), &pb.NodeGroupTargetSizeRequest{Id: "gpu"})
	wantCode(t, err, codes.Unavailable)
}

func TestTemplateNode(t *testing.T) {
	h := newHarness(t, false, nil)
	ctx := context.Background()
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
	defer conn.Close()
	c := pb.NewCloudProviderClient(conn)
	ctx := context.Background()
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
