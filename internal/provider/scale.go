package provider

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugin"
	pb "github.com/oldsj/cluster-autoscaler-wol-provider/proto/externalgrpc"
)

// deleteWait is the longest a scale-down call waits for its power-offs before answering. A power-off
// still running then carries on in the background, bounded by powerOffTimeout.
const deleteWait = 25 * time.Second

// NodeGroupIncreaseSize powers on the next delta machines that are off: in config order, machines
// whose last wake failed last. The plugins run outside the lock.
func (p *Provider) NodeGroupIncreaseSize(ctx context.Context, req *pb.NodeGroupIncreaseSizeRequest) (*pb.NodeGroupIncreaseSizeResponse, error) {
	p.mu.Lock()
	g, err := p.group(req.GetId())
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	delta := int(req.GetDelta())
	if delta <= 0 {
		p.mu.Unlock()
		return nil, status.Errorf(codes.InvalidArgument, "delta must be positive, got %d", delta)
	}
	if err := p.refreshIfStale(ctx); err != nil {
		p.mu.Unlock()
		return nil, err
	}
	if size := p.targetSize(g); size+delta > g.MaxSize {
		p.mu.Unlock()
		return nil, status.Errorf(codes.InvalidArgument, "%s: size %d + %d is over max %d", g.ID, size, delta, g.MaxSize)
	}
	var candidates []*machine
	for _, spec := range g.Machines {
		if m := p.machines[spec.Name]; p.wakeable(m) {
			candidates = append(candidates, m)
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].failedWakes < candidates[j].failedWakes })
	if len(candidates) < delta {
		p.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition,
			"%s: %d machine(s) can be powered on, %d wanted (a machine still powering off, or off for less than minOffTime, can't be yet)",
			g.ID, len(candidates), delta)
	}
	chosen := candidates[:delta]
	if p.dryRun {
		for _, m := range chosen {
			p.log.Info("dry-run: would power on machine", "machine", m.spec.Name, "group", g.ID, "plugin", m.spec.PowerOn.Name)
		}
		p.mu.Unlock()
		return nil, errDryRun
	}
	now := p.now()
	targets := make([]plugin.Target, len(chosen))
	for i, m := range chosen {
		m.wokeAt, m.lastSent = now, now // counted as starting from here, so a concurrent call skips it
		targets[i] = m.target()
	}
	p.mu.Unlock()

	var failed []string
	for i, m := range chosen {
		if err := m.pl.On.PowerOn(ctx, targets[i]); err != nil {
			p.log.Error("power-on failed", "machine", m.spec.Name, "group", g.ID, "plugin", m.spec.PowerOn.Name, "err", err)
			p.mu.Lock()
			m.wokeAt = time.Time{}
			p.mu.Unlock()
			failed = append(failed, fmt.Sprintf("%s: %v", m.spec.Name, err))
			continue
		}
		p.log.Info("powered on machine", "machine", m.spec.Name, "group", g.ID, "plugin", m.spec.PowerOn.Name)
	}
	if len(failed) > 0 {
		return nil, status.Errorf(codes.Internal, "powering on: %s", strings.Join(failed, "; "))
	}
	return &pb.NodeGroupIncreaseSizeResponse{}, nil
}

// NodeGroupDeleteNodes powers off the machines behind the given nodes, which Cluster Autoscaler has
// drained. Safety rules:
//
//   - A machine with a registered node is powered off only if that node carries Cluster
//     Autoscaler's ToBeDeletedByClusterAutoscaler taint, read fresh from the API server.
//   - A placeholder ("fake") node, which Cluster Autoscaler sends for an instance it thinks never
//     registered, is refused when the machine does have a registered node.
//   - The power-off is recorded on the node before the plugin runs, so a restart resumes it, and
//     the plugin runs on its own timeout, apart from this request.
func (p *Provider) NodeGroupDeleteNodes(ctx context.Context, req *pb.NodeGroupDeleteNodesRequest) (*pb.NodeGroupDeleteNodesResponse, error) {
	p.mu.Lock()
	g, err := p.group(req.GetId())
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	if err := p.refresh(ctx); err != nil {
		p.mu.Unlock()
		return nil, err
	}
	var targets []*machine
	seen := map[*machine]bool{}
	for _, n := range req.GetNodes() {
		m := p.machineFor(n)
		if m == nil || m.group != g {
			p.mu.Unlock()
			return nil, status.Errorf(codes.InvalidArgument, "node %q (%s) isn't in group %s", n.GetName(), n.GetProviderID(), g.ID)
		}
		if reason := n.GetAnnotations()[fakeNodeAnnotation]; reason != "" && m.node != "" {
			p.mu.Unlock()
			p.log.Error("refusing to power off a machine for a placeholder node: it has a registered node", "machine", m.spec.Name,
				"node", m.node, "placeholder", n.GetName(), "reason", reason)
			return nil, status.Errorf(codes.FailedPrecondition,
				"%q is a placeholder for an instance Cluster Autoscaler thinks never registered (%s), but machine %s has node %s registered; not powering it off. Check the node's spec.providerID",
				n.GetName(), reason, m.spec.Name, m.node)
		}
		if m.node != "" && p.phase(m) != stopping && !p.snapshot[m.node].ToBeDeleted {
			p.mu.Unlock()
			return nil, status.Errorf(codes.FailedPrecondition,
				"node %s doesn't carry Cluster Autoscaler's %s taint; not powering off machine %s", m.node, "ToBeDeletedByClusterAutoscaler", m.spec.Name)
		}
		if !seen[m] {
			seen[m] = true
			targets = append(targets, m)
		}
	}
	// Only machines still running or starting reduce the size, so a retry is idempotent.
	removing := 0
	for _, m := range targets {
		if ph := p.phase(m); ph == running || ph == creating {
			removing++
		}
	}
	if size := p.targetSize(g); size-removing < g.MinSize {
		p.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "%s: size %d - %d is under min %d", g.ID, size, removing, g.MinSize)
	}
	var toPowerOff []*machine
	for _, m := range targets {
		switch ph := p.phase(m); {
		case ph == stopping:
			p.log.Info("already powering off", "machine", m.spec.Name, "group", g.ID)
		case m.node == "" && ph == creating:
			if p.dryRun {
				p.log.Info("dry-run: would stop counting a starting machine", "machine", m.spec.Name, "group", g.ID)
				continue
			}
			// It can't be un-woken. If it boots anyway, it registers and is scaled down like any node.
			p.log.Warn("stopped counting a machine that hasn't registered; it may still boot", "machine", m.spec.Name, "group", g.ID)
			m.wokeAt = time.Time{}
		case m.node == "":
			p.log.Info("already off", "machine", m.spec.Name, "group", g.ID)
		default:
			toPowerOff = append(toPowerOff, m)
		}
	}
	if p.dryRun {
		for _, m := range toPowerOff {
			p.log.Info("dry-run: would power off machine", "machine", m.spec.Name, "node", m.node, "group", g.ID,
				"plugin", m.spec.PowerOff.Name, "approval", p.approver != nil)
		}
		p.mu.Unlock()
		// The approval hook is asked as it would be for real (with DryRun set in its templates), so a
		// dry run shows the whole decision. Whatever it answers, nothing is switched.
		if p.approver != nil {
			for _, m := range toPowerOff {
				p.log.Info("dry-run: approval answered", "machine", m.spec.Name, "group", g.ID, "answer", answerOf(p.approve(ctx, m)))
			}
		}
		return nil, errDryRun
	}
	p.mu.Unlock()

	if p.approver != nil {
		for _, m := range toPowerOff {
			if err := p.approve(ctx, m); err != nil {
				return nil, err
			}
		}
	}

	p.mu.Lock()
	now := p.now()
	done := make(chan error, len(toPowerOff))
	type job struct {
		m       *machine
		t       plugin.Target
		wasDown bool
	}
	var jobs []job
	for _, m := range toPowerOff {
		if p.phase(m) == stopping || m.node == "" {
			continue // a concurrent call got there first
		}
		if err := p.nodes.SetPowerOff(ctx, m.node, now); err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Unavailable, "recording the power-off on node %s: %v", m.node, err)
		}
		up := p.snapshot[m.node].Ready || (m.probed && m.up)
		m.offAt, m.powerOffRunning = now, true
		jobs = append(jobs, job{m, m.target(), !up})
	}
	p.mu.Unlock()

	for _, j := range jobs {
		p.log.Info("powering off machine", "machine", j.t.Name, "node", j.t.Node, "group", g.ID, "plugin", j.m.spec.PowerOff.Name)
		p.goAsync(func() { done <- p.powerOff(j.m, j.t, j.wasDown) })
	}
	wait := deleteWait
	if dl, ok := ctx.Deadline(); ok {
		wait = min(wait, time.Until(dl)-2*time.Second)
	}
	timer := time.NewTimer(max(wait, 0))
	defer timer.Stop()
	var failed []string
wait:
	for range jobs {
		select {
		case err := <-done:
			if err != nil {
				failed = append(failed, err.Error())
			}
		case <-timer.C:
			p.log.Info("power-off still running; answering Cluster Autoscaler now, it carries on in the background", "group", g.ID)
			break wait
		}
	}
	if len(failed) > 0 {
		return nil, status.Errorf(codes.Internal, "powering off: %s", strings.Join(failed, "; "))
	}
	return &pb.NodeGroupDeleteNodesResponse{}, nil
}

// powerOff runs one power-off plugin call on its own timeout. A failure on a machine whose node was
// Ready gives the node back (annotation and deletion taint removed) and is returned. On a node that
// was already NotReady it's tolerated: the machine is most likely off already.
func (p *Provider) powerOff(m *machine, t plugin.Target, wasDown bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.PowerOffTimeout.Duration)
	err := m.pl.Off.PowerOff(ctx, t)
	cancel()
	p.mu.Lock()
	defer p.mu.Unlock()
	m.powerOffRunning = false
	switch {
	case err == nil:
		p.log.Info("power-off sent", "machine", t.Name, "node", t.Node)
		return nil
	case wasDown:
		p.log.Warn("power-off of a NotReady machine failed; treating it as off", "machine", t.Name, "node", t.Node, "err", err)
		return nil
	default:
		p.log.Error("power-off failed; giving the node back", "machine", t.Name, "node", t.Node, "err", err)
		p.abandonPowerOff(context.Background(), m)
		return fmt.Errorf("%s: %w", t.Name, err)
	}
}

// approve asks the approval hook about one machine. It waits up to approval.wait for a pending
// answer; still pending then, it fails, so Cluster Autoscaler rolls back and asks again later, and
// the next attempt checks the same request instead of starting a new one.
func (p *Provider) approve(ctx context.Context, m *machine) error {
	a := p.cfg.Approval
	deadline := time.Now().Add(a.Wait.Duration)
	for {
		p.mu.Lock()
		if !m.approvalAt.IsZero() && p.now().Sub(m.approvalAt) >= a.Expire.Duration {
			m.approvalTicket, m.approvalAt = "", time.Time{}
		}
		ticket, t := m.approvalTicket, m.target()
		t.DryRun = p.dryRun
		p.mu.Unlock()

		d, next, err := p.approver.Approve(ctx, t, ticket)

		p.mu.Lock()
		switch d {
		case plugin.Approved, plugin.Denied:
			m.approvalTicket, m.approvalAt = "", time.Time{}
		case plugin.Pending:
			if m.approvalTicket == "" && next != "" {
				m.approvalAt = p.now()
			}
			m.approvalTicket = next
		}
		p.mu.Unlock()

		switch {
		case d == plugin.Approved:
			p.log.Info("power-off approved", "machine", t.Name)
			return nil
		case d == plugin.Denied:
			p.log.Warn("power-off denied", "machine", t.Name, "err", err)
			return status.Errorf(codes.PermissionDenied, "power-off of %s denied: %v", t.Name, err)
		case err != nil:
			return status.Errorf(codes.Unavailable, "asking approval to power off %s: %v", t.Name, err)
		case time.Now().After(deadline):
			p.log.Info("power-off awaiting approval", "machine", t.Name)
			return status.Errorf(codes.Unavailable, "power-off of %s is awaiting approval; Cluster Autoscaler will ask again", t.Name)
		}
		select {
		case <-ctx.Done():
			return status.Errorf(codes.Unavailable, "power-off of %s is awaiting approval: %v", t.Name, ctx.Err())
		case <-time.After(a.PollInterval.Duration):
		}
	}
}

// answerOf names approve's result for the dry-run log: approved, denied, or pending (awaiting an
// answer, or the hook couldn't be reached).
func answerOf(err error) string {
	switch status.Code(err) {
	case codes.OK:
		return "approved"
	case codes.PermissionDenied:
		return "denied"
	default:
		return "pending"
	}
}

// NodeGroupDecreaseTargetSize stops counting powered-on machines that haven't registered, latest
// first. It never touches a registered node. With fewer starting machines than asked (for example
// after bootTimeout already stopped counting them), it stops counting those there are and succeeds.
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
	var starting []*machine
	for _, m := range p.inGroup(g, creating) {
		if m.node == "" {
			starting = append(starting, m)
		}
	}
	if len(starting) < -delta {
		p.log.Info("asked to decrease by more than the machines starting; decreasing by those", "group", g.ID,
			"asked", -delta, "starting", len(starting))
	}
	sort.SliceStable(starting, func(i, j int) bool { return starting[i].wokeAt.After(starting[j].wokeAt) })
	for _, m := range starting[:min(len(starting), -delta)] {
		p.log.Info("stopped counting a starting machine", "machine", m.spec.Name, "group", g.ID)
		m.wokeAt, m.upSince = time.Time{}, time.Time{}
	}
	return &pb.NodeGroupDecreaseTargetSizeResponse{}, nil
}
