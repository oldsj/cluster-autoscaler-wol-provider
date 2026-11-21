package provider

import (
	"context"
	"fmt"
	"maps"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/config"
	pb "github.com/oldsj/cluster-autoscaler-wol-provider/proto/externalgrpc"
)

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

// NodeGroupNodes lists the group's machines that have a Node object, are starting, or are powering
// off. A machine whose node has been NotReady for long counts as off for the target size but is
// still listed, so Cluster Autoscaler sees its node as belonging to an instance.
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
	for _, spec := range g.Machines {
		m := p.machines[spec.Name]
		var state pb.InstanceStatus_InstanceState
		switch ph := p.phase(m); {
		case ph == stopping:
			state = pb.InstanceStatus_instanceDeleting
		case ph == creating:
			state = pb.InstanceStatus_instanceCreating
		case m.node != "":
			state = pb.InstanceStatus_instanceRunning
		default:
			continue
		}
		id := p.instanceID(m)
		if node, ok := p.snapshot[m.node]; ok && m.node != "" && node.ProviderID != "" {
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

func (p *Provider) pbGroup(g *config.NodeGroup) *pb.NodeGroup {
	return &pb.NodeGroup{Id: g.ID, MinSize: int32(g.MinSize), MaxSize: int32(g.MaxSize),
		Debug: fmt.Sprintf("%s: %d machine(s), min %d, max %d", g.ID, len(g.Machines), g.MinSize, g.MaxSize)}
}
