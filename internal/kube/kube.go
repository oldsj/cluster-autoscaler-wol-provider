// Package kube reads the nodes the provider manages and makes the few changes it needs: setting an
// empty spec.providerID, recording a power-off on the node, removing Cluster Autoscaler's deletion
// taint after a failed power-off, and deleting the Node object of a machine that's off.
package kube

import (
	"context"
	"encoding/json"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

// ToBeDeletedTaint is the taint Cluster Autoscaler puts on a node it's about to remove, before it
// drains it. The provider powers a registered node off only when it carries this taint.
const ToBeDeletedTaint = "ToBeDeletedByClusterAutoscaler"

// PowerOffAnnotation records, on the Node object, when the provider asked the machine to power off,
// so a restarted provider carries on where it left off.
const PowerOffAnnotation = "wol-provider/power-off-requested"

// Node is what the provider needs to know about a registered node.
type Node struct {
	Ready bool
	// ReadyChanged is the last transition time of the Ready condition.
	ReadyChanged time.Time
	ProviderID   string
	Labels       map[string]string
	// ToBeDeleted is true while the node carries Cluster Autoscaler's deletion taint.
	ToBeDeleted bool
	// PowerOffAt is the provider's power-off annotation, or zero.
	PowerOffAt time.Time
}

// Nodes lists and changes Node objects.
type Nodes interface {
	List(ctx context.Context) (map[string]Node, error)
	Delete(ctx context.Context, name string) error
	// SetProviderID sets spec.providerID on a node whose providerID is empty. The API server refuses
	// any change once it's set, so this can't overwrite one set meanwhile by something else.
	SetProviderID(ctx context.Context, name, providerID string) error
	// SetPowerOff sets the power-off annotation to at, or removes it when at is zero.
	SetPowerOff(ctx context.Context, name string, at time.Time) error
	// RemoveTaint removes a taint by key; a node without it, or already gone, isn't an error.
	RemoveTaint(ctx context.Context, name, key string) error
}

// Client implements Nodes with client-go.
type Client struct {
	Clientset kubernetes.Interface
}

// List returns every node by name.
func (c Client) List(ctx context.Context) (map[string]Node, error) {
	list, err := c.Clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make(map[string]Node, len(list.Items))
	for i := range list.Items {
		out[list.Items[i].Name] = FromObject(&list.Items[i])
	}
	return out, nil
}

// FromObject extracts what the provider needs from a Node.
func FromObject(n *corev1.Node) Node {
	out := Node{ProviderID: n.Spec.ProviderID, Labels: n.Labels}
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			out.Ready = c.Status == corev1.ConditionTrue
			out.ReadyChanged = c.LastTransitionTime.Time
		}
	}
	for _, t := range n.Spec.Taints {
		if t.Key == ToBeDeletedTaint {
			out.ToBeDeleted = true
		}
	}
	if v, ok := n.Annotations[PowerOffAnnotation]; ok {
		if at, err := time.Parse(time.RFC3339, v); err == nil {
			out.PowerOffAt = at
		}
	}
	return out
}

// Delete removes a Node object; one that's already gone isn't an error.
func (c Client) Delete(ctx context.Context, name string) error {
	err := c.Clientset.CoreV1().Nodes().Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// SetProviderID patches spec.providerID. No resourceVersion precondition: node status updates would
// make it conflict often, and the API server's own rule (empty to a value, once) is the guard.
func (c Client) SetProviderID(ctx context.Context, name, providerID string) error {
	return c.mergePatch(ctx, name, map[string]any{"spec": map[string]any{"providerID": providerID}})
}

// SetPowerOff sets or removes the power-off annotation.
func (c Client) SetPowerOff(ctx context.Context, name string, at time.Time) error {
	var v any // nil removes it
	if !at.IsZero() {
		v = at.UTC().Format(time.RFC3339)
	}
	err := c.mergePatch(ctx, name, map[string]any{"metadata": map[string]any{"annotations": map[string]any{PowerOffAnnotation: v}}})
	if at.IsZero() && apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// RemoveTaint removes the taint with key from the node, retrying on conflict.
func (c Client) RemoveTaint(ctx context.Context, name, key string) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		n, err := c.Clientset.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		kept := n.Spec.Taints[:0]
		for _, t := range n.Spec.Taints {
			if t.Key != key {
				kept = append(kept, t)
			}
		}
		if len(kept) == len(n.Spec.Taints) {
			return nil
		}
		n.Spec.Taints = kept
		_, err = c.Clientset.CoreV1().Nodes().Update(ctx, n, metav1.UpdateOptions{FieldManager: "wol-provider"})
		return err
	})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (c Client) mergePatch(ctx context.Context, name string, v any) error {
	patch, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = c.Clientset.CoreV1().Nodes().Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{FieldManager: "wol-provider"})
	return err
}
