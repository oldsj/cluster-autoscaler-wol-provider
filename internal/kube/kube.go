// Package kube reads which nodes are registered and Ready, sets a node's empty spec.providerID, and
// removes the Node object of a machine that has been powered off.
package kube

import (
	"context"
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// Node is what the provider needs to know about a registered node.
type Node struct {
	Ready      bool
	ProviderID string
	Labels     map[string]string
}

// Nodes lists and deletes Node objects.
type Nodes interface {
	List(ctx context.Context) (map[string]Node, error)
	Delete(ctx context.Context, name string) error
	// SetProviderID sets spec.providerID on a node whose providerID is empty. The API server refuses
	// any change once it's set, so this can't overwrite one set meanwhile by something else.
	SetProviderID(ctx context.Context, name, providerID string) error
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
	for _, n := range list.Items {
		out[n.Name] = Node{Ready: Ready(&n), ProviderID: n.Spec.ProviderID, Labels: n.Labels}
	}
	return out, nil
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
	patch, err := json.Marshal(map[string]any{"spec": map[string]any{"providerID": providerID}})
	if err != nil {
		return err
	}
	_, err = c.Clientset.CoreV1().Nodes().Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{FieldManager: "wol-provider"})
	return err
}

// Ready reports the node's Ready condition.
func Ready(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
