// Package kube reads which nodes are registered and Ready, and removes the Node object of a machine
// that has been powered off.
package kube

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Node is what the provider needs to know about a registered node.
type Node struct {
	Ready      bool
	ProviderID string
}

// Nodes lists and deletes Node objects.
type Nodes interface {
	List(ctx context.Context) (map[string]Node, error)
	Delete(ctx context.Context, name string) error
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
		out[n.Name] = Node{Ready: Ready(&n), ProviderID: n.Spec.ProviderID}
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

// Ready reports the node's Ready condition.
func Ready(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
