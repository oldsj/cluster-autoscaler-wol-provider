package kube

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestListAndDelete(t *testing.T) {
	cs := fake.NewClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Spec: corev1.NodeSpec{ProviderID: "wol://node-a"},
			Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"},
			Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionUnknown}}}},
	)
	c := Client{Clientset: cs}
	ctx := context.Background()
	nodes, err := c.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !nodes["node-a"].Ready || nodes["node-a"].ProviderID != "wol://node-a" || nodes["node-b"].Ready {
		t.Fatalf("nodes %+v", nodes)
	}
	if err := c.Delete(ctx, "node-b"); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, "node-b"); err != nil {
		t.Fatalf("deleting a missing node: %v", err)
	}
	if nodes, _ = c.List(ctx); len(nodes) != 1 {
		t.Fatalf("after delete %+v", nodes)
	}
}
