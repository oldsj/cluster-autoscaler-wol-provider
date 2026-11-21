package kube

import (
	"context"
	"testing"
	"time"

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

func TestTaintAnnotationAndReadyTime(t *testing.T) {
	changed := metav1.NewTime(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	cs := fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n"},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{
			{Key: ToBeDeletedTaint, Value: "1", Effect: corev1.TaintEffectNoSchedule},
			{Key: "other", Effect: corev1.TaintEffectNoSchedule}}},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse, LastTransitionTime: changed}}}})
	c := Client{Clientset: cs}
	ctx := context.Background()
	at := time.Date(2026, 1, 1, 13, 0, 0, 0, time.UTC)
	if err := c.SetPowerOff(ctx, "n", at); err != nil {
		t.Fatal(err)
	}
	nodes, _ := c.List(ctx)
	n := nodes["n"]
	if n.Ready || !n.ReadyChanged.Equal(changed.Time) || !n.ToBeDeleted || !n.PowerOffAt.Equal(at) {
		t.Fatalf("%+v", n)
	}
	if err := c.RemoveTaint(ctx, "n", ToBeDeletedTaint); err != nil {
		t.Fatal(err)
	}
	if err := c.SetPowerOff(ctx, "n", time.Time{}); err != nil {
		t.Fatal(err)
	}
	nodes, _ = c.List(ctx)
	if n = nodes["n"]; n.ToBeDeleted || !n.PowerOffAt.IsZero() {
		t.Fatalf("after clearing %+v", n)
	}
	obj, _ := cs.CoreV1().Nodes().Get(ctx, "n", metav1.GetOptions{})
	if len(obj.Spec.Taints) != 1 || obj.Spec.Taints[0].Key != "other" {
		t.Fatalf("taints %v", obj.Spec.Taints)
	}
	if err := c.RemoveTaint(ctx, "gone", ToBeDeletedTaint); err != nil {
		t.Fatalf("missing node: %v", err)
	}
	if err := c.SetPowerOff(ctx, "gone", time.Time{}); err != nil {
		t.Fatalf("missing node: %v", err)
	}
}
