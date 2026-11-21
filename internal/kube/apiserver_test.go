package kube

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// TestSetProviderIDAgainstAPIServer checks the API server's rule the provider relies on: a node's
// spec.providerID can be set once, from empty, and never changed. It runs against a disposable
// cluster (CI uses kind) named by WOL_PROVIDER_TEST_KUBECONFIG, creating and deleting one Node object
// with no kubelet behind it.
func TestSetProviderIDAgainstAPIServer(t *testing.T) {
	path := os.Getenv("WOL_PROVIDER_TEST_KUBECONFIG")
	if path == "" {
		t.Skip("WOL_PROVIDER_TEST_KUBECONFIG not set")
	}
	rc, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		t.Fatal(err)
	}
	cs := kubernetes.NewForConfigOrDie(rc)
	ctx := context.Background()
	name := "wol-provider-test-" + strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-"))
	if len(name) > 63 {
		name = name[:63]
	}
	_ = cs.CoreV1().Nodes().Delete(ctx, name, metav1.DeleteOptions{})
	if _, err := cs.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.CoreV1().Nodes().Delete(context.Background(), name, metav1.DeleteOptions{}) })
	c := Client{Clientset: cs}

	nodes, err := c.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	n := nodes[name]
	if n.ProviderID != "" {
		t.Fatalf("new node %+v", n)
	}
	if err := c.SetProviderID(ctx, name, "wol://a"); err != nil {
		t.Fatalf("setting an empty providerID: %v", err)
	}
	nodes, _ = c.List(ctx)
	n = nodes[name]
	if n.ProviderID != "wol://a" {
		t.Fatalf("providerID %q", n.ProviderID)
	}
	err = c.SetProviderID(ctx, name, "wol://b")
	if err == nil || !strings.Contains(err.Error(), "providerID") {
		t.Fatalf("changing a set providerID: got %v, want the API server to refuse", err)
	}
	if err := c.SetProviderID(ctx, name, "wol://a"); err != nil {
		t.Fatalf("setting the same providerID again: %v", err)
	}

	// The power-off annotation round-trips, and removing the deletion taint keeps other taints.
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := c.SetPowerOff(ctx, name, at); err != nil {
		t.Fatal(err)
	}
	node, err := cs.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	node.Spec.Taints = []corev1.Taint{{Key: ToBeDeletedTaint, Value: "1", Effect: corev1.TaintEffectNoSchedule},
		{Key: "example.com/keep", Effect: corev1.TaintEffectNoSchedule}}
	if _, err := cs.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	nodes, _ = c.List(ctx)
	if n = nodes[name]; !n.PowerOffAt.Equal(at) || !n.ToBeDeleted {
		t.Fatalf("after annotate and taint: %+v", n)
	}
	if err := c.RemoveTaint(ctx, name, ToBeDeletedTaint); err != nil {
		t.Fatal(err)
	}
	if err := c.SetPowerOff(ctx, name, time.Time{}); err != nil {
		t.Fatal(err)
	}
	node, _ = cs.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if _, ok := node.Annotations[PowerOffAnnotation]; ok || len(node.Spec.Taints) != 1 || node.Spec.Taints[0].Key != "example.com/keep" {
		t.Fatalf("after clearing: annotations %v taints %v", node.Annotations, node.Spec.Taints)
	}
	// Both are no-ops on a node that's gone.
	if err := c.RemoveTaint(ctx, "wol-provider-test-missing", ToBeDeletedTaint); err != nil {
		t.Fatal(err)
	}
	if err := c.SetPowerOff(ctx, "wol-provider-test-missing", time.Time{}); err != nil {
		t.Fatal(err)
	}
}
