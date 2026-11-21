package icmp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugin"
)

func TestSettings(t *testing.T) {
	for _, bad := range []string{`{}`, `{"host":"example.com"}`, `{"host":"2001:db8::1"}`} {
		if _, err := New(json.RawMessage(bad)); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
}

// Pinging loopback needs ping sockets; skip where the sandbox doesn't allow them.
func TestLoopback(t *testing.T) {
	p, err := New(json.RawMessage(`{"host":"127.0.0.1","timeout":"1s"}`))
	if err != nil {
		t.Fatal(err)
	}
	up, err := p.Probe(context.Background(), plugin.Target{})
	if err != nil && strings.Contains(err.Error(), "ping_group_range") {
		t.Skip(err)
	}
	if err != nil || !up {
		t.Fatalf("loopback: %v %v", up, err)
	}
}
