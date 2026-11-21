package tcp

import (
	"context"
	"encoding/json"
	"net"
	"testing"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugin"
)

func TestProbe(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	p, err := New(json.RawMessage(`{"address":"` + addr + `","timeout":"500ms"}`))
	if err != nil {
		t.Fatal(err)
	}
	if up, err := p.Probe(context.Background(), plugin.Target{}); !up || err != nil {
		t.Fatalf("listening: %v %v", up, err)
	}
	_ = lis.Close()
	if up, err := p.Probe(context.Background(), plugin.Target{}); up || err != nil {
		t.Fatalf("closed: %v %v", up, err)
	}
	if _, err := New(json.RawMessage(`{"address":"192.0.2.1"}`)); err == nil {
		t.Fatal("address without port accepted")
	}
}
