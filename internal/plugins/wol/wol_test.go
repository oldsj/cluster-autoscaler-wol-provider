package wol

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugin"
)

func TestMagicPacket(t *testing.T) {
	mac, _ := net.ParseMAC("00:00:5e:00:53:01")
	p, err := MagicPacket(mac)
	if err != nil {
		t.Fatal(err)
	}
	if len(p) != 102 || !bytes.Equal(p[:6], bytes.Repeat([]byte{0xff}, 6)) {
		t.Fatalf("header %x", p[:6])
	}
	for i := range 16 {
		if !bytes.Equal(p[6+6*i:12+6*i], mac) {
			t.Fatalf("repetition %d: %x", i, p[6+6*i:12+6*i])
		}
	}
}

func TestSettings(t *testing.T) {
	for _, bad := range []string{`{}`, `{"mac":"zz"}`, `{"mac":"00:00:00:00:fe:80:00:00:00:00:00:00:02:00:5e:10:00:00:00:01"}`,
		`{"mac":"00:00:5e:00:53:01","broadcast":"192.0.2.255"}`, `{"mac":"00:00:5e:00:53:01","typo":1}`} {
		if _, err := New(json.RawMessage(bad)); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
	p, err := New(json.RawMessage(`{"mac":"00:00:5e:00:53:01"}`))
	if err != nil || p.(*WOL).broadcast != "255.255.255.255:9" {
		t.Fatalf("%v %v", p, err)
	}
}

func TestSendsPacket(t *testing.T) {
	lis, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lis.Close() }()
	p, err := New(json.RawMessage(`{"mac":"00:00:5e:00:53:01","broadcast":"` + lis.LocalAddr().String() + `"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.PowerOn(context.Background(), plugin.Target{Name: "a"}); err != nil {
		t.Fatal(err)
	}
	mac, _ := net.ParseMAC("00:00:5e:00:53:01")
	want, _ := MagicPacket(mac)
	buf := make([]byte, 200)
	_ = lis.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _, err := lis.ReadFrom(buf)
	if err != nil || !bytes.Equal(buf[:n], want) {
		t.Fatalf("got %x, %v", buf[:n], err)
	}
}
