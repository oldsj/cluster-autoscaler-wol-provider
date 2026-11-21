// Package icmp is the icmp probe: a machine is up when it answers an ICMP echo (ping).
//
// Settings:
//
//	host: 192.0.2.11   # an IPv4 address; required
//	timeout: 2s        # default 2s
//
// It sends unprivileged ICMP ("ping sockets"), which needs no capabilities but does need the
// provider's group ID inside the kernel's net.ipv4.ping_group_range. Most distributions and
// container runtimes allow every group; if yours doesn't, set the sysctl for the pod (it's a safe
// sysctl) or on the host, or use the tcp probe.
package icmp

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugin"
)

// Settings are the icmp probe's settings.
type Settings struct {
	Host    string          `json:"host"`
	Timeout plugin.Duration `json:"timeout,omitempty"`
}

// ICMP pings one address.
type ICMP struct {
	addr    net.IP
	timeout time.Duration
}

var seq atomic.Uint32

// New builds the probe from settings.
func New(raw json.RawMessage) (plugin.Probe, error) {
	var s Settings
	if err := plugin.Decode(raw, &s); err != nil {
		return nil, fmt.Errorf("icmp: %w", err)
	}
	ip := net.ParseIP(s.Host).To4()
	if ip == nil {
		return nil, fmt.Errorf("icmp: host must be an IPv4 address, got %q", s.Host)
	}
	return &ICMP{addr: ip, timeout: s.Timeout.Or(2 * time.Second)}, nil
}

// Probe sends one echo request and waits for the reply. No reply is "down"; failing to open the
// socket is an error.
func (p *ICMP) Probe(ctx context.Context, _ plugin.Target) (bool, error) {
	c, err := icmp.ListenPacket("udp4", "0.0.0.0")
	if err != nil {
		return false, fmt.Errorf("icmp: %w (check net.ipv4.ping_group_range)", err)
	}
	defer func() { _ = c.Close() }()
	deadline := time.Now().Add(p.timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := c.SetDeadline(deadline); err != nil {
		return false, fmt.Errorf("icmp: %w", err)
	}
	s := int(seq.Add(1) & 0xffff)
	msg := icmp.Message{Type: ipv4.ICMPTypeEcho, Body: &icmp.Echo{ID: os.Getpid() & 0xffff, Seq: s, Data: []byte("wol-provider")}}
	b, err := msg.Marshal(nil)
	if err != nil {
		return false, fmt.Errorf("icmp: %w", err)
	}
	if _, err := c.WriteTo(b, &net.UDPAddr{IP: p.addr}); err != nil {
		return false, nil
	}
	buf := make([]byte, 1500)
	for {
		n, from, err := c.ReadFrom(buf)
		if err != nil {
			return false, nil // timed out
		}
		reply, err := icmp.ParseMessage(1, buf[:n])
		if err != nil || reply.Type != ipv4.ICMPTypeEchoReply {
			continue
		}
		// With ping sockets the kernel rewrites the ID, so match on the sender and sequence.
		if echo, ok := reply.Body.(*icmp.Echo); ok && echo.Seq == s {
			if u, ok := from.(*net.UDPAddr); ok && u.IP.Equal(p.addr) {
				return true, nil
			}
		}
	}
}
