// Package tcp is the tcp probe: a machine is up when a TCP connection to an address succeeds, for
// example its SSH port or its kubelet's 10250.
//
// Settings:
//
//	address: "192.0.2.11:22"   # required
//	timeout: 2s                # default 2s
package tcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugin"
)

// Settings are the tcp probe's settings.
type Settings struct {
	Address string          `json:"address"`
	Timeout plugin.Duration `json:"timeout,omitempty"`
}

// TCP probes one address.
type TCP struct {
	address string
	timeout time.Duration
}

// New builds the probe from settings.
func New(raw json.RawMessage) (plugin.Probe, error) {
	var s Settings
	if err := plugin.Decode(raw, &s); err != nil {
		return nil, fmt.Errorf("tcp: %w", err)
	}
	if _, _, err := net.SplitHostPort(s.Address); err != nil {
		return nil, fmt.Errorf("tcp: address: %w", err)
	}
	return &TCP{address: s.Address, timeout: s.Timeout.Or(2 * time.Second)}, nil
}

// Probe reports whether a connection succeeds within the timeout. A refused or timed-out connection
// is "down", not an error.
func (p *TCP) Probe(ctx context.Context, _ plugin.Target) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	c, err := (&net.Dialer{}).DialContext(ctx, "tcp", p.address)
	if err != nil {
		return false, nil
	}
	_ = c.Close()
	return true, nil
}
