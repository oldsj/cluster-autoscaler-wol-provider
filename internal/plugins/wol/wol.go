// Package wol powers machines on with Wake-on-LAN magic packets.
//
// Settings:
//
//	mac: "00:00:5e:00:53:01"     # required
//	broadcast: "192.0.2.255:9"   # host:port; default 255.255.255.255:9
//
// Needs Wake-on-LAN enabled in the machine's firmware and network card for the state it's shut down
// into, and a broadcast that reaches it: a broadcast doesn't cross subnets, so run the provider with
// hostNetwork on a node in the machine's subnet, or use a directed broadcast your router forwards.
package wol

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugin"
)

// Settings are the wol plugin's settings.
type Settings struct {
	MAC       string `json:"mac"`
	Broadcast string `json:"broadcast,omitempty"`
}

// WOL sends Wake-on-LAN magic packets over UDP.
type WOL struct {
	mac       net.HardwareAddr
	broadcast string
	// Dial opens the UDP socket; nil means net.Dialer. Tests replace it.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)
}

// New builds the plugin from settings.
func New(raw json.RawMessage) (plugin.PowerOn, error) {
	var s Settings
	if err := plugin.Decode(raw, &s); err != nil {
		return nil, fmt.Errorf("wol: %w", err)
	}
	mac, err := net.ParseMAC(s.MAC)
	if err != nil {
		return nil, fmt.Errorf("wol: mac: %w", err)
	}
	if len(mac) != 6 {
		return nil, fmt.Errorf("wol: mac: need a 6-byte MAC, got %d bytes", len(mac))
	}
	if s.Broadcast == "" {
		s.Broadcast = "255.255.255.255:9"
	}
	if _, _, err := net.SplitHostPort(s.Broadcast); err != nil {
		return nil, fmt.Errorf("wol: broadcast: %w", err)
	}
	return &WOL{mac: mac, broadcast: s.Broadcast}, nil
}

// MagicPacket is 6 bytes of 0xFF followed by the MAC address 16 times.
func MagicPacket(mac net.HardwareAddr) ([]byte, error) {
	if len(mac) != 6 {
		return nil, fmt.Errorf("wol: need a 6-byte MAC, got %d bytes", len(mac))
	}
	p := make([]byte, 0, 102)
	for range 6 {
		p = append(p, 0xff)
	}
	for range 16 {
		p = append(p, mac...)
	}
	return p, nil
}

// PowerOn sends the magic packet three times, 100 ms apart, since UDP can drop one. Sending can't
// confirm the machine woke; the node registering does.
func (w *WOL) PowerOn(ctx context.Context, _ plugin.Target) error {
	packet, err := MagicPacket(w.mac)
	if err != nil {
		return err
	}
	dial := w.Dial
	if dial == nil {
		dial = (&net.Dialer{Control: broadcastControl}).DialContext
	}
	conn, err := dial(ctx, "udp4", w.broadcast)
	if err != nil {
		return fmt.Errorf("wol: %w", err)
	}
	defer func() { _ = conn.Close() }()
	for i := range 3 {
		if i > 0 {
			time.Sleep(100 * time.Millisecond)
		}
		if _, err := conn.Write(packet); err != nil {
			return fmt.Errorf("wol: send to %s: %w", w.broadcast, err)
		}
	}
	return nil
}
