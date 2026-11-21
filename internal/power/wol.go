package power

import (
	"context"
	"fmt"
	"net"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/config"
)

// WOL sends Wake-on-LAN magic packets over UDP.
type WOL struct {
	// Dial opens the UDP socket; nil means net.Dialer. Tests replace it.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)
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

// PowerOn sends the magic packet for m.MAC to m.Broadcast. Sending can't confirm the machine woke;
// the node registering does.
func (w WOL) PowerOn(ctx context.Context, m config.Machine) error {
	mac, err := net.ParseMAC(m.MAC)
	if err != nil {
		return fmt.Errorf("wol: %w", err)
	}
	packet, err := MagicPacket(mac)
	if err != nil {
		return err
	}
	dial := w.Dial
	if dial == nil {
		dial = (&net.Dialer{Control: broadcastControl}).DialContext
	}
	conn, err := dial(ctx, "udp4", m.Broadcast)
	if err != nil {
		return fmt.Errorf("wol: %w", err)
	}
	defer conn.Close()
	if _, err := conn.Write(packet); err != nil {
		return fmt.Errorf("wol: send to %s: %w", m.Broadcast, err)
	}
	return nil
}
