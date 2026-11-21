// Package power switches machines on and off: Wake-on-LAN to start, the Talos API to stop, and later
// SSH for hosts that aren't Talos.
package power

import (
	"context"
	"errors"
	"fmt"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/config"
)

// ErrNotImplemented is returned by methods that are named in the config but not written yet.
var ErrNotImplemented = errors.New("power method not implemented")

// On starts a machine.
type On interface {
	PowerOn(ctx context.Context, m config.Machine) error
}

// Off stops a machine.
type Off interface {
	PowerOff(ctx context.Context, m config.Machine) error
}

// Methods resolves a machine's configured methods.
type Methods struct {
	On  map[string]On
	Off map[string]Off
}

// PowerOn starts m with its configured method.
func (ms Methods) PowerOn(ctx context.Context, m config.Machine) error {
	on, ok := ms.On[m.PowerOn]
	if !ok {
		return fmt.Errorf("no power-on method %q", m.PowerOn)
	}
	return on.PowerOn(ctx, m)
}

// PowerOff stops m with its configured method.
func (ms Methods) PowerOff(ctx context.Context, m config.Machine) error {
	off, ok := ms.Off[m.PowerOff]
	if !ok {
		return fmt.Errorf("no power-off method %q", m.PowerOff)
	}
	return off.PowerOff(ctx, m)
}

// SSH will power off hosts that aren't Talos (for example a Windows hypervisor) over SSH.
type SSH struct{}

// PowerOff isn't written yet.
func (SSH) PowerOff(context.Context, config.Machine) error {
	return fmt.Errorf("ssh: %w", ErrNotImplemented)
}
