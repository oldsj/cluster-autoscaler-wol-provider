// Package execcmd is the exec plugin: run a command in the provider's container to power a machine
// on or off. The default image has no shell or tools, so mount the program (or build an image FROM
// this one with it added).
//
// Settings:
//
//	command: ["/opt/power/switch", "{{ .Name }}", "{{ .Action }}"]   # required; each arg a template
//	env: {POWER_API: "https://hub.example"}                         # added to the provider's env
//	timeout: 60s                                                     # default 60s
//
// Exit status 0 succeeds; anything else fails, with the end of stderr in the error.
package execcmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugin"
)

// Settings are the exec plugin's settings.
type Settings struct {
	Command []string          `json:"command"`
	Env     map[string]string `json:"env,omitempty"`
	Timeout plugin.Duration   `json:"timeout,omitempty"`
}

// Command is one configured command.
type Command struct {
	args    []plugin.Template
	env     []string
	timeout time.Duration
}

// NewCommand builds a command from settings.
func NewCommand(raw json.RawMessage) (*Command, error) {
	var s Settings
	if err := plugin.Decode(raw, &s); err != nil {
		return nil, fmt.Errorf("exec: %w", err)
	}
	if len(s.Command) == 0 || s.Command[0] == "" {
		return nil, fmt.Errorf("exec: command is required")
	}
	c := &Command{timeout: s.Timeout.Or(60 * time.Second)}
	for i, a := range s.Command {
		t, err := plugin.ParseTemplate(fmt.Sprintf("command[%d]", i), a)
		if err != nil {
			return nil, fmt.Errorf("exec: %w", err)
		}
		c.args = append(c.args, t)
	}
	for k, v := range s.Env {
		c.env = append(c.env, k+"="+v)
	}
	return c, nil
}

// Run runs the command for t.
func (c *Command) Run(ctx context.Context, t plugin.Target, action string) error {
	args := make([]string, len(c.args))
	for i, tp := range c.args {
		a, err := tp.Render(t, map[string]string{"Action": action})
		if err != nil {
			return fmt.Errorf("exec: %w", err)
		}
		args[i] = a
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Env = append(os.Environ(), c.env...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if len(msg) > 512 {
			msg = msg[len(msg)-512:]
		}
		return fmt.Errorf("exec: %s %s: %w: %s", args[0], action, err, msg)
	}
	return nil
}

// On runs the command to power machines on.
type On struct{ *Command }

// PowerOn runs the command with Action "on".
func (o On) PowerOn(ctx context.Context, t plugin.Target) error { return o.Run(ctx, t, "on") }

// Off runs the command to power machines off.
type Off struct{ *Command }

// PowerOff runs the command with Action "off".
func (o Off) PowerOff(ctx context.Context, t plugin.Target) error { return o.Run(ctx, t, "off") }

// NewOn builds the power-on plugin.
func NewOn(raw json.RawMessage) (plugin.PowerOn, error) {
	c, err := NewCommand(raw)
	if err != nil {
		return nil, err
	}
	return On{c}, nil
}

// NewOff builds the power-off plugin.
func NewOff(raw json.RawMessage) (plugin.PowerOff, error) {
	c, err := NewCommand(raw)
	if err != nil {
		return nil, err
	}
	return Off{c}, nil
}
