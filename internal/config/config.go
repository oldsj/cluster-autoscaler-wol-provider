// Package config reads the provider's YAML configuration: node groups made of existing machines,
// and how each machine is powered on and off.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"
)

// Config is the whole configuration file.
type Config struct {
	// ProviderIDPrefix is prepended to a machine's name to form its instance ID, "wol://<name>" by
	// default. Cluster Autoscaler matches nodes to instances by spec.providerID; the provider sets it
	// on each matched node whose providerID is empty (see AssignProviderIDs).
	ProviderIDPrefix string `json:"providerIDPrefix,omitempty"`
	// AssignProviderIDs sets spec.providerID on a matched node whose providerID is empty. The API
	// server allows that once: a providerID can go from empty to a value, never change after. It
	// also happens with --dry-run, since it switches nothing on or off. Default true.
	AssignProviderIDs *bool `json:"assignProviderIDs,omitempty"`
	// GPULabel is the node label Cluster Autoscaler uses to recognise GPU nodes. Optional.
	GPULabel string `json:"gpuLabel,omitempty"`
	// BootTimeout is how long a woken machine may take to register as a node before it's no longer
	// counted as starting. Keep Cluster Autoscaler's --max-node-provision-time below it. Default 20m.
	BootTimeout Duration `json:"bootTimeout,omitempty"`
	// WakeResendInterval is how often the power-on is sent again while a woken machine hasn't
	// registered, since a magic packet can be lost or arrive while the machine is still shutting
	// down. Default 30s.
	WakeResendInterval Duration `json:"wakeResendInterval,omitempty"`
	// MinOffTime is how long a machine stays off before it may be woken again, so a power-on doesn't
	// arrive mid-shutdown. Default 1m.
	MinOffTime Duration `json:"minOffTime,omitempty"`
	// PowerOffTimeout bounds one power-off plugin call. It runs apart from Cluster Autoscaler's
	// request, which only waits for it up to its own deadline. Default 2m.
	PowerOffTimeout Duration `json:"powerOffTimeout,omitempty"`
	// ShutdownTimeout is how long a machine may stay Ready after its power-off was requested before
	// that's treated as a failure: the provider then removes Cluster Autoscaler's deletion taint so
	// the node is usable again. Default 5m.
	ShutdownTimeout Duration `json:"shutdownTimeout,omitempty"`
	// NotReadyGrace is how long a node may be NotReady, with no probe saying its machine is up,
	// before the machine counts as off (crashed, or switched off by hand) and may be woken.
	// Default 10m.
	NotReadyGrace Duration `json:"notReadyGrace,omitempty"`
	// ProbeInterval is how often machines with a probe are probed. Default 15s.
	ProbeInterval Duration `json:"probeInterval,omitempty"`
	// Defaults are settings shared by every machine that uses a plugin, by plugin name; a machine's
	// own settings override them key by key. For example the Talos client certificate, or the SSH
	// user and key.
	Defaults Defaults `json:"defaults,omitempty"`
	// Approval, if set, must approve each power-off. Off by default.
	Approval *Approval `json:"approval,omitempty"`
	// NodeGroups are the groups Cluster Autoscaler may scale.
	NodeGroups []NodeGroup `json:"nodeGroups"`
}

// Defaults are per-plugin shared settings.
type Defaults struct {
	PowerOn  map[string]json.RawMessage `json:"powerOn,omitempty"`
	PowerOff map[string]json.RawMessage `json:"powerOff,omitempty"`
	Probe    map[string]json.RawMessage `json:"probe,omitempty"`
}

// Approval is a hook asked before each power-off; see the README.
type Approval struct {
	// HTTP is the request, with the http plugin's settings.
	HTTP json.RawMessage `json:"http"`
	// Wait is how long one scale-down call waits for a pending approval before failing, so Cluster
	// Autoscaler rolls back and asks again later. Keep it under Cluster Autoscaler's grpc_timeout.
	// Default 20s.
	Wait Duration `json:"wait,omitempty"`
	// PollInterval is how often a pending approval is checked while waiting. Default 2s.
	PollInterval Duration `json:"pollInterval,omitempty"`
	// Expire is how long a pending or approved answer is kept for the next attempt; after it, the
	// next scale-down asks again. Default 1h.
	Expire Duration `json:"expire,omitempty"`
}

// Plugin selects one plugin by name, with its settings: written {wol: {mac: ...}} in YAML.
type Plugin struct {
	Name     string
	Settings json.RawMessage
}

// UnmarshalJSON reads an object with exactly one key, the plugin name.
func (p *Plugin) UnmarshalJSON(b []byte) error {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return fmt.Errorf("want {<plugin>: {settings}}: %w", err)
	}
	if len(m) != 1 {
		return fmt.Errorf("want exactly one plugin, {<plugin>: {settings}}, got %d keys", len(m))
	}
	for k, v := range m {
		p.Name, p.Settings = k, v
	}
	return nil
}

// MarshalJSON writes the one-key form back.
func (p Plugin) MarshalJSON() ([]byte, error) {
	if p.Name == "" {
		return []byte("null"), nil
	}
	return json.Marshal(map[string]json.RawMessage{p.Name: p.Settings})
}

// NodeGroup is a set of existing machines that are switched on and off as a group grows and shrinks.
type NodeGroup struct {
	ID      string `json:"id"`
	MinSize int    `json:"minSize"`
	MaxSize int    `json:"maxSize"`
	// Template describes a node of this group as it registers, for scaling up from zero.
	Template *Template `json:"template,omitempty"`
	// Machines are woken in the order listed. Which ones are powered off is Cluster Autoscaler's
	// choice.
	Machines []Machine `json:"machines"`
}

// Template is the shape of a node in a group: what Cluster Autoscaler simulates scheduling onto.
type Template struct {
	Labels   map[string]string `json:"labels,omitempty"`
	Taints   []corev1.Taint    `json:"taints,omitempty"`
	Capacity map[string]string `json:"capacity"`
}

// Machine is one physical or virtual machine and the node it registers as.
type Machine struct {
	// Name identifies the machine; its instance ID is ProviderIDPrefix + Name. Default: Node.
	// Required with NodeSelector.
	Name string `json:"name,omitempty"`
	// Node is the Kubernetes node name the machine registers as. Set Node or NodeSelector.
	Node string `json:"node,omitempty"`
	// NodeSelector matches the machine's node by labels instead of by name; it must match at most
	// one node.
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// ProviderID is the machine's instance ID when its node already has a spec.providerID (set by
	// its kubelet or another controller): give that value here, and the provider uses it as-is.
	// Empty means ProviderIDPrefix + Name.
	ProviderID string `json:"providerID,omitempty"`
	// PowerOn is the plugin that starts the machine, for example {wol: {mac: ...}}.
	PowerOn Plugin `json:"powerOn"`
	// PowerOff is the plugin that stops it, for example {ssh: {host: ...}}.
	PowerOff Plugin `json:"powerOff"`
	// Probe is an optional plugin that tells whether the machine is up apart from its node, for
	// example {tcp: {address: ...}}. Without one, the node's Ready condition is all the provider
	// goes by.
	Probe *Plugin `json:"probe,omitempty"`
}

// InstanceID is the ID Cluster Autoscaler knows a machine by: its providerID, or the prefix and its
// name.
func (c *Config) InstanceID(m *Machine) string {
	if m.ProviderID != "" {
		return m.ProviderID
	}
	return c.ProviderIDPrefix + m.Name
}

// Duration is a time.Duration written as "10m" in YAML.
type Duration struct{ time.Duration }

// UnmarshalJSON reads a Go duration string.
func (d *Duration) UnmarshalJSON(b []byte) error {
	s := string(b)
	if len(s) < 2 || s[0] != '"' {
		return fmt.Errorf("duration must be a string like \"10m\", got %s", s)
	}
	v, err := time.ParseDuration(s[1 : len(s)-1])
	if err != nil {
		return err
	}
	if v < 0 {
		return fmt.Errorf("duration %s is negative", v)
	}
	d.Duration = v
	return nil
}

// Load reads and validates a configuration file.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(b)
}

// Parse reads and validates configuration YAML, filling in defaults.
func Parse(b []byte) (*Config, error) {
	var c Config
	if err := yaml.UnmarshalStrict(b, &c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if c.ProviderIDPrefix == "" {
		c.ProviderIDPrefix = "wol://"
	}
	for _, d := range []struct {
		v   *Duration
		def time.Duration
	}{
		{&c.BootTimeout, 20 * time.Minute},
		{&c.WakeResendInterval, 30 * time.Second},
		{&c.MinOffTime, time.Minute},
		{&c.PowerOffTimeout, 2 * time.Minute},
		{&c.NotReadyGrace, 10 * time.Minute},
		{&c.ProbeInterval, 15 * time.Second},
	} {
		if d.v.Duration == 0 {
			d.v.Duration = d.def
		}
	}
	if c.AssignProviderIDs == nil {
		t := true
		c.AssignProviderIDs = &t
	}
	if a := c.Approval; a != nil {
		if a.Wait.Duration == 0 {
			a.Wait.Duration = 20 * time.Second
		}
		if a.PollInterval.Duration == 0 {
			a.PollInterval.Duration = 2 * time.Second
		}
		if a.Expire.Duration == 0 {
			a.Expire.Duration = time.Hour
		}
	}
	if c.ShutdownTimeout.Duration == 0 {
		c.ShutdownTimeout.Duration = 5 * time.Minute
	}
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return &c, nil
}

func (c *Config) validate() error {
	if len(c.NodeGroups) == 0 {
		return fmt.Errorf("no nodeGroups")
	}
	if c.Approval != nil && len(c.Approval.HTTP) == 0 {
		return fmt.Errorf("approval: http is required")
	}
	groups := map[string]bool{}
	nodes := map[string]bool{}
	names := map[string]bool{}
	ids := map[string]string{} // instance ID -> machine
	for i := range c.NodeGroups {
		g := &c.NodeGroups[i]
		if g.ID == "" {
			return fmt.Errorf("nodeGroups[%d]: id is required", i)
		}
		if groups[g.ID] {
			return fmt.Errorf("node group %q listed twice", g.ID)
		}
		groups[g.ID] = true
		if len(g.Machines) == 0 {
			return fmt.Errorf("node group %q: no machines", g.ID)
		}
		if g.MinSize < 0 || g.MaxSize < g.MinSize || g.MaxSize > len(g.Machines) {
			return fmt.Errorf("node group %q: need 0 <= minSize <= maxSize <= %d machines", g.ID, len(g.Machines))
		}
		if g.Template != nil {
			for name, q := range g.Template.Capacity {
				if _, err := resource.ParseQuantity(q); err != nil {
					return fmt.Errorf("node group %q: capacity %s: %w", g.ID, name, err)
				}
			}
		}
		for j := range g.Machines {
			m := &g.Machines[j]
			switch {
			case m.Node == "" && len(m.NodeSelector) == 0:
				return fmt.Errorf("node group %q machines[%d]: set node or nodeSelector", g.ID, j)
			case m.Node != "" && len(m.NodeSelector) > 0:
				return fmt.Errorf("node group %q machines[%d]: set node or nodeSelector, not both", g.ID, j)
			case m.Name == "" && m.Node == "":
				return fmt.Errorf("node group %q machines[%d]: name is required with nodeSelector", g.ID, j)
			}
			if m.Name == "" {
				m.Name = m.Node
			}
			if names[m.Name] {
				return fmt.Errorf("machine %q is in more than one place", m.Name)
			}
			names[m.Name] = true
			if m.Node != "" {
				if nodes[m.Node] {
					return fmt.Errorf("node %q is in more than one place", m.Node)
				}
				nodes[m.Node] = true
			}
			if err := c.validateMachine(m); err != nil {
				return fmt.Errorf("machine %q: %w", m.Name, err)
			}
			id := c.InstanceID(m)
			if other, ok := ids[id]; ok {
				return fmt.Errorf("machines %q and %q have the same instance ID %q", other, m.Name, id)
			}
			ids[id] = m.Name
		}
	}
	return nil
}

func (c *Config) validateMachine(m *Machine) error {
	// The plugins check their own settings when they're built, at startup.
	if m.PowerOn.Name == "" {
		return fmt.Errorf("powerOn is required")
	}
	if m.PowerOff.Name == "" {
		return fmt.Errorf("powerOff is required")
	}
	return nil
}
