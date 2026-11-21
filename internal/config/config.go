// Package config reads the provider's YAML configuration: node groups made of existing machines,
// and how each machine is powered on and off.
package config

import (
	"fmt"
	"net"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"
)

// Config is the whole configuration file.
type Config struct {
	// ProviderIDPrefix is prepended to a machine's node name to form its instance ID. Each node's
	// spec.providerID must equal it (kubelet --provider-id), or Cluster Autoscaler can't match nodes
	// to instances. Default "wol://".
	ProviderIDPrefix string `json:"providerIDPrefix,omitempty"`
	// GPULabel is the node label Cluster Autoscaler uses to recognise GPU nodes. Optional.
	GPULabel string `json:"gpuLabel,omitempty"`
	// BootTimeout is how long a woken machine may take to register as a node before it's no longer
	// counted as starting. Keep Cluster Autoscaler's --max-node-provision-time at or below it.
	// Default 15m.
	BootTimeout Duration `json:"bootTimeout,omitempty"`
	// ShutdownTimeout is how long a machine may stay Ready after its power-off was requested before
	// that's logged as a failure. Default 5m.
	ShutdownTimeout Duration `json:"shutdownTimeout,omitempty"`
	// Talos holds the client certificate used by the "talos" power-off method.
	Talos *TalosCredentials `json:"talos,omitempty"`
	// NodeGroups are the groups Cluster Autoscaler may scale.
	NodeGroups []NodeGroup `json:"nodeGroups"`
}

// TalosCredentials are file paths, typically a mounted Kubernetes Secret. The certificate needs a
// Talos role that may call Shutdown (os:operator or above).
type TalosCredentials struct {
	CAFile   string `json:"caFile"`
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
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
	// Node is the Kubernetes node name the machine registers as.
	Node string `json:"node"`
	// PowerOn is the method used to start it: "wol".
	PowerOn string `json:"powerOn"`
	// PowerOff is the method used to stop it: "talos" or "ssh" (not implemented yet).
	PowerOff string `json:"powerOff"`
	// MAC and Broadcast are for "wol". Broadcast is host:port; default 255.255.255.255:9.
	MAC       string `json:"mac,omitempty"`
	Broadcast string `json:"broadcast,omitempty"`
	// TalosEndpoint is the machine's Talos API, host:port, for "talos".
	TalosEndpoint string `json:"talosEndpoint,omitempty"`
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
	if c.BootTimeout.Duration == 0 {
		c.BootTimeout.Duration = 15 * time.Minute
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
	groups := map[string]bool{}
	nodes := map[string]bool{}
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
			if m.Node == "" {
				return fmt.Errorf("node group %q machines[%d]: node is required", g.ID, j)
			}
			if nodes[m.Node] {
				return fmt.Errorf("node %q is in more than one place", m.Node)
			}
			nodes[m.Node] = true
			if err := c.validateMachine(m); err != nil {
				return fmt.Errorf("node %q: %w", m.Node, err)
			}
		}
	}
	return nil
}

func (c *Config) validateMachine(m *Machine) error {
	switch m.PowerOn {
	case "wol":
		if _, err := net.ParseMAC(m.MAC); err != nil {
			return fmt.Errorf("mac: %w", err)
		}
		if m.Broadcast == "" {
			m.Broadcast = "255.255.255.255:9"
		}
		if _, _, err := net.SplitHostPort(m.Broadcast); err != nil {
			return fmt.Errorf("broadcast: %w", err)
		}
	default:
		return fmt.Errorf("powerOn %q: want wol", m.PowerOn)
	}
	switch m.PowerOff {
	case "talos":
		if c.Talos == nil {
			return fmt.Errorf("powerOff talos needs the top-level talos credentials")
		}
		if _, _, err := net.SplitHostPort(m.TalosEndpoint); err != nil {
			return fmt.Errorf("talosEndpoint: %w", err)
		}
	case "ssh":
		// Accepted so a config can name it; the method itself returns an error until it's written.
	default:
		return fmt.Errorf("powerOff %q: want talos or ssh", m.PowerOff)
	}
	return nil
}
