// Package plugin defines how machines are powered on, powered off and probed. The provider's core
// only knows these interfaces; each method (Wake-on-LAN, HTTP, exec, SSH, Talos, TCP, ICMP) lives in
// its own package under internal/plugins and is registered by the command, so none is assumed.
package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"
)

// Target names the machine a plugin acts on. Plugins may use it in templates.
type Target struct {
	Name   string // machine name; the instance ID is the provider ID prefix + Name
	Node   string // node name, if known
	Group  string // node group ID
	DryRun bool   // the provider runs --dry-run: nothing will be switched whatever the answer
}

// PowerOn starts one machine. It returns once the request is sent; the node registering confirms it.
type PowerOn interface {
	PowerOn(ctx context.Context, t Target) error
}

// PowerOff stops one machine, which Cluster Autoscaler has already drained.
type PowerOff interface {
	PowerOff(ctx context.Context, t Target) error
}

// Probe reports whether a machine is up (for example, answering on a port), independent of its
// Kubernetes node. It lets the provider tell a booting machine from one that's off.
type Probe interface {
	Probe(ctx context.Context, t Target) (up bool, err error)
}

// Factories build a plugin instance from one machine's settings (JSON, decoded strictly).
type (
	OnFactory    func(settings json.RawMessage) (PowerOn, error)
	OffFactory   func(settings json.RawMessage) (PowerOff, error)
	ProbeFactory func(settings json.RawMessage) (Probe, error)
)

// Registry is the set of available plugins by name.
type Registry struct {
	On    map[string]OnFactory
	Off   map[string]OffFactory
	Probe map[string]ProbeFactory
}

// Names lists a factory map's keys, for error messages.
func Names[F any](m map[string]F) string {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// Decode reads settings into v, refusing unknown fields, so a typo fails at startup.
func Decode(settings json.RawMessage, v any) error {
	if len(settings) == 0 || string(settings) == "null" {
		settings = []byte("{}")
	}
	d := json.NewDecoder(bytes.NewReader(settings))
	d.DisallowUnknownFields()
	return d.Decode(v)
}

// Merge overlays a machine's settings on shared defaults, key by key at the top level.
func Merge(defaults, settings json.RawMessage) (json.RawMessage, error) {
	if len(defaults) == 0 {
		return settings, nil
	}
	base := map[string]json.RawMessage{}
	if err := json.Unmarshal(defaults, &base); err != nil {
		return nil, fmt.Errorf("defaults: %w", err)
	}
	over := map[string]json.RawMessage{}
	if len(settings) > 0 && string(settings) != "null" {
		if err := json.Unmarshal(settings, &over); err != nil {
			return nil, err
		}
	}
	maps.Copy(base, over)
	return json.Marshal(base)
}

// Template is a Go text/template over a Target: {{ .Name }}, {{ .Node }}, {{ .Group }}.
type Template struct{ t *template.Template }

// ParseTemplate parses s; an empty s renders empty.
func ParseTemplate(name, s string) (Template, error) {
	t, err := template.New(name).Option("missingkey=error").Parse(s)
	if err != nil {
		return Template{}, fmt.Errorf("%s: %w", name, err)
	}
	return Template{t}, nil
}

// Render executes the template for t, with extra fields (for example the action) merged in.
func (tp Template) Render(t Target, extra map[string]string) (string, error) {
	if tp.t == nil {
		return "", nil
	}
	data := map[string]string{"Name": t.Name, "Node": t.Node, "Group": t.Group, "DryRun": strconv.FormatBool(t.DryRun)}
	maps.Copy(data, extra)
	var b strings.Builder
	if err := tp.t.Execute(&b, data); err != nil {
		return "", err
	}
	return b.String(), nil
}

// Duration is a time.Duration written as "10s" in settings.
type Duration struct{ time.Duration }

// UnmarshalJSON reads a Go duration string.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"10s\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}

// Or returns d, or def when d is zero.
func (d Duration) Or(def time.Duration) time.Duration {
	if d.Duration == 0 {
		return def
	}
	return d.Duration
}

// Decision is an approval hook's answer.
type Decision int

const (
	Pending Decision = iota
	Approved
	Denied
)

func (d Decision) String() string { return [...]string{"pending", "approved", "denied"}[d] }

// Approver asks whether a machine may be powered off. With an empty ticket it starts a new request;
// with the ticket a pending answer returned, it checks that request again.
type Approver interface {
	Approve(ctx context.Context, t Target, ticket string) (d Decision, nextTicket string, err error)
}
