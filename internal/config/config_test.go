package config

import (
	"strings"
	"testing"
	"time"
)

const machineA = `{node: node-a, powerOn: {wol: {mac: "00:00:5e:00:53:01"}}, powerOff: {ssh: {}}}`

func TestDefaults(t *testing.T) {
	c, err := Parse([]byte("nodeGroups:\n  - {id: g, minSize: 0, maxSize: 1, machines: [" + machineA + "]}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.ProviderIDPrefix != "wol://" || c.BootTimeout.Duration != 20*time.Minute || c.ShutdownTimeout.Duration != 5*time.Minute ||
		c.MinOffTime.Duration != time.Minute || c.PowerOffTimeout.Duration != 2*time.Minute || !*c.AssignProviderIDs {
		t.Fatalf("defaults: %+v", c)
	}
	m := c.NodeGroups[0].Machines[0]
	if m.Name != "node-a" || m.PowerOn.Name != "wol" || string(m.PowerOn.Settings) != `{"mac":"00:00:5e:00:53:01"}` || m.PowerOff.Name != "ssh" {
		t.Fatalf("machine %+v", m)
	}
}

func TestDurations(t *testing.T) {
	c, err := Parse([]byte("bootTimeout: 7m\nnodeGroups:\n  - {id: g, maxSize: 1, machines: [" + machineA + "]}\n"))
	if err != nil || c.BootTimeout.Duration != 7*time.Minute {
		t.Fatalf("%v %v", c, err)
	}
}

func TestInvalid(t *testing.T) {
	for name, tc := range map[string]struct{ yaml, want string }{
		"no groups":       {"nodeGroups: []", "no nodeGroups"},
		"unknown field":   {"nodeGroups: []\nfoo: 1", "unknown field"},
		"max > machines":  {"nodeGroups:\n  - {id: g, maxSize: 2, machines: [" + machineA + "]}", "maxSize"},
		"min > max":       {"nodeGroups:\n  - {id: g, minSize: 1, maxSize: 0, machines: [" + machineA + "]}", "maxSize"},
		"duplicate node":  {"nodeGroups:\n  - {id: g, maxSize: 1, machines: [" + machineA + "]}\n  - {id: h, maxSize: 1, machines: [" + machineA + "]}", "more than one"},
		"duplicate group": {"nodeGroups:\n  - {id: g, maxSize: 0, machines: [" + machineA + "]}\n  - {id: g, maxSize: 0, machines: [{node: b, powerOn: {wol: {}}, powerOff: {ssh: {}}}]}", "twice"},
		"bad capacity":    {"nodeGroups:\n  - {id: g, maxSize: 1, template: {capacity: {cpu: lots}}, machines: [" + machineA + "]}", "capacity"},
		"no node":         {"nodeGroups:\n  - {id: g, maxSize: 1, machines: [{powerOn: {wol: {}}, powerOff: {ssh: {}}}]}", "node or nodeSelector"},
		"node and sel":    {"nodeGroups:\n  - {id: g, maxSize: 1, machines: [{node: n, nodeSelector: {a: b}, powerOn: {wol: {}}, powerOff: {ssh: {}}}]}", "not both"},
		"sel no name":     {"nodeGroups:\n  - {id: g, maxSize: 1, machines: [{nodeSelector: {a: b}, powerOn: {wol: {}}, powerOff: {ssh: {}}}]}", "name is required"},
		"two plugins":     {"nodeGroups:\n  - {id: g, maxSize: 1, machines: [{node: n, powerOn: {wol: {}, http: {}}, powerOff: {ssh: {}}}]}", "exactly one plugin"},
		"no powerOff":     {"nodeGroups:\n  - {id: g, maxSize: 1, machines: [{node: n, powerOn: {wol: {}}}]}", "powerOff is required"},
		"plugin string":   {"nodeGroups:\n  - {id: g, maxSize: 1, machines: [{node: n, powerOn: wol, powerOff: {ssh: {}}}]}", "plugin"},
		"negative":        {"minOffTime: -1m\nnodeGroups:\n  - {id: g, maxSize: 1, machines: [" + machineA + "]}", "negative"},
		"approval no req": {"approval: {wait: 5s}\nnodeGroups:\n  - {id: g, maxSize: 1, machines: [" + machineA + "]}", "approval"},
		"same providerID": {"nodeGroups:\n  - {id: g, maxSize: 2, machines: [{node: n1, providerID: \"x://same\", powerOn: {wol: {}}, powerOff: {ssh: {}}}, {node: n2, providerID: \"x://same\", powerOn: {wol: {}}, powerOff: {ssh: {}}}]}", "same instance ID"},
		"ID clash":        {"nodeGroups:\n  - {id: g, maxSize: 2, machines: [{node: n2, powerOn: {wol: {}}, powerOff: {ssh: {}}}, {name: x, nodeSelector: {a: b}, providerID: \"wol://n2\", powerOn: {wol: {}}, powerOff: {ssh: {}}}]}", "same instance ID"},
		"bad duration":    {"bootTimeout: 5\nnodeGroups:\n  - {id: g, maxSize: 1, machines: [" + machineA + "]}", "duration"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error containing %q", err, tc.want)
			}
		})
	}
}

func TestExampleConfig(t *testing.T) {
	if _, err := Load("../../examples/config.yaml"); err != nil {
		t.Fatal(err)
	}
}
