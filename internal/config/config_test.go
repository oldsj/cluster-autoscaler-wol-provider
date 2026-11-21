package config

import (
	"strings"
	"testing"
	"time"
)

const machineA = `{node: node-a, powerOn: wol, mac: "00:00:5e:00:53:01", powerOff: ssh}`

func TestDefaults(t *testing.T) {
	c, err := Parse([]byte("nodeGroups:\n  - {id: g, minSize: 0, maxSize: 1, machines: [" + machineA + "]}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.ProviderIDPrefix != "wol://" || c.BootTimeout.Duration != 15*time.Minute || c.ShutdownTimeout.Duration != 5*time.Minute {
		t.Fatalf("defaults: %+v", c)
	}
	if got := c.NodeGroups[0].Machines[0].Broadcast; got != "255.255.255.255:9" {
		t.Fatalf("broadcast default %q", got)
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
		"bad mac":         {"nodeGroups:\n  - {id: g, maxSize: 1, machines: [{node: n, powerOn: wol, mac: zz, powerOff: ssh}]}", "mac"},
		"talos no creds":  {"nodeGroups:\n  - {id: g, maxSize: 1, machines: [{node: n, powerOn: wol, mac: \"00:00:5e:00:53:01\", powerOff: talos, talosEndpoint: \"192.0.2.1:50000\"}]}", "credentials"},
		"talos no port":   {"talos: {caFile: a, certFile: b, keyFile: c}\nnodeGroups:\n  - {id: g, maxSize: 1, machines: [{node: n, powerOn: wol, mac: \"00:00:5e:00:53:01\", powerOff: talos, talosEndpoint: \"192.0.2.1\"}]}", "talosEndpoint"},
		"unknown off":     {"nodeGroups:\n  - {id: g, maxSize: 1, machines: [{node: n, powerOn: wol, mac: \"00:00:5e:00:53:01\", powerOff: ipmi}]}", "powerOff"},
		"duplicate node":  {"nodeGroups:\n  - {id: g, maxSize: 1, machines: [" + machineA + "]}\n  - {id: h, maxSize: 1, machines: [" + machineA + "]}", "more than one"},
		"duplicate group": {"nodeGroups:\n  - {id: g, maxSize: 0, machines: [" + machineA + "]}\n  - {id: g, maxSize: 0, machines: [{node: b, powerOn: wol, mac: \"00:00:5e:00:53:02\", powerOff: ssh}]}", "twice"},
		"bad capacity":    {"nodeGroups:\n  - {id: g, maxSize: 1, template: {capacity: {cpu: lots}}, machines: [" + machineA + "]}", "capacity"},
		"no node":         {"nodeGroups:\n  - {id: g, maxSize: 1, machines: [{powerOn: wol, mac: \"00:00:5e:00:53:01\", powerOff: ssh}]}", "node or nodeSelector"},
		"node and sel":    {"nodeGroups:\n  - {id: g, maxSize: 1, machines: [{node: n, nodeSelector: {a: b}, powerOn: wol, mac: \"00:00:5e:00:53:01\", powerOff: ssh}]}", "not both"},
		"sel no name":     {"nodeGroups:\n  - {id: g, maxSize: 1, machines: [{nodeSelector: {a: b}, powerOn: wol, mac: \"00:00:5e:00:53:01\", powerOff: ssh}]}", "name is required"},
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
