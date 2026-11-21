package execcmd

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugin"
)

func TestRun(t *testing.T) {
	out := filepath.Join(t.TempDir(), "out")
	raw, _ := json.Marshal(map[string]any{
		"command": []string{"/bin/sh", "-c", `echo "$1 $2 $GREETING" > "$3"`, "sh", "{{ .Name }}", "{{ .Action }}", out},
		"env":     map[string]string{"GREETING": "hello"},
	})
	on, err := NewOn(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := on.PowerOn(context.Background(), plugin.Target{Name: "box-1"}); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(out)
	if strings.TrimSpace(string(b)) != "box-1 on hello" {
		t.Fatalf("got %q", b)
	}
}

func TestFailure(t *testing.T) {
	off, _ := NewOff(json.RawMessage(`{"command":["/bin/sh","-c","echo no route >&2; exit 3"]}`))
	err := off.PowerOff(context.Background(), plugin.Target{Name: "box-1"})
	if err == nil || !strings.Contains(err.Error(), "no route") {
		t.Fatalf("got %v", err)
	}
	slow, _ := NewOff(json.RawMessage(`{"command":["/bin/sh","-c","sleep 5"],"timeout":"100ms"}`))
	if err := slow.PowerOff(context.Background(), plugin.Target{}); err == nil {
		t.Fatal("timeout not enforced")
	}
}

func TestSettings(t *testing.T) {
	for _, bad := range []string{`{}`, `{"command":[]}`, `{"command":["x"],"typo":1}`, `{"command":["{{ .Nope"]}`} {
		if _, err := NewCommand(json.RawMessage(bad)); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
}
