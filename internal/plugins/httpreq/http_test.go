package httpreq

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugin"
)

type seen struct {
	mu   sync.Mutex
	reqs []string // "METHOD path auth body"
}

func (s *seen) add(r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reqs = append(s.reqs, r.Method+" "+r.URL.Path+" "+r.Header.Get("Authorization")+" "+string(b))
}

func settings(t *testing.T, url string, extra map[string]any) json.RawMessage {
	t.Helper()
	m := map[string]any{"url": url}
	for k, v := range extra {
		m[k] = v
	}
	b, _ := json.Marshal(m)
	return b
}

func TestPowerOnOff(t *testing.T) {
	var s seen
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.add(r)
		if strings.Contains(r.URL.Path, "broken") {
			http.Error(w, "plug offline", http.StatusBadGateway)
		}
	}))
	defer srv.Close()
	token := filepath.Join(t.TempDir(), "auth")
	if err := os.WriteFile(token, []byte("Bearer s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	raw := settings(t, srv.URL+"/plug/{{ .Name }}/{{ .Action }}", map[string]any{
		"headerFiles": map[string]string{"Authorization": token},
		"body":        `{"group":"{{ .Group }}"}`,
	})
	on, err := NewOn(raw)
	if err != nil {
		t.Fatal(err)
	}
	off, _ := NewOff(raw)
	tg := plugin.Target{Name: "box-1", Group: "batch"}
	if err := on.PowerOn(context.Background(), tg); err != nil {
		t.Fatal(err)
	}
	if err := off.PowerOff(context.Background(), tg); err != nil {
		t.Fatal(err)
	}
	if err := off.PowerOff(context.Background(), plugin.Target{Name: "broken"}); err == nil || !strings.Contains(err.Error(), "502") {
		t.Fatalf("got %v, want a 502 error", err)
	}
	want := []string{
		`POST /plug/box-1/on Bearer s3cret {"group":"batch"}`,
		`POST /plug/box-1/off Bearer s3cret {"group":"batch"}`,
	}
	for i, w := range want {
		if s.reqs[i] != w {
			t.Errorf("request %d: %q, want %q", i, s.reqs[i], w)
		}
	}
}

func TestSettings(t *testing.T) {
	for _, bad := range []string{`{}`, `{"url":"x","typo":1}`, `{"url":"{{ .Nope"}`, `{"url":"x","caFile":"/nonexistent"}`} {
		if _, err := NewRequest(json.RawMessage(bad)); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
}

func TestApprovalPendingThenApproved(t *testing.T) {
	var s seen
	polls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.add(r)
		switch r.URL.Path {
		case "/ask":
			w.Header().Set("Location", "/status/42")
			w.WriteHeader(http.StatusAccepted)
		case "/status/42":
			polls++
			if polls < 2 {
				w.WriteHeader(http.StatusAccepted)
				return
			}
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	a, err := NewApprover(settings(t, srv.URL+"/ask", nil))
	if err != nil {
		t.Fatal(err)
	}
	ctx, tg := context.Background(), plugin.Target{Name: "box-1"}
	d, ticket, err := a.Approve(ctx, tg, "")
	if err != nil || d != plugin.Pending || ticket != srv.URL+"/status/42" {
		t.Fatalf("ask: %v %q %v", d, ticket, err)
	}
	d, ticket, err = a.Approve(ctx, tg, ticket)
	if err != nil || d != plugin.Pending || ticket != srv.URL+"/status/42" {
		t.Fatalf("poll 1: %v %q %v", d, ticket, err)
	}
	d, _, err = a.Approve(ctx, tg, ticket)
	if err != nil || d != plugin.Approved {
		t.Fatalf("poll 2: %v %v", d, err)
	}
	if !strings.HasPrefix(s.reqs[1], "GET /status/42") {
		t.Fatalf("poll request %q", s.reqs[1])
	}
}

func TestApprovalDeniedAndRepeat(t *testing.T) {
	status := http.StatusAccepted
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
	defer srv.Close()
	a, _ := NewApprover(settings(t, srv.URL, nil))
	ctx, tg := context.Background(), plugin.Target{Name: "box-1"}
	d, ticket, _ := a.Approve(ctx, tg, "")
	if d != plugin.Pending || ticket != pollSame {
		t.Fatalf("%v %q", d, ticket)
	}
	status = http.StatusForbidden
	if d, _, err := a.Approve(ctx, tg, ticket); d != plugin.Denied || err == nil {
		t.Fatalf("%v %v", d, err)
	}
}
