package ssh

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	gossh "golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugin"
)

// server is an in-process SSH server that accepts one client key and records the commands it's
// asked to run. "poweroff-like" commands drop the connection without an exit status, as a host that
// powers off does; "fail" exits 1.
type server struct {
	addr     string
	hostKey  gossh.PublicKey
	mu       sync.Mutex
	commands []string
}

func newServer(t *testing.T, clientKey gossh.PublicKey) *server {
	t.Helper()
	_, hostPriv, _ := ed25519.GenerateKey(rand.Reader)
	hostSigner, _ := gossh.NewSignerFromKey(hostPriv)
	cfg := &gossh.ServerConfig{PublicKeyCallback: func(_ gossh.ConnMetadata, k gossh.PublicKey) (*gossh.Permissions, error) {
		if string(k.Marshal()) != string(clientKey.Marshal()) {
			return nil, os.ErrPermission
		}
		return nil, nil
	}}
	cfg.AddHostKey(hostSigner)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	s := &server{addr: lis.Addr().String(), hostKey: hostSigner.PublicKey()}
	go func() {
		for {
			nc, err := lis.Accept()
			if err != nil {
				return
			}
			go s.serve(nc, cfg)
		}
	}()
	return s
}

func (s *server) serve(nc net.Conn, cfg *gossh.ServerConfig) {
	defer func() { _ = nc.Close() }()
	_, chans, reqs, err := gossh.NewServerConn(nc, cfg)
	if err != nil {
		return
	}
	go gossh.DiscardRequests(reqs)
	for nch := range chans {
		ch, creqs, err := nch.Accept()
		if err != nil {
			return
		}
		for req := range creqs {
			if req.Type != "exec" {
				_ = req.Reply(false, nil)
				continue
			}
			n := binary.BigEndian.Uint32(req.Payload[:4])
			cmd := string(req.Payload[4 : 4+n])
			_ = req.Reply(true, nil)
			s.mu.Lock()
			s.commands = append(s.commands, cmd)
			s.mu.Unlock()
			status := uint32(0)
			switch {
			case strings.Contains(cmd, "poweroff"), strings.Contains(cmd, "shutdown.exe"):
				_ = nc.Close() // the host is gone
				return
			case cmd == "fail":
				_, _ = ch.Stderr().Write([]byte("sudo: a password is required\n"))
				status = 1
			}
			_, _ = ch.SendRequest("exit-status", false, binary.BigEndian.AppendUint32(nil, status))
			_ = ch.Close()
		}
	}
}

type keys struct{ key, knownHosts string }

func writeKeys(t *testing.T, s *server, priv ed25519.PrivateKey) keys {
	t.Helper()
	dir := t.TempDir()
	block, err := gossh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatal(err)
	}
	k := keys{key: filepath.Join(dir, "id_ed25519"), knownHosts: filepath.Join(dir, "known_hosts")}
	_ = os.WriteFile(k.key, pem.EncodeToMemory(block), 0o600)
	_ = os.WriteFile(k.knownHosts, []byte(knownhosts.Line([]string{knownhosts.Normalize(s.addr)}, s.hostKey)+"\n"), 0o600)
	return k
}

func newPlugin(t *testing.T, s *server, k keys, extra map[string]any) plugin.PowerOff {
	t.Helper()
	host, port, _ := net.SplitHostPort(s.addr)
	p, _ := strconv.Atoi(port)
	m := map[string]any{"host": host, "port": p, "user": "ops", "keyFile": k.key, "knownHostsFile": k.knownHosts}
	for key, v := range extra {
		m[key] = v
	}
	raw, _ := json.Marshal(m)
	off, err := New(raw)
	if err != nil {
		t.Fatal(err)
	}
	return off
}

func clientKey(t *testing.T) (ed25519.PrivateKey, gossh.PublicKey) {
	t.Helper()
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	sp, _ := gossh.NewPublicKey(pub)
	return priv, sp
}

func TestPowerOff(t *testing.T) {
	priv, pub := clientKey(t)
	s := newServer(t, pub)
	k := writeKeys(t, s, priv)
	ctx, tg := context.Background(), plugin.Target{Name: "box-1"}

	// Default command; the connection drops with no exit status, as when the host powers off.
	if err := newPlugin(t, s, k, nil).PowerOff(ctx, tg); err != nil {
		t.Fatalf("default command: %v", err)
	}
	// A Windows host.
	if err := newPlugin(t, s, k, map[string]any{"command": "shutdown.exe /s /t 0"}).PowerOff(ctx, tg); err != nil {
		t.Fatalf("windows command: %v", err)
	}
	// A command that exits 0 and leaves the connection up.
	if err := newPlugin(t, s, k, map[string]any{"command": "ok"}).PowerOff(ctx, tg); err != nil {
		t.Fatalf("exit 0: %v", err)
	}
	err := newPlugin(t, s, k, map[string]any{"command": "fail"}).PowerOff(ctx, tg)
	if err == nil || !strings.Contains(err.Error(), "exited 1") || !strings.Contains(err.Error(), "password is required") {
		t.Fatalf("exit 1: got %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if want := []string{"sudo poweroff", "shutdown.exe /s /t 0", "ok", "fail"}; strings.Join(s.commands, "|") != strings.Join(want, "|") {
		t.Fatalf("commands %q", s.commands)
	}
}

func TestRefusesUnknownHostAndKey(t *testing.T) {
	priv, pub := clientKey(t)
	s := newServer(t, pub)
	k := writeKeys(t, s, priv)
	ctx := context.Background()

	other := newServer(t, pub) // same client key, different host key: not in known_hosts
	if err := newPlugin(t, other, k, nil).PowerOff(ctx, plugin.Target{}); err == nil || !strings.Contains(err.Error(), "key") {
		t.Fatalf("unknown host key: got %v", err)
	}
	wrong, _ := clientKey(t)
	k2 := writeKeys(t, s, wrong)
	if err := newPlugin(t, s, k2, nil).PowerOff(ctx, plugin.Target{}); err == nil {
		t.Fatal("wrong client key accepted")
	}
	if len(s.commands)+len(other.commands) != 0 {
		t.Fatalf("ran %v %v", s.commands, other.commands)
	}
}

func TestSettings(t *testing.T) {
	for _, bad := range []string{`{}`, `{"host":"h"}`, `{"host":"h","user":"u"}`, `{"host":"h","user":"u","keyFile":"k"}`,
		`{"host":"h","user":"u","keyFile":"k","knownHostsFile":"f","typo":1}`} {
		if _, err := New(json.RawMessage(bad)); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
	p, err := New(json.RawMessage(`{"host":"192.0.2.11","user":"u","keyFile":"k","insecureIgnoreHostKey":true}`))
	if err != nil || p.(*SSH).s.Command != DefaultCommand || p.(*SSH).Address() != "192.0.2.11:22" {
		t.Fatalf("%v %v", p, err)
	}
}
