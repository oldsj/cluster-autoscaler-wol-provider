// Package ssh is the ssh plugin: power a machine off by running a command on it over SSH. It works
// for any host with an SSH server: Linux, BSD, macOS, or Windows with OpenSSH Server.
//
// Settings (user, keyFile and knownHostsFile usually go in defaults.powerOff.ssh):
//
//	host: 192.0.2.11                                # required
//	port: 22                                         # default 22
//	user: ops                                        # required
//	keyFile: /etc/wol-provider/ssh/id_ed25519        # private key, typically a mounted Secret
//	knownHostsFile: /etc/wol-provider/ssh/known_hosts # required, unless insecureIgnoreHostKey
//	insecureIgnoreHostKey: false                     # accept any host key; not recommended
//	command: sudo poweroff                           # default "sudo poweroff"
//	timeout: 30s                                     # connect and run; default 30s
//
// The user needs to be allowed to run the command without a password (for example a sudoers line
// `ops ALL=(root) NOPASSWD: /usr/sbin/poweroff`). For a Windows host, use `shutdown.exe /s /t 0` and
// an administrator's key (C:\ProgramData\ssh\administrators_authorized_keys).
//
// A command that powers the host off often takes the connection down before it reports an exit
// status. That counts as success; a reported non-zero exit status is a failure.
package ssh

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	gossh "golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugin"
)

// DefaultCommand is run when the settings give none.
const DefaultCommand = "sudo poweroff"

// Settings are the ssh plugin's settings.
type Settings struct {
	Host                  string          `json:"host"`
	Port                  int             `json:"port,omitempty"`
	User                  string          `json:"user"`
	KeyFile               string          `json:"keyFile"`
	KnownHostsFile        string          `json:"knownHostsFile,omitempty"`
	InsecureIgnoreHostKey bool            `json:"insecureIgnoreHostKey,omitempty"`
	Command               string          `json:"command,omitempty"`
	Timeout               plugin.Duration `json:"timeout,omitempty"`
}

// SSH runs the power-off command on the host.
type SSH struct {
	s Settings
}

// New builds the plugin from settings.
func New(raw json.RawMessage) (plugin.PowerOff, error) {
	var s Settings
	if err := plugin.Decode(raw, &s); err != nil {
		return nil, fmt.Errorf("ssh: %w", err)
	}
	switch {
	case s.Host == "":
		return nil, fmt.Errorf("ssh: host is required")
	case s.User == "":
		return nil, fmt.Errorf("ssh: user is required")
	case s.KeyFile == "":
		return nil, fmt.Errorf("ssh: keyFile is required")
	case s.KnownHostsFile == "" && !s.InsecureIgnoreHostKey:
		return nil, fmt.Errorf("ssh: knownHostsFile is required (or insecureIgnoreHostKey: true)")
	}
	if s.Port == 0 {
		s.Port = 22
	}
	if s.Command == "" {
		s.Command = DefaultCommand
	}
	return &SSH{s: s}, nil
}

// Address is host:port.
func (p *SSH) Address() string { return net.JoinHostPort(p.s.Host, strconv.Itoa(p.s.Port)) }

func (p *SSH) clientConfig() (*gossh.ClientConfig, error) {
	// Read on every power-off, so a rotated Secret is picked up without a restart.
	key, err := os.ReadFile(p.s.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("ssh: key: %w", err)
	}
	signer, err := gossh.ParsePrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("ssh: key: %w", err)
	}
	hostKey := gossh.InsecureIgnoreHostKey() //nolint:gosec // only when the settings ask for it
	if !p.s.InsecureIgnoreHostKey {
		if hostKey, err = knownhosts.New(p.s.KnownHostsFile); err != nil {
			return nil, fmt.Errorf("ssh: knownHostsFile: %w", err)
		}
	}
	return &gossh.ClientConfig{User: p.s.User, Auth: []gossh.AuthMethod{gossh.PublicKeys(signer)},
		HostKeyCallback: hostKey, Timeout: p.s.Timeout.Or(30 * time.Second)}, nil
}

// PowerOff connects and runs the command.
func (p *SSH) PowerOff(ctx context.Context, t plugin.Target) error {
	cfg, err := p.clientConfig()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, p.s.Timeout.Or(30*time.Second))
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", p.Address())
	if err != nil {
		return fmt.Errorf("ssh: %s: %w", t.Name, err)
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	c, chans, reqs, err := gossh.NewClientConn(conn, p.Address(), cfg)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("ssh: %s: %w", t.Name, err)
	}
	client := gossh.NewClient(c, chans, reqs)
	defer func() { _ = client.Close() }()
	session, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("ssh: %s: session: %w", t.Name, err)
	}
	defer func() { _ = session.Close() }()
	var stderr bytes.Buffer
	session.Stderr = &stderr
	err = session.Run(p.s.Command)
	var exitErr *gossh.ExitError
	var missing *gossh.ExitMissingError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &exitErr):
		return fmt.Errorf("ssh: %s: %q exited %d: %s", t.Name, p.s.Command, exitErr.ExitStatus(), tail(stderr.String()))
	case errors.As(err, &missing), errors.Is(err, io.EOF), ctx.Err() == nil && isClosed(err):
		// The host went down before it reported an exit status: the command did its job.
		return nil
	default:
		return fmt.Errorf("ssh: %s: %q: %w", t.Name, p.s.Command, err)
	}
}

func isClosed(err error) bool {
	return errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "connection reset")
}

func tail(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 512 {
		s = s[len(s)-512:]
	}
	return s
}
