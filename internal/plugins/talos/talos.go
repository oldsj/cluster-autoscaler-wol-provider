// Package talos powers Talos Linux machines off through the Talos API. It's optional: nothing
// outside this package imports Talos.
//
// Settings (caFile, certFile and keyFile usually go in defaults.powerOff.talos):
//
//	endpoint: "192.0.2.11:50000"            # the machine's Talos API; required
//	caFile: /etc/wol-provider/talos/ca.crt   # a Talos client certificate, typically a mounted
//	certFile: /etc/wol-provider/talos/tls.crt #   Secret, whose role may call Shutdown:
//	keyFile: /etc/wol-provider/talos/tls.key  #   os:operator or os:admin
//
// Generate a certificate with `talosctl config new --roles os:operator`.
package talos

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"os"

	talosclient "github.com/siderolabs/talos/pkg/machinery/client"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugin"
)

// Settings are the talos plugin's settings.
type Settings struct {
	Endpoint string `json:"endpoint"`
	CAFile   string `json:"caFile"`
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
}

// Shutdowner is the part of the Talos client this uses; tests replace it.
type Shutdowner interface {
	Shutdown(ctx context.Context, opts ...talosclient.ShutdownOption) error
	Close() error
}

// Talos shuts a machine down through its API.
type Talos struct {
	Settings Settings
	// Connect opens a client to one endpoint; nil means the Talos machinery client. Tests replace it.
	Connect func(ctx context.Context, endpoint string, tlsConfig *tls.Config) (Shutdowner, error)
}

// New builds the plugin from settings.
func New(raw json.RawMessage) (plugin.PowerOff, error) {
	var s Settings
	if err := plugin.Decode(raw, &s); err != nil {
		return nil, fmt.Errorf("talos: %w", err)
	}
	if _, _, err := net.SplitHostPort(s.Endpoint); err != nil {
		return nil, fmt.Errorf("talos: endpoint: %w", err)
	}
	if s.CAFile == "" || s.CertFile == "" || s.KeyFile == "" {
		return nil, fmt.Errorf("talos: caFile, certFile and keyFile are required")
	}
	return &Talos{Settings: s}, nil
}

// TLSConfig reads the certificate files. They're read on every power-off so a rotated Secret is
// picked up without a restart.
func (t *Talos) TLSConfig() (*tls.Config, error) {
	ca, err := os.ReadFile(t.Settings.CAFile)
	if err != nil {
		return nil, fmt.Errorf("talos: ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("talos: ca: no PEM certificates in %s", t.Settings.CAFile)
	}
	cert, err := tls.LoadX509KeyPair(t.Settings.CertFile, t.Settings.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("talos: client certificate: %w", err)
	}
	return &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}, nil
}

// PowerOff asks the machine to shut down gracefully: Talos stops its services, including the
// kubelet, then powers off.
func (t *Talos) PowerOff(ctx context.Context, target plugin.Target) error {
	tlsConfig, err := t.TLSConfig()
	if err != nil {
		return err
	}
	connect := t.Connect
	if connect == nil {
		connect = func(ctx context.Context, endpoint string, tlsConfig *tls.Config) (Shutdowner, error) {
			return talosclient.New(ctx, talosclient.WithEndpoints(endpoint), talosclient.WithTLSConfig(tlsConfig))
		}
	}
	c, err := connect(ctx, t.Settings.Endpoint, tlsConfig)
	if err != nil {
		return fmt.Errorf("talos: connect %s: %w", t.Settings.Endpoint, err)
	}
	defer func() { _ = c.Close() }()
	if err := c.Shutdown(ctx, talosclient.WithShutdownForce(false)); err != nil {
		return fmt.Errorf("talos: shutdown %s: %w", target.Name, err)
	}
	return nil
}
