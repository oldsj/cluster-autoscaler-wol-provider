package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/config"
	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugin"
	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugins/execcmd"
	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugins/httpreq"
	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugins/icmp"
	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugins/ssh"
	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugins/talos"
	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugins/tcp"
	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugins/wol"
	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/provider"
)

// registry is every plugin this binary carries. The core packages know none of them by name.
var registry = plugin.Registry{
	On: map[string]plugin.OnFactory{
		"wol":  wol.New,
		"http": httpreq.NewOn,
		"exec": execcmd.NewOn,
	},
	Off: map[string]plugin.OffFactory{
		"ssh":   ssh.New,
		"talos": talos.New,
		"http":  httpreq.NewOff,
		"exec":  execcmd.NewOff,
	},
	Probe: map[string]plugin.ProbeFactory{
		"tcp":  tcp.New,
		"icmp": icmp.New,
	},
}

// buildPlugins builds each machine's plugins from its settings over the config's per-plugin
// defaults, so a bad setting fails at startup, not at the first scale-up.
func buildPlugins(cfg *config.Config, reg plugin.Registry) (map[string]provider.Plugins, error) {
	out := map[string]provider.Plugins{}
	for _, g := range cfg.NodeGroups {
		for _, m := range g.Machines {
			var pl provider.Plugins
			var err error
			if pl.On, err = build(reg.On, cfg.Defaults.PowerOn, m.PowerOn, "powerOn"); err != nil {
				return nil, fmt.Errorf("machine %q: %w", m.Name, err)
			}
			if pl.Off, err = build(reg.Off, cfg.Defaults.PowerOff, m.PowerOff, "powerOff"); err != nil {
				return nil, fmt.Errorf("machine %q: %w", m.Name, err)
			}
			if m.Probe != nil {
				if pl.Probe, err = build(reg.Probe, cfg.Defaults.Probe, *m.Probe, "probe"); err != nil {
					return nil, fmt.Errorf("machine %q: %w", m.Name, err)
				}
			}
			out[m.Name] = pl
		}
	}
	return out, nil
}

func build[F ~func(json.RawMessage) (T, error), T any](factories map[string]F, defaults map[string]json.RawMessage, p config.Plugin, kind string) (T, error) {
	var zero T
	f, ok := factories[p.Name]
	if !ok {
		return zero, fmt.Errorf("%s: unknown plugin %q (have %s)", kind, p.Name, plugin.Names(factories))
	}
	settings, err := plugin.Merge(defaults[p.Name], p.Settings)
	if err != nil {
		return zero, fmt.Errorf("%s %s: %w", kind, p.Name, err)
	}
	v, err := f(settings)
	if err != nil {
		return zero, fmt.Errorf("%s %s: %w", kind, p.Name, err)
	}
	return v, nil
}

// buildApprover builds the approval hook, or nil when there is none.
func buildApprover(cfg *config.Config) (plugin.Approver, error) {
	if cfg.Approval == nil {
		return nil, nil
	}
	a, err := httpreq.NewApprover(cfg.Approval.HTTP)
	if err != nil {
		return nil, fmt.Errorf("approval: %w", err)
	}
	return a, nil
}

// tlsFlags are the server's TLS settings. Mutual TLS is required unless insecure is set: anyone who
// can call this service can power machines off.
type tlsFlags struct {
	certFile, keyFile, clientCA string
	insecure                    bool
}

// credentials returns the server's transport credentials, or nil for plaintext with insecure set.
func (f tlsFlags) credentials() (credentials.TransportCredentials, error) {
	set := f.certFile != "" || f.keyFile != "" || f.clientCA != ""
	switch {
	case f.insecure && set:
		return nil, errors.New("--insecure can't be combined with TLS flags")
	case f.insecure:
		return nil, nil
	case f.certFile == "" || f.keyFile == "" || f.clientCA == "":
		return nil, errors.New("mutual TLS is required: set --tls-cert-file, --tls-key-file and --tls-client-ca-file (or --insecure, for testing only)")
	}
	cert, err := tls.LoadX509KeyPair(f.certFile, f.keyFile)
	if err != nil {
		return nil, fmt.Errorf("server certificate: %w", err)
	}
	pem, err := os.ReadFile(f.clientCA)
	if err != nil {
		return nil, fmt.Errorf("client CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("client CA: no PEM certificates in %s", f.clientCA)
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}), nil
}
