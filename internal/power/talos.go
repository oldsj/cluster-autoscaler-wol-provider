package power

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	talosclient "github.com/siderolabs/talos/pkg/machinery/client"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/config"
)

// Shutdowner is the part of the Talos client this uses; tests replace it.
type Shutdowner interface {
	Shutdown(ctx context.Context, opts ...talosclient.ShutdownOption) error
	Close() error
}

// Talos powers a Talos Linux machine off through its API, with a client certificate that has a
// role allowed to call Shutdown (os:operator or os:admin).
type Talos struct {
	Credentials config.TalosCredentials
	// Connect opens a client to one endpoint; nil means the Talos machinery client. Tests replace it.
	Connect func(ctx context.Context, endpoint string, tlsConfig *tls.Config) (Shutdowner, error)
}

// TLSConfig reads the certificate files. They're read on every power-off so a rotated Secret is
// picked up without a restart.
func (t Talos) TLSConfig() (*tls.Config, error) {
	ca, err := os.ReadFile(t.Credentials.CAFile)
	if err != nil {
		return nil, fmt.Errorf("talos: ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("talos: ca: no PEM certificates in %s", t.Credentials.CAFile)
	}
	cert, err := tls.LoadX509KeyPair(t.Credentials.CertFile, t.Credentials.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("talos: client certificate: %w", err)
	}
	return &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}, nil
}

// PowerOff asks the machine at m.TalosEndpoint to shut down gracefully: Talos stops its services,
// including the kubelet, then powers off. Cluster Autoscaler has drained the node already.
func (t Talos) PowerOff(ctx context.Context, m config.Machine) error {
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
	c, err := connect(ctx, m.TalosEndpoint, tlsConfig)
	if err != nil {
		return fmt.Errorf("talos: connect %s: %w", m.TalosEndpoint, err)
	}
	defer c.Close()
	if err := c.Shutdown(ctx, talosclient.WithShutdownForce(false)); err != nil {
		return fmt.Errorf("talos: shutdown %s: %w", m.Node, err)
	}
	return nil
}
