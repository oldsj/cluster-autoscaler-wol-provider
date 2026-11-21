package talos

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	talosclient "github.com/siderolabs/talos/pkg/machinery/client"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/plugin"
)

type fakeTalos struct {
	shutdowns int
	err       error
	closed    bool
}

func (f *fakeTalos) Shutdown(context.Context, ...talosclient.ShutdownOption) error {
	f.shutdowns++
	return f.err
}

func (f *fakeTalos) Close() error {
	f.closed = true
	return nil
}

// writeCerts writes a throwaway CA and client certificate.
func writeCerts(t *testing.T) Settings {
	t.Helper()
	dir := t.TempDir()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test", Organization: []string{"os:operator"}},
		NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	write := func(name string, b []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	return Settings{
		Endpoint: "192.0.2.11:50000",
		CAFile:   write("ca.crt", certPEM),
		CertFile: write("tls.crt", certPEM),
		KeyFile:  write("tls.key", pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})),
	}
}

func TestTalosPowerOff(t *testing.T) {
	fake := &fakeTalos{}
	var endpoint string
	tal := &Talos{Settings: writeCerts(t), Connect: func(_ context.Context, ep string, c *tls.Config) (Shutdowner, error) {
		if len(c.Certificates) != 1 || c.RootCAs == nil {
			t.Fatalf("tls config without client cert or CA")
		}
		endpoint = ep
		return fake, nil
	}}
	m := plugin.Target{Name: "node-a"}
	if err := tal.PowerOff(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if fake.shutdowns != 1 || !fake.closed || endpoint != tal.Settings.Endpoint {
		t.Fatalf("shutdowns %d closed %v endpoint %q", fake.shutdowns, fake.closed, endpoint)
	}
	fake.err = errors.New("permission denied")
	if err := tal.PowerOff(context.Background(), m); err == nil {
		t.Fatal("shutdown error not returned")
	}
}

func TestTalosMissingCerts(t *testing.T) {
	tal := &Talos{Settings: Settings{Endpoint: "192.0.2.11:50000", CAFile: "/nonexistent/ca.crt"}}
	if err := tal.PowerOff(context.Background(), plugin.Target{}); err == nil {
		t.Fatal("no error for missing certificates")
	}
}

func TestSettings(t *testing.T) {
	for _, bad := range []string{`{}`, `{"endpoint":"192.0.2.11"}`, `{"endpoint":"192.0.2.11:50000"}`} {
		if _, err := New(json.RawMessage(bad)); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
	if _, err := New(json.RawMessage(`{"endpoint":"192.0.2.11:50000","caFile":"a","certFile":"b","keyFile":"c"}`)); err != nil {
		t.Fatal(err)
	}
}
