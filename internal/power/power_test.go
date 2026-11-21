package power

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	talosclient "github.com/siderolabs/talos/pkg/machinery/client"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/config"
)

func TestMagicPacket(t *testing.T) {
	mac, _ := net.ParseMAC("00:00:5e:00:53:01")
	p, err := MagicPacket(mac)
	if err != nil {
		t.Fatal(err)
	}
	if len(p) != 102 || !bytes.Equal(p[:6], bytes.Repeat([]byte{0xff}, 6)) {
		t.Fatalf("header %x", p[:6])
	}
	for i := range 16 {
		if !bytes.Equal(p[6+6*i:12+6*i], mac) {
			t.Fatalf("repetition %d: %x", i, p[6+6*i:12+6*i])
		}
	}
	long, _ := net.ParseMAC("00:00:00:00:fe:80:00:00:00:00:00:00:02:00:5e:10:00:00:00:01")
	if _, err := MagicPacket(long); err == nil {
		t.Fatal("accepted a 20-byte address")
	}
}

func TestWOLSendsPacket(t *testing.T) {
	lis, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	m := config.Machine{Node: "node-a", MAC: "00:00:5e:00:53:01", Broadcast: lis.LocalAddr().String()}
	if err := (WOL{}).PowerOn(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 200)
	_ = lis.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _, err := lis.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	mac, _ := net.ParseMAC(m.MAC)
	want, _ := MagicPacket(mac)
	if !bytes.Equal(buf[:n], want) {
		t.Fatalf("got %x", buf[:n])
	}
}

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
func writeCerts(t *testing.T) config.TalosCredentials {
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
	return config.TalosCredentials{
		CAFile:   write("ca.crt", certPEM),
		CertFile: write("tls.crt", certPEM),
		KeyFile:  write("tls.key", pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})),
	}
}

func TestTalosPowerOff(t *testing.T) {
	fake := &fakeTalos{}
	var endpoint string
	tal := Talos{Credentials: writeCerts(t), Connect: func(_ context.Context, ep string, c *tls.Config) (Shutdowner, error) {
		if len(c.Certificates) != 1 || c.RootCAs == nil {
			t.Fatalf("tls config without client cert or CA")
		}
		endpoint = ep
		return fake, nil
	}}
	m := config.Machine{Node: "node-a", TalosEndpoint: "192.0.2.11:50000"}
	if err := tal.PowerOff(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if fake.shutdowns != 1 || !fake.closed || endpoint != m.TalosEndpoint {
		t.Fatalf("shutdowns %d closed %v endpoint %q", fake.shutdowns, fake.closed, endpoint)
	}
	fake.err = errors.New("permission denied")
	if err := tal.PowerOff(context.Background(), m); err == nil {
		t.Fatal("shutdown error not returned")
	}
}

func TestTalosMissingCerts(t *testing.T) {
	tal := Talos{Credentials: config.TalosCredentials{CAFile: "/nonexistent/ca.crt"}}
	if err := tal.PowerOff(context.Background(), config.Machine{}); err == nil {
		t.Fatal("no error for missing certificates")
	}
}

func TestSSHNotImplemented(t *testing.T) {
	if err := (SSH{}).PowerOff(context.Background(), config.Machine{}); !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("got %v", err)
	}
}

func TestMethodsDispatch(t *testing.T) {
	ms := Methods{On: map[string]On{}, Off: map[string]Off{"ssh": SSH{}}}
	if err := ms.PowerOn(context.Background(), config.Machine{PowerOn: "wol"}); err == nil {
		t.Fatal("missing method not reported")
	}
	if err := ms.PowerOff(context.Background(), config.Machine{PowerOff: "ssh"}); !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("got %v", err)
	}
}
