package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/config"
)

func writePair(t *testing.T, dir string) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kder, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile = filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kder}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

// Review #2: mutual TLS is required unless --insecure, and no partial TLS setup is accepted.
func TestTLSFlags(t *testing.T) {
	dir := t.TempDir()
	cert, key := writePair(t, dir)
	for name, tc := range map[string]struct {
		f       tlsFlags
		wantErr string
		creds   bool
	}{
		"nothing":          {tlsFlags{}, "mutual TLS is required", false},
		"client CA alone":  {tlsFlags{clientCA: cert}, "mutual TLS is required", false},
		"no client CA":     {tlsFlags{certFile: cert, keyFile: key}, "mutual TLS is required", false},
		"insecure and tls": {tlsFlags{certFile: cert, keyFile: key, insecure: true}, "can't be combined", false},
		"bad CA":           {tlsFlags{certFile: cert, keyFile: key, clientCA: key}, "no PEM certificates", false},
		"insecure":         {tlsFlags{insecure: true}, "", false},
		"mtls":             {tlsFlags{certFile: cert, keyFile: key, clientCA: cert}, "", true},
	} {
		t.Run(name, func(t *testing.T) {
			c, err := tc.f.credentials()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("got %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || (c != nil) != tc.creds {
				t.Fatalf("creds %v err %v", c, err)
			}
		})
	}
}

func TestBuildPlugins(t *testing.T) {
	cfg, err := config.Parse([]byte(`
defaults:
  powerOff:
    ssh: {user: ops, keyFile: /k/id, knownHostsFile: /k/known_hosts}
  powerOn:
    wol: {broadcast: "192.0.2.255:9"}
nodeGroups:
  - id: g
    maxSize: 2
    machines:
      - node: a
        powerOn: {wol: {mac: "00:00:5e:00:53:01"}}
        powerOff: {ssh: {host: 192.0.2.11}}
        probe: {tcp: {address: "192.0.2.11:22"}}
      - node: b
        powerOn: {http: {url: "http://192.0.2.50/on"}}
        powerOff: {talos: {endpoint: "192.0.2.12:50000", caFile: /t/ca, certFile: /t/crt, keyFile: /t/key}}
`))
	if err != nil {
		t.Fatal(err)
	}
	pl, err := buildPlugins(cfg, registry)
	if err != nil {
		t.Fatal(err)
	}
	if pl["a"].On == nil || pl["a"].Off == nil || pl["a"].Probe == nil || pl["b"].Probe != nil {
		t.Fatalf("plugins %+v", pl)
	}
}

func TestBuildPluginsErrors(t *testing.T) {
	for name, tc := range map[string]struct{ yaml, want string }{
		"unknown":       {`{node: a, powerOn: {carrier-pigeon: {}}, powerOff: {ssh: {host: h, user: u, keyFile: k, knownHostsFile: f}}}`, `unknown plugin "carrier-pigeon"`},
		"bad setting":   {`{node: a, powerOn: {wol: {mac: "nope"}}, powerOff: {ssh: {host: h, user: u, keyFile: k, knownHostsFile: f}}}`, "mac"},
		"missing field": {`{node: a, powerOn: {wol: {mac: "00:00:5e:00:53:01"}}, powerOff: {ssh: {}}}`, "powerOff ssh"},
		"on-only off":   {`{node: a, powerOn: {wol: {mac: "00:00:5e:00:53:01"}}, powerOff: {wol: {}}}`, "unknown plugin"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := config.Parse([]byte("nodeGroups:\n  - {id: g, maxSize: 1, machines: [" + tc.yaml + "]}\n"))
			if err != nil {
				t.Fatal(err)
			}
			_, err = buildPlugins(cfg, registry)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
}

func TestExampleConfigBuilds(t *testing.T) {
	cfg, err := config.Load("../../examples/config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := buildPlugins(cfg, registry); err != nil {
		t.Fatal(err)
	}
	if _, err := buildApprover(cfg); err != nil {
		t.Fatal(err)
	}
}
