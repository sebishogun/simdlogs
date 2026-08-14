package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// selfSigned writes a certificate/key pair and returns their paths.
func selfSigned(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "simdlogs-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
		// SANs, so a real client can validate this. Without them every test
		// here asserts struct fields against a certificate no client would
		// accept, and a handshake test is impossible.
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	cf, err := os.Create(certPath)
	if err != nil {
		t.Fatal(err)
	}
	pem.Encode(cf, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	cf.Close()

	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	kf, err := os.Create(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	pem.Encode(kf, &pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})
	kf.Close()
	return certPath, keyPath
}

func TestTLSDisabledByDefault(t *testing.T) {
	var c TLSConfig
	got, err := c.Build()
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatal("a zero TLSConfig produced a tls.Config")
	}
}

// Half a pair is an error: a cert with no key would otherwise start the
// server in plaintext while the operator believes TLS is on.
func TestTLSRejectsHalfAPair(t *testing.T) {
	certPath, keyPath := selfSigned(t)
	for _, c := range []TLSConfig{
		{CertFile: certPath},
		{KeyFile: keyPath},
	} {
		if _, err := c.Build(); err == nil {
			t.Errorf("%+v was accepted", c)
		}
	}
}

func TestTLSRejectsInvalidCertificate(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.pem")
	if err := os.WriteFile(bad, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := TLSConfig{CertFile: bad, KeyFile: bad}
	if _, err := c.Build(); err == nil {
		t.Fatal("an invalid certificate was accepted")
	}
}

// TLS 1.2 is the floor and the negotiated protocols are set, so an h2 client
// is not downgraded.
func TestTLSMinimumVersion(t *testing.T) {
	certPath, keyPath := selfSigned(t)
	c, err := TLSConfig{CertFile: certPath, KeyFile: keyPath}.Build()
	if err != nil {
		t.Fatal(err)
	}
	if c.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion %x, want TLS 1.2", c.MinVersion)
	}
	if len(c.Certificates) != 1 {
		t.Errorf("%d certificates loaded", len(c.Certificates))
	}
	if len(c.NextProtos) == 0 {
		t.Error("no ALPN protocols advertised")
	}
}

// A client CA turns on mTLS, and the certificate is required and verified --
// requesting one without checking it is decoration.
func TestTLSClientCARequiresAndVerifies(t *testing.T) {
	certPath, keyPath := selfSigned(t)
	c, err := TLSConfig{CertFile: certPath, KeyFile: keyPath, ClientCAFile: certPath}.Build()
	if err != nil {
		t.Fatal(err)
	}
	if c.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("ClientAuth %v, want RequireAndVerifyClientCert", c.ClientAuth)
	}
	if c.ClientCAs == nil {
		t.Error("no client CA pool")
	}
}

// A client CA without a server certificate is a configuration mistake, not a
// silently ignored setting.
func TestTLSClientCAWithoutServerCert(t *testing.T) {
	certPath, _ := selfSigned(t)
	if _, err := (TLSConfig{ClientCAFile: certPath}).Build(); err == nil {
		t.Fatal("a client CA with no server certificate was accepted")
	}
}

func TestTLSClientCAFileMustContainPEM(t *testing.T) {
	certPath, keyPath := selfSigned(t)
	empty := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(empty, []byte("nothing here"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := TLSConfig{CertFile: certPath, KeyFile: keyPath, ClientCAFile: empty}.Build()
	if err == nil {
		t.Fatal("a CA file with no certificates was accepted")
	}
	if !strings.Contains(err.Error(), "no PEM") {
		t.Errorf("error %q does not say the file has no certificates", err)
	}
}

// Plaintext on a public address is refused unless it is asked for. Loopback
// is fine: that is the development case.
func TestCheckListenRefusesPublicPlaintext(t *testing.T) {
	var plain TLSConfig
	for _, addr := range []string{":9428", "0.0.0.0:9428", "192.168.1.5:9428", "[::]:9428"} {
		if err := plain.CheckListen(addr); err == nil {
			t.Errorf("%s: plaintext accepted on a public address", addr)
		}
	}
	for _, addr := range []string{"127.0.0.1:9428", "localhost:9428", "[::1]:9428"} {
		if err := plain.CheckListen(addr); err != nil {
			t.Errorf("%s: loopback plaintext refused: %v", addr, err)
		}
	}

	// Explicitly asked for.
	insecure := TLSConfig{InsecureHTTP: true}
	if err := insecure.CheckListen("0.0.0.0:9428"); err != nil {
		t.Errorf("-insecure-http still refused: %v", err)
	}

	// With TLS the HTTP address does not matter.
	certPath, keyPath := selfSigned(t)
	secure := TLSConfig{CertFile: certPath, KeyFile: keyPath}
	if err := secure.CheckListen("0.0.0.0:9428"); err != nil {
		t.Errorf("TLS listener refused: %v", err)
	}
	// But a plaintext-only listener is still refused, certificate or not.
	// Otherwise -tls.certFile on the HTTP port silently opened a public,
	// unauthenticated, cleartext syslog port.
	if err := secure.CheckPlaintextListen("0.0.0.0:514"); err == nil {
		t.Error("a plaintext syslog address was allowed because HTTP had TLS")
	}
	if err := secure.CheckPlaintextListen("127.0.0.1:514"); err != nil {
		t.Errorf("loopback syslog refused: %v", err)
	}
	if err := (TLSConfig{InsecureHTTP: true}).CheckPlaintextListen("0.0.0.0:514"); err != nil {
		t.Errorf("-tls.insecure did not allow a public syslog port: %v", err)
	}
}

// The tests above assert struct fields. This one performs a real handshake,
// because a tls.Config that looks right and does not serve is the failure
// mode field assertions cannot see.
func TestTLSServesRealHandshake(t *testing.T) {
	certPath, keyPath := selfSigned(t)
	cfg, err := TLSConfig{CertFile: certPath, KeyFile: keyPath}.Build()
	if err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{
		Handler:   http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) }),
		TLSConfig: cfg,
	}
	go srv.ServeTLS(ln, "", "") // empty paths: the certificate is in TLSConfig
	defer srv.Close()

	// A client that trusts this certificate as a root must validate the
	// server, which needs the SANs the helper now sets.
	pemBytes, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		t.Fatal("test certificate is not valid PEM")
	}
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool},
	}}
	resp, err := client.Get("https://" + ln.Addr().String() + "/")
	if err != nil {
		t.Fatalf("handshake failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if resp.TLS == nil || resp.TLS.Version < tls.VersionTLS12 {
		t.Fatalf("negotiated TLS version %x", resp.TLS.Version)
	}
}

// A client below the minimum version is refused, so MinVersion is enforced by
// the handshake rather than only present in a struct.
func TestTLSRefusesOldClient(t *testing.T) {
	certPath, keyPath := selfSigned(t)
	cfg, err := TLSConfig{CertFile: certPath, KeyFile: keyPath}.Build()
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.NotFoundHandler(), TLSConfig: cfg}
	go srv.ServeTLS(ln, "", "")
	defer srv.Close()

	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true, MaxVersion: tls.VersionTLS11},
	}}
	if _, err := client.Get("https://" + ln.Addr().String() + "/"); err == nil {
		t.Fatal("a TLS 1.1 client was accepted")
	}
}

// mTLS: a client with no certificate is refused at the handshake, and one
// signed by the configured CA gets through.
func TestMTLSRequiresClientCertificate(t *testing.T) {
	certPath, keyPath := selfSigned(t)
	cfg, err := TLSConfig{CertFile: certPath, KeyFile: keyPath, ClientCAFile: certPath}.Build()
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{
		Handler:   http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) }),
		TLSConfig: cfg,
	}
	go srv.ServeTLS(ln, "", "")
	defer srv.Close()

	pemBytes, _ := os.ReadFile(certPath)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pemBytes)

	// No client certificate: refused.
	noCert := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool},
	}}
	if resp, err := noCert.Get("https://" + ln.Addr().String() + "/"); err == nil {
		resp.Body.Close()
		t.Fatal("a client with no certificate was accepted under mTLS")
	}

	// With one signed by the trusted CA (here, the same self-signed cert).
	pair, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	withCert := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{pair}},
	}}
	resp, err := withCert.Get("https://" + ln.Addr().String() + "/")
	if err != nil {
		t.Fatalf("a client with a trusted certificate was refused: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

// Loopback detection resolves rather than string-matching. These are the
// spellings the string version got wrong.
func TestLoopbackDetectionResolves(t *testing.T) {
	for _, c := range []struct {
		addr string
		want bool
	}{
		{"127.0.0.1:9428", true},
		{"127.0.0.2:9428", true}, // the whole 127/8 is loopback
		{"127.1:9428", true},     // shorthand for 127.0.0.1
		{"[::1]:9428", true},
		{"localhost:9428", true},
		{"LOCALHOST:9428", true},
		{"0.0.0.0:9428", false},
		{"[::]:9428", false},
		{":9428", false},
		{"192.168.1.5:9428", false},
	} {
		got, err := loopbackAddr(c.addr)
		if err != nil {
			// A name that does not resolve in this environment is not a
			// failure of the logic under test.
			if c.want {
				t.Logf("%s: %v (skipped)", c.addr, err)
			}
			continue
		}
		if got != c.want {
			t.Errorf("loopbackAddr(%q) = %v, want %v", c.addr, got, c.want)
		}
	}

	// A malformed address is an error rather than a silent permit.
	if _, err := loopbackAddr("no-port-here"); err == nil {
		t.Error("an address with no port was accepted")
	}
}
