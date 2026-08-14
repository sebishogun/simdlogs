package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strings"
)

// TLSConfig is the listener's transport security.
type TLSConfig struct {
	CertFile string
	KeyFile  string
	// ClientCAFile turns on mTLS: a client must present a certificate signed
	// by one of these CAs.
	ClientCAFile string
	// InsecureHTTP allows serving plaintext on a non-loopback address. It is
	// explicit because the default has to be the safe one: a server that
	// binds 0.0.0.0 without TLS is shipping tenant log data in clear text,
	// and that should take a deliberate flag rather than being what happens
	// when the operator forgets.
	InsecureHTTP bool
}

// Enabled reports whether TLS was asked for at all. It is true for half a
// pair on purpose: Build then reports precisely which file is missing,
// instead of CheckListen reporting the unrelated "plaintext refused".
func (t TLSConfig) Enabled() bool { return t.CertFile != "" || t.KeyFile != "" }

// Build validates the configuration and returns the tls.Config, or nil when
// TLS is off.
//
// TLS 1.2 is the floor and 1.3 is preferred. Below 1.2 the available cipher
// suites are ones no current deployment should be negotiating.
func (t TLSConfig) Build() (*tls.Config, error) {
	if !t.Enabled() {
		if t.ClientCAFile != "" {
			return nil, fmt.Errorf("config: -tls.clientCAFile needs -tls.certFile and -tls.keyFile")
		}
		return nil, nil
	}
	if t.CertFile == "" || t.KeyFile == "" {
		return nil, fmt.Errorf("config: TLS needs both -tls.certFile and -tls.keyFile")
	}
	cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("config: loading the certificate: %w", err)
	}
	c := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h2", "http/1.1"},
	}
	if t.ClientCAFile != "" {
		pem, err := os.ReadFile(t.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("config: reading the client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("config: %s contains no PEM certificates", t.ClientCAFile)
		}
		c.ClientCAs = pool
		// Require and verify: a client certificate that is merely requested
		// and not checked is decoration.
		c.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return c, nil
}

// CheckListen reports whether serving addr on the HTTP listener is allowed.
// With TLS configured the listener is encrypted, so any address is fine.
func (t TLSConfig) CheckListen(addr string) error {
	if t.Enabled() {
		return nil
	}
	return t.CheckPlaintextListen(addr)
}

// CheckPlaintextListen is CheckListen for a listener that is plaintext no
// matter what TLS is configured -- the native syslog ports.
//
// They need their own check because CheckListen returns early once a
// certificate is configured: -tls.certFile on the HTTP listener silently
// exempted the syslog port, which then bound every interface in the clear
// while the docs promised the opposite. Syslog here is also unauthenticated
// and writes to the default tenant, so it is the larger hole of the two.
func (t TLSConfig) CheckPlaintextListen(addr string) error {
	if t.InsecureHTTP {
		return nil
	}
	loop, err := loopbackAddr(addr)
	if err != nil {
		return fmt.Errorf("config: cannot determine whether %s is loopback: %w", addr, err)
	}
	if loop {
		return nil
	}
	return fmt.Errorf("config: refusing to serve plaintext on %s; "+
		"configure -tls.certFile/-tls.keyFile, put a terminating proxy in front "+
		"and bind loopback, or pass -insecure-http to accept it", addr)
}

// loopbackAddr reports whether addr binds only loopback interfaces.
//
// It resolves rather than matching strings. String matching got four
// spellings of loopback wrong -- "127.1", "LOCALHOST", "localhost." and
// "[::1%lo]" were all refused -- and, worse, trusted the name "localhost"
// without resolving it, so a hosts file mapping localhost to a routable
// address produced plaintext on a public interface with the check green.
//
// An empty or unspecified host (":9428", "0.0.0.0", "[::]") binds every
// interface and is never loopback. Everything else is resolved, and every
// resulting address must be loopback: a name with one loopback and one public
// address is a public bind.
func loopbackAddr(addr string) (bool, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false, fmt.Errorf("%q is not host:port: %w", addr, err)
	}
	if host == "" {
		return false, nil // every interface
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsUnspecified() {
			return false, nil
		}
		return ip.IsLoopback(), nil
	}
	// A zone ("::1%lo") parses only through ResolveIPAddr.
	if i := strings.IndexByte(host, '%'); i >= 0 {
		if ipa, err := net.ResolveIPAddr("ip", host); err == nil {
			return ipa.IP.IsLoopback(), nil
		}
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return false, fmt.Errorf("resolving %q: %w", host, err)
	}
	if len(ips) == 0 {
		return false, fmt.Errorf("%q resolved to no addresses", host)
	}
	for _, ip := range ips {
		if !ip.IsLoopback() {
			return false, nil
		}
	}
	return true, nil
}
