package api

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/config"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// budgetedSyslogServer starts a syslog listener whose default tenant is over
// its storage budget before the first byte arrives.
//
// The disk is full BEFORE the server is built, not flipped mid-test: the
// free-space sample is cached for a couple of seconds so a burst of writes
// does not become a burst of statfs calls, and a test that flipped it and
// expected the very next frame to see it would be asserting the cache away.
func budgetedSyslogServer(t *testing.T, cfg SyslogConfig) (*Server, string, *net.UDPAddr) {
	t.Helper()
	t.Cleanup(storage.SetDiskUsageForTest(func(string) (storage.DiskUsage, error) {
		return storage.DiskUsage{Total: 1 << 30, Free: 0}, nil
	}))
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.def.store.SetQuota(storage.QuotaConfig{
		ReserveWarnBytes: 1000, ReserveRejectBytes: 100,
	}); err != nil {
		t.Fatal(err)
	}
	udp, tcp, err := srv.ListenSyslogConfig("127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	return srv, tcp.Addr().String(), udp.LocalAddr().(*net.UDPAddr)
}

// The native transport is subject to the storage budget.
//
// It was not. checkStorage covers the HTTP mux and these listeners take bytes
// off a socket with no middleware anywhere near them, so with the filesystem
// past the reject reserve and every HTTP insert answering 507, one TCP frame
// and one UDP datagram each still landed a row and the rejection counters
// stayed at zero -- the exact one-side-only shape checkStorage's own comment
// argued against, on the one path it did not cover.
func TestSyslogRefusesWritesPastTheReserve(t *testing.T) {
	for _, tc := range []struct {
		name string
		send func(t *testing.T, tcpAddr string, udpAddr *net.UDPAddr)
	}{
		{"tcp", func(t *testing.T, tcpAddr string, _ *net.UDPAddr) {
			c, err := net.Dial("tcp", tcpAddr)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if _, err := fmt.Fprint(c, msg5424+"\n"); err != nil {
				t.Fatal(err)
			}
			// The listener has to have read the frame before the assertion.
			// Closing the write side ends its scan, which is the signal the
			// message was consumed rather than still in a socket buffer.
			c.(*net.TCPConn).CloseWrite()
			io := make([]byte, 1)
			c.SetReadDeadline(time.Now().Add(2 * time.Second))
			c.Read(io) // returns at EOF once the listener closes its side
		}},
		{"udp", func(t *testing.T, _ string, udpAddr *net.UDPAddr) {
			c, err := net.DialUDP("udp", nil, udpAddr)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if _, err := c.Write([]byte(msg5424)); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			disk0, _ := storage.RejectedWrites()
			srv, tcpAddr, udpAddr := budgetedSyslogServer(t, SyslogConfig{FlushLines: 1})
			tc.send(t, tcpAddr, udpAddr)

			// UDP is fire-and-forget, so give the listener a moment to have
			// dropped it. A row that was going to land has landed by then.
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				if d, _ := storage.RejectedWrites(); d > disk0 {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}

			if n := rowCountOf(t, srv); n != 0 {
				t.Errorf("%d rows landed past the reject reserve", n)
			}
			if d, _ := storage.RejectedWrites(); d == disk0 {
				t.Errorf("the refusal was not counted (disk rejections still %d)", disk0)
			}
		})
	}
}

// With room on the disk the same message lands, so the guard is a budget and
// not a listener that stopped working.
func TestSyslogStillWritesWithRoomOnTheDisk(t *testing.T) {
	t.Cleanup(storage.SetDiskUsageForTest(func(string) (storage.DiskUsage, error) {
		return storage.DiskUsage{Total: 1 << 30, Free: 1 << 29}, nil
	}))
	srv, addr := syslogTestServer(t, SyslogConfig{FlushLines: 1})
	if err := srv.def.store.SetQuota(storage.QuotaConfig{
		ReserveWarnBytes: 1000, ReserveRejectBytes: 100,
	}); err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprint(c, msg5424+"\n"); err != nil {
		t.Fatal(err)
	}
	c.(*net.TCPConn).CloseWrite()
	buf := make([]byte, 1)
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	c.Read(buf)
	c.Close()
	if n := rowCountOf(t, srv); n != 1 {
		t.Fatalf("%d rows, want 1", n)
	}
}

// A filesystem that refuses the store open answers 507, not 400.
//
// withTenant opens the store before checkStorage can run, so this failure
// never reached the budget middleware at all: authStatus's default fired and a
// server-side storage condition was reported with a client-error code -- and
// the server's absolute path in the body. An agent treats 400 as permanent and
// drops a batch it could have re-sent.
func TestAnUnwritableTenantDirectoryIs507(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores the permission bits this test sets")
	}
	dir := t.TempDir()
	c := config.Config{Dir: dir, Limits: config.DefaultLimits()}
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Read-only AFTER construction, so the server starts and the failure is a
	// NEW tenant's directory that cannot be created -- which is the case that
	// reaches the resolver at request time. Chmod before construction only
	// tests that the process refuses to start, which is a different thing and
	// the right behaviour for it.
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o755) })

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/insert/jsonline",
		strings.NewReader(`{"_time":1,"_msg":"x"}`+"\n"))
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("AccountID", "77")
	req.Header.Set("ProjectID", "0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusInsufficientStorage {
		t.Fatalf("an unwritable tenant directory returned %d (%s), want 507",
			resp.StatusCode, b)
	}
}
