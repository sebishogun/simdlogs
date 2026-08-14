package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/config"
)

// Close must wait for an in-flight request rather than unmapping the store
// under it. This is the same hazard the reference counting fixed inside the
// store, at the process level.
func TestCloseWaitsForInFlightRequest(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	mux := http.NewServeMux()
	// A path the tenant allowlist knows: withTenant only resolves a tenant
	// for routes that touch tenant data, so a made-up path would never mark
	// one busy and this test would assert nothing.
	const slowPath = "/select/logsql/query"
	mux.HandleFunc(slowPath, func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusOK)
	})
	ts := httptest.NewServer(srv.withTenant(mux))
	defer ts.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		resp, err := http.Get(ts.URL + slowPath)
		if err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		close(release)
		wg.Wait()
		t.Fatal("the request never reached the handler; withTenant refused it")
	}

	// The tenant the request is using must be marked busy, so eviction and
	// shutdown can both see it.
	srv.mu.Lock()
	var busy bool
	for _, tn := range srv.tenants {
		if tn.inFlight.Load() > 0 {
			busy = true
		}
	}
	srv.mu.Unlock()
	if !busy {
		t.Fatal("an in-flight request did not mark its tenant busy")
	}

	close(release)
	wg.Wait()
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
}

// A live tail must not be cut by a server-wide write deadline. WriteTimeout
// is absolute rather than idle-based, so setting it would kill every tail at
// the timeout regardless of activity.
func TestLiveTailIsNotCutByWriteDeadline(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/select/logsql/tail?query=*", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("tail request failed immediately: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("tail -> %d", resp.StatusCode)
	}

	// Ingest while the tail is open; the connection must still be alive.
	go func() {
		time.Sleep(200 * time.Millisecond)
		http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson",
			strings.NewReader(fmt.Sprintf(`{"_time":%d,"level":"info"}`+"\n", time.Now().UnixNano())))
	}()

	buf := make([]byte, 512)
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			return // streamed a row: the tail is working
		}
		if err != nil {
			if ctx.Err() != nil {
				return // our own context, not a server deadline
			}
			t.Fatalf("tail stream closed by the server: %v", err)
		}
	}
}

// The syslog listeners are returned so a caller can close them, and closing
// them actually stops the listener. main.go discarded both, so they accepted
// data all the way through shutdown into writers that were being flushed.
func TestSyslogListenersAreClosable(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	udp, tcp, err := srv.ListenSyslog("127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot bind a syslog listener here: %v", err)
	}
	if udp == nil || tcp == nil {
		t.Fatalf("ListenSyslog returned udp=%v tcp=%v; both are needed to shut down", udp, tcp)
	}
	tcpAddr := tcp.Addr().String()

	// It accepts before closing.
	conn, err := net.DialTimeout("tcp", tcpAddr, time.Second)
	if err != nil {
		t.Fatalf("listener did not accept: %v", err)
	}
	conn.Close()

	if err := tcp.Close(); err != nil {
		t.Fatalf("closing the TCP listener: %v", err)
	}
	if err := udp.Close(); err != nil {
		t.Fatalf("closing the UDP listener: %v", err)
	}

	// After closing it must refuse.
	if c2, err := net.DialTimeout("tcp", tcpAddr, 300*time.Millisecond); err == nil {
		c2.Close()
		t.Fatal("the TCP syslog listener still accepts after Close")
	}
}

// Close is safe to call twice: a shutdown path that runs it from both a
// signal handler and a defer must not panic on a closed channel.
func TestCloseIsIdempotent(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// Close must not block when the caller never closed the syslog listeners
// itself. It waits on the accept goroutines, so it has to close what they are
// blocked on -- otherwise Close hung forever, and its doc states no such
// precondition.
func TestCloseWithOpenSyslogListeners(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := srv.ListenSyslog("127.0.0.1:0"); err != nil {
		t.Skipf("cannot bind a syslog listener here: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- srv.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close blocked with syslog listeners still open")
	}
}

// A live syslog connection at shutdown must not panic the process. The
// per-connection goroutine calls Flush after every line, and once Close has
// shut the writer that used to send on a closed channel -- in a bare
// goroutine, outside recoverPanic.
func TestShutdownWithLiveSyslogConnection(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	_, tcp, err := srv.ListenSyslog("127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot bind a syslog listener here: %v", err)
	}

	conn, err := net.DialTimeout("tcp", tcp.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	// Keep writing while the server shuts down.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				conn.Close()
				return
			default:
				conn.Write([]byte("<134>1 2026-08-14T00:00:00Z host app - - - hello\n"))
				time.Sleep(time.Millisecond)
			}
		}
	}()

	time.Sleep(50 * time.Millisecond) // let some lines land
	done := make(chan error, 1)
	go func() { done <- srv.Close() }()
	select {
	case err := <-done:
		close(stop)
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		close(stop)
		t.Fatal("Close blocked with a live syslog connection")
	}
}
