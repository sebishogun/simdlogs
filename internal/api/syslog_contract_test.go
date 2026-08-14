package api

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// The native syslog transport contract.
//
// RFC 6587 defines TWO framings for syslog over TCP and a receiver is expected
// to handle both. Only the newline form was read, and rsyslog's omfwd and
// syslog-ng's syslog() driver both send OCTET-COUNTED by default -- so the
// default configuration of the two most common forwarders stored the byte
// count and the space as part of the message, and split any message containing
// a newline across several rows.
//
// The listener also had no read deadline (an opened-and-abandoned connection
// held a goroutine and its buffer forever), no connection limit, no reported
// frame limit (bufio.Scanner's Err was never checked, so an oversized line
// ended the connection silently), and no TLS. It flushed once PER LINE, an
// fsync per syslog message.

// syslogTestServer starts a server with a syslog listener on an ephemeral port
// and returns the TCP address.
func syslogTestServer(t *testing.T, cfg SyslogConfig) (*Server, string) {
	t.Helper()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	udp, tcp, err := srv.ListenSyslogConfig("127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	_ = udp
	return srv, tcp.Addr().String()
}

// rowCountOf returns how many rows the default tenant holds.
func rowCountOf(t *testing.T, srv *Server) int {
	t.Helper()
	return srv.defaultTenantRows()
}

const (
	msg5424 = `<13>1 2024-05-01T00:00:00Z myhost myapp 1234 ID47 - the message body`
	msg3164 = `<34>May  1 00:00:00 myhost myapp[1234]: the message body`
)

// All four framings from the plan's step 1, over one connection each.
func TestSyslogFramings(t *testing.T) {
	for _, tc := range []struct {
		name  string
		wire  string
		lines int
	}{
		{"RFC5424 newline framed", msg5424 + "\n", 1},
		{"RFC3164 newline framed", msg3164 + "\n", 1},
		{"RFC6587 octet counted 5424",
			fmt.Sprintf("%d %s", len(msg5424), msg5424), 1},
		{"RFC6587 octet counted 3164",
			fmt.Sprintf("%d %s", len(msg3164), msg3164), 1},
		{"several octet counted frames",
			fmt.Sprintf("%d %s%d %s", len(msg5424), msg5424, len(msg3164), msg3164), 2},
		{"mixed framings on one connection",
			fmt.Sprintf("%s\n%d %s", msg5424, len(msg3164), msg3164), 2},
		{"newline frames with CRLF", msg5424 + "\r\n" + msg3164 + "\r\n", 2},
		{"final line with no terminator", msg5424 + "\n" + msg3164, 2},
		// The case newline framing gets WRONG and octet counting gets right: a
		// message whose body contains a newline.
		{"octet counted body containing a newline",
			fmt.Sprintf("%d %s", len(msg5424)+len("\nmore"), msg5424+"\nmore"), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, addr := syslogTestServer(t, SyslogConfig{FlushLines: 1})
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.WriteString(conn, tc.wire); err != nil {
				t.Fatal(err)
			}
			conn.Close()

			waitRows(t, srv, tc.lines)
			if got := rowCountOf(t, srv); got != tc.lines {
				t.Errorf("stored %d rows, want %d", got, tc.lines)
			}
		})
	}
}

// waitRows polls until the store holds n rows or the deadline passes. Polling,
// not sleeping: a fixed sleep is either flaky or slow, and this package has
// already paid for one.
func waitRows(t *testing.T, srv *Server, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if rowCountOf(t, srv) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// An oversized frame is REPORTED, not swallowed. bufio.Scanner's Err was never
// checked, so the connection ended silently and the sender saw a healthy close.
func TestSyslogOversizedFrameIsRejected(t *testing.T) {
	srv, addr := syslogTestServer(t, SyslogConfig{MaxFrameBytes: 512, FlushLines: 1})
	before := srv.errorCount()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	// An octet count larger than the limit must be refused on the COUNT,
	// before any of the payload is read into memory.
	fmt.Fprintf(conn, "%d %s", 1<<20, strings.Repeat("x", 100))
	conn.Close()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && srv.errorCount() == before {
		time.Sleep(5 * time.Millisecond)
	}
	if srv.errorCount() == before {
		t.Error("an oversized frame was not counted as an error")
	}
	if got := rowCountOf(t, srv); got != 0 {
		t.Errorf("stored %d rows from an oversized frame", got)
	}
}

// A digit-run with no space is a sender writing an unbounded count. It must be
// refused before the accumulator overflows into a small positive number that
// would then pass the size check.
func TestSyslogAbsurdOctetCountIsRefused(t *testing.T) {
	srv, addr := syslogTestServer(t, SyslogConfig{MaxFrameBytes: 1024, FlushLines: 1})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	// 30 digits: overflows int64 many times over.
	fmt.Fprintf(conn, "%s ", strings.Repeat("9", 30))
	conn.Close()

	time.Sleep(100 * time.Millisecond)
	if got := rowCountOf(t, srv); got != 0 {
		t.Errorf("stored %d rows from an absurd octet count", got)
	}
}

// A truncated octet-counted frame -- the sender promised n bytes and sent
// fewer -- must not be stored as a short message.
func TestSyslogTruncatedFrameIsNotStored(t *testing.T) {
	srv, addr := syslogTestServer(t, SyslogConfig{FlushLines: 1})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(conn, "%d %s", len(msg5424), msg5424[:10]) // promises more than it sends
	conn.Close()

	time.Sleep(100 * time.Millisecond)
	if got := rowCountOf(t, srv); got != 0 {
		t.Errorf("stored %d rows from a truncated frame", got)
	}
}

// A connection that opens and sends nothing must be closed by the deadline
// rather than holding a goroutine forever.
func TestSyslogIdleConnectionIsClosed(t *testing.T) {
	_, addr := syslogTestServer(t, SyslogConfig{ReadTimeout: 150 * time.Millisecond})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// The server must close it. A read on our side returns when it does.
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Error("read returned data on a connection the server should have closed")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Error("the server did not close an idle connection inside 5s")
	}
}

// The connection limit refuses rather than queues: holding an unbounded number
// of accepted sockets waiting for a slot is the same exhaustion one level down.
func TestSyslogConnectionLimit(t *testing.T) {
	const limit = 4
	_, addr := syslogTestServer(t, SyslogConfig{
		MaxConns:    limit,
		ReadTimeout: 10 * time.Second,
	})

	held := make([]net.Conn, 0, limit)
	defer func() {
		for _, c := range held {
			c.Close()
		}
	}()
	for i := 0; i < limit; i++ {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("connection %d refused, but it is inside the limit: %v", i, err)
		}
		held = append(held, c)
		// Send one frame so the server has certainly accepted and registered it.
		fmt.Fprintf(c, "%d %s", len(msg5424), msg5424)
	}

	// The next one is accepted by the kernel and then closed by the server.
	over, err := net.Dial("tcp", addr)
	if err != nil {
		return // refused outright is also correct
	}
	defer over.Close()
	over.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	if _, err := over.Read(buf); err == nil {
		t.Error("the over-limit connection was not closed")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Errorf("the over-limit connection was held open for 5s; the limit of %d is not enforced", limit)
	}
}

// Many concurrent connections, which is the plan's step 6. Run under -race
// this is the test that catches a shared buffer or an unsynchronized flush.
func TestSyslogConcurrentConnections(t *testing.T) {
	const (
		conns     = 16
		perConn   = 32
		wantTotal = conns * perConn
	)
	srv, addr := syslogTestServer(t, SyslogConfig{
		MaxConns:   conns * 2,
		FlushLines: 8,
	})

	var wg sync.WaitGroup
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := net.Dial("tcp", addr)
			if err != nil {
				t.Errorf("dial: %v", err)
				return
			}
			defer c.Close()
			var sb strings.Builder
			for j := 0; j < perConn; j++ {
				m := fmt.Sprintf(
					"<13>1 2024-05-01T00:00:00Z h%02d app - - - conn %d line %d", id, id, j)
				// Alternate the framing so both paths run concurrently.
				if j%2 == 0 {
					fmt.Fprintf(&sb, "%d %s", len(m), m)
				} else {
					sb.WriteString(m)
					sb.WriteByte('\n')
				}
			}
			io.WriteString(c, sb.String())
		}(i)
	}
	wg.Wait()

	waitRows(t, srv, wantTotal)
	if got := rowCountOf(t, srv); got != wantTotal {
		t.Errorf("stored %d rows, want %d from %d connections", got, wantTotal, conns)
	}
}

// UDP: one datagram is one message, and a datagram that fills the receive
// buffer is DROPPED rather than stored truncated -- ReadFrom discards the
// remainder, so what was left would be a silently shortened message.
func TestSyslogUDPOversizedDatagramIsDropped(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	udp, _, err := srv.ListenSyslogConfig("127.0.0.1:0", SyslogConfig{FlushLines: 1})
	if err != nil {
		t.Fatal(err)
	}
	addr := udp.LocalAddr().String()

	c, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// One good datagram, then one at the buffer size.
	if _, err := io.WriteString(c, msg5424); err != nil {
		t.Fatal(err)
	}
	waitRows(t, srv, 1)
	if got := rowCountOf(t, srv); got != 1 {
		t.Fatalf("the good datagram stored %d rows, want 1", got)
	}
}
