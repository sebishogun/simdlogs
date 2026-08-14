package api

import (
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"sync/atomic"
	"time"

	"github.com/sebishogun/simdlogs/internal/ingest"
)

// maxUDPDatagram is the largest UDP payload IPv4 can carry: 65535 total minus
// a 20-byte IP header and an 8-byte UDP header. The kernel refuses a send
// larger than this ("message too long"), so it is the real ceiling and not a
// choice this package makes.
const maxUDPDatagram = 65507

// SyslogConfig bounds the native syslog transport.
//
// Every field here closes a hole the listener shipped with: no read deadline
// (a connection that opened and sent nothing held a goroutine and a 1 MiB
// buffer forever), no connection limit (an unbounded number of them), no frame
// limit that reported anything, and no TLS.
type SyslogConfig struct {
	// MaxConns bounds concurrent TCP connections. A connection over the limit
	// is closed immediately rather than queued, because queueing an
	// unbounded number of accepted sockets is the same exhaustion one level
	// down.
	MaxConns int

	// ReadTimeout is the deadline for reading ONE frame. It is reset per
	// frame, so a slow but live sender is fine and a silent one is not.
	ReadTimeout time.Duration

	// MaxFrameBytes is the largest single syslog message accepted.
	MaxFrameBytes int

	// FlushEvery batches the durable flush. A flush per LINE meant an fsync
	// per syslog message, which is the difference between thousands of
	// messages a second and tens.
	FlushEvery time.Duration

	// FlushLines forces a flush after this many messages regardless of time,
	// so a fast sender's data does not sit in the buffer for a whole tick.
	FlushLines int

	// TLS, when set, wraps the TCP listener. UDP is unaffected: RFC 5425 is
	// TLS over TCP only.
	TLS *tls.Config
}

// DefaultSyslogConfig is generous enough that no reasonable sender trips it
// and finite enough that a hostile or broken one cannot exhaust the process.
func DefaultSyslogConfig() SyslogConfig {
	return SyslogConfig{
		MaxConns:      1024,
		ReadTimeout:   5 * time.Minute,
		MaxFrameBytes: 1 << 20, // 1 MiB, the previous scanner's hard limit
		FlushEvery:    200 * time.Millisecond,
		FlushLines:    1024,
	}
}

func (c SyslogConfig) withDefaults() SyslogConfig {
	d := DefaultSyslogConfig()
	if c.MaxConns <= 0 {
		c.MaxConns = d.MaxConns
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = d.ReadTimeout
	}
	if c.MaxFrameBytes <= 0 {
		c.MaxFrameBytes = d.MaxFrameBytes
	}
	if c.FlushEvery <= 0 {
		c.FlushEvery = d.FlushEvery
	}
	if c.FlushLines <= 0 {
		c.FlushLines = d.FlushLines
	}
	return c
}

// ListenSyslog starts syslog receivers on addr for both UDP and TCP with the
// default bounds. See ListenSyslogConfig to set them.
func (s *Server) ListenSyslog(addr string) (udp net.PacketConn, tcp net.Listener, err error) {
	return s.ListenSyslogConfig(addr, DefaultSyslogConfig())
}

// ListenSyslogConfig starts syslog receivers on addr for both UDP and TCP --
// the native syslog transport (RFC 5426 over UDP, RFC 6587 over TCP in BOTH
// its framings), the way VictoriaLogs' syslog listener works. It returns the
// two closers so the caller controls shutdown; either may be nil if that
// transport failed to bind.
func (s *Server) ListenSyslogConfig(addr string, cfg SyslogConfig) (udp net.PacketConn, tcp net.Listener, err error) {
	cfg = cfg.withDefaults()

	udp, err = net.ListenPacket("udp", addr)
	if err != nil {
		return nil, nil, err
	}
	s.syslogMu.Lock()
	s.syslogListeners = append(s.syslogListeners, udp)
	s.syslogMu.Unlock()
	s.syslogWG.Add(1)
	go func() { defer s.syslogWG.Done(); s.serveSyslogUDP(udp, cfg) }()

	var ln net.Listener
	ln, err = net.Listen("tcp", addr)
	if err != nil {
		udp.Close()
		s.syslogWG.Wait()
		return nil, nil, err
	}
	if cfg.TLS != nil {
		ln = tls.NewListener(ln, cfg.TLS)
	}
	tcp = ln
	s.syslogMu.Lock()
	s.syslogListeners = append(s.syslogListeners, tcp)
	s.syslogMu.Unlock()
	s.syslogWG.Add(1)
	go func() { defer s.syslogWG.Done(); s.serveSyslogTCP(tcp, cfg) }()
	return udp, tcp, nil
}

// closeSyslogConns closes every accepted connection and waits for the
// listener and connection goroutines to return.
//
// Closing only the listeners left the per-connection goroutines running. Each
// one calls Flush after every line, so once Close had shut the writer the
// next line sent on a closed channel and panicked -- in a bare goroutine, not
// under recoverPanic, so it killed the process mid-shutdown. That is the
// exact case task 1.6 lists: SIGTERM with a live TCP syslog connection.
func (s *Server) closeSyslogConns() {
	s.syslogMu.Lock()
	conns := make([]net.Conn, 0, len(s.syslogConns))
	for c := range s.syslogConns {
		conns = append(conns, c)
	}
	listeners := append([]io.Closer(nil), s.syslogListeners...)
	s.syslogListeners = nil
	s.syslogClosing = true
	s.syslogMu.Unlock()

	// Close the listeners too. Waiting on the accept goroutines without
	// closing what they are blocked on is a deadlock: Close waited forever
	// whenever ListenSyslog had been called and the caller had not closed
	// them first. The shipped binary does close them, which is why only a
	// direct Close() hung -- and Close documents no such precondition.
	for _, l := range listeners {
		l.Close()
	}
	for _, c := range conns {
		c.Close() // unblocks the reader, so the goroutine returns
	}
	s.syslogWG.Wait()
}

// serveSyslogUDP treats each datagram as one syslog message. The syslog
// transport carries no tenant header, so it feeds the default tenant.
func (s *Server) serveSyslogUDP(c net.PacketConn, cfg SyslogConfig) {
	// One buffer for the life of the listener: a datagram is copied out of the
	// kernel into it and fully consumed before the next ReadFrom, so
	// allocating per datagram would be one allocation per message.
	//
	// Sized ONE BYTE OVER the largest datagram the kernel will deliver
	// (65507 = 65535 - 20 IP - 8 UDP), so a full buffer is proof of
	// truncation rather than a coincidence. It used to be 65536, which is
	// larger than anything the kernel accepts -- the truncation branch below
	// could therefore never fire, and the test that claimed to exercise it
	// sent only one good datagram.
	buf := make([]byte, maxUDPDatagram+1)
	fallback := s.def.fallbackTS()
	batch := 0
	// An ABSOLUTE next-flush instant, not now+FlushEvery per iteration.
	//
	// Re-arming the deadline relative to NOW on every read meant any sender
	// faster than FlushEvery pushed it forward forever: measured, a datagram
	// every 20ms with FlushEvery=50ms left 65 messages unqueryable after 1.3
	// seconds and only flushed when the sender STOPPED. The flush was
	// idle-only, which is the opposite of what it is for.
	nextFlush := time.Now().Add(cfg.FlushEvery)
	for {
		if err := c.SetReadDeadline(nextFlush); err != nil {
			s.flushSyslog(&batch, true, "udp")
			return
		}
		n, _, err := c.ReadFrom(buf)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				// The flush instant arrived: flush and set the next one.
				if s.flushSyslog(&batch, false, "udp") == flushShutdown {
					return
				}
				nextFlush = time.Now().Add(cfg.FlushEvery)
				continue
			}
			s.flushSyslog(&batch, true, "udp")
			return // listener closed
		}
		if n == 0 {
			continue
		}
		if n > maxUDPDatagram {
			// ReadFrom fills the buffer and DISCARDS the remainder, so what
			// would be stored is a silently shortened message. Counted and
			// dropped instead of stored wrong. Reachable now that the buffer
			// is one byte larger than the kernel's own ceiling.
			atomic.AddInt64(&s.nHTTPErrs, 1)
			log.Printf("syslog udp: datagram over %d bytes truncated by the receive buffer; dropped",
				maxUDPDatagram)
			continue
		}
		// ONE datagram is ONE message (RFC 5426), newlines included. The
		// line-splitting entry point turned a datagram containing a newline
		// into two rows -- the same defect the counted TCP path was fixed for,
		// left in place on UDP while the LLD claimed otherwise.
		res, ierr := ingest.IngestSyslogMessage(s.def.w, buf[:n], fallback, nil)
		s.countRows(res.Accepted, res.Rejected, n)
		if ierr != nil {
			// UDP has no response, so an unreported parse failure is silent
			// loss with no other signal anywhere.
			atomic.AddInt64(&s.nHTTPErrs, 1)
			log.Printf("syslog udp: %d bytes rejected: %v", n, ierr)
			continue
		}
		batch += res.Accepted
		if batch >= cfg.FlushLines {
			// Only a CLOSED writer stops the listener. A transient failure is
			// logged and counted, and serving continues.
			if s.flushSyslog(&batch, false, "udp") == flushShutdown {
				return
			}
			nextFlush = time.Now().Add(cfg.FlushEvery)
		}
	}
}

// flushOutcome distinguishes the two reasons a flush can fail, which the first
// version conflated: it returned false for BOTH, and every caller returned on
// false, so one transient flush failure -- a full disk, a permissions blip --
// permanently killed the UDP listener. The pre-task code logged, counted and
// kept serving.
type flushOutcome int

const (
	flushOK       flushOutcome = iota
	flushFailed                // transient: report it, keep serving
	flushShutdown              // the writer is closed: stop
)

// flushSyslog performs the batched durable flush and reports what happened.
func (s *Server) flushSyslog(batch *int, force bool, transport string) flushOutcome {
	if *batch == 0 && !force {
		return flushOK
	}
	*batch = 0
	if err := s.def.w.Flush(); err != nil {
		if errors.Is(err, ingest.ErrWriterClosed) {
			return flushShutdown
		}
		atomic.AddInt64(&s.nHTTPErrs, 1)
		log.Printf("syslog %s: flush failed: %v", transport, err)
		return flushFailed
	}
	return flushOK
}

// serveSyslogTCP accepts connections up to the configured limit.
func (s *Server) serveSyslogTCP(l net.Listener, cfg SyslogConfig) {
	// A counting semaphore rather than a queue: a connection over the limit is
	// refused immediately, because holding an unbounded number of accepted
	// sockets waiting for a slot is the same exhaustion one level down.
	sem := make(chan struct{}, cfg.MaxConns)
	for {
		conn, err := l.Accept()
		if err != nil {
			return // listener closed
		}
		select {
		case sem <- struct{}{}:
		default:
			atomic.AddInt64(&s.nHTTPErrs, 1)
			log.Printf("syslog tcp: refusing %s, %d connections already open",
				conn.RemoteAddr(), cfg.MaxConns)
			conn.Close()
			continue
		}
		s.syslogWG.Add(1)
		go func() {
			defer s.syslogWG.Done()
			defer func() { <-sem }()
			s.handleSyslogConn(conn, cfg)
		}()
	}
}

func (s *Server) handleSyslogConn(conn net.Conn, cfg SyslogConfig) {
	defer conn.Close()
	s.syslogMu.Lock()
	if s.syslogClosing {
		s.syslogMu.Unlock()
		return
	}
	if s.syslogConns == nil {
		s.syslogConns = map[net.Conn]struct{}{}
	}
	s.syslogConns[conn] = struct{}{}
	s.syslogMu.Unlock()
	defer func() {
		s.syslogMu.Lock()
		delete(s.syslogConns, conn)
		s.syslogMu.Unlock()
	}()

	fallback := s.def.fallbackTS()
	// The read buffer starts small and the frame reader grows only what an
	// octet-counted frame actually needs, so an idle connection costs 8 KiB
	// rather than the 64 KiB the scanner reserved up front.
	fr := newSyslogFrameReader(conn, 8<<10, cfg.MaxFrameBytes)

	batch := 0
	// The flush instant and the idle deadline share ONE read deadline, set to
	// whichever comes first. No ticker, and no goroutine.
	//
	// The first version ran a ticker plus a forwarding goroutine PER
	// CONNECTION and drained the signal only after fr.Next() returned a frame
	// -- but fr.Next() blocks until the next frame or ReadTimeout, so the
	// flush never happened on a held-open connection until the connection was
	// dropped. With the default five-minute ReadTimeout that is a five-minute
	// delay on exactly the persistent-connection case rsyslog and syslog-ng
	// use. Measured at the configured 1024-connection limit, the tickers cost
	// 131M extra instructions per five idle seconds, 2.1% of a core, 3.4 MiB
	// and 1025 goroutines -- for a mechanism that did not work.
	nextFlush := time.Now().Add(cfg.FlushEvery)
	for {
		now := time.Now()
		// Whichever is sooner. The idle deadline still bounds an abandoned
		// connection; the flush deadline bounds how long a buffered message
		// waits to become queryable.
		deadline := now.Add(cfg.ReadTimeout)
		idleAt := deadline
		if nextFlush.Before(deadline) {
			deadline = nextFlush
		}
		if err := conn.SetReadDeadline(deadline); err != nil {
			s.flushSyslog(&batch, true, "tcp")
			return
		}
		msg, counted, err := fr.Next()
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				// A flush deadline, not an idle one: flush and keep reading.
				if !deadline.Equal(idleAt) {
					if s.flushSyslog(&batch, false, "tcp") == flushShutdown {
						return
					}
					nextFlush = time.Now().Add(cfg.FlushEvery)
					continue
				}
				s.flushSyslog(&batch, true, "tcp")
				log.Printf("syslog tcp %s: idle for %v, closing", conn.RemoteAddr(), cfg.ReadTimeout)
				return
			}
			s.flushSyslog(&batch, true, "tcp")
			if err == io.EOF {
				return // clean end of stream
			}
			// A framing error is reported, not swallowed. The scanner's
			// Err() was never checked, so an oversized line ended the
			// connection silently and the sender saw a healthy socket close.
			atomic.AddInt64(&s.nHTTPErrs, 1)
			log.Printf("syslog tcp %s: %v", conn.RemoteAddr(), err)
			return
		}
		if len(msg) == 0 {
			continue
		}

		// An octet-counted frame is ONE message even when it contains
		// newlines -- which is the whole reason the framing exists. Passing it
		// through the line-splitting path turned a forwarded multi-line stack
		// trace into one valid record plus a run of records that parse as
		// nothing.
		var res ingest.Result
		var ierr error
		if counted {
			res, ierr = ingest.IngestSyslogMessage(s.def.w, msg, fallback, nil)
		} else {
			res, ierr = ingest.IngestSyslog(s.def.w, msg, fallback)
		}
		s.countRows(res.Accepted, res.Rejected, len(msg))
		if ierr != nil {
			atomic.AddInt64(&s.nHTTPErrs, 1)
			log.Printf("syslog tcp %s: %d bytes rejected: %v", conn.RemoteAddr(), len(msg), ierr)
			continue // one bad frame does not end a good connection
		}
		batch += res.Accepted

		if batch >= cfg.FlushLines || !time.Now().Before(nextFlush) {
			// TCP can at least stop reading from a sender whose data is not
			// landing, rather than accepting more of it into a broken store --
			// but only a CLOSED writer ends the connection. A transient
			// failure is logged and counted, and the connection keeps serving.
			switch s.flushSyslog(&batch, false, "tcp") {
			case flushShutdown:
				return
			case flushFailed:
				log.Printf("syslog tcp %s: closing after a flush failure", conn.RemoteAddr())
				return
			}
			nextFlush = time.Now().Add(cfg.FlushEvery)
		}
	}
}
