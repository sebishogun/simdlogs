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
	buf := make([]byte, 64*1024) // max practical syslog datagram
	fallback := s.def.fallbackTS()
	batch := 0
	// A read deadline is what makes the batch flush on TIME as well as on
	// count. Without it a lone datagram sat unflushed until FlushLines more
	// arrived, so a low-rate sender's messages were never queryable -- the
	// batching fix's own regression, and exactly the kind a per-line flush
	// cannot have.
	for {
		if err := c.SetReadDeadline(time.Now().Add(cfg.FlushEvery)); err != nil {
			s.flushSyslog(&batch, true, "udp")
			return
		}
		n, _, err := c.ReadFrom(buf)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				// Idle tick: flush whatever is buffered and keep serving.
				if !s.flushSyslog(&batch, false, "udp") {
					return
				}
				continue
			}
			s.flushSyslog(&batch, true, "udp")
			return // listener closed
		}
		if n == 0 {
			continue
		}
		if n == len(buf) {
			// ReadFrom fills the buffer and DISCARDS the rest of an oversized
			// datagram, so the message stored would be a silent truncation.
			// Counted and dropped instead of stored wrong.
			atomic.AddInt64(&s.nHTTPErrs, 1)
			log.Printf("syslog udp: datagram of %d bytes or more truncated by the receive buffer; dropped", n)
			continue
		}
		res, ierr := ingest.IngestSyslog(s.def.w, buf[:n], fallback)
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
			if !s.flushSyslog(&batch, false, "udp") {
				return
			}
		}
	}
}

// flushSyslog performs the batched durable flush and reports failure. Returns
// false when the writer is closed, which means shutdown.
func (s *Server) flushSyslog(batch *int, force bool, transport string) bool {
	if *batch == 0 && !force {
		return true
	}
	*batch = 0
	if err := s.def.w.Flush(); err != nil {
		if errors.Is(err, ingest.ErrWriterClosed) {
			return false // shutting down
		}
		atomic.AddInt64(&s.nHTTPErrs, 1)
		log.Printf("syslog %s: flush failed: %v", transport, err)
		return false
	}
	return true
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
	flushTick := time.NewTicker(cfg.FlushEvery)
	defer flushTick.Stop()
	// The ticker fires on another goroutine's schedule, so the flush it
	// triggers is signalled rather than performed there: two goroutines
	// flushing the same writer concurrently is a race the writer does not
	// promise to survive.
	timeUp := make(chan struct{}, 1)
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-flushTick.C:
				select {
				case timeUp <- struct{}{}:
				default:
				}
			case <-done:
				return
			}
		}
	}()

	for {
		// Reset per frame: a slow but live sender keeps its connection, a
		// silent one loses it. Without this an opened-and-abandoned connection
		// held a goroutine forever.
		if err := conn.SetReadDeadline(time.Now().Add(cfg.ReadTimeout)); err != nil {
			s.flushSyslog(&batch, true, "tcp")
			return
		}
		msg, counted, err := fr.Next()
		if err != nil {
			s.flushSyslog(&batch, true, "tcp")
			if err == io.EOF {
				return // clean end of stream
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				log.Printf("syslog tcp %s: idle for %v, closing", conn.RemoteAddr(), cfg.ReadTimeout)
				return
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

		flushNow := batch >= cfg.FlushLines
		if !flushNow {
			select {
			case <-timeUp:
				flushNow = true
			default:
			}
		}
		if flushNow {
			// TCP can at least stop reading from a sender whose data is not
			// landing, rather than accepting more of it into a broken store.
			if !s.flushSyslog(&batch, false, "tcp") {
				return
			}
		}
	}
}
