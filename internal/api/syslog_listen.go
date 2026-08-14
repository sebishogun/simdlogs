package api

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"sync/atomic"

	"github.com/sebishogun/simdlogs/internal/ingest"
)

// ListenSyslog starts syslog receivers on addr for both UDP and TCP -- the
// native syslog transport (RFC5426 over UDP, RFC6587 newline-framed over TCP),
// the way VictoriaLogs' syslog listener works. It returns the two closers so
// the caller controls shutdown; either may be nil if that transport failed to
// bind. A UDP datagram is one message; a TCP connection is a newline-delimited
// stream.
func (s *Server) ListenSyslog(addr string) (udp net.PacketConn, tcp net.Listener, err error) {
	udp, err = net.ListenPacket("udp", addr)
	if err != nil {
		return nil, nil, err
	}
	s.syslogMu.Lock()
	s.syslogListeners = append(s.syslogListeners, udp)
	s.syslogMu.Unlock()
	s.syslogWG.Add(1)
	go func() { defer s.syslogWG.Done(); s.serveSyslogUDP(udp) }()

	tcp, err = net.Listen("tcp", addr)
	if err != nil {
		udp.Close()
		s.syslogWG.Wait()
		return nil, nil, err
	}
	s.syslogMu.Lock()
	s.syslogListeners = append(s.syslogListeners, tcp)
	s.syslogMu.Unlock()
	s.syslogWG.Add(1)
	go func() { defer s.syslogWG.Done(); s.serveSyslogTCP(tcp) }()
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
		c.Close() // unblocks the scanner, so the goroutine returns
	}
	s.syslogWG.Wait()
}

// serveSyslogUDP treats each datagram as one syslog message. The syslog
// transport carries no tenant header, so it feeds the default tenant.
func (s *Server) serveSyslogUDP(c net.PacketConn) {
	buf := make([]byte, 64*1024) // max practical syslog datagram
	fallback := s.def.fallbackTS()
	for {
		n, _, err := c.ReadFrom(buf)
		if err != nil {
			return // listener closed
		}
		if n == 0 {
			continue
		}
		ingest.IngestSyslog(s.def.w, buf[:n], fallback)
		// UDP has no response to fail, so a dropped flush error here is
		// silent loss with no other signal. Log it and count it.
		if err := s.def.w.Flush(); err != nil {
			if errors.Is(err, ingest.ErrWriterClosed) {
				return // shutting down
			}
			atomic.AddInt64(&s.nHTTPErrs, 1)
			log.Printf("syslog udp: flush failed, %d bytes lost: %v", n, err)
		}
	}
}

// serveSyslogTCP reads newline-framed messages from each connection.
func (s *Server) serveSyslogTCP(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			return // listener closed
		}
		s.syslogWG.Add(1)
		go func() { defer s.syslogWG.Done(); s.handleSyslogConn(conn) }()
	}
}

func (s *Server) handleSyslogConn(conn net.Conn) {
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
	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		ingest.IngestSyslog(s.def.w, line, fallback)
		// TCP can at least stop reading from a sender whose data is not
		// landing, rather than accepting more of it into a broken store.
		if err := s.def.w.Flush(); err != nil {
			if errors.Is(err, ingest.ErrWriterClosed) {
				return // shutting down
			}
			atomic.AddInt64(&s.nHTTPErrs, 1)
			log.Printf("syslog tcp %s: flush failed, closing connection: %v", conn.RemoteAddr(), err)
			return
		}
	}
}
