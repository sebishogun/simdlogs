package api

import (
	"bufio"
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
	go s.serveSyslogUDP(udp)

	tcp, err = net.Listen("tcp", addr)
	if err != nil {
		udp.Close()
		return nil, nil, err
	}
	go s.serveSyslogTCP(tcp)
	return udp, tcp, nil
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
		go s.handleSyslogConn(conn)
	}
}

func (s *Server) handleSyslogConn(conn net.Conn) {
	defer conn.Close()
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
			atomic.AddInt64(&s.nHTTPErrs, 1)
			log.Printf("syslog tcp %s: flush failed, closing connection: %v", conn.RemoteAddr(), err)
			return
		}
	}
}
