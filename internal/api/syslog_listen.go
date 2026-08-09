package api

import (
	"bufio"
	"net"

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

// serveSyslogUDP treats each datagram as one syslog message.
func (s *Server) serveSyslogUDP(c net.PacketConn) {
	buf := make([]byte, 64*1024) // max practical syslog datagram
	fallback := s.fallbackTS()
	for {
		n, _, err := c.ReadFrom(buf)
		if err != nil {
			return // listener closed
		}
		if n == 0 {
			continue
		}
		ingest.IngestSyslog(s.w, buf[:n], fallback)
		s.w.Flush()
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
	fallback := s.fallbackTS()
	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		ingest.IngestSyslog(s.w, line, fallback)
		s.w.Flush()
	}
}
