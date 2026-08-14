package api

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

// RFC 6587 syslog-over-TCP framing.
//
// The RFC defines TWO framings and a receiver is expected to handle both:
//
//	octet-counting:      "123 <13>1 2024-05-01T00:00:00Z host app - - - msg"
//	non-transparent:     "<13>1 2024-05-01T00:00:00Z host app - - - msg\n"
//
// Only the newline form was read. rsyslog's `omfwd` sends OCTET-COUNTED by
// default over TCP, and syslog-ng's `syslog()` driver does too, so the default
// configuration of the two most common forwarders produced garbage: the count
// and the space were stored as part of the message, and a message containing a
// newline was split into several rows.
//
// The two are distinguishable without ambiguity: a frame begins with a decimal
// digit if and only if it is octet-counted, because a non-transparent frame
// begins with '<'.

// syslogFrameReader reads one syslog message per Next call, in either framing.
//
// It holds ONE buffer for the octet-counted path and returns a subslice of the
// bufio reader's own buffer for the newline path, so a message costs no
// allocation in the common case. The caller must not retain the returned bytes
// past the next Next.
type syslogFrameReader struct {
	br  *bufio.Reader
	buf []byte // grown as needed for octet-counted frames, reused across frames
	max int    // largest frame accepted, in bytes
}

var (
	errFrameTooLarge = errors.New("syslog frame exceeds the configured maximum")
	errBadFrameCount = errors.New("syslog octet count is not a decimal number")
)

func newSyslogFrameReader(r io.Reader, bufSize, max int) *syslogFrameReader {
	return &syslogFrameReader{br: bufio.NewReaderSize(r, bufSize), max: max}
}

// Next returns the next message and whether it was OCTET-COUNTED. The
// distinction matters to the caller: a counted frame is exactly one message
// even when it contains newlines -- which is the whole reason the framing
// exists -- so it must not be split again.
//
// The returned slice is valid until the following call.  io.EOF marks a clean
// end of stream.
func (f *syslogFrameReader) Next() (msg []byte, counted bool, err error) {
	// Skip any framing whitespace between messages. rsyslog emits none, but a
	// hand-written sender frequently leaves a stray newline after a
	// octet-counted frame.
	for {
		c, rerr := f.br.ReadByte()
		if rerr != nil {
			return nil, false, rerr
		}
		if c == '\n' || c == '\r' || c == ' ' || c == '\t' {
			continue
		}
		if uerr := f.br.UnreadByte(); uerr != nil {
			return nil, false, uerr
		}
		if c >= '0' && c <= '9' {
			m, e := f.octetCounted()
			return m, true, e
		}
		m, e := f.newlineFramed()
		return m, false, e
	}
}

// octetCounted reads "<count> <count bytes>".
func (f *syslogFrameReader) octetCounted() ([]byte, error) {
	n := 0
	digits := 0
	for {
		c, err := f.br.ReadByte()
		if err != nil {
			return nil, err
		}
		if c == ' ' {
			break
		}
		if c < '0' || c > '9' {
			return nil, fmt.Errorf("%w: %q", errBadFrameCount, c)
		}
		digits++
		// Bound the count as it is READ, not after: a sender can otherwise
		// write digits forever, and the accumulator overflows into a small
		// positive number that then passes the size check.
		if digits > 10 {
			return nil, errFrameTooLarge
		}
		n = n*10 + int(c-'0')
		if n > f.max {
			return nil, fmt.Errorf("%w: %d > %d", errFrameTooLarge, n, f.max)
		}
	}
	if digits == 0 {
		return nil, errBadFrameCount
	}
	if n == 0 {
		return nil, nil // an empty frame is legal and carries nothing
	}
	if cap(f.buf) < n {
		f.buf = make([]byte, n)
	}
	f.buf = f.buf[:n]
	if _, err := io.ReadFull(f.br, f.buf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// A truncated frame is not a clean end of stream: the sender
			// promised n bytes and sent fewer.
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return f.buf, nil
}

// newlineFramed reads up to the next '\n'.
func (f *syslogFrameReader) newlineFramed() ([]byte, error) {
	line, err := f.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		// The line is longer than the read buffer. Keep consuming until the
		// newline or the limit, so ONE oversized line does not desynchronize
		// every message after it -- the previous scanner simply stopped, which
		// silently ended the connection.
		total := len(line)
		for {
			more, err2 := f.br.ReadSlice('\n')
			total += len(more)
			if total > f.max {
				return nil, fmt.Errorf("%w: %d > %d", errFrameTooLarge, total, f.max)
			}
			if err2 == bufio.ErrBufferFull {
				continue
			}
			if err2 != nil {
				return nil, err2
			}
			return nil, fmt.Errorf("%w: %d bytes", errFrameTooLarge, total)
		}
	}
	if err != nil && err != io.EOF {
		return nil, err
	}
	if len(line) > f.max {
		return nil, fmt.Errorf("%w: %d > %d", errFrameTooLarge, len(line), f.max)
	}
	// Trim the terminator; a bare EOF with no newline still yields the line.
	for len(line) > 0 && (line[len(line)-1] == '\n' || line[len(line)-1] == '\r') {
		line = line[:len(line)-1]
	}
	if err == io.EOF && len(line) == 0 {
		return nil, io.EOF
	}
	return line, nil
}
