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
// Only the newline form was read, so an octet-counted sender's messages were
// stored wrong: the count became the HOSTNAME field and the priority became
// the app name, because the parser read the count and space as the start of
// the message. A counted message containing a newline was also split.
//
// syslog-ng's `syslog()` driver defaults to octet-counting over TCP. rsyslog's
// `omfwd` does NOT -- its default is TCP_Framing="traditional", newline
// framing, because (its own documentation says) few implementations support
// octet-counting. An earlier version of this comment claimed both, which is
// wrong for rsyslog.
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
//
// A line longer than the READ BUFFER is accumulated and returned, not refused.
// The first version returned an error on every path out of the ErrBufferFull
// branch, which made the effective ceiling the 8 KiB read buffer rather than
// MaxFrameBytes -- and the caller closes the connection on a framing error, so
// one long message also took the good messages behind it. The old scanner
// handled a 500 KB line; this is the size that matters, because a forwarded
// stack trace or a JSON payload routinely exceeds 8 KiB.
func (f *syslogFrameReader) newlineFramed() ([]byte, error) {
	line, err := f.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		// Accumulate into the reusable buffer. ReadSlice's return aliases the
		// reader's own buffer and is invalidated by the next read, so the
		// bytes must be copied out as they arrive.
		f.buf = append(f.buf[:0], line...)
		for {
			more, err2 := f.br.ReadSlice('\n')
			if len(f.buf)+len(more) > f.max {
				return nil, fmt.Errorf("%w: %d > %d", errFrameTooLarge, len(f.buf)+len(more), f.max)
			}
			f.buf = append(f.buf, more...)
			if err2 == bufio.ErrBufferFull {
				continue
			}
			if err2 != nil && err2 != io.EOF {
				return nil, err2
			}
			// EOF with no newline still yields what was read: the sender
			// closed mid-line, and dropping it silently reported a clean end
			// of stream for data that was lost.
			return trimEOL(f.buf), nil
		}
	}
	if err != nil && err != io.EOF {
		return nil, err
	}
	// The terminator is not part of the message, so it is trimmed BEFORE the
	// size test: a message of exactly MaxFrameBytes was refused because its
	// newline pushed the count one over.
	line = trimEOL(line)
	if len(line) > f.max {
		return nil, fmt.Errorf("%w: %d > %d", errFrameTooLarge, len(line), f.max)
	}
	if err == io.EOF && len(line) == 0 {
		return nil, io.EOF
	}
	return line, nil
}

func trimEOL(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}
