package ingest

import (
	"strconv"
	"strings"
	"time"
)

// IngestSyslog parses syslog lines (RFC5424 preferred, RFC3164 BSD as a
// fallback) and appends one record per line. PRI is decoded to facility and a
// named severity; the structured header fields (hostname, app, procid, msgid)
// become fields and the free text becomes _msg. A line that is not syslog at
// all is stored whole as _msg so nothing is dropped.
func IngestSyslog(w *Writer, data []byte, fallback func() int64) (Result, error) {
	return IngestSyslogOpts(w, data, fallback, nil)
}

// IngestSyslogOpts is IngestSyslog with the request's field mappings applied.
func IngestSyslogOpts(w *Writer, data []byte, fallback func() int64, opts *Options) (Result, error) {
	var res Result
	mapped := !opts.Empty()
	fields := map[string]string{}
	for len(data) > 0 {
		nl := indexByte(data, '\n')
		var line []byte
		if nl < 0 {
			line, data = data, nil
		} else {
			line, data = data[:nl], data[nl+1:]
		}
		if len(trimSpace(line)) == 0 {
			continue
		}
		for k := range fields {
			delete(fields, k)
		}
		ordinal := res.Accepted + res.Rejected
		ts, ok, tsErr := parseSyslogInto(string(line), fields)
		if tsErr != nil {
			// See ErrTimeOutOfRange. Refused and COUNTED, like every other
			// protocol in this package -- see parseSyslogInto for what this
			// replaced.
			res.Reject(ordinal)
			res.WarnAt(ordinal, "%v", tsErr)
			continue
		}
		if !ok {
			ts = fallback()
		}
		if mapped {
			opts.apply(fields)
		}
		addOrReject(w, ts, fields, opts, &res, ordinal)
	}
	return res, nil
}

// IngestSyslogMessage ingests ONE syslog message, newlines included.
//
// The line-splitting entry point above is right for UDP (one datagram, one
// message) and for newline-framed TCP. It is WRONG for an RFC 6587
// octet-counted frame, which is the framing that exists precisely so a message
// CAN contain newlines: a forwarded multi-line stack trace arrives as one
// counted frame, and splitting it again turns line two onward into records
// that parse as nothing.
func IngestSyslogMessage(w *Writer, msg []byte, fallback func() int64, opts *Options) (Result, error) {
	var res Result
	if len(trimSpace(msg)) == 0 {
		return res, nil
	}
	fields := map[string]string{}
	ts, ok, tsErr := parseSyslogInto(string(msg), fields)
	if tsErr != nil {
		res.Reject(0)
		res.WarnAt(0, "%v", tsErr)
		return res, nil
	}
	if !ok {
		ts = fallback()
	}
	if !opts.Empty() {
		opts.apply(fields)
	}
	addOrReject(w, ts, fields, opts, &res, res.Accepted+res.Rejected)
	return res, nil
}

var severityName = [8]string{"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"}

// parseSyslogInto fills fields from one syslog line and returns its timestamp.
//
// THREE OUTCOMES, the same three parseTime has. ok is false with a nil error
// when no timestamp could be read at all (the caller substitutes one, which is
// RFC 5424 relay behaviour for a missing or unparseable stamp); a non-nil error
// is a stamp that PARSED and cannot be stored, and the caller REFUSES the
// record and counts it.
//
// The third outcome used to be folded into the second, justified as "the
// datagram has no client to report a per-record rejection to". That is true of
// the UDP listener and FALSE of `/insert/syslog`, which is an HTTP request with
// a client on the other end -- and it was the wrong trade even on the datagram,
// because stamping a year-9999 line with the receiver's clock files it under an
// instant nobody sent, which is the fabrication ErrTimeOutOfRange exists to
// stop. Both transports count the rejection now: the HTTP route reports it in
// the response, and the listener adds it to the same rejected counter every
// other malformed frame lands in.
func parseSyslogInto(line string, fields map[string]string) (int64, bool, error) {
	rest := line
	// PRI: <N> at the very start. N = facility*8 + severity.
	if strings.HasPrefix(rest, "<") {
		if end := strings.IndexByte(rest, '>'); end > 1 {
			if pri, err := strconv.Atoi(rest[1:end]); err == nil && pri >= 0 {
				fields["facility"] = strconv.Itoa(pri / 8)
				fields["severity"] = severityName[pri%8]
				rest = rest[end+1:]
			}
		}
	}
	// RFC5424: a version digit followed by a space (`1 ...`).
	if len(rest) >= 2 && rest[0] == '1' && rest[1] == ' ' {
		return parse5424(rest[2:], fields)
	}
	return parse3164(rest, fields)
}

// parse5424 handles `TIMESTAMP HOST APP PROCID MSGID [SD] MSG`; a "-" means the
// field is absent. Structured data (the [...] section) is passed through as one
// field rather than fully unpacked.
func parse5424(rest string, fields map[string]string) (int64, bool, error) {
	f := splitN(rest, 6) // ts, host, app, procid, msgid, tail(sd+msg)
	setIf := func(k, v string) {
		if v != "" && v != "-" {
			fields[k] = v
		}
	}
	var ts int64
	haveTS := false
	var tsErr error
	if len(f) > 0 {
		// An out-of-range timestamp is REFUSED, not stamped `now`. See
		// parseSyslogInto. The fields are still filled in below so the caller
		// has the record to name in its warning.
		t, ok, err := parseTime(f[0])
		switch {
		case err != nil:
			tsErr = err
		case ok:
			ts, haveTS = t, true
		}
	}
	if len(f) > 1 {
		setIf("hostname", f[1])
	}
	if len(f) > 2 {
		setIf("app_name", f[2])
	}
	if len(f) > 3 {
		setIf("proc_id", f[3])
	}
	if len(f) > 4 {
		setIf("msg_id", f[4])
	}
	if len(f) > 5 {
		tail := strings.TrimLeft(f[5], " ")
		switch {
		case strings.HasPrefix(tail, "["): // structured data element(s), then the message
			if end := strings.IndexByte(tail, ']'); end >= 0 {
				fields["structured_data"] = tail[:end+1]
				tail = strings.TrimSpace(tail[end+1:])
			}
		case tail == "-": // nil SD, no message
			tail = ""
		case strings.HasPrefix(tail, "- "): // nil SD placeholder before the message
			tail = tail[2:]
		}
		fields["_msg"] = tail
	}
	return ts, haveTS, tsErr
}

// parse3164 handles the BSD form `Mon _2 15:04:05 HOST TAG: MSG`. The yearless
// timestamp is completed with the current year; if it will not parse, ok is
// false and the caller supplies a timestamp.
// parse3164's own conversion cannot go out of range: `time.Stamp` is yearless
// and AddDate shifts it to the CURRENT year, so the result is always within a
// year of now. It returns a nil error to keep one signature across both forms.
func parse3164(rest string, fields map[string]string) (int64, bool, error) {
	ts, haveTS := int64(0), false
	if len(rest) >= 15 {
		stamp := rest[:15]
		if t, err := time.Parse(time.Stamp, stamp); err == nil {
			t = t.AddDate(time.Now().Year(), 0, 0) // Stamp parses to year 0; shift to this year
			ts, haveTS = t.UnixNano(), true
			rest = strings.TrimSpace(rest[15:])
		}
	}
	parts := splitN(rest, 2) // host, tag+msg
	if len(parts) > 0 && parts[0] != "" {
		fields["hostname"] = parts[0]
	}
	if len(parts) > 1 {
		tail := parts[1]
		if c := strings.IndexByte(tail, ':'); c > 0 && c < 48 { // TAG: message
			fields["app_name"] = strings.TrimSuffix(tail[:c], ":")
			tail = strings.TrimSpace(tail[c+1:])
		}
		fields["_msg"] = tail
	}
	return ts, haveTS, nil
}

// splitN splits on runs of spaces into at most n fields, the last holding the
// remainder (so a message keeps its internal spaces).
func splitN(s string, n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n-1; i++ {
		s = strings.TrimLeft(s, " ")
		sp := strings.IndexByte(s, ' ')
		if sp < 0 {
			break
		}
		out = append(out, s[:sp])
		s = s[sp+1:]
	}
	out = append(out, strings.TrimLeft(s, " "))
	return out
}
