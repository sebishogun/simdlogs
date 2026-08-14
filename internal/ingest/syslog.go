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
		ts, ok := parseSyslogInto(string(line), fields)
		if !ok {
			ts = fallback()
		}
		if mapped {
			opts.apply(fields)
		}
		addWithStream(w, ts, fields, opts)
		res.Accepted++
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
	ts, ok := parseSyslogInto(string(msg), fields)
	if !ok {
		ts = fallback()
	}
	if !opts.Empty() {
		opts.apply(fields)
	}
	addWithStream(w, ts, fields, opts)
	res.Accepted++
	return res, nil
}

var severityName = [8]string{"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"}

// parseSyslogInto fills fields from one syslog line and returns its timestamp.
// ok is false when no timestamp could be read (the caller substitutes one);
// fields are populated regardless.
func parseSyslogInto(line string, fields map[string]string) (int64, bool) {
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
func parse5424(rest string, fields map[string]string) (int64, bool) {
	f := splitN(rest, 6) // ts, host, app, procid, msgid, tail(sd+msg)
	setIf := func(k, v string) {
		if v != "" && v != "-" {
			fields[k] = v
		}
	}
	var ts int64
	haveTS := false
	if len(f) > 0 {
		if t, ok := parseTime(f[0]); ok {
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
	return ts, haveTS
}

// parse3164 handles the BSD form `Mon _2 15:04:05 HOST TAG: MSG`. The yearless
// timestamp is completed with the current year; if it will not parse, ok is
// false and the caller supplies a timestamp.
func parse3164(rest string, fields map[string]string) (int64, bool) {
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
	return ts, haveTS
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
