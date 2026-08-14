package ingest

import (
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
)

// IngestJournald parses the systemd journal export format (what
// systemd-journal-upload sends): entries are blocks of fields separated by a
// blank line. A field is either `NAME=value\n` (text) or `NAME\n` followed by
// a little-endian uint64 length and that many raw bytes then a newline
// (binary, so a value may contain newlines -- which is why this parses bytes,
// not lines). __REALTIME_TIMESTAMP (microseconds) sets the time, MESSAGE
// becomes _msg, and other names are lowercased with a leading underscore
// stripped (_HOSTNAME -> hostname).
func IngestJournald(w *Writer, data []byte, fallback func() int64) (Result, error) {
	return IngestJournaldOpts(w, data, fallback, nil)
}

// IngestJournaldOpts is IngestJournald with the request's field mappings applied.
func IngestJournaldOpts(w *Writer, data []byte, fallback func() int64, opts *Options) (Result, error) {
	var res Result
	mapped := !opts.Empty()
	fields := map[string]string{}
	var ts int64
	haveTS := false
	reset := func() {
		for k := range fields {
			delete(fields, k)
		}
		ts, haveTS = 0, false
	}
	emit := func() {
		if len(fields) == 0 {
			// An entry whose fields all failed to store -- most often one
			// carrying only a __REALTIME_TIMESTAMP -- was dropped with no
			// count, so a sender saw a 202 for records that were not there.
			if haveTS {
				res.Rejected++
				res.Warn(0, "entry carries a timestamp and no storable field")
			}
			reset()
			return
		}
		if !haveTS {
			ts = fallback()
		}
		if mapped {
			opts.apply(fields)
		}
		addWithStream(w, ts, fields, opts)
		res.Accepted++
		reset()
	}
	set := func(name string, val []byte) {
		if name == "__REALTIME_TIMESTAMP" {
			if us, err := strconv.ParseInt(string(val), 10, 64); err == nil {
				ts, haveTS = us*1000, true // microseconds -> nanoseconds
			}
			return
		}
		key := strings.ToLower(strings.TrimLeft(name, "_"))
		if key == "message" {
			key = "_msg"
		}
		if key != "" {
			fields[key] = string(val)
		}
	}

	i, n := 0, len(data)
	for i < n {
		if data[i] == '\n' { // blank line: entry boundary
			emit()
			i++
			continue
		}
		ns := i
		for i < n && data[i] != '\n' && data[i] != '=' {
			i++
		}
		name := string(data[ns:i])
		switch {
		case i < n && data[i] == '=': // text field
			i++
			vs := i
			for i < n && data[i] != '\n' {
				i++
			}
			set(name, data[vs:i])
			if i < n {
				i++ // consume newline
			}
		case i < n && data[i] == '\n': // binary field: length-prefixed value
			i++
			// A malformed length DISCARDS THE REST OF THE UPLOAD, so it must
			// be reported. Both of these branches used to set i = n and fall
			// out of the loop with no rejection count and no warning: every
			// entry after the bad field was lost and the request answered
			// success. IngestJournald could not return a failure at all, which
			// is why the listener's error handling was unreachable.
			if i+8 > n {
				res.Rejected++
				res.Warn(int64(i), "binary field %q: %d bytes of length prefix, need 8; "+
					"the remainder of the upload is not parseable", name, n-i)
				return res, envelopeErr(errJournaldTruncated)
			}
			ln := binary.LittleEndian.Uint64(data[i : i+8])
			i += 8
			// Compared as uint64 against the REMAINING bytes, so a length near
			// 2^64 cannot wrap when it is narrowed to int on a 32-bit build.
			if ln > uint64(n-i) {
				res.Rejected++
				res.Warn(int64(i), "binary field %q declares %d bytes, %d remain; "+
					"the remainder of the upload is not parseable", name, ln, n-i)
				return res, envelopeErr(errJournaldTruncated)
			}
			set(name, data[i:i+int(ln)])
			i += int(ln)
			if i < n && data[i] == '\n' {
				i++
			}
		default:
			// A name that ends at EOF with neither '=' nor a newline: the
			// upload was cut mid-field. Same treatment -- reported, not
			// silently dropped.
			if len(trimSpace(data[ns:])) > 0 {
				res.Rejected++
				res.Warn(int64(ns), "field %q ends without a value separator; "+
					"the upload is truncated", name)
				emit()
				return res, envelopeErr(errJournaldTruncated)
			}
			i = n
		}
	}
	emit() // trailing entry with no closing blank line
	return res, nil
}

var errJournaldTruncated = errors.New("journal export is truncated")
