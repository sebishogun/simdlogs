package ingest

import (
	"encoding/binary"
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
func IngestJournald(w *Writer, data []byte, fallback func() int64) (ingested, skipped int) {
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
			return
		}
		if !haveTS {
			ts = fallback()
		}
		w.Add(ts, fields)
		ingested++
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
			if i+8 > n {
				i = n
				break
			}
			ln := binary.LittleEndian.Uint64(data[i : i+8])
			i += 8
			if ln > uint64(n-i) {
				i = n
				break
			}
			set(name, data[i:i+int(ln)])
			i += int(ln)
			if i < n && data[i] == '\n' {
				i++
			}
		default:
			i = n
		}
	}
	emit() // trailing entry with no closing blank line
	return ingested, skipped
}
