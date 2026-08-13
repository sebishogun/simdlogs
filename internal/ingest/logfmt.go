package ingest

import "strings"

// IngestLogfmt parses logfmt lines (`key=value key2="quoted value" flag`) and
// appends each as a record -- the format Heroku, Go-kit, and many services
// emit. Keys without a value become an empty field; a bare word becomes a
// present-but-empty field. Timestamp comes from _time/@timestamp or the
// fallback. Malformed nothing here -- every line yields a record.
func IngestLogfmt(w *Writer, data []byte, fallback func() int64) (ingested, skipped int) {
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
		var ts int64
		haveTS := false
		parseLogfmtLine(line, func(k, v string) {
			if isTimeKey(k) {
				if t, ok := parseTime(v); ok {
					// Stored once, in the timestamp column -- see jsonline.
					ts, haveTS = t, true
					return
				}
			}
			fields[k] = v
		})
		if len(fields) == 0 {
			skipped++
			continue
		}
		if !haveTS {
			ts = fallback()
		}
		w.Add(ts, fields)
		ingested++
	}
	return ingested, skipped
}

// parseLogfmtLine tokenizes one logfmt line, calling emit per key/value.
func parseLogfmtLine(line []byte, emit func(k, v string)) {
	i, n := 0, len(line)
	for i < n {
		for i < n && (line[i] == ' ' || line[i] == '\t') {
			i++
		}
		if i >= n {
			return
		}
		ks := i
		for i < n && line[i] != '=' && line[i] != ' ' && line[i] != '\t' {
			i++
		}
		key := string(line[ks:i])
		val := ""
		if i < n && line[i] == '=' {
			i++
			if i < n && line[i] == '"' {
				i++
				var sb strings.Builder
				for i < n && line[i] != '"' {
					if line[i] == '\\' && i+1 < n {
						i++
						sb.WriteByte(line[i])
						i++
						continue
					}
					sb.WriteByte(line[i])
					i++
				}
				if i < n {
					i++ // closing quote
				}
				val = sb.String()
			} else {
				vs := i
				for i < n && line[i] != ' ' && line[i] != '\t' {
					i++
				}
				val = string(line[vs:i])
			}
		}
		if key != "" {
			emit(key, val)
		}
	}
}
