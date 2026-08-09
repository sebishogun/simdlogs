package ingest

import (
	"encoding/json"
	"strings"
)

// IngestDatadog ingests the Datadog logs intake body: a JSON array of log
// objects (or a single object). "message" becomes _msg, "ddtags" is split into
// comma-separated key:value fields, "timestamp"/"date" set the time (ms since
// epoch as a number, or an RFC3339/ns string), and every other attribute
// becomes a field. https://docs.datadoghq.com/api/latest/logs/ (/api/v2/logs).
func IngestDatadog(w *Writer, data []byte, fallback func() int64) (ingested, skipped int) {
	trim := trimSpace(data)
	var entries []map[string]json.RawMessage
	if len(trim) > 0 && trim[0] == '[' {
		if err := json.Unmarshal(trim, &entries); err != nil {
			return 0, 0
		}
	} else {
		var one map[string]json.RawMessage
		if err := json.Unmarshal(trim, &one); err != nil {
			return 0, 0
		}
		entries = []map[string]json.RawMessage{one}
	}
	fields := map[string]string{}
	for _, e := range entries {
		for k := range fields {
			delete(fields, k)
		}
		var ts int64
		haveTS := false
		for k, raw := range e {
			switch k {
			case "message":
				fields["_msg"] = rawToString(raw)
			case "ddtags":
				for _, kv := range strings.Split(rawToString(raw), ",") {
					if i := strings.IndexByte(kv, ':'); i > 0 {
						fields[kv[:i]] = kv[i+1:]
					}
				}
			case "timestamp", "date":
				if t, ok := ddTime(raw); ok {
					ts, haveTS = t, true
				}
			default:
				fields[k] = rawToString(raw)
			}
		}
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

// ddTime reads a Datadog timestamp: a JSON number is milliseconds since epoch
// (Datadog's default), a string is parsed as ns/RFC3339 via parseTime.
func ddTime(raw json.RawMessage) (int64, bool) {
	var f float64
	if json.Unmarshal(raw, &f) == nil {
		return int64(f) * 1_000_000, true // ms -> ns
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return parseTime(s)
	}
	return 0, false
}

// rawToString renders a JSON value as a plain string: a JSON string is
// unquoted, anything else (number, bool, object) keeps its source bytes.
func rawToString(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw)
}
