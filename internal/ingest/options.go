package ingest

import "strings"

// Options are the per-request field mappings the reference accepts as query
// args on every insert endpoint. A shipper -- Filebeat, Vector, Fluent Bit,
// Promtail -- is configured with these, so ignoring them is not a cosmetic
// gap: the agent's message lands in a field nothing searches, and its
// timestamp is replaced by ingest time.
//
//	_time_field=ts          take the record's time from `ts`
//	_msg_field=message      the message lives in `message`, store it as _msg
//	_stream_fields=app,env  these fields identify the stream
//	ignore_fields=a,b       drop these before storing
//	extra_fields=env=prod   add these to every record
type Options struct {
	TimeField    string
	MsgField     string
	StreamFields []string
	IgnoreFields []string
	ExtraFields  [][2]string
}

// Empty reports whether the options change nothing, so the ingest loops can
// skip the mapping with one predictable branch per record.
func (o *Options) Empty() bool {
	return o == nil || (o.TimeField == "" && o.MsgField == "" && len(o.StreamFields) == 0 &&
		len(o.IgnoreFields) == 0 && len(o.ExtraFields) == 0)
}

// isTime reports whether key carries the record's timestamp. A configured
// _time_field replaces the defaults rather than adding to them, which is what
// lets a shipper whose records have their own `_time` string still be read.
func (o *Options) isTime(key string) bool {
	if o != nil && o.TimeField != "" {
		return key == o.TimeField
	}
	return isTimeKey(key)
}

// apply rewrites one record's fields: rename the message field, drop the
// ignored ones, add the extras. Called once per record, after parsing.
func (o *Options) apply(fields map[string]string) {
	if o == nil {
		return
	}
	if o.MsgField != "" && o.MsgField != "_msg" {
		if v, ok := fields[o.MsgField]; ok {
			fields["_msg"] = v
			delete(fields, o.MsgField)
		}
	}
	for _, f := range o.IgnoreFields {
		delete(fields, f)
	}
	for _, kv := range o.ExtraFields {
		fields[kv[0]] = kv[1]
	}
	// A configured time field is consumed by the timestamp, never stored: it
	// would otherwise be a second copy of the record's time.
	if o.TimeField != "" {
		delete(fields, o.TimeField)
	}
}

// ParseOptions reads the options out of a request's query values. get is the
// caller's URL query lookup, kept as a function so this package does not
// depend on net/http.
func ParseOptions(get func(string) string) Options {
	return Options{
		TimeField:    get("_time_field"),
		MsgField:     get("_msg_field"),
		StreamFields: splitList(get("_stream_fields")),
		IgnoreFields: splitList(get("ignore_fields")),
		ExtraFields:  parsePairs(get("extra_fields")),
	}
}

func splitList(v string) []string {
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// parsePairs reads `k=v,k2=v2` -- the extra_fields spelling.
func parsePairs(v string) [][2]string {
	if v == "" {
		return nil
	}
	var out [][2]string
	for _, p := range strings.Split(v, ",") {
		if eq := strings.IndexByte(p, '='); eq > 0 {
			k := strings.TrimSpace(p[:eq])
			if k != "" {
				out = append(out, [2]string{k, strings.TrimSpace(p[eq+1:])})
			}
		}
	}
	return out
}

// addWithStream writes one record, honouring a per-request _stream_fields.
// The writer's own stream fields are a deployment-wide default; a request that
// names its own must not be forced through them, and must not end up with two
// _stream columns in the same group.
func addWithStream(w *Writer, ts int64, fields map[string]string, o *Options) {
	if o != nil && len(o.StreamFields) > 0 {
		if sv := buildStreamLabel(o.StreamFields, fields); sv != "" {
			fields["_stream"] = sv
		}
	}
	w.Add(ts, fields)
}
