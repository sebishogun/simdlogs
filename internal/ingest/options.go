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
// RecordLimits bounds one record. They are applied by the writer rather than
// by each parser, so a limit cannot be enforced on one protocol and forgotten
// on the next.
type RecordLimits struct {
	MaxFields     int
	MaxNameBytes  int
	MaxValueBytes int
}

// addWithStreamVec is addWithStream for a record carrying embeddings.
func addWithStreamVec(w *Writer, ts int64, fields map[string]string, o *Options,
	vecs map[string][]float32,
) {
	if len(vecs) == 0 {
		// The caller already parsed the embeddings, so a refusal here can only
		// come from a DIFFERENT configured field arriving as text; it is the
		// same refusal and the caller's Result records it.
		_ = addWithStream(w, ts, fields, o)
		return
	}
	if o != nil && len(o.StreamFields) > 0 {
		if sv := buildStreamLabel(o.StreamFields, fields); sv != "" {
			fields["_stream"] = sv
		} else {
			delete(fields, "_stream")
		}
	}
	w.AddVectors(ts, fields, vecs)
}

// addOrReject writes one record, or counts a refusal against its ordinal with
// the reason attached. Reports whether the record was stored.
//
// One helper rather than the same four lines at nine call sites, for the reason
// the nine call sites exist: a rule applied at eight of them is a rule this
// repository has already shipped as a defect more than once.
func addOrReject(w *Writer, ts int64, fields map[string]string, o *Options, res *Result, ordinal int) bool {
	if err := addWithStream(w, ts, fields, o); err != nil {
		res.Reject(ordinal)
		res.WarnAt(ordinal, "%s", err)
		return false
	}
	res.Accepted++
	return true
}

// addWithStream writes one record and returns an error when it cannot be
// stored AS GIVEN. Every non-JSON-lines protocol goes through here -- Loki,
// logfmt, OTLP in both encodings, Datadog, syslog and journald -- so a rule
// stated here is stated once for all of them.
//
// # The embedding a record was ingested for
//
// Writer.Add DROPS a configured vector field arriving as an ordinary string,
// with a comment explaining why storing 768 floats as dictionary text would be
// the worst case for the dictionary. That is right; the drop is not. Only the
// JSON-lines parser built the float path, so every other protocol stored the
// line, answered the client 2xx, and left the row invisible to the vector
// search it was ingested for. Measured: the same embedding through
// /insert/jsonline is `dim=4 data=[1 2 3 4]` in the store, and through logfmt
// the column is absent, both at accepted=1 rejected=0.
//
// Writer.ValidateVector was written for exactly this -- its doc said "exported
// so the PARSE path refuses the record, with a reason, counted in
// Result.Rejected, rather than the writer silently zero-filling it" -- and it
// had no caller in the year it existed. splitVectors is that path, with a
// better answer than refusing: a vector that PARSES is stored, and only one
// that does not is refused. ValidateVector is deleted rather than left beside
// it, because two functions asking the same question is how they come to
// disagree.
//
// Costs one atomic load per record when no vector field is configured, which is
// almost every deployment.
func addWithStream(w *Writer, ts int64, fields map[string]string, o *Options) error {
	var vecs map[string][]float32
	if w.hasVec.Load() {
		var err error
		if vecs, err = splitVectors(w, fields); err != nil {
			return err
		}
	}
	// The override is a property of the request, not of the row. A row whose
	// label comes out empty still belongs to the overriding request and must
	// not silently pick up the deployment default -- that mixed two labelling
	// schemes inside one column.
	if o != nil && len(o.StreamFields) > 0 {
		if sv := buildStreamLabel(o.StreamFields, fields); sv != "" {
			fields["_stream"] = sv
		} else {
			delete(fields, "_stream")
		}
		if len(vecs) > 0 {
			w.AddStreamOverriddenVectors(ts, fields, vecs)
			return nil
		}
		w.AddStreamOverridden(ts, fields)
		return nil
	}
	if len(vecs) > 0 {
		w.AddVectors(ts, fields, vecs)
		return nil
	}
	w.Add(ts, fields)
	return nil
}

// splitVectors moves every configured embedding out of fields, parsed.
//
// The field is DELETED from fields on success: it is stored as floats, and
// leaving the text behind would put the same data in the dictionary as well,
// which is the cost Writer.Add's drop exists to avoid.
func splitVectors(w *Writer, fields map[string]string) (map[string][]float32, error) {
	flds := w.VectorFields()
	var vecs map[string][]float32
	for name, dim := range flds {
		text, ok := fields[name]
		if !ok {
			continue
		}
		v, err := ParseVector(make([]float32, 0, dim), name, text, dim)
		if err != nil {
			// Refused, not dropped. A record whose embedding is unusable is a
			// record the caller can fix; one stored without it is a row nobody
			// can find and nobody was told about.
			return nil, err
		}
		if vecs == nil {
			vecs = make(map[string][]float32, len(flds))
		}
		vecs[name] = v
		delete(fields, name)
	}
	return vecs, nil
}
