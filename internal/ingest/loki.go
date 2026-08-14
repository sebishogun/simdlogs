package ingest

import (
	"encoding/json"
	"errors"
)

// lokiPush is Grafana Loki's push payload: streams of label sets, each with a
// list of [timestamp-ns, line] entries. A third element carries structured
// metadata as a JSON object -- Loki 3.x's home for the high-cardinality
// attributes that must NOT become labels, which is exactly where a trace id or
// a request id ends up. It used to be discarded, so those fields were accepted
// with a 204 and then were not there.
// https://grafana.com/docs/loki push API.
type lokiPush struct {
	Streams []struct {
		Stream map[string]string   `json:"stream"`
		Values [][]json.RawMessage `json:"values"`
	} `json:"streams"`
}

var errNoStreams = errors.New("no streams field: not a Loki push payload")

// IngestLoki ingests a Loki push body: each stream's labels become fields, its
// log line becomes _msg, and the nanosecond timestamp is taken from the pair
// (fallback when absent or unparseable). One record per value entry.
func IngestLoki(w *Writer, data []byte, fallback func() int64) (Result, error) {
	return IngestLokiOpts(w, data, fallback, nil)
}

// IngestLokiOpts is IngestLoki with the request's field mappings applied.
func IngestLokiOpts(w *Writer, data []byte, fallback func() int64, opts *Options) (Result, error) {
	var res Result
	mapped := !opts.Empty()
	var p lokiPush
	if err := json.Unmarshal(data, &p); err != nil {
		// A push whose JSON does not parse is a failed request, not an empty
		// one. Returning zero records and no error made it answer 200, so a
		// misconfigured agent looked healthy while nothing was stored.
		return res, encodingErr(err)
	}
	if p.Streams == nil {
		return res, envelopeErr(errNoStreams)
	}
	fields := map[string]string{}
	for _, st := range p.Streams {
		for _, ent := range st.Values {
			if len(ent) < 2 {
				res.Rejected++
				res.Warn(0, "stream entry has fewer than two elements")
				continue
			}
			var tsStr, line string
			_ = json.Unmarshal(ent[0], &tsStr)
			_ = json.Unmarshal(ent[1], &line)
			// The optional third element: structured metadata. A malformed one
			// is a warning against that entry, not a silent drop and not a
			// failed batch -- the line itself is still worth storing.
			var meta map[string]string
			if len(ent) >= 3 {
				if err := json.Unmarshal(ent[2], &meta); err != nil {
					res.Warn(0, "entry's structured metadata is not an object: %v", err)
					meta = nil
				}
			}
			for k := range fields {
				delete(fields, k)
			}
			for k, v := range st.Stream {
				fields[k] = v
			}
			// After the labels: an entry's own metadata is more specific than
			// its stream's labels, and the protobuf path applies it in the same
			// order.
			// Same guard as the protobuf path: an empty key was stored as a
			// field with no name, and an empty value erased the stream label
			// it collided with. Without both, the two encodings disagree on
			// exactly the input proto3 makes most likely.
			for k, v := range meta {
				if k == "" || v == "" {
					continue
				}
				fields[k] = v
			}
			fields["_msg"] = line
			ts, ok := parseTime(tsStr)
			if !ok {
				ts = fallback()
			}
			if mapped {
				opts.apply(fields)
			}
			addWithStream(w, ts, fields, opts)
			res.Accepted++
		}
	}
	return res, nil
}
