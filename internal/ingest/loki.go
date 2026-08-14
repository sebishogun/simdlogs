package ingest

import (
	"encoding/json"
	"errors"
)

// lokiPush is Grafana Loki's push payload: streams of label sets, each with a
// list of [timestamp-ns, line] pairs (an optional third element carries
// structured metadata, which we ignore). https://grafana.com/docs/loki push API.
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
			for k := range fields {
				delete(fields, k)
			}
			for k, v := range st.Stream {
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
