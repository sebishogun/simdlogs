package ingest

import "encoding/json"

// lokiPush is Grafana Loki's push payload: streams of label sets, each with a
// list of [timestamp-ns, line] pairs (an optional third element carries
// structured metadata, which we ignore). https://grafana.com/docs/loki push API.
type lokiPush struct {
	Streams []struct {
		Stream map[string]string   `json:"stream"`
		Values [][]json.RawMessage `json:"values"`
	} `json:"streams"`
}

// IngestLoki ingests a Loki push body: each stream's labels become fields, its
// log line becomes _msg, and the nanosecond timestamp is taken from the pair
// (fallback when absent or unparseable). One record per value entry.
func IngestLoki(w *Writer, data []byte, fallback func() int64) (ingested, skipped int) {
	var p lokiPush
	if err := json.Unmarshal(data, &p); err != nil {
		return 0, 0
	}
	fields := map[string]string{}
	for _, st := range p.Streams {
		for _, ent := range st.Values {
			if len(ent) < 2 {
				skipped++
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
			w.Add(ts, fields)
			ingested++
		}
	}
	return ingested, skipped
}
