package api

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/sebishogun/simdlogs/internal/query"
	"sync/atomic"
	"time"
)

// SetBackends puts the node in select-router mode: /select/logsql/query fans
// out to these peer base URLs and merges their rows, the way VictoriaLogs'
// vmselect queries a set of vmstorage nodes. With no backends the node serves
// its own store.
func (s *Server) SetBackends(urls []string) { s.backends = urls }

// SetReplicas sets the replication factor: the backends partition into shards
// of r replicas each. A write goes to every replica of its shard; a read takes
// one live replica per shard, so data survives a replica loss without being
// double-counted. r<=1 is plain sharding (no replication).
func (s *Server) SetReplicas(r int) { s.replicas = r }

// SetMaxRows caps how many rows a bare (no-pipe) select may return; 0 = unlimited.
// Over the cap the query errors rather than truncating silently.
func (s *Server) SetMaxRows(n int) { s.maxRows = n }

// shards groups the backends into replica sets of size max(1, replicas).
func (s *Server) shards() [][]string {
	r := s.replicas
	if r < 1 {
		r = 1
	}
	var out [][]string
	for i := 0; i < len(s.backends); i += r {
		hi := i + r
		if hi > len(s.backends) {
			hi = len(s.backends)
		}
		out = append(out, s.backends[i:hi])
	}
	return out
}

// getFromShard tries each replica in the shard in turn until one returns a
// body, so a read tolerates a downed replica. ok is false if all replicas fail.
func (s *Server) getFromShard(r *http.Request, shard []string, path string, post []byte) ([]byte, bool) {
	for _, b := range shard {
		var req *http.Request
		var err error
		if post != nil {
			req, err = http.NewRequestWithContext(r.Context(), "POST", b+path, bytes.NewReader(post))
			if req != nil {
				req.Header.Set("Content-Type", "application/json")
			}
		} else {
			req, err = http.NewRequestWithContext(r.Context(), "GET", b+path+"?"+r.URL.RawQuery, nil)
		}
		if err != nil {
			continue
		}
		for _, h := range []string{"AccountID", "ProjectID"} {
			if v := r.Header.Get(h); v != "" {
				req.Header.Set(h, v)
			}
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			continue // replica down: try the next
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 500 {
			continue
		}
		return body, true
	}
	return nil, false
}

// isWritePath reports whether a path ingests data (and so, in router mode,
// should forward to a storage node rather than be served locally).
func isWritePath(p string) bool {
	switch {
	case strings.HasPrefix(p, "/insert"), p == "/_bulk", p == "/v1/logs", p == "/v1/input",
		strings.HasPrefix(p, "/api/"), strings.HasPrefix(p, "/loki"):
		return true
	}
	return false
}

// routeWrites forwards ingest to a storage node when the node is in router
// mode; everything else falls through. Reads are handled downstream
// (federatedSelect), so only writes are intercepted here.
func (s *Server) routeWrites(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(s.backends) > 0 && isWritePath(r.URL.Path) {
			s.forwardWrite(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// forwardWrite sends the ingest body to one storage node, round-robin, and
// relays its response -- so a burst of inserts spreads across the cluster.
// It round-robins over shards and writes the record to every replica in the
// chosen shard, so a replica loss never loses data.
func (s *Server) forwardWrite(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	shards := s.shards()
	shard := shards[int(atomic.AddInt64(&s.rr, 1)-1)%len(shards)]
	var lastResp *http.Response
	var okAny bool
	for _, b := range shard { // replicate to every member of the shard
		req, err := http.NewRequestWithContext(r.Context(), r.Method, b+r.URL.Path, bytes.NewReader(body))
		if err != nil {
			continue
		}
		req.Header = r.Header.Clone()
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			continue
		}
		if lastResp != nil {
			lastResp.Body.Close()
		}
		lastResp, okAny = resp, true
	}
	if !okAny {
		http.Error(w, "all replicas unreachable", 502)
		return
	}
	defer lastResp.Body.Close()
	w.WriteHeader(lastResp.StatusCode)
	io.Copy(w, lastResp.Body)
}

// federatedSelect queries every backend concurrently, merges the NDJSON rows
// in time order (newest first), and applies the limit across the merged set.
// Row merge is exact -- rows are independent, so concatenate-sort-limit is the
// correct distributed answer. Tenant headers propagate so each backend answers
// for the same tenant.
func (s *Server) federatedSelect(w http.ResponseWriter, r *http.Request) {
	// Rows are kept as slices into each shard's response body, not strings: the
	// merge touches every row of every shard, and a string per row was the
	// router's cost on a big result (a Scanner's Text() copies each line).
	type row struct {
		t    int64
		line []byte
	}
	var mu sync.Mutex
	var all []row
	var wg sync.WaitGroup
	for _, sh := range s.shards() { // one live replica per shard
		wg.Add(1)
		go func(sh []string) {
			defer wg.Done()
			body, ok := s.getFromShard(r, sh, "/select/logsql/query", nil)
			if !ok {
				return
			}
			local := make([]row, 0, bytes.Count(body, []byte{'\n'}))
			for start := 0; start < len(body); {
				e := bytes.IndexByte(body[start:], '\n')
				var line []byte
				if e < 0 {
					line, start = body[start:], len(body)
				} else {
					line, start = body[start:start+e], start+e+1
				}
				if len(line) == 0 {
					continue
				}
				local = append(local, row{t: rowLineTime(bytesToString(line)), line: line})
			}
			mu.Lock()
			all = append(all, local...)
			mu.Unlock()
		}(sh)
	}
	wg.Wait()

	sort.Slice(all, func(i, j int) bool { return all[i].t > all[j].t }) // newest first
	limit := 0
	if v := r.FormValue("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	bw := bufio.NewWriter(w)
	defer bw.Flush()
	for i, rw := range all {
		if limit > 0 && i >= limit {
			break
		}
		bw.Write(rw.line)
		bw.WriteByte('\n')
	}
}

// fanOut sends the GET to one live replica of each shard concurrently and
// returns the response bodies (one per shard), so replicated data is read once.
func (s *Server) fanOut(r *http.Request, path string) [][]byte {
	shards := s.shards()
	out := make([][]byte, len(shards))
	var wg sync.WaitGroup
	for i, sh := range shards {
		wg.Add(1)
		go func(i int, sh []string) {
			defer wg.Done()
			if body, ok := s.getFromShard(r, sh, path, nil); ok {
				out[i] = body
			}
		}(i, sh)
	}
	wg.Wait()
	return out
}

// fanOutPost sends the same POST (body + tenant headers) to every backend and
// returns the response bodies -- the ES surface fans out this way, since its
// query is a JSON body rather than URL params.
func (s *Server) fanOutPost(r *http.Request, path string, body []byte) [][]byte {
	shards := s.shards()
	out := make([][]byte, len(shards))
	var wg sync.WaitGroup
	for i, sh := range shards {
		wg.Add(1)
		go func(i int, sh []string) {
			defer wg.Done()
			if b, ok := s.getFromShard(r, sh, path, body); ok {
				out[i] = b
			}
		}(i, sh)
	}
	wg.Wait()
	return out
}

// federatedESCount sums the ES _count across storage nodes.
func (s *Server) federatedESCount(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	total := 0
	for _, b := range s.fanOutPost(r, "/_count", body) {
		var v struct {
			Count int `json:"count"`
		}
		if json.Unmarshal(b, &v) == nil {
			total += v.Count
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"count": total})
}

// federatedESSearch merges ES _search hits across storage nodes.
func (s *Server) federatedESSearch(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var hits []json.RawMessage
	total := 0
	for _, b := range s.fanOutPost(r, "/_search", body) {
		var v struct {
			Hits struct {
				Total struct {
					Value int `json:"value"`
				} `json:"total"`
				Hits []json.RawMessage `json:"hits"`
			} `json:"hits"`
		}
		if json.Unmarshal(b, &v) == nil {
			total += v.Hits.Total.Value
			hits = append(hits, v.Hits.Hits...)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"hits": map[string]any{
			"total": map[string]any{"value": total, "relation": "eq"},
			"hits":  hits,
		},
	})
}

// federatedValueCounts fans a GET out to the storage nodes and merges the
// value/hits list found under key, summing hits per value -- the cluster form
// of streams/stream_ids/stream_field_values.
func (s *Server) federatedValueCounts(w http.ResponseWriter, r *http.Request, path, key string) {
	w.Header().Set("Content-Type", "application/json")
	counts := map[string]int{}
	for _, b := range s.fanOut(r, path) {
		var v map[string][]query.ValueCount
		if json.Unmarshal(b, &v) == nil {
			for _, vc := range v[key] {
				counts[vc.Value] += vc.Count
			}
		}
	}
	out := make([]query.ValueCount, 0, len(counts))
	for val, c := range counts {
		out = append(out, query.ValueCount{Value: val, Count: c})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Value < out[j].Value
	})
	json.NewEncoder(w).Encode(map[string]any{key: out})
}

// federatedStrings merges a string list under key across storage nodes (union).
func (s *Server) federatedStrings(w http.ResponseWriter, r *http.Request, path, key string) {
	w.Header().Set("Content-Type", "application/json")
	seen := map[string]struct{}{}
	for _, b := range s.fanOut(r, path) {
		var v map[string][]string
		if json.Unmarshal(b, &v) == nil {
			for _, x := range v[key] {
				seen[x] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for x := range seen {
		out = append(out, x)
	}
	sort.Strings(out)
	json.NewEncoder(w).Encode(map[string]any{key: out})
}

// federatedMatrix merges stats_query_range across storage nodes by concatenating
// each shard's series (shards hold disjoint groups, so a series is one shard's).
func (s *Server) federatedMatrix(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var result []json.RawMessage
	for _, b := range s.fanOut(r, "/select/logsql/stats_query_range") {
		var v struct {
			Data struct {
				Result []json.RawMessage `json:"result"`
			} `json:"data"`
		}
		if json.Unmarshal(b, &v) == nil {
			result = append(result, v.Data.Result...)
		}
	}
	json.NewEncoder(w).Encode(map[string]any{
		"status": "success",
		"data":   map[string]any{"resultType": "matrix", "result": result},
	})
}

// federatedStatsQuery merges stats across storage nodes: a total count is
// summed; a group-by count sums each value's hits. (avg/quantile across shards
// need sum+count / sketch merge -- a follow-up; count and count-by are exact.)
func (s *Server) federatedStatsQuery(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	bodies := s.fanOut(r, "/select/logsql/stats_query")
	if r.FormValue("by") == "" {
		total := 0
		for _, b := range bodies {
			var v struct {
				Count int `json:"count"`
			}
			if json.Unmarshal(b, &v) == nil {
				total += v.Count
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"count": total})
		return
	}
	type vc struct {
		Value string `json:"value"`
		Hits  int    `json:"hits"`
	}
	merged := map[string]int{}
	for _, b := range bodies {
		var v struct {
			Stats []vc `json:"stats"`
		}
		if json.Unmarshal(b, &v) == nil {
			for _, s := range v.Stats {
				merged[s.Value] += s.Hits
			}
		}
	}
	stats := make([]vc, 0, len(merged))
	for val, h := range merged {
		stats = append(stats, vc{val, h})
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].Hits > stats[j].Hits })
	json.NewEncoder(w).Encode(map[string]any{"stats": stats})
}

// federatedHits sums per-bucket histogram counts across storage nodes.
func (s *Server) federatedHits(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	type hit struct {
		Time  string `json:"_time"`
		Count int    `json:"hits"`
	}
	merged := map[string]int{}
	for _, b := range s.fanOut(r, "/select/logsql/hits") {
		var v struct {
			Hits []hit `json:"hits"`
		}
		if json.Unmarshal(b, &v) == nil {
			for _, h := range v.Hits {
				merged[h.Time] += h.Count
			}
		}
	}
	out := make([]hit, 0, len(merged))
	for tm, c := range merged {
		out = append(out, hit{tm, c})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Time < out[j].Time })
	json.NewEncoder(w).Encode(map[string]any{"hits": out})
}

// rowLineTime extracts and parses the "_time":"..." value from a result line
// for merge ordering; unparseable lines sort oldest.
func rowLineTime(line string) int64 {
	const k = `"_time":"`
	i := strings.Index(line, k)
	if i < 0 {
		return 0
	}
	i += len(k)
	j := strings.IndexByte(line[i:], '"')
	if j < 0 {
		return 0
	}
	if ns, ok := fastRFC3339Nano(line[i : i+j]); ok {
		return ns
	}
	if t, err := time.Parse(time.RFC3339Nano, line[i:i+j]); err == nil {
		return t.UnixNano()
	}
	return 0
}

// bytesToString views b as a string without copying. Safe here: the bytes are a
// slice of an already-read response body that is never mutated.
func bytesToString(b []byte) string { return unsafe.String(unsafe.SliceData(b), len(b)) }

// fastRFC3339Nano parses the exact shape simdlogs emits --
// 2006-01-02T15:04:05[.fraction]Z -- without time.Parse, which showed up as the
// federated merge's dominant cost (the router parses a timestamp for EVERY row
// returned by every shard, just to order the merge). Anything else returns
// ok=false and the caller falls back to the general parser.
func fastRFC3339Nano(s string) (int64, bool) {
	if len(s) < 20 || s[len(s)-1] != 'Z' || s[4] != '-' || s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':' {
		return 0, false
	}
	d2 := func(at int) (int, bool) {
		a, b := s[at], s[at+1]
		if a < '0' || a > '9' || b < '0' || b > '9' {
			return 0, false
		}
		return int(a-'0')*10 + int(b-'0'), true
	}
	y1, ok1 := d2(0)
	y2, ok2 := d2(2)
	mo, ok3 := d2(5)
	dy, ok4 := d2(8)
	hh, ok5 := d2(11)
	mi, ok6 := d2(14)
	ss, ok7 := d2(17)
	if !(ok1 && ok2 && ok3 && ok4 && ok5 && ok6 && ok7) || mo < 1 || mo > 12 {
		return 0, false
	}
	year := y1*100 + y2
	// Days from the civil epoch (Howard Hinnant's algorithm): exact, no tables.
	yy := year
	if mo <= 2 {
		yy--
	}
	era := yy / 400
	if yy < 0 {
		era = (yy - 399) / 400
	}
	yoe := yy - era*400
	mp := (mo + 9) % 12
	doy := (153*mp+2)/5 + dy - 1
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	days := int64(era)*146097 + int64(doe) - 719468

	ns := (days*86400 + int64(hh)*3600 + int64(mi)*60 + int64(ss)) * 1e9
	if len(s) > 20 { // fractional seconds: ".123456789Z"
		if s[19] != '.' {
			return 0, false
		}
		frac, scale := int64(0), int64(100_000_000)
		for k := 20; k < len(s)-1; k++ {
			c := s[k]
			if c < '0' || c > '9' {
				return 0, false
			}
			if scale > 0 {
				frac += int64(c-'0') * scale
				scale /= 10
			}
		}
		ns += frac
	} else if len(s) != 20 {
		return 0, false
	}
	return ns, true
}
