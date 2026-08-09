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
	"sync/atomic"
	"time"
)

// SetBackends puts the node in select-router mode: /select/logsql/query fans
// out to these peer base URLs and merges their rows, the way VictoriaLogs'
// vmselect queries a set of vmstorage nodes. With no backends the node serves
// its own store.
func (s *Server) SetBackends(urls []string) { s.backends = urls }

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
// Round-robin gives one copy per record; replication (R copies) is a follow-up.
func (s *Server) forwardWrite(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	idx := int(atomic.AddInt64(&s.rr, 1)-1) % len(s.backends)
	req, err := http.NewRequestWithContext(r.Context(), r.Method, s.backends[idx]+r.URL.Path, bytes.NewReader(body))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	req.Header = r.Header.Clone()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, err.Error(), 502)
		return
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// federatedSelect queries every backend concurrently, merges the NDJSON rows
// in time order (newest first), and applies the limit across the merged set.
// Row merge is exact -- rows are independent, so concatenate-sort-limit is the
// correct distributed answer. Tenant headers propagate so each backend answers
// for the same tenant.
func (s *Server) federatedSelect(w http.ResponseWriter, r *http.Request) {
	type row struct {
		t    int64
		line string
	}
	var mu sync.Mutex
	var all []row
	var wg sync.WaitGroup
	raw := r.URL.RawQuery
	for _, b := range s.backends {
		wg.Add(1)
		go func(b string) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(r.Context(), "GET", b+"/select/logsql/query?"+raw, nil)
			if err != nil {
				return
			}
			for _, h := range []string{"AccountID", "ProjectID"} {
				if v := r.Header.Get(h); v != "" {
					req.Header.Set(h, v)
				}
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			sc := bufio.NewScanner(resp.Body)
			sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
			var local []row
			for sc.Scan() {
				line := sc.Text()
				if line == "" {
					continue
				}
				local = append(local, row{t: rowLineTime(line), line: line})
			}
			mu.Lock()
			all = append(all, local...)
			mu.Unlock()
		}(b)
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
		bw.WriteString(rw.line)
		bw.WriteByte('\n')
	}
}

// fanOut sends the same GET (path + the request's raw query + tenant headers)
// to every backend concurrently and returns the response bodies. A backend
// that errors contributes nothing rather than failing the whole query.
func (s *Server) fanOut(r *http.Request, path string) [][]byte {
	out := make([][]byte, len(s.backends))
	var wg sync.WaitGroup
	for i, b := range s.backends {
		wg.Add(1)
		go func(i int, b string) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(r.Context(), "GET", b+path+"?"+r.URL.RawQuery, nil)
			if err != nil {
				return
			}
			for _, h := range []string{"AccountID", "ProjectID"} {
				if v := r.Header.Get(h); v != "" {
					req.Header.Set(h, v)
				}
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			out[i] = body
		}(i, b)
	}
	wg.Wait()
	return out
}

// fanOutPost sends the same POST (body + tenant headers) to every backend and
// returns the response bodies -- the ES surface fans out this way, since its
// query is a JSON body rather than URL params.
func (s *Server) fanOutPost(r *http.Request, path string, body []byte) [][]byte {
	out := make([][]byte, len(s.backends))
	var wg sync.WaitGroup
	for i, b := range s.backends {
		wg.Add(1)
		go func(i int, b string) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(r.Context(), "POST", b+path, bytes.NewReader(body))
			if err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/json")
			for _, h := range []string{"AccountID", "ProjectID"} {
				if v := r.Header.Get(h); v != "" {
					req.Header.Set(h, v)
				}
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			rb, _ := io.ReadAll(resp.Body)
			out[i] = rb
		}(i, b)
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
	if t, err := time.Parse(time.RFC3339Nano, line[i:i+j]); err == nil {
		return t.UnixNano()
	}
	return 0
}
