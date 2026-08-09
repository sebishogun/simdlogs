package api

import (
	"bufio"
	"bytes"
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
