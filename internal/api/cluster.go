package api

import (
	"bufio"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SetBackends puts the node in select-router mode: /select/logsql/query fans
// out to these peer base URLs and merges their rows, the way VictoriaLogs'
// vmselect queries a set of vmstorage nodes. With no backends the node serves
// its own store.
func (s *Server) SetBackends(urls []string) { s.backends = urls }

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
