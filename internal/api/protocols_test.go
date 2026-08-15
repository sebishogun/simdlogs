package api

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/sebishogun/simdlogs/internal/config"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/ingest"
	"github.com/sebishogun/simdlogs/internal/storage"
)

func TestRecoverPanic(t *testing.T) {
	h := recoverPanic(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/x", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("panic handled with %d, want 500", rec.Code)
	}
}

func TestServerClose(t *testing.T) {
	dir := t.TempDir()
	srv, err := NewServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	postBody(t, ts, `{"_time":1,"service":"a"}`+"\n")
	ts.Close()
	if err := srv.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Reopen the same directory: the data survived shutdown.
	srv2, err := NewServer(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer srv2.Close()
	ts2 := httptest.NewServer(srv2.Handler())
	defer ts2.Close()
	if m := statsBy(t, ts2, "service"); m["a"] != 1 {
		t.Fatalf("after close/reopen = %v, want a:1", m)
	}
}

func TestWebUI(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	r, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if r.StatusCode != 200 || !strings.Contains(string(b), "LogsQL explorer") {
		t.Fatalf("ui: status %d, has title %v", r.StatusCode, strings.Contains(string(b), "LogsQL explorer"))
	}
	if ct := r.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("ui content-type %q", ct)
	}
	// An unknown path is a 404, not the UI.
	r2, err := http.Get(ts.URL + "/definitely-not-a-route")
	if err != nil {
		t.Fatal(err)
	}
	r2.Body.Close()
	if r2.StatusCode != 404 {
		t.Fatalf("unknown path status %d, want 404", r2.StatusCode)
	}
}

func TestAlerting(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	// Timestamps INSIDE the rule's window. They used to be nanoseconds 1 and
	// 2 -- 1970 -- which every rule saw because every rule evaluated over all
	// history. A windowed rule correctly ignores them, which is the whole
	// point: an alert on "errors in the last hour" must not be satisfied by
	// errors from 1970, and with the old window it could never fall back below
	// its threshold either.
	now := time.Now().UnixNano()
	postBody(t, ts, recentAt(now-int64(time.Minute), "error")+recentAt(now-int64(time.Second), "error"))
	if err := srv.AddAlertRule(config.AlertRule{Name: "many_errors", Query: "level:=error",
		Op: ">", Threshold: 1, Window: config.Duration(time.Hour),
		Interval: config.Duration(time.Hour)}); err != nil { // 2 > 1 -> firing
		t.Fatal(err)
	}
	if err := srv.AddAlertRule(config.AlertRule{Name: "too_many_errors", Query: "level:=error",
		Op: ">", Threshold: 5, Window: config.Duration(time.Hour),
		Interval: config.Duration(time.Hour)}); err != nil { // 2 > 5 -> not
		t.Fatal(err)
	}
	var resp struct {
		Alerts []struct {
			Name   string  `json:"name"`
			Firing bool    `json:"firing"`
			Value  float64 `json:"value"`
		}
	}
	getJSON(t, ts.URL+"/alerts", &resp)
	got := map[string]bool{}
	for _, a := range resp.Alerts {
		got[a.Name] = a.Firing
		if a.Value != 2 {
			t.Fatalf("alert %s value = %v want 2", a.Name, a.Value)
		}
	}
	if !got["many_errors"] || got["too_many_errors"] {
		t.Fatalf("alert firing states = %v want many_errors:true too_many_errors:false", got)
	}
}

func TestMetricsFromLogs(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	now := time.Now().UnixNano()
	postBody(t, ts, recentAt(now-int64(3*time.Minute), "error")+
		recentAt(now-int64(2*time.Minute), "error")+
		recentAt(now-int64(time.Minute), "info"))
	if err := srv.AddMetricRule(config.MetricRule{Name: "by_level", Query: "*", By: "level",
		Window: config.Duration(time.Hour), Interval: config.Duration(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := srv.AddMetricRule(config.MetricRule{Name: "errors", Query: "level:=error",
		Window: config.Duration(time.Hour), Interval: config.Duration(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	r, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r.Body)
	r.Body.Close()
	body := string(b)
	for _, want := range []string{
		`logs_by_level{level="error"} 2`,
		`logs_by_level{level="info"} 1`,
		"logs_errors 2",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics-from-logs missing %q:\n%s", want, body)
		}
	}
}

func TestMultitenancyIsolation(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	post := func(acc, body string) {
		req, _ := http.NewRequest("POST", ts.URL+"/insert/jsonline", strings.NewReader(body))
		if acc != "" {
			req.Header.Set("AccountID", acc)
		}
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}
	post("1", `{"_time":1,"service":"tenant1"}`+"\n")
	post("2", `{"_time":2,"service":"tenant2"}`+"\n")

	byService := func(acc string) map[string]int {
		req, _ := http.NewRequest("GET", ts.URL+"/select/logsql/stats_query?query=*&by=service", nil)
		if acc != "" {
			req.Header.Set("AccountID", acc)
		}
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var st struct {
			Stats []struct {
				Value string `json:"value"`
				Hits  int    `json:"hits"`
			}
		}
		json.NewDecoder(r.Body).Decode(&st)
		r.Body.Close()
		m := map[string]int{}
		for _, v := range st.Stats {
			m[v.Value] = v.Hits
		}
		return m
	}
	if m := byService("1"); m["tenant1"] != 1 || m["tenant2"] != 0 {
		t.Fatalf("tenant 1 sees %v, want only tenant1", m)
	}
	if m := byService("2"); m["tenant2"] != 1 || m["tenant1"] != 0 {
		t.Fatalf("tenant 2 sees %v, want only tenant2", m)
	}
	if m := byService(""); len(m) != 0 { // default 0:0 tenant has nothing
		t.Fatalf("default tenant should be empty, saw %v", m)
	}
}

func TestMetrics(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	postBody(t, ts, `{"_time":1,"service":"a","_msg":"x"}`+"\n")
	statsBy(t, ts, "service") // one query request

	r, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r.Body)
	r.Body.Close()
	body := string(b)
	for _, want := range []string{
		"simdlogs_groups 1",
		"simdlogs_rows 1",
		"simdlogs_insert_requests_total 1",
		"simdlogs_query_requests_total 1",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
}

func TestBackupRestore(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	// Two separate inserts -> two immutable group files.
	postBody(t, ts, `{"_time":1,"service":"a","_msg":"x"}`+"\n"+`{"_time":2,"service":"a","_msg":"y"}`+"\n")
	postBody(t, ts, `{"_time":3,"service":"b","_msg":"z"}`+"\n")

	r, err := http.Get(ts.URL + "/admin/backup")
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r.Body); err != nil {
		t.Fatal(err)
	}
	r.Body.Close()

	// A per-tenant backup restores into that tenant's store dir; the default
	// tenant lives under tenant-0-0.
	dir := t.TempDir()
	if err := storage.RestoreTar(&buf, filepath.Join(dir, "tenant-0-0")); err != nil {
		t.Fatalf("restore: %v", err)
	}
	srv2, err := NewServer(dir)
	if err != nil {
		t.Fatalf("open restored store: %v", err)
	}
	ts2 := httptest.NewServer(srv2.Handler())
	defer ts2.Close()
	if st := statsBy(t, ts2, "service"); st["a"] != 2 || st["b"] != 1 {
		t.Fatalf("restored stats by service = %v want a:2 b:1", st)
	}
}

func TestPerStreamRetention(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	srv.SetStreamFields([]string{"app"})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	old := time.Now().Add(-100 * 24 * time.Hour).UnixNano()
	postBody(t, ts, fmt.Sprintf(`{"_time":%d,"app":"chatty","_msg":"x"}`+"\n", old))
	postBody(t, ts, fmt.Sprintf(`{"_time":%d,"app":"important","_msg":"y"}`+"\n", old))

	// chatty: 1h (drops the old group); important: 1000d (keeps it).
	dropped := srv.EnforceRetentionPerStream(0, map[string]time.Duration{
		`{app="chatty"}`:    time.Hour,
		`{app="important"}`: 1000 * 24 * time.Hour,
	})
	if dropped != 1 {
		t.Fatalf("per-stream retention dropped %d, want 1 (chatty)", dropped)
	}
	if st := statsBy(t, ts, "app"); st["important"] != 1 || st["chatty"] != 0 {
		t.Fatalf("after per-stream retention = %v, want only important", st)
	}
}

func TestRetention(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	old := time.Now().Add(-100 * 24 * time.Hour).UnixNano()
	recent := time.Now().UnixNano()
	postBody(t, ts, fmt.Sprintf(`{"_time":%d,"service":"a","_msg":"old"}`+"\n", old))
	postBody(t, ts, fmt.Sprintf(`{"_time":%d,"service":"b","_msg":"new"}`+"\n", recent))
	if n := srv.def.store.Len(); n != 2 {
		t.Fatalf("want 2 groups before retention, got %d", n)
	}
	if dropped := srv.EnforceRetention(24 * time.Hour); dropped != 1 {
		t.Fatalf("dropped %d groups, want 1", dropped)
	}
	if n := srv.def.store.Len(); n != 1 {
		t.Fatalf("want 1 group after retention, got %d", n)
	}
	if st := statsBy(t, ts, "service"); st["a"] != 0 || st["b"] != 1 {
		t.Fatalf("after retention stats by service = %v want only b:1", st)
	}
}

func TestStreamModel(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	srv.SetStreamFields([]string{"app", "host"})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	body := `{"_time":1,"app":"nginx","host":"h1","_msg":"a"}
{"_time":2,"app":"nginx","host":"h1","_msg":"b"}
{"_time":3,"app":"redis","host":"h2","_msg":"c"}
`
	r, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()

	// _stream is synthesized (keys sorted) from the declared stream fields.
	st := statsBy(t, ts, "_stream")
	if st[`{app="nginx",host="h1"}`] != 2 || st[`{app="redis",host="h2"}`] != 1 {
		t.Fatalf("stats by _stream = %v", st)
	}
	// The selector queries the underlying label fields.
	q := "_stream:{app=\"nginx\"}"
	r, err = http.Get(ts.URL + "/select/logsql/query?query=" + url.QueryEscape(q))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if n := len(strings.Split(strings.TrimSpace(string(b)), "\n")); n != 2 {
		t.Fatalf("_stream:{app=nginx} returned %d rows, want 2\n%s", n, b)
	}
}

func statsBy(t *testing.T, ts *httptest.Server, field string) map[string]int {
	t.Helper()
	var st struct {
		Stats []struct {
			Value string `json:"value"`
			Hits  int    `json:"hits"`
		}
	}
	getJSON(t, ts.URL+"/select/logsql/stats_query?query=*&by="+field, &st)
	m := map[string]int{}
	for _, v := range st.Stats {
		m[v.Value] = v.Hits
	}
	return m
}

func TestSyslogIngestHTTP(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	body := "<11>1 2023-11-14T22:13:20Z host1 app1 - - - boom\n" +
		"<11>1 2023-11-14T22:13:21Z host2 app2 - - - boom2\n" +
		"<14>1 2023-11-14T22:13:22Z host3 app3 - - - ok\n"
	r, err := http.Post(ts.URL+"/insert/syslog", "text/plain", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if r.StatusCode != http.StatusNoContent {
		t.Fatalf("syslog status %d want 204", r.StatusCode)
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()
	// PRI 11 -> severity err, PRI 14 -> severity info.
	if s := statsBy(t, ts, "severity"); s["err"] != 2 || s["info"] != 1 {
		t.Fatalf("syslog stats by severity = %v want err:2 info:1", s)
	}
}

func TestSyslogListenUDP(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	udp, tcp, err := srv.ListenSyslog("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	defer tcp.Close()

	c, err := net.Dial("udp", udp.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("<14>1 2023-11-14T22:13:22Z h app - - - datagram")); err != nil {
		t.Fatal(err)
	}

	// UDP + async flush: poll the query until the record lands (or time out).
	deadline := time.Now().Add(3 * time.Second)
	for {
		if statsBy(t, ts, "severity")["info"] == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("syslog UDP datagram never became queryable")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestLogfmtIngest(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	body := `level=error service=api msg="timed out" _time=1700000000000000000
level=info service=db msg=ok _time=1700000000001000000
level=error service=api msg="also timed out" _time=1700000000002000000
`
	r, err := http.Post(ts.URL+"/insert/logfmt", "text/plain", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()

	var st struct {
		Stats []struct {
			Value string `json:"value"`
			Hits  int    `json:"hits"`
		}
	}
	getJSON(t, ts.URL+"/select/logsql/stats_query?query=*&by=level", &st)
	got := map[string]int{}
	for _, v := range st.Stats {
		got[v.Value] = v.Hits
	}
	if got["error"] != 2 || got["info"] != 1 {
		t.Fatalf("logfmt stats by level = %v want error:2 info:1", got)
	}
}

func TestLokiIngest(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	body := `{"streams":[
		{"stream":{"service":"api","level":"error"},"values":[["1700000000000000000","boom"],["1700000000001000000","boom2"]]},
		{"stream":{"service":"db","level":"info"},"values":[["1700000000002000000","ok"]]}
	]}`
	r, err := http.Post(ts.URL+"/loki/api/v1/push", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if r.StatusCode != http.StatusNoContent {
		t.Fatalf("loki push status %d want 204", r.StatusCode)
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()

	var st struct {
		Stats []struct {
			Value string `json:"value"`
			Hits  int    `json:"hits"`
		}
	}
	getJSON(t, ts.URL+"/select/logsql/stats_query?query=*&by=level", &st)
	got := map[string]int{}
	for _, v := range st.Stats {
		got[v.Value] = v.Hits
	}
	if got["error"] != 2 || got["info"] != 1 {
		t.Fatalf("loki stats by level = %v want error:2 info:1", got)
	}
}

func TestDatadogIngest(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	// Array of intake objects: ms timestamp, message->_msg, ddtags split to fields.
	body := `[
		{"message":"boom","service":"api","status":"error","ddtags":"env:prod,team:core","timestamp":1700000000000},
		{"message":"ok","service":"db","status":"info","ddtags":"env:prod","timestamp":1700000000001}
	]`
	r, err := http.Post(ts.URL+"/api/v2/logs", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if r.StatusCode != http.StatusAccepted {
		t.Fatalf("datadog intake status %d want 202", r.StatusCode)
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()

	statsBy := func(field string) map[string]int {
		var st struct {
			Stats []struct {
				Value string `json:"value"`
				Hits  int    `json:"hits"`
			}
		}
		getJSON(t, ts.URL+"/select/logsql/stats_query?query=*&by="+field, &st)
		m := map[string]int{}
		for _, v := range st.Stats {
			m[v.Value] = v.Hits
		}
		return m
	}
	if s := statsBy("status"); s["error"] != 1 || s["info"] != 1 {
		t.Fatalf("datadog stats by status = %v want error:1 info:1", s)
	}
	if e := statsBy("env"); e["prod"] != 2 { // ddtags split into a field
		t.Fatalf("datadog ddtags env = %v want prod:2", e)
	}
}

func TestJournaldIngest(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	var b bytes.Buffer
	// Entry 1: uses a binary length-prefixed MESSAGE whose value contains a
	// newline -- only correct byte-level framing recovers it intact.
	b.WriteString("__REALTIME_TIMESTAMP=1700000000000000\n")
	b.WriteString("PRIORITY=3\n")
	b.WriteString("MESSAGE\n")
	msg := "line1\nline2"
	var ln [8]byte
	binary.LittleEndian.PutUint64(ln[:], uint64(len(msg)))
	b.Write(ln[:])
	b.WriteString(msg)
	b.WriteByte('\n')
	b.WriteString("_HOSTNAME=host1\n")
	b.WriteString("\n") // entry boundary
	// Entry 2: all text fields.
	b.WriteString("__REALTIME_TIMESTAMP=1700000000001000\nPRIORITY=6\nMESSAGE=ok\n_HOSTNAME=host2\n")

	r, err := http.Post(ts.URL+"/insert/journald", "application/vnd.fdo.journal", &b)
	if err != nil {
		t.Fatal(err)
	}
	if r.StatusCode != http.StatusAccepted {
		t.Fatalf("journald status %d want 202", r.StatusCode)
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()

	if p := statsBy(t, ts, "priority"); p["3"] != 1 || p["6"] != 1 {
		t.Fatalf("journald stats by priority = %v want 3:1 6:1", p)
	}
	if h := statsBy(t, ts, "hostname"); h["host1"] != 1 || h["host2"] != 1 { // leading _ stripped
		t.Fatalf("journald stats by hostname = %v want host1:1 host2:1", h)
	}
	if m := statsBy(t, ts, "_msg"); m["line1\nline2"] != 1 || m["ok"] != 1 { // binary value round-trips
		t.Fatalf("journald _msg = %v want the newline-containing message intact", m)
	}
}

func TestOTLPLogsIngest(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	body := `{"resourceLogs":[{"resource":{"attributes":[{"key":"service","value":{"stringValue":"api"}}]},"scopeLogs":[{"logRecords":[
		{"timeUnixNano":"1700000000000000000","severityText":"ERROR","body":{"stringValue":"boom"},"attributes":[{"key":"code","value":{"intValue":"500"}}]},
		{"timeUnixNano":"1700000000001000000","severityText":"ERROR","body":{"stringValue":"boom2"}},
		{"timeUnixNano":"1700000000002000000","severityText":"INFO","body":{"stringValue":"ok"}}
	]}]}]}`
	r, err := http.Post(ts.URL+"/v1/logs", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if r.StatusCode != 200 {
		t.Fatalf("otlp status %d want 200", r.StatusCode)
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()
	if s := statsBy(t, ts, "severity"); s["ERROR"] != 2 || s["INFO"] != 1 {
		t.Fatalf("otlp stats by severity = %v want ERROR:2 INFO:1", s)
	}
	if s := statsBy(t, ts, "service"); s["api"] != 3 { // resource attribute propagated to every record
		t.Fatalf("otlp resource attr service = %v want api:3", s)
	}
	if s := statsBy(t, ts, "code"); s["500"] != 1 { // record attribute, int encoded as string
		t.Fatalf("otlp record attr code = %v want 500:1", s)
	}
}

// TestBulkDocsCompaction guards the in-place compaction: it aliases the
// caller's buffer, so an off-by-one writes a document over one not yet read.
//
// Replaces TestStripBulkActions. The old function detected a delete with
// bytes.Contains(line, `"delete"`), so indexing into an index NAMED delete was
// read as a delete action and the whole rest of the body desynchronized; its
// third case below pins that the parser now reads the action's KEY.
func TestBulkDocsCompaction(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"pairs", "{\"create\":{}}\n{\"a\":1}\n{\"index\":{}}\n{\"b\":2}\n", "{\"a\":1}\n{\"b\":2}\n"},
		{"delete carries no doc", "{\"delete\":{\"_id\":\"1\"}}\n{\"create\":{}}\n{\"a\":1}\n", "{\"a\":1}\n"},
		// An index NAMED delete is an index action, not a delete: the value
		// contains the word, the KEY does not.
		{"index named delete", "{\"index\":{\"_index\":\"delete\"}}\n{\"a\":1}\n{\"index\":{}}\n{\"b\":2}\n",
			"{\"a\":1}\n{\"b\":2}\n"},
		{"no trailing newline", "{\"create\":{}}\n{\"a\":1}", "{\"a\":1}\n"},
		{"blank lines", "{\"create\":{}}\n\n{\"a\":1}\n\n", "{\"a\":1}\n"},
		{"action with no doc", "{\"create\":{}}\n", ""},
		// update's source is a wrapper, not a document: it must not be stored.
		{"update is not a document", "{\"update\":{\"_id\":\"1\"}}\n{\"doc\":{\"a\":1}}\n{\"create\":{}}\n{\"b\":2}\n",
			"{\"b\":2}\n"},
		{"empty", "", ""},
	}
	for _, c := range cases {
		body := []byte(c.in)
		ops, perr := parseBulk(body)
		if perr != "" {
			t.Errorf("%s: parse failed: %s", c.name, perr)
			continue
		}
		got := string(bulkDocs(ops, body))
		if got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}

// TestESBulkParallelPath drives a body over MinParallelBytes so the sharded
// ingest branch is covered, not just the small-body writer.
func TestESBulkParallelPath(t *testing.T) {
	t.Parallel()
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	var sb strings.Builder
	const n = 20000
	for i := 0; i < n; i++ {
		sb.WriteString("{\"create\":{\"_index\":\"logs\"}}\n")
		fmt.Fprintf(&sb, "{\"@timestamp\":\"2023-11-14T22:13:20Z\",\"level\":\"error\",\"seq\":\"%d\"}\n", i)
	}
	if sb.Len() < ingest.MinParallelBytes {
		t.Fatalf("body %d bytes is under MinParallelBytes %d -- test would not cover the parallel path",
			sb.Len(), ingest.MinParallelBytes)
	}
	r, err := http.Post(ts.URL+"/insert/elasticsearch/_bulk", "application/x-ndjson", strings.NewReader(sb.String()))
	if err != nil {
		t.Fatal(err)
	}
	var resp struct {
		Items  []any `json:"items"`
		Errors bool  `json:"errors"`
	}
	json.NewDecoder(r.Body).Decode(&resp)
	r.Body.Close()
	if len(resp.Items) != n || resp.Errors {
		t.Fatalf("bulk items = %d errors = %v, want %d false", len(resp.Items), resp.Errors, n)
	}
	cbody := `{"query":{"bool":{"filter":[{"term":{"level":"error"}}]}}}`
	r, _ = http.Post(ts.URL+"/_count", "application/json", strings.NewReader(cbody))
	var cnt struct{ Count int }
	json.NewDecoder(r.Body).Decode(&cnt)
	r.Body.Close()
	if cnt.Count != n {
		t.Fatalf("bulk _count = %d want %d", cnt.Count, n)
	}
}

func TestESBulkIngest(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	// Action/doc pairs with @timestamp; a delete carries no doc.
	bulk := `{"index":{"_index":"logs"}}
{"@timestamp":"2023-11-14T22:13:20Z","level":"error","service":"api"}
{"create":{"_index":"logs"}}
{"@timestamp":"2023-11-14T22:13:21Z","level":"info","service":"db"}
{"delete":{"_index":"logs","_id":"9"}}
{"index":{"_index":"logs"}}
{"@timestamp":"2023-11-14T22:13:22Z","level":"error","service":"auth"}
`
	r, err := http.Post(ts.URL+"/_bulk", "application/x-ndjson", strings.NewReader(bulk))
	if err != nil {
		t.Fatal(err)
	}
	var resp struct {
		Errors bool `json:"errors"`
		Items  []struct {
			Index *struct {
				Status int `json:"status"`
			} `json:"index"`
			Create *struct {
				Status int `json:"status"`
			} `json:"create"`
			Delete *struct {
				Status int                    `json:"status"`
				Error  *struct{ Type string } `json:"error"`
			} `json:"delete"`
		} `json:"items"`
	}
	json.NewDecoder(r.Body).Decode(&resp)
	r.Body.Close()
	// ONE ITEM PER ACTION, including the delete. This used to be one item per
	// INGESTED DOCUMENT -- three for four actions -- and Elasticsearch clients
	// match items to their requests BY POSITION, so the delete's absence shifted
	// the third action's status onto the fourth action's document.
	if len(resp.Items) != 4 {
		t.Fatalf("bulk items = %d want 4, one per action", len(resp.Items))
	}
	if resp.Items[0].Index == nil || resp.Items[0].Index.Status != 201 {
		t.Errorf("item 0 (index) = %+v, want status 201", resp.Items[0])
	}
	if resp.Items[1].Create == nil || resp.Items[1].Create.Status != 201 {
		t.Errorf("item 1 (create) = %+v, want status 201", resp.Items[1])
	}
	// delete is REJECTED explicitly rather than swallowed: this store is
	// append-only, and a client asking for a deletion that silently did not
	// happen has been told the wrong thing.
	if resp.Items[2].Delete == nil || resp.Items[2].Delete.Status != 400 {
		t.Errorf("item 2 (delete) = %+v, want status 400", resp.Items[2])
	}
	if resp.Items[2].Delete != nil && resp.Items[2].Delete.Error == nil {
		t.Error("the rejected delete carries no error object")
	}
	if resp.Items[3].Index == nil || resp.Items[3].Index.Status != 201 {
		t.Errorf("item 3 (index) = %+v, want status 201", resp.Items[3])
	}
	if !resp.Errors {
		t.Error("errors = false though the delete was rejected")
	}
	// The two error docs are found via the ES _count DSL (@timestamp mapped).
	cbody := `{"query":{"bool":{"filter":[{"term":{"level":"error"}}]}}}`
	r, _ = http.Post(ts.URL+"/_count", "application/json", strings.NewReader(cbody))
	var cnt struct{ Count int }
	json.NewDecoder(r.Body).Decode(&cnt)
	r.Body.Close()
	if cnt.Count != 2 {
		t.Fatalf("bulk _count level=error = %d want 2", cnt.Count)
	}
}

// recentAt is one NDJSON record at an explicit nanosecond timestamp.
//
// Rule fixtures need timestamps inside the rule's window: a rule evaluates
// over the last `window` and a record stamped in 1970 is outside every window
// a rule may configure. Before rules had windows, every fixture timestamp
// worked because every rule read all of history.
func recentAt(ts int64, level string) string {
	return fmt.Sprintf(`{"_time":%d,"level":%q}`, ts, level) + "\n"
}
