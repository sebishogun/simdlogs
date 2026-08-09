package api

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

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
		Items []any `json:"items"`
	}
	json.NewDecoder(r.Body).Decode(&resp)
	r.Body.Close()
	if len(resp.Items) != 3 {
		t.Fatalf("bulk items = %d want 3 (delete has no doc)", len(resp.Items))
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
