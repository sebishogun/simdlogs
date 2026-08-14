package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
)

// The route contract gate.
//
// internal/bench's TestAPISurface and TestLogsQLCompat are REPORTS: they need a
// staged VictoriaLogs binary and SIMDLOGS_COMPAT=1, they compare against
// whatever the reference happens to do, and most of what they find they log
// rather than fail. Nothing in ordinary CI asserted what this server's own
// routes answer, so the following all shipped:
//
//   - /select/logsql/query streamed NDJSON under Content-Type text/plain,
//     while the router's federatedSelect answered the SAME path as
//     application/x-ndjson -- a client's behaviour depended on deployment mode
//   - /select/sql and /select/logsql/stats_query_range likewise announced
//     text/plain for NDJSON and JSON bodies
//   - /insert/jsonline and /insert/logfmt returned a JSON result object as
//     text/plain
//   - /admin/backup answered 200 with an error string appended to a truncated
//     tar when the archive failed partway
//
// So this asserts status, Content-Type and body shape for every route the mux
// registers, against this server alone. It is deliberately not env-gated: a
// contract that is only checked when someone sets an environment variable is a
// contract that regresses between checks.

// contract is one route's answer to one well-formed request.
type contract struct {
	method string
	path   string // with query string
	ctIn   string // request Content-Type, for the write routes
	body   string
	token  string

	wantStatus int
	wantCT     string // exact match on the media type, parameters ignored
	// check inspects the body. nil means the body is not part of the contract
	// (an HTML page, a tar), which is stated rather than skipped silently.
	check func(t *testing.T, body []byte)
}

func jsonObject(fields ...string) func(*testing.T, []byte) {
	return func(t *testing.T, body []byte) {
		t.Helper()
		var m map[string]any
		if err := json.Unmarshal(body, &m); err != nil {
			t.Errorf("body is not a JSON object: %v\n%s", err, first(body, 200))
			return
		}
		for _, f := range fields {
			if _, ok := m[f]; !ok {
				t.Errorf("JSON object has no field %q; keys are %v", f, keysOf(m))
			}
		}
	}
}

// ndjson requires every non-empty line to be its own JSON object -- the
// property that makes the stream parseable one line at a time, which is the
// entire reason the format is used here.
func ndjson(minLines int) func(*testing.T, []byte) {
	return func(t *testing.T, body []byte) {
		t.Helper()
		n := 0
		for i, ln := range strings.Split(string(body), "\n") {
			ln = strings.TrimSpace(ln)
			if ln == "" {
				continue
			}
			var m map[string]any
			if err := json.Unmarshal([]byte(ln), &m); err != nil {
				t.Errorf("line %d is not a JSON object: %v\n%s", i, err, first(ln, 200))
				return
			}
			n++
		}
		if n < minLines {
			t.Errorf("%d NDJSON lines, want at least %d", n, minLines)
		}
	}
}

func promEnvelope(wantType string) func(*testing.T, []byte) {
	return func(t *testing.T, body []byte) {
		t.Helper()
		var env struct {
			Status string `json:"status"`
			Data   struct {
				ResultType string            `json:"resultType"`
				Result     []json.RawMessage `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &env); err != nil {
			t.Errorf("not a Prometheus envelope: %v\n%s", err, first(body, 200))
			return
		}
		if env.Status != "success" {
			t.Errorf("status = %q, want success", env.Status)
		}
		if env.Data.ResultType != wantType {
			t.Errorf("resultType = %q, want %q", env.Data.ResultType, wantType)
		}
	}
}

func keysOf(m map[string]any) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func first(v any, n int) string {
	var s string
	switch x := v.(type) {
	case []byte:
		s = string(x)
	case string:
		s = x
	}
	s = strings.ReplaceAll(s, "\n", "\\n")
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

const (
	ctJSON   = "application/json"
	ctNDJSON = "application/x-ndjson"
	ctText   = "text/plain"
	ctHTML   = "text/html"
	ctProm   = "text/plain" // Prometheus exposition carries version= parameters
)

func routeContracts() []contract {
	q := url.QueryEscape("*")
	stats := url.QueryEscape("* | stats count() n")
	nd := `{"_time":"2024-05-01T00:00:00Z","_msg":"probe","level":"info"}` + "\n"

	return []contract{
		// --- select: NDJSON row streams ---
		{method: "GET", path: "/select/logsql/query?query=" + q + "&limit=5", token: tokQuery,
			wantStatus: 200, wantCT: ctNDJSON, check: ndjson(1)},
		{method: "GET", path: "/select/sql?query=" + url.QueryEscape("SELECT level FROM logs LIMIT 2"),
			token: tokQuery, wantStatus: 200, wantCT: ctNDJSON, check: ndjson(1)},

		// --- select: JSON documents ---
		{method: "GET", path: "/select/logsql/hits?query=" + q + "&step=1h", token: tokQuery,
			wantStatus: 200, wantCT: ctJSON, check: jsonObject("hits")},
		{method: "GET", path: "/select/logsql/facets?query=" + q, token: tokQuery,
			wantStatus: 200, wantCT: ctJSON, check: jsonObject("facets")},
		{method: "GET", path: "/select/logsql/field_names?query=" + q, token: tokQuery,
			wantStatus: 200, wantCT: ctJSON, check: jsonObject("values")},
		{method: "GET", path: "/select/logsql/field_values?query=" + q + "&field=level", token: tokQuery,
			wantStatus: 200, wantCT: ctJSON, check: jsonObject("values")},
		{method: "GET", path: "/select/logsql/stream_field_names?query=" + q, token: tokQuery,
			wantStatus: 200, wantCT: ctJSON, check: jsonObject("values")},
		{method: "GET", path: "/select/logsql/stream_field_values?query=" + q + "&field=level", token: tokQuery,
			wantStatus: 200, wantCT: ctJSON, check: jsonObject("values")},
		{method: "GET", path: "/select/logsql/stream_ids?query=" + q, token: tokQuery,
			wantStatus: 200, wantCT: ctJSON, check: jsonObject("values")},
		{method: "GET", path: "/select/logsql/streams?query=" + q, token: tokQuery,
			wantStatus: 200, wantCT: ctJSON, check: jsonObject("values")},

		// --- select: the Prometheus envelope, instant and range ---
		{method: "GET", path: "/select/logsql/stats_query?query=" + stats, token: tokQuery,
			wantStatus: 200, wantCT: ctJSON, check: promEnvelope("vector")},
		{method: "GET", path: "/select/logsql/stats_query_range?query=" + stats +
			"&start=1714521600&end=1714608000&step=1h", token: tokQuery,
			wantStatus: 200, wantCT: ctJSON, check: promEnvelope("matrix")},

		// --- ingest: this server's own result object ---
		{method: "POST", path: "/insert/jsonline", ctIn: ctNDJSON, body: nd, token: tokIngest,
			wantStatus: 200, wantCT: ctJSON, check: jsonObject("ingested", "skipped")},
		{method: "POST", path: "/insert/logfmt", ctIn: ctNDJSON, body: "_msg=hi level=info\n",
			token: tokIngest, wantStatus: 200, wantCT: ctJSON, check: jsonObject("ingested", "skipped")},

		// --- ingest: the status codes each protocol's clients require. These
		// differ on purpose -- Loki wants 204, Datadog 202, Elasticsearch a
		// 200 with a per-item body -- and a "tidy-up" that made them uniform
		// would break every one of those agents.
		{method: "POST", path: "/insert/elasticsearch/_bulk", ctIn: ctNDJSON,
			body: "{\"create\":{}}\n" + nd, token: tokIngest,
			wantStatus: 200, wantCT: ctJSON, check: jsonObject("took", "errors", "items")},
		{method: "POST", path: "/_bulk", ctIn: ctNDJSON, body: "{\"create\":{}}\n" + nd, token: tokIngest,
			wantStatus: 200, wantCT: ctJSON, check: jsonObject("took", "errors", "items")},
		{method: "POST", path: "/insert/loki/api/v1/push", ctIn: ctJSON,
			body:  `{"streams":[{"stream":{"app":"p"},"values":[["1714521600000000000","probe"]]}]}`,
			token: tokIngest, wantStatus: 204},
		{method: "POST", path: "/loki/api/v1/push", ctIn: ctJSON,
			body:  `{"streams":[{"stream":{"app":"p"},"values":[["1714521600000000000","probe"]]}]}`,
			token: tokIngest, wantStatus: 204},
		{method: "POST", path: "/insert/datadog/api/v2/logs", ctIn: ctJSON,
			body: `[{"message":"probe","ddsource":"p"}]`, token: tokIngest, wantStatus: 202},
		{method: "POST", path: "/api/v2/logs", ctIn: ctJSON,
			body: `[{"message":"probe","ddsource":"p"}]`, token: tokIngest, wantStatus: 202},
		{method: "POST", path: "/v1/input", ctIn: ctJSON,
			body: `[{"message":"probe","ddsource":"p"}]`, token: tokIngest, wantStatus: 202},
		{method: "POST", path: "/insert/datadog/api/v1/validate", ctIn: ctJSON, token: tokIngest,
			wantStatus: 200},
		{method: "POST", path: "/insert/opentelemetry/v1/logs", ctIn: ctJSON,
			body: `{"resourceLogs":[]}`, token: tokIngest, wantStatus: 200, wantCT: ctJSON},
		{method: "POST", path: "/v1/logs", ctIn: ctJSON, body: `{"resourceLogs":[]}`,
			token: tokIngest, wantStatus: 200, wantCT: ctJSON},
		{method: "POST", path: "/insert/journald", ctIn: "application/octet-stream",
			body: "MESSAGE=probe\n\n", token: tokIngest, wantStatus: 202},
		{method: "POST", path: "/insert/syslog", ctIn: ctText,
			body: "<13>1 2024-05-01T00:00:00Z h a - - - probe\n", token: tokIngest, wantStatus: 204},

		// --- vector search: a POST, because the k-NN target vector is a body.
		// A GET reaches it and answers 400 "EOF" (no body to decode), which is
		// what task 6.7 is about; the contract here is the reachable form.
		{method: "POST", path: "/select/vector", ctIn: ctJSON,
			body: `{"field":"emb","vector":[0.1,0.2,0.3],"k":2}`, token: tokQuery,
			wantStatus: 200, wantCT: ctNDJSON},

		// --- Elasticsearch read surface ---
		{method: "POST", path: "/_search", ctIn: ctJSON, body: `{"size":2}`, token: tokQuery,
			wantStatus: 200, wantCT: ctJSON, check: jsonObject("hits")},
		{method: "POST", path: "/_count", ctIn: ctJSON, body: `{}`, token: tokQuery,
			wantStatus: 200, wantCT: ctJSON, check: jsonObject("count")},

		// --- ops ---
		{method: "GET", path: "/metrics", token: tokAdmin, wantStatus: 200, wantCT: ctProm},
		{method: "GET", path: "/alerts", token: tokQuery, wantStatus: 200, wantCT: ctJSON,
			check: jsonObject("alerts")},
		{method: "GET", path: "/flags", token: tokAdmin, wantStatus: 200, wantCT: ctText},
		{method: "GET", path: "/health", wantStatus: 200, wantCT: ctText},
		{method: "GET", path: "/-/healthy", wantStatus: 200, wantCT: ctText},
		{method: "GET", path: "/-/ready", wantStatus: 200, wantCT: ctText},
		{method: "GET", path: "/insert/ready", wantStatus: 200},

		// --- UI: body shape is not part of the contract, the media type is ---
		{method: "GET", path: "/vmui", token: tokQuery, wantStatus: 200, wantCT: ctHTML},
		{method: "GET", path: "/select/vmui", token: tokQuery, wantStatus: 200, wantCT: ctHTML},
		{method: "GET", path: "/", token: tokQuery, wantStatus: 200, wantCT: ctHTML},

		// --- backup: a tar, and the body is bytes rather than a shape ---
		{method: "GET", path: "/admin/backup", token: tokAdmin, wantStatus: 200,
			wantCT: "application/x-tar"},
	}
}

// contractExempt names the routes deliberately absent from the table, each
// with the reason. The completeness gate below reads this, so a new route
// cannot be added without either a contract or a stated reason.
var contractExempt = map[string]string{
	"/select/logsql/tail": "a streaming endpoint that never returns; covered by tail_test.go",
}

func TestRouteContracts(t *testing.T) {
	_, ts := authedServer(t)

	// One record, so every read route has something to answer with.
	resp := do(t, ts, http.MethodPost, "/insert/jsonline", tokIngest, nil,
		`{"_time":"2024-05-01T00:00:00Z","_msg":"boom","level":"error","service":"api"}`+"\n")
	resp.Body.Close()

	for _, c := range routeContracts() {
		name := c.method + " " + strings.Split(c.path, "?")[0]
		t.Run(name, func(t *testing.T) {
			hdr := map[string]string{}
			if c.ctIn != "" {
				hdr["Content-Type"] = c.ctIn
			}
			resp := do(t, ts, c.method, c.path, c.token, hdr, c.body)
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatalf("reading body: %v", err)
			}

			if resp.StatusCode != c.wantStatus {
				t.Errorf("status = %d, want %d\n%s", resp.StatusCode, c.wantStatus, first(body, 300))
			}
			if c.wantCT != "" {
				// The media type only; charset and version parameters are the
				// server's business.
				got := resp.Header.Get("Content-Type")
				if mt, _, _ := strings.Cut(got, ";"); strings.TrimSpace(mt) != c.wantCT {
					t.Errorf("Content-Type = %q, want media type %q", got, c.wantCT)
				}
			}
			if c.check != nil && resp.StatusCode == c.wantStatus {
				c.check(t, body)
			}
		})
	}
}

// Every registered route is either in the contract table or in the exemption
// map with a reason. Enumerated from the mux rather than listed by hand --
// the hand-written list is what let /_search and /_count ship unauthenticated.
func TestEveryRouteHasAContract(t *testing.T) {
	srv, _ := authedServer(t)

	covered := map[string]bool{}
	for _, c := range routeContracts() {
		covered[strings.Split(c.path, "?")[0]] = true
	}

	paths := srv.registeredPaths()
	if len(paths) != srv.routeCount() {
		t.Fatalf("enumerated %d routes, the mux registered %d", len(paths), srv.routeCount())
	}
	if len(paths) < 40 {
		t.Fatalf("only %d routes enumerated; this gate is not seeing the whole mux", len(paths))
	}
	for _, p := range paths {
		if covered[p] {
			continue
		}
		if reason, ok := contractExempt[p]; ok {
			if reason == "" {
				t.Errorf("%s is exempt with an empty reason", p)
			}
			continue
		}
		t.Errorf("%s has no contract and no exemption: add it to routeContracts, "+
			"or to contractExempt with the reason it cannot have one", p)
	}

	// The exemption map must not outlive its routes either.
	reg := map[string]bool{}
	for _, p := range paths {
		reg[p] = true
	}
	for p := range contractExempt {
		if !reg[p] {
			t.Errorf("%s is exempted but is no longer registered; drop the exemption", p)
		}
	}
}

// A backup that fails PARTWAY cannot be reported with a status code -- the 200
// and the first bytes are already on the wire. It must abandon the response so
// the client sees a truncated transfer, never a clean 200 with an error string
// appended to a plausible-looking archive, which is discovered only at restore
// time.
//
// The first attempt at this panicked with http.ErrAbortHandler and did not
// work: recoverPanic is the OUTERMOST middleware and converted the sentinel
// into a 500, which after the 200 was already sent became "TARBYTESinternal
// error" under a 200. Only net/http's own conn.serve honours the sentinel, so
// recoverPanic now re-panics it. This test is what makes that verifiable.
func TestAbortedResponseIsNotACleanTwoHundred(t *testing.T) {
	// A handler that writes some bytes and then aborts, wrapped in exactly the
	// middleware chain the server uses.
	h := recoverPanic(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-tar")
		w.Write([]byte("TARBYTES"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		panic(http.ErrAbortHandler)
	}))
	ts := httptest.NewServer(h)
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	if err != nil {
		return // the transport rejected it outright: also a failure signal
	}
	body, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()

	if readErr == nil {
		t.Errorf("the client read a COMPLETE body from an aborted response: status %d, body %q. "+
			"A truncated backup that arrives as a clean 200 is discovered at restore time.",
			resp.StatusCode, body)
	}
	if strings.Contains(string(body), "internal error") {
		t.Errorf("the abort was converted into an error page appended to the payload: %q", body)
	}
}

// The other half of the same contract: a panic that is NOT the sentinel must
// still become a 500, because one malformed request must never crash the
// process. Re-panicking the sentinel must not have broken that.
func TestOrdinaryPanicIsStillAFiveHundred(t *testing.T) {
	h := recoverPanic(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("something went wrong")
	}))
	ts := httptest.NewServer(h)
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatalf("an ordinary panic killed the connection instead of answering 500: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
}
