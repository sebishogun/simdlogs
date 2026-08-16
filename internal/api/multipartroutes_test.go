package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

// NO registered route leaves a multipart temp file behind.
//
// guard parses a multipart body before the middleware chain replaces the
// request, so the deferred RemoveAll reaches the form the handler used. That
// parse is now opt-in per route (routeSpec.form), because doing it everywhere
// read and buffered the body of routes that never look at one and CONSUMED the
// body of the three that read it themselves.
//
// An opt-in list is only as good as the knowledge that went into it, and
// getting one entry wrong is silent: a route whose handler parses a form while
// `form` is false writes a temp file per request that nothing removes -- the
// exact defect the mechanism exists to prevent, reintroduced by omission.
// Measured on the tree before that mechanism existed: 198 MiB of
// /tmp/multipart-* left behind by one probe run.
//
// So the list is not trusted. This posts a spilling multipart body to EVERY
// path Handler() registered and fails on any file left behind, whatever the
// status code -- a route that answers 415 or 405 leaks nothing, and one that
// answers 200 having parsed a form on a copy leaks every time.
//
// TMPDIR is redirected into the test's own directory, so the count is this
// test's files and not whatever else the machine has in /tmp.
func TestNoRouteLeavesAMultipartTempFileBehind(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)

	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())

	paths := srv.registeredPaths()
	if len(paths) < 20 {
		t.Fatalf("Handler() registered %d routes; this gate is meant to cover "+
			"all of them and is measuring almost nothing", len(paths))
	}

	// A FILE part past the threshold: multipart.ReadForm spills only parts
	// that carry a filename. A value part stays in memory and errors past the
	// budget, which is how an earlier version of this check created no files
	// at all and passed with the fix removed.
	// PAST 32 MiB, and it cannot be made cheaper by lowering a constant: a
	// handler's own r.FormValue calls ParseMultipartForm with net/http's
	// defaultMaxMemory, which is 32 MiB and not ours, and a handler-side parse
	// is precisely what a leaking route does. A smaller body spills nowhere
	// and the gate proves nothing -- which is what the positive control below
	// caught when this was tried.
	pad := strings.Repeat("x", multipartMemory+(1<<20))
	body := "--B\r\nContent-Disposition: form-data; name=\"query\"\r\n\r\n*\r\n" +
		"--B\r\nContent-Disposition: form-data; name=\"pad\"; filename=\"pad.bin\"\r\n" +
		"Content-Type: application/octet-stream\r\n\r\n" + pad + "\r\n--B--\r\n"

	// A POSITIVE CONTROL first, because the check below is an absence and an
	// absence has two causes.
	//
	// A file that exists WHILE a request is in flight is the mechanism
	// working, not a leak -- guard removes it when the handler returns -- so
	// sampling the directory after each request sees nothing on a healthy
	// server AND nothing on a server where no file was ever created. The first
	// version of this gate could not tell those apart, and would have passed
	// with a spill threshold too high to spill.
	//
	// This handler is wired exactly like a route whose spec forgot form:true:
	// no pre-parse, and a handler that parses a form on a request COPY, which
	// is what every middleware below guard produces. It must leave a file.
	leakySpec := readSpec()
	leakySpec.form = false
	leaky := httptest.NewServer(srv.guard(leakySpec, func(w http.ResponseWriter, r *http.Request) {
		cp := r.WithContext(context.WithValue(r.Context(), struct{ k string }{"probe"}, 1))
		_ = cp.FormValue("query")
		w.WriteHeader(200)
	}))
	ctrlBefore := tempMultipartNames(t, tmp)
	if code, _ := postDeadline(t, leaky, "/", `multipart/form-data; boundary=B`, body, ""); code != 200 {
		t.Fatalf("the positive control answered %d", code)
	}
	leaky.Close()
	if n := newNames(ctrlBefore, tempMultipartNames(t, tmp)); len(n) == 0 {
		t.Fatal("the positive control -- a handler that parses a form on a copy " +
			"with no pre-parse -- left NO temp file, so this gate cannot see a " +
			"leak and the check below proves nothing. The spill threshold or the " +
			"file part is wrong.")
	} else {
		for _, name := range n {
			os.Remove(tmp + "/" + name)
		}
	}

	// Now every registered route. A file seen right after the request may
	// still be in flight -- /select/logsql/tail is meant to stay open -- so
	// what is seen is recorded and judged after ts.Close(), which waits for
	// every handler.
	pending := map[string]string{} // temp file name -> the route that made it
	codes := map[string]int{}
	before := tempMultipartNames(t, tmp)
	for _, path := range paths {
		p := path
		if strings.HasSuffix(p, "/") && p != "/" {
			p += "x" // a prefix pattern needs something after it
		}
		was := tempMultipartNames(t, tmp)
		// A DEADLINE per request, because the tail would otherwise hold this
		// loop open forever. The route is not skipped: a tail that spills a
		// file and then blocks is the worst version of this defect.
		code, _ := postDeadline(t, ts, p, `multipart/form-data; boundary=B`, body, "")
		codes[p] = code
		for _, n := range newNames(was, tempMultipartNames(t, tmp)) {
			pending[n] = p
		}
	}

	ts.Close() // waits for every handler, including the disconnected tail
	after := tempMultipartNames(t, tmp)
	for _, n := range newNames(before, after) {
		p, ok := pending[n]
		if !ok {
			p = "(unattributed)"
		}
		t.Errorf("%s (answered %d) left the multipart temp file %s behind:\n"+
			"  a handler parsed a form on a request copy, so nothing removes it; "+
			"either the route's spec needs form:true or the handler must stop "+
			"parsing one", p, codes[p], n)
	}
}

// The routes whose body is a DOCUMENT read it themselves, so nothing upstream
// may consume it.
//
// guard's multipart parse was unconditional, and it drained the body before
// esCount and esSearch decoded it. Measured, a JSON document sent with a
// multipart Content-Type -- the header a proxy or a mis-set client library
// puts on a body that is not one -- against the same document sent four other
// ways. A REAL multipart envelope is a different case and answers 400 on all
// three routes, correctly: it is not the document these routes take.
//
//	                      before the parse   with the parse
//	/_count   node               200                400 "simdlogs: EOF"
//	          router             200                503
//	/_search  node               200                400
//
// docs/lld/cluster.md states the rule these three break: "their body is a JSON
// document, read unconditionally whatever the content type says."
func TestADocumentBodyIsReadUnderEveryFraming(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	if code, rb := postRaw(t, ts, "/insert/jsonline", "application/x-ndjson",
		`{"_msg":"a","lvl":"info"}`+"\n"+`{"_msg":"b","lvl":"error"}`+"\n"); code/100 != 2 {
		t.Fatalf("the fixture did not ingest: %d %.200s", code, rb)
	}

	// THREE routes, not two. /select/vector decodes a JSON document as well,
	// and it went from 200 to 400 "EOF" under multipart when the pre-parse was
	// added -- bisected: 200 at 39e5716, 400 at 5ec8672, while /_count was
	// fixed in the same commit. It is also the one that reads parameters
	// beside the document, so `start`/`end` come from the URL now.
	for _, tc := range []struct{ path, doc string }{
		{"/_count", `{"query":{"match_all":{}}}`},
		{"/_search", `{"query":{"match_all":{}}}`},
		{"/select/vector", `{"field":"emb","vector":[1,0,0],"k":2}`},
	} {
		path, doc := tc.path, tc.doc
		// The reference answer, from a framing that never went near the
		// multipart path, so the multipart case is compared against a real
		// answer rather than against "not an error".
		wantCode, wantBody := postRaw(t, ts, path, "application/json", doc)
		if wantCode != 200 {
			t.Fatalf("%s with application/json answered %d: %.200s", path, wantCode, wantBody)
		}
		for _, ct := range []string{
			`multipart/form-data; boundary=B`,
			`multipart/form-data; boundary=B; charset=utf-8`,
			`application/x-www-form-urlencoded`,
			`text/plain`,
			``,
		} {
			code, rb := postRaw(t, ts, path, ct, doc)
			if code != wantCode || rb != wantBody {
				t.Errorf("%s with Content-Type %q answered %d %.150s;\n"+
					"  application/json answers %d %.150s -- the body is a JSON "+
					"document whatever the framing claims", path, ct, code, rb, wantCode, wantBody)
			}
		}
	}
}

// postDeadline POSTs with a bounded wait and returns 0 for a request that did
// not finish in time.
func postDeadline(t *testing.T, ts *httptest.Server, path, ct, body, token string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	c := &http.Client{Timeout: 3 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b := make([]byte, 512)
	n, _ := resp.Body.Read(b)
	return resp.StatusCode, string(b[:n])
}

func tempMultipartNames(t *testing.T, dir string) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("cannot read %s: %v", dir, err)
	}
	out := map[string]bool{}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "multipart-") {
			out[e.Name()] = true
		}
	}
	return out
}

func newNames(before, after map[string]bool) []string {
	var out []string
	for n := range after {
		if !before[n] {
			out = append(out, n)
		}
	}
	return out
}

// The same sweep against an AUTHENTICATED server, where the request is copied.
//
// The leak needs two things: a handler that parses a form, and a middleware
// that replaced the request first -- net/http removes the temp file by looking
// at the request the SERVER holds, so a form parsed on a copy is never cleaned
// up. On a server with authentication off, withTenant makes no copy for the
// health routes, so net/http cleans up correctly and the sweep above passes
// even where a handler does parse a form.
//
// withPrincipal DOES copy, and /health, /-/healthy and /-/ready are registered
// bare -- outside guard, so no pre-parse, no RemoveAll and no MaxBytesReader.
// Measured before `format` moved to the URL, a 33 MiB multipart POST to each on
// an authenticated server:
//
//	/health     200  multipart-105144472
//	/-/healthy  200  multipart-842413133
//	/-/ready    200  multipart-920247530     all three survived the close
//
// Unbounded, and on routes that answer unauthenticated callers by design. The
// no-auth sweep is the one configuration in which they do not leak, and it was
// the only one being run.
func TestNoRouteLeavesATempFileBehindOnAnAuthenticatedServer(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)

	srv, ts := authedServer(t)
	paths := srv.registeredPaths()
	if len(paths) < 20 {
		t.Fatalf("Handler() registered %d routes", len(paths))
	}

	pad := strings.Repeat("x", multipartMemory+(1<<20))
	body := "--B\r\nContent-Disposition: form-data; name=\"query\"\r\n\r\n*\r\n" +
		"--B\r\nContent-Disposition: form-data; name=\"pad\"; filename=\"pad.bin\"\r\n" +
		"Content-Type: application/octet-stream\r\n\r\n" + pad + "\r\n--B--\r\n"

	pending := map[string]string{}
	codes := map[string]int{}
	before := tempMultipartNames(t, tmp)
	// The admin token: the widest role, so a route is reached rather than
	// turned away at 403 before its handler runs.
	for _, path := range paths {
		p := path
		if strings.HasSuffix(p, "/") && p != "/" {
			p += "x"
		}
		was := tempMultipartNames(t, tmp)
		code, _ := postDeadline(t, ts, p, `multipart/form-data; boundary=B`, body, tokAdmin)
		codes[p] = code
		for _, n := range newNames(was, tempMultipartNames(t, tmp)) {
			pending[n] = p
		}
	}
	ts.Close()
	for _, n := range newNames(before, tempMultipartNames(t, tmp)) {
		p, ok := pending[n]
		if !ok {
			p = "(unattributed)"
		}
		t.Errorf("%s (answered %d) left the multipart temp file %s behind on an "+
			"authenticated server: a handler parsed a form on the request copy "+
			"withPrincipal made", p, codes[p], n)
	}
}

// A route that reads a document must not read a form as well — and the body
// after the document is where that goes wrong.
//
// json.Decode stops at the end of the first value, so a body that is a JSON
// document FOLLOWED by a multipart envelope leaves the rest for whatever reads
// next. With `form: false` there is no pre-parse and no deferred RemoveAll, so
// an `r.FormValue` in the handler parses that tail on the request copy and the
// temp file it spills is never removed. Measured on /select/vector before its
// window moved to the URL:
//
//	timeWindow    (r.FormValue)  200, start applied, ONE temp file per request
//	timeWindowURL (r.URL.Query)  200, start ignored, no temp file
//
// The gate above cannot see this shape: it posts a pure multipart envelope,
// which fails the JSON decode before any form is touched.
func TestADocumentRouteReadsNoFormFromTheBodyTail(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)

	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())

	pad := strings.Repeat("x", multipartMemory+(1<<20))
	// A document, a PREAMBLE, then a multipart envelope carrying a `start`
	// that would narrow the window to nothing if it were read.
	//
	// The preamble is what makes the shape reachable. json.Decoder reads
	// AHEAD into its own buffer and those bytes never return to r.Body, so a
	// boundary placed immediately after the document is swallowed and the
	// later parse finds nothing -- no leak, and a test written that way passes
	// against the defect. Everything before the first boundary is a legal
	// multipart preamble, so 64 KiB of it puts the boundary past the
	// read-ahead and the tail is parseable again.
	body := `{"field":"emb","vector":[1,0,0],"k":2}` + "\n" +
		strings.Repeat("preamble\r\n", 8<<10) +
		"--B\r\nContent-Disposition: form-data; name=\"start\"\r\n\r\n2099-01-01T00:00:00Z\r\n" +
		"--B\r\nContent-Disposition: form-data; name=\"pad\"; filename=\"pad.bin\"\r\n" +
		"Content-Type: application/octet-stream\r\n\r\n" + pad + "\r\n--B--\r\n"

	before := tempMultipartNames(t, tmp)
	code, rb := postDeadline(t, ts, "/select/vector", `multipart/form-data; boundary=B`, body, "")
	if code != 200 {
		t.Fatalf("answered %d: %.200s", code, rb)
	}
	ts.Close()
	if leaked := newNames(before, tempMultipartNames(t, tmp)); len(leaked) > 0 {
		t.Errorf("a document route parsed the multipart tail of its own body and "+
			"left %d temp file(s) behind: %v -- the parameters must come from the "+
			"URL, where a body cannot reach them", len(leaked), leaked)
	}
}

// The admin routes do not buffer a body they never read.
//
// `adminSpec().form` was true, justified by a reader that had already been
// moved to the URL, and the leak gate cannot see it: no admin handler parses a
// form, so nothing is left behind either way. What IS visible is the cost --
// guard's pre-parse reads and buffers the whole body first.
//
// Asserted with a wide margin rather than an exact number: a 4 MiB body costs
// 8.5 to 17 MiB of TotalAlloc with the pre-parse on -- the spread is the
// route's own work, not noise -- and about 0.15 MiB with it off, so a limit of
// half the body size separates them without being a benchmark.
func TestAnAdminRouteDoesNotBufferABodyItNeverReads(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	const size = 4 << 20
	pad := strings.Repeat("x", size)
	body := "--B\r\nContent-Disposition: form-data; name=\"pad\"; filename=\"pad.bin\"\r\n" +
		"Content-Type: application/octet-stream\r\n\r\n" + pad + "\r\n--B--\r\n"

	for _, path := range []string{"/flags", "/admin/backup", "/admin/acknowledge-degraded"} {
		var m0, m1 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m0)
		postDeadline(t, ts, path, `multipart/form-data; boundary=B`, body, "")
		runtime.ReadMemStats(&m1)
		if grew := m1.TotalAlloc - m0.TotalAlloc; grew > size/2 {
			t.Errorf("%s allocated %d bytes for a %d-byte body it never reads: "+
				"the request-shape guard is parsing a form for a route that has "+
				"no parameters", path, grew, size)
		}
	}
}
