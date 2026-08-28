package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The embedded UI, tested against the shapes it actually consumes.
//
// The histogram read `j.hits[i]._time` and `.hits` -- a bag of {time, count}
// objects -- while /select/logsql/hits returns the reference's DENSE shape:
// one entry per series with parallel `timestamps` and `values` arrays. Every
// value was undefined, every bar computed a NaN height, and the graph was
// permanently empty. It failed silently because the fetch's catch() swallowed
// everything, including the error that would have said so.
//
// A page cannot be exercised without a browser, so these tests do the two
// things that would have caught it: assert the RESPONSE SHAPE the page parses,
// and assert the page's parsing reads those field names.

func uiServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return srv, ts
}

// The hits endpoint returns dense parallel arrays, and the page reads them.
func TestTheUIReadsTheHitsShapeTheEndpointReturns(t *testing.T) {
	_, ts := uiServer(t)
	now := time.Now()
	var body strings.Builder
	for i := 0; i < 5; i++ {
		fmt.Fprintf(&body, `{"_time":%d,"level":"error"}`+"\n",
			now.Add(-time.Duration(i)*time.Minute).UnixNano())
	}
	postBody(t, ts, body.String())

	// A bounded window: the response is dense, so an unbounded one asks for a
	// bucket per step since 1970 regardless of how much data exists.
	from := now.Add(-time.Hour).UnixNano()
	to := now.Add(time.Minute).UnixNano()
	resp, err := http.Get(fmt.Sprintf("%s/select/logsql/hits?query=*&step=1m&start=%d&end=%d",
		ts.URL, from, to))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	var got struct {
		Hits []struct {
			Fields     map[string]string `json:"fields"`
			Timestamps []string          `json:"timestamps"`
			Values     []int             `json:"values"`
			Total      int               `json:"total"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("hits is not JSON: %s", raw)
	}
	if len(got.Hits) == 0 {
		t.Fatalf("no series: %s", raw)
	}
	se := got.Hits[0]
	if len(se.Timestamps) == 0 || len(se.Timestamps) != len(se.Values) {
		t.Fatalf("timestamps and values are not parallel: %d and %d",
			len(se.Timestamps), len(se.Values))
	}
	sum := 0
	for _, v := range se.Values {
		sum += v
	}
	if sum != 5 {
		t.Fatalf("values sum to %d, want 5: %s", sum, raw)
	}

	// And the PAGE reads those field names. This is the assertion that would
	// have caught the original defect: the page parsed `_time` and `hits`,
	// which appear nowhere in this response.
	page := string(uiHTML)
	for _, want := range []string{"timestamps", "values"} {
		if !strings.Contains(page, want) {
			t.Errorf("the page never reads %q, which is what the endpoint returns", want)
		}
	}
	// Against the CODE, not the comments: the file explains the old shape in
	// prose, and a substring search over the whole file would match the
	// explanation rather than an actual read.
	code := stripJSComments(page)
	for _, gone := range []string{"h.hits", "a._time < b._time"} {
		if strings.Contains(code, gone) {
			t.Errorf("the page still reads %q, a shape this endpoint does not return", gone)
		}
	}
}

// The page carries no tenant selector. It sent an arbitrary AccountID header,
// so on a deployment without -auth.config the UI was a free tenant switcher.
func TestTheUIHasNoTenantSelector(t *testing.T) {
	page := string(uiHTML)
	for _, gone := range []string{`id="tenant"`, `"AccountID": t`, "tenant \" + i"} {
		if strings.Contains(page, gone) {
			t.Errorf("the page still lets the browser choose a tenant: %q", gone)
		}
	}
	if !strings.Contains(page, "function headers() { return {}; }") {
		t.Error("the page still sends tenant headers of its own")
	}
}

// The UI response carries the browser-side hardening. It renders LOG CONTENT
// -- arbitrary attacker-influenced strings that arrived through an ingest
// endpoint -- so a CSP is what stands between an escaping bug and script
// execution with the operator's session.
func TestTheUICarriesSecurityHeaders(t *testing.T) {
	_, ts := uiServer(t)
	for _, path := range []string{"/vmui", "/select/vmui", "/"} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("%s returned %d", path, resp.StatusCode)
		}
		h := resp.Header
		csp := h.Get("Content-Security-Policy")
		for _, want := range []string{
			"default-src 'none'", "frame-ancestors 'none'",
			"connect-src 'self'", "base-uri 'none'", "form-action 'none'",
		} {
			if !strings.Contains(csp, want) {
				t.Errorf("%s: CSP is missing %q: %q", path, want, csp)
			}
		}
		if h.Get("X-Frame-Options") != "DENY" {
			t.Errorf("%s: X-Frame-Options = %q", path, h.Get("X-Frame-Options"))
		}
		if h.Get("X-Content-Type-Options") != "nosniff" {
			t.Errorf("%s: X-Content-Type-Options = %q", path, h.Get("X-Content-Type-Options"))
		}
		if h.Get("Referrer-Policy") != "no-referrer" {
			// The query is in the URL, and a query is a search over someone's
			// logs.
			t.Errorf("%s: Referrer-Policy = %q", path, h.Get("Referrer-Policy"))
		}
	}
}

// The page cancels an in-flight query and pages with the cursor.
func TestTheUICancelsAndPages(t *testing.T) {
	page := string(uiHTML)
	for _, want := range []string{
		"AbortController",   // cancellation
		"signal: signal",    // ...actually passed to fetch
		"page_size",         // pagination is opt-in per request
		"X-Simdlogs-Cursor", // the cursor is a header, not a body field
		`id="cancel"`,
		`id="more"`,
	} {
		if !strings.Contains(page, want) {
			t.Errorf("the page does not use %q", want)
		}
	}
	// The old unbounded `limit` parameter is gone: it fetched everything and
	// had no way to continue.
	if strings.Contains(page, `p.set("limit", $("limit").value`) {
		t.Error("the page still uses the unbounded limit parameter")
	}
}

// A query error reaches the user rather than being swallowed.
func TestTheUISurfacesQueryErrors(t *testing.T) {
	page := string(uiHTML)
	if strings.Contains(page, ".catch(function () {});") {
		t.Error("the page still has an empty catch; that is why the broken " +
			"histogram went unnoticed")
	}
	if !strings.Contains(page, "histogram unavailable") {
		t.Error("a failing histogram fetch reports nothing to the user")
	}
}

// The UI is served only on its own paths -- "/" is a catch-all in ServeMux.
func TestTheUIServesOnlyItsOwnPaths(t *testing.T) {
	_, ts := uiServer(t)
	resp, err := http.Get(ts.URL + "/no/such/page")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("an unknown path returned %d, want 404", resp.StatusCode)
	}
}

// A histogram response is bounded whatever the caller asks for.
//
// The response is dense: one bucket per step across the whole window, present
// or not. Its size is (window / step) and has nothing to do with how much data
// matched, so an empty store answered a default query with about 29 million
// buckets -- tens of megabytes of RFC3339 timestamps. The UI issued exactly
// that on every page load.
//
// An UNSPECIFIED window is defaulted; an EXPLICIT one too wide for its step is
// refused. A caller that named no range did not ask for all of history, it
// just did not say, and answering an unstated question with a 413 breaks every
// client that was getting away with it.
func TestAHistogramResponseIsAlwaysBounded(t *testing.T) {
	_, ts := uiServer(t)
	postBody(t, ts, fmt.Sprintf(`{"_time":%d,"level":"error"}`+"\n", time.Now().UnixNano()))

	// No window: defaulted, and the answer is small.
	resp, err := http.Get(ts.URL + "/select/logsql/hits?query=*&step=1m")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("an unspecified window returned %d: %.200s", resp.StatusCode, body)
	}
	var got struct {
		Hits []struct {
			Timestamps []string `json:"timestamps"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("not JSON: %.200s", body)
	}
	for _, se := range got.Hits {
		if len(se.Timestamps) > 10_000 {
			t.Fatalf("%d buckets from an unspecified window; it was not defaulted",
				len(se.Timestamps))
		}
	}
	if len(body) > 1<<20 {
		t.Fatalf("a default histogram is %d bytes", len(body))
	}

	// An explicit window too wide for its step IS refused: that one was asked
	// for.
	resp, err = http.Get(fmt.Sprintf(
		"%s/select/logsql/hits?query=*&step=1ms&start=%d&end=%d",
		ts.URL, time.Now().Add(-100*time.Hour).UnixNano(), time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("100 hours at a 1ms step returned %d and %d bytes, want 413",
			resp.StatusCode, len(body))
	}
	if !strings.Contains(string(body), "Narrow the time range or increase the step") {
		t.Errorf("the refusal does not say what to do: %s", body)
	}

	// And the page always sends a window, so it never relies on the default.
	if !strings.Contains(string(uiHTML), `p.set("start", String(fromN))`) {
		t.Error("the page does not bound its histogram window")
	}
}

// stripJSComments removes `//` line comments so an assertion about what the
// page DOES is not satisfied by what the page SAYS. The file documents the
// shapes it used to read, and those explanations must not count as reads.
func stripJSComments(src string) string {
	var b strings.Builder
	for _, line := range strings.Split(src, "\n") {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, "//") {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}
