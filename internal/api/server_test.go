package api

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

func TestServerInsertAndQuery(t *testing.T) {
	t.Parallel()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	var body strings.Builder
	base := int64(1_700_000_000_000_000_000)
	wantErr := 0
	for i := 0; i < 20_000; i++ {
		lvl := "info"
		if i%5 == 0 {
			lvl = "error"
			wantErr++
		}
		fmt.Fprintf(&body, `{"_time":%d,"level":%q,"service":"api","_msg":"m%d"}`+"\n",
			base+int64(i)*1000, lvl, i)
	}
	resp, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson", strings.NewReader(body.String()))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("insert status %d", resp.StatusCode)
	}

	// Query the error lines back.
	q := http.MethodGet
	_ = q
	r, err := http.Get(ts.URL + "/select/logsql/query?query=" + "level:=error")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	n := 0
	buf := make([]byte, 4096)
	var acc strings.Builder
	for {
		k, e := r.Body.Read(buf)
		acc.Write(buf[:k])
		if e != nil {
			break
		}
	}
	for _, line := range strings.Split(strings.TrimSpace(acc.String()), "\n") {
		if strings.Contains(line, `"level":"error"`) {
			n++
		}
	}
	if n != wantErr {
		t.Fatalf("query returned %d error rows, want %d", n, wantErr)
	}

	// Hits endpoint returns buckets without error.
	h, err := http.Get(ts.URL + "/select/logsql/hits?query=level:=error")
	if err != nil {
		t.Fatal(err)
	}
	h.Body.Close()
	if h.StatusCode != 200 {
		t.Fatalf("hits status %d", h.StatusCode)
	}
}

func TestMaxRowsCap(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	srv.SetMaxRows(5)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	// ingest 20 records
	var body strings.Builder
	for i := 0; i < 20; i++ {
		body.WriteString(`{"_msg":"line ` + strconv.Itoa(i) + `","app":"x"}` + "\n")
	}
	post, _ := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson", strings.NewReader(body.String()))
	if post != nil {
		post.Body.Close()
	}
	// a bare select of all 20 exceeds the cap of 5 -> 400, not a truncated 200
	resp, err := http.Get(ts.URL + `/select/logsql/query?query=` + url.QueryEscape("*"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("over-cap bare select = %d (want 400): %s", resp.StatusCode, b)
	}
	// an explicit limit within the cap succeeds
	r2, err := http.Get(ts.URL + `/select/logsql/query?query=` + url.QueryEscape("* | limit 3"))
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Body.Close()
	if r2.StatusCode != 200 {
		t.Errorf("| limit 3 = %d want 200", r2.StatusCode)
	}
}
