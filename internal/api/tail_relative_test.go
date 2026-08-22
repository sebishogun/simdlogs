package api

import (
	"bufio"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// A RELATIVE `_time:` FILTER ON A TAIL KEEPS DELIVERING.
//
// `tail` copied its query with `backlog := *q` -- shallow, so the Preds
// backing array and the Filter tree were shared -- and `RunPipeline` resolves
// a relative `_time:` IN PLACE. So the backlog's resolve froze the live
// query's window at the request instant, and every row ingested afterwards was
// filtered out against a window already past. Measured before the fix, three
// rows ingested after the stream opened:
//
//	_time:1s                           0 of 3 delivered
//	_time:5m                           0 of 3
//	_time:>=5m                         0 of 3
//	_time:[2020-01-01…, 2030-01-01…]   3 of 3
//	*                                  3 of 3
//
// HTTP 200, headers sent, connection open, nothing ever arriving. A silently
// dead stream on a documented endpoint.
//
// The shallow copy is only half of it: `resolveTimePreds` is IDEMPOTENT, so
// even a private copy resolved once would freeze at the first poll. A tail's
// relative window has to move with the stream, which is why the loop clones
// and re-resolves per poll rather than reusing one query.
func TestATailWithARelativeTimeFilterKeepsDelivering(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		wantLive    int
	}{
		{"relative seconds", `_time:10s`, 3},
		{"relative minutes", `_time:5m`, 3},
		{"one-sided relative", `_time:>=5m`, 3},
		// The controls: these worked before the fix, and must keep working.
		{"absolute window", `_time:[2020-01-01T00:00:00Z, 2030-01-01T00:00:00Z]`, 3},
		{"no time filter", `*`, 3},
		// A non-time filter, to show the delivery path itself is fine.
		//
		// `_msg:live0` and not `_msg:live`: LogsQL filters on WORDS, and
		// "live0" is one token, so `_msg:live` matches none of the three. That
		// is the filter behaving correctly and a case asserting 3 for it would
		// be asserting a bug. One row is the right answer and the reviewer's
		// own table measured the same.
		{"field filter", `_msg:live0`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := realShard(t, nil)

			req, err := http.NewRequest("GET",
				node.URL+"/select/logsql/tail?query="+urlEscape(tc.query), nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := node.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Fatalf("tail answered %d", resp.StatusCode)
			}

			// Ingest AFTER the stream is open, which is the case the frozen
			// window silently dropped.
			lines := make([]string, 3)
			for i := range lines {
				lines[i] = fmt.Sprintf(`{"_time":%d,"_msg":"live%d"}`,
					time.Now().UnixNano(), i)
			}
			time.Sleep(150 * time.Millisecond)
			post, err := node.Client().Post(node.URL+"/insert/jsonline",
				"application/x-ndjson", strings.NewReader(strings.Join(lines, "\n")+"\n"))
			if err != nil {
				t.Fatal(err)
			}
			post.Body.Close()

			// Read until we have what we expect or the deadline passes. A
			// COUNT with a deadline, not a fixed sleep: a sleep long enough to
			// be reliable is a slow test, and one short enough to be fast is a
			// flaky one.
			got := 0
			done := make(chan struct{})
			go func() {
				defer close(done)
				sc := bufio.NewScanner(resp.Body)
				for sc.Scan() {
					if strings.Contains(sc.Text(), `"live`) {
						got++
						if got >= tc.wantLive {
							return
						}
					}
				}
			}()
			select {
			case <-done:
			case <-time.After(8 * time.Second):
			}
			resp.Body.Close()

			if got < tc.wantLive {
				t.Errorf("%q delivered %d of %d rows ingested after the stream "+
					"opened. A relative window on a tail has to move with the "+
					"stream; a frozen one is a 200 with the connection open "+
					"and nothing ever arriving", tc.query, got, tc.wantLive)
			}
		})
	}
}
