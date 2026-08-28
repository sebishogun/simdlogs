package api

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/query"
)

// A cursor is a resume point the server hands out and the client hands back,
// so everything in it is attacker-controlled on the way back. These tests are
// about what happens when it comes back changed.

func testSigner(t *testing.T) *cursorSigner {
	t.Helper()
	c, err := newCursorSigner()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func samplePayload() cursorPayload {
	return cursorPayload{
		tenant:    "7:3",
		queryHash: queryHash("level:error", 100, 200),
		dir:       query.Oldest,
		key:       query.RowKey{Time: 1234567890, Group: 42, Row: 7},
	}
}

// A cursor round-trips exactly, including the tuple that resumes the walk.
func TestACursorRoundTrips(t *testing.T) {
	c := testSigner(t)
	p := samplePayload()
	got, err := c.decode(c.encode(p), p.tenant, p.queryHash, p.dir)
	if err != nil {
		t.Fatal(err)
	}
	if got != p.key {
		t.Fatalf("key = %+v, want %+v", got, p.key)
	}
}

// Every field a client could change on the way back is refused.
func TestACursorIsRefusedWhenAnythingAboutItChanged(t *testing.T) {
	c := testSigner(t)
	p := samplePayload()
	tok := c.encode(p)

	for _, tc := range []struct {
		name   string
		tenant string
		qh     [32]byte
		dir    query.Direction
	}{
		{"another tenant", "8:0", p.queryHash, p.dir},
		{"another query", p.tenant, queryHash("level:info", 100, 200), p.dir},
		{"the same query in another window", p.tenant, queryHash("level:error", 100, 999), p.dir},
		{"the other direction", p.tenant, p.queryHash, query.Newest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := c.decode(tok, tc.tenant, tc.qh, tc.dir); !errors.Is(err, ErrBadCursor) {
				t.Fatalf("err = %v, want ErrBadCursor", err)
			}
		})
	}
}

// A forged or edited token does not verify. The tuple is base64 of a signed
// payload, so a client that decodes it, edits the tenant and re-encodes gets a
// MAC failure rather than another tenant's rows.
func TestAnEditedCursorDoesNotVerify(t *testing.T) {
	c := testSigner(t)
	p := samplePayload()
	tok := c.encode(p)
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		t.Fatal(err)
	}

	// Every single-byte flip in the payload -- version, query hash, direction,
	// tuple, tenant length, tenant -- and in the MAC itself.
	for i := range raw {
		edited := make([]byte, len(raw))
		copy(edited, raw)
		edited[i] ^= 0x01
		s := base64.RawURLEncoding.EncodeToString(edited)
		if _, err := c.decode(s, p.tenant, p.queryHash, p.dir); err == nil {
			t.Fatalf("a cursor with byte %d flipped verified", i)
		}
	}

	// And a token signed by a DIFFERENT process. A cursor is valid for the
	// life of the key that issued it; there is no cross-process meaning to
	// claim and none is claimed.
	other := testSigner(t)
	if _, err := other.decode(tok, p.tenant, p.queryHash, p.dir); !errors.Is(err, ErrBadCursor) {
		t.Fatalf("another signer accepted the cursor: %v", err)
	}
}

// Malformed input is refused rather than panicking. The token is a request
// parameter, so this is the shape a fuzzer hands it.
func TestMalformedCursorsAreRefused(t *testing.T) {
	c := testSigner(t)
	p := samplePayload()
	for _, tok := range []string{
		"", "!", "AAAA", strings.Repeat("A", 200),
		base64.RawURLEncoding.EncodeToString([]byte{1}),
		base64.RawURLEncoding.EncodeToString(make([]byte, 87)), // one byte short
	} {
		if _, err := c.decode(tok, p.tenant, p.queryHash, p.dir); err == nil {
			t.Fatalf("%q was accepted", tok)
		}
	}
	// A payload whose tenant-length field lies about the tail.
	raw, _ := base64.RawURLEncoding.DecodeString(c.encode(p))
	body := raw[:len(raw)-32]
	body[54], body[55] = 0xFF, 0xFF // claim a 65535-byte tenant
	if _, err := c.decode(base64.RawURLEncoding.EncodeToString(append(body, raw[len(raw)-32:]...)),
		p.tenant, p.queryHash, p.dir); err == nil {
		t.Fatal("a lying length prefix was accepted")
	}
}

// End to end over HTTP: page through a store and see every row exactly once.
func TestPaginationOverHTTPReturnsEveryRowOnce(t *testing.T) {
	const rows = 97
	ts := streamServer(t, rows, -1)

	for _, dir := range []string{"oldest", "newest"} {
		t.Run(dir, func(t *testing.T) {
			seen := map[string]bool{}
			cursor := ""
			for page := 0; ; page++ {
				if page > rows {
					t.Fatal("paging did not terminate")
				}
				u := fmt.Sprintf("%s/select/logsql/query?query=*&page_size=10&direction=%s",
					ts.URL, dir)
				if cursor != "" {
					u += "&cursor=" + cursor
				}
				resp, err := http.Get(u)
				if err != nil {
					t.Fatal(err)
				}
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 {
					t.Fatalf("page %d: %d %s", page, resp.StatusCode, b)
				}
				for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
					if line == "" {
						continue
					}
					if seen[line] {
						t.Fatalf("row returned twice on page %d: %s", page, line)
					}
					seen[line] = true
				}
				cursor = resp.Header.Get("X-Simdlogs-Cursor")
				if cursor == "" {
					break
				}
			}
			if len(seen) != rows {
				t.Fatalf("%d distinct rows across all pages, want %d", len(seen), rows)
			}
		})
	}
}

// A cursor handed to a different query is refused over HTTP, with 400 rather
// than a wrong page.
func TestAHTTPCursorCannotCrossQueries(t *testing.T) {
	ts := streamServer(t, 50, -1)

	resp, err := http.Get(ts.URL + "/select/logsql/query?query=*&page_size=5")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	cursor := resp.Header.Get("X-Simdlogs-Cursor")
	if cursor == "" {
		t.Fatal("no cursor on a page that should have more")
	}

	for _, u := range []string{
		"/select/logsql/query?query=app%3Ax&page_size=5&cursor=" + cursor,
		"/select/logsql/query?query=*&page_size=5&direction=newest&cursor=" + cursor,
	} {
		code, body := bodyOf(t, ts.URL+u)
		if code != 400 {
			t.Errorf("%s returned %d (%s), want 400", u, code, body)
		}
	}
	// The cursor still works for the query it was issued for.
	code, body := bodyOf(t, ts.URL+"/select/logsql/query?query=*&page_size=5&cursor="+cursor)
	if code != 200 {
		t.Fatalf("the original query with its own cursor returned %d: %s", code, body)
	}
}

// Without page_size the endpoint answers exactly as before, so the total order
// is opt-in and no existing client's answer changes.
func TestPaginationIsOptIn(t *testing.T) {
	ts := streamServer(t, 30, -1)
	resp, err := http.Get(ts.URL + "/select/logsql/query?query=*")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("X-Simdlogs-Cursor"); got != "" {
		t.Errorf("an unpaginated request got a cursor: %q", got)
	}
	b, _ := io.ReadAll(resp.Body)
	if n := strings.Count(strings.TrimSpace(string(b)), "\n") + 1; n != 30 {
		t.Fatalf("%d rows, want 30", n)
	}
}

// An unparseable direction is refused rather than silently treated as oldest.
func TestAnUnknownDirectionIsRefused(t *testing.T) {
	ts := streamServer(t, 10, -1)
	code, body := bodyOf(t, ts.URL+"/select/logsql/query?query=*&page_size=5&direction=sideways")
	if code != 400 {
		t.Fatalf("%d (%s), want 400", code, body)
	}
}

func TestServerRejectsAForeignCursor(t *testing.T) {
	ts := streamServer(t, 20, -1)
	other := testSigner(t)
	tok := other.encode(cursorPayload{
		tenant: "0:0", queryHash: queryHash("*", 0, 0), dir: query.Oldest,
		key: query.RowKey{},
	})
	code, body := bodyOf(t, ts.URL+"/select/logsql/query?query=*&page_size=5&cursor="+tok)
	if code != 400 {
		t.Fatalf("a cursor from another process returned %d (%s), want 400", code, body)
	}
}
