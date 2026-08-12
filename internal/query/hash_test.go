package query

import "testing"

func TestHashPipe(t *testing.T) {
	pq, err := ParseLogsQL(`* | hash(_msg) as h`)
	if err != nil {
		t.Fatal(err)
	}
	_ = pq
	// Determinism: same input -> same hash.
	a := (&HashPipe{Field: "_msg", As: "h"}).apply([]Row{{Fields: []Field{{"_msg", "abc"}}}})
	b := (&HashPipe{Field: "_msg", As: "h"}).apply([]Row{{Fields: []Field{{"_msg", "abc"}}}})
	if rowField(a[0], "h") != rowField(b[0], "h") || rowField(a[0], "h") == "" {
		t.Fatalf("hash not stable/nonempty: %q vs %q", rowField(a[0], "h"), rowField(b[0], "h"))
	}
	if c := (&HashPipe{Field: "_msg", As: "h"}).apply([]Row{{Fields: []Field{{"_msg", "abd"}}}}); rowField(c[0], "h") == rowField(a[0], "h") {
		t.Fatal("different inputs hashed equal")
	}
}
