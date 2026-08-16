package api

import (
	"os"
	"regexp"
	"strconv"
	"testing"
)

// The route count in the documents is the count the mux actually registers.
//
// "42 routes" appeared in docs/compatibility.md, docs/lld/cluster.md and this
// package's own test comment. The real number was 46 and had been for a while:
// a count written once and repeated three times is three claims nobody checks,
// and the last four routes were added without any of them moving.
func TestTheDocumentedRouteCountIsTheRealOne(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	srv.Handler()
	got := srv.routeCountForTest()

	for _, doc := range []string{
		"../../docs/compatibility.md",
		"../../docs/lld/cluster.md",
	} {
		b, err := os.ReadFile(doc)
		if err != nil {
			t.Fatalf("%s: %v", doc, err)
		}
		// Every "<n> routes" in the document must be the real count.
		re := regexp.MustCompile(`(\d+) routes`)
		for _, m := range re.FindAllStringSubmatch(string(b), -1) {
			n, err := strconv.Atoi(m[1])
			if err != nil {
				continue
			}
			if n != got {
				t.Errorf("%s says %q and the mux registers %d", doc, m[0], got)
			}
		}
	}
	if len(surfaceRoutes()) != got {
		t.Errorf("surfaceRoutes() classifies %d routes and the mux registers %d",
			len(surfaceRoutes()), got)
	}
}
