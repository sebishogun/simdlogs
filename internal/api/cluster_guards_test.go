package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// Guards a reviewer could delete with the whole suite still green.
//
// Each of these was load-bearing in argument and untested in fact. A guard you
// can remove without a test noticing is a guard that was never doing anything,
// whatever its comment claims -- so each one below is exercised through the
// behaviour it protects, not through the predicate it happens to call.

// askShard must not retry a replica for a class where retrying cannot help.
//
// The existing test asserted the retryAnotherReplica() PREDICATE, which is a
// pure function of the class -- so deleting the caller left it green. What
// matters is that the caller stops: an unauthorized peer is the router's own
// credential, and every replica refuses it identically, so retrying turns one
// 401 into N and delays the report.
func TestAnUnauthorizedShardIsNotRetriedAcrossReplicas(t *testing.T) {
	var hits int
	peer := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits++
			w.Header().Set(HdrProtocolVersion, "1")
			w.WriteHeader(http.StatusUnauthorized)
		}))
	}
	a, b := peer(), peer()
	defer a.Close()
	defer b.Close()

	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	srv.SetBackends([]string{a.URL, b.URL})
	srv.SetReplicas(2) // one shard, two replicas
	front := httptest.NewServer(srv.Handler())
	defer front.Close()

	hits = 0
	queryRows(t, front, "*")
	if hits != 1 {
		t.Fatalf("an unauthorized shard was asked %d times; every replica refuses "+
			"the router's credential identically, so retrying only delays the "+
			"report", hits)
	}
}

// A group with no rows is refused by AdoptGroup.
//
// Nothing tested it, so the check was deletable. A zero-row group is not a
// harmless no-op: it occupies a manifest record and a file, and repair would
// copy it between replicas forever because its digest is in the union and
// adopting it changes nothing.
func TestAdoptRefusesAZeroRowGroup(t *testing.T) {
	st, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	empty := (&storage.Group{Rows: 0, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: nil},
	}}).Marshal()
	ok, err := st.AdoptGroup(storage.DigestForTest(empty), empty)
	if err == nil && ok {
		t.Fatal("adopted a group with no rows")
	}
	if n := st.TotalRows(); n != 0 {
		t.Fatalf("%d rows landed from a refused adoption", n)
	}
}

// Only the FIRST _time in a round-tripped row is lifted into Row.Time.
//
// A row carrying two _time fields is malformed, and the guard decides which one
// wins. Without it the last one wins, so two shards emitting the same row with
// a duplicated key would order it differently -- and nothing tested it.
func TestOnlyTheFirstTimeFieldIsLifted(t *testing.T) {
	row := jsonLineToRow([]byte(
		`{"_time":"2026-06-01T12:00:00Z","_msg":"x","_time":"2026-06-01T13:00:00Z"}`))
	if row.NoTime {
		t.Fatal("no time was lifted at all")
	}
	want := int64(1780315200000000000) // 2026-06-01T12:00:00Z
	if row.Time != want {
		t.Fatalf("Time is %d, want the FIRST _time (%d): a later duplicate must not "+
			"override it, or two shards emitting the same malformed row order it "+
			"differently", row.Time, want)
	}
}
