package ingest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// A group's column order must be a function of the DATA, not of the process
// that wrote it.
//
// It was neither: new columns were registered in `range` order over the
// record's field map, which Go randomises per process. Two nodes given
// identical records built groups whose columns were in different orders, so the
// same row read back from two shards came out with its fields permuted -- and a
// client reading NDJSON from a router saw the shape change depending on which
// shard answered.
//
// This test cannot see the randomisation on its own: one process has one seed,
// so every writer inside one `go test` agrees with every other regardless. What
// it pins is the property that makes the seed irrelevant -- the order is
// sorted, which is the same on every process and every machine. A regression to
// map order fails here on the first record whose fields are not already sorted.
func TestColumnOrderIsSortedNotMapOrder(t *testing.T) {
	// Field names whose sorted order is NOT their insertion order and not the
	// reverse either, so neither a stable-insertion bug nor a reversed one
	// passes by accident.
	rec := map[string]string{
		"zeta": "1", "_msg": "hello", "alpha": "2", "mu": "3", "beta": "4",
	}
	want := []string{"_time", "_msg", "alpha", "beta", "mu", "zeta"}

	for i := 0; i < 16; i++ { // several writers, one per store
		dir := t.TempDir()
		st, err := storage.OpenStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		w := NewWriterWorkers(st, 1)
		cp := map[string]string{}
		for k, v := range rec {
			cp[k] = v
		}
		w.Add(int64(1_700_000_000_000_000_000+i), cp)
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		w.Close()

		sn, err := st.Snapshot(0, 1<<62)
		if err != nil {
			t.Fatal(err)
		}
		if len(sn.Groups) != 1 {
			t.Fatalf("%d groups, want 1", len(sn.Groups))
		}
		got := sn.Groups[0].ColumnNames()
		sn.Close()
		st.Close()

		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("writer %d stored columns %v, want %v", i, got, want)
		}
	}
}

// Columns discovered across SEVERAL records keep first-seen order between
// records, and sort only within one record.
//
// The sort is per-record on purpose: sorting the whole column list on every
// record would put a field that first appeared in record 900 ahead of one from
// record 1, which is a worse order for a reader and a needless cost on the
// per-row path. What must be deterministic is the order two processes agree on,
// and record-by-record first-seen with a sort inside each record is that.
func TestColumnOrderAcrossRecordsIsFirstSeenThenSorted(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w := NewWriterWorkers(st, 1)
	w.Add(1_700_000_000_000_000_000, map[string]string{"b": "1", "a": "2"})
	w.Add(1_700_000_001_000_000_000, map[string]string{"z": "3", "c": "4"})
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	w.Close()

	sn, err := st.Snapshot(0, 1<<62)
	if err != nil {
		t.Fatal(err)
	}
	defer sn.Close()
	got := strings.Join(sn.Groups[0].ColumnNames(), ",")
	if want := "_time,a,b,c,z"; got != want {
		t.Fatalf("columns %q, want %q", got, want)
	}
}

// Registering new columns in order must not put an allocation on the per-row
// path. The scratch slice is reused, so a row that introduces no new column
// allocates exactly what it did before.
func BenchmarkAddSteadyState(b *testing.B) {
	dir := b.TempDir()
	st, err := storage.OpenStore(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer st.Close()
	w := NewWriterWorkers(st, 1)
	defer w.Close()
	rec := map[string]string{
		"_msg": "a log line of ordinary length", "level": "error",
		"host": "node-01", "service": "api", "trace": "abc123",
	}
	w.Add(1_700_000_000_000_000_000, rec) // columns exist after this
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Add(int64(1_700_000_000_000_000_000+i), rec)
	}
	b.StopTimer()
	_ = fmt.Sprint(w.bytes)
}
