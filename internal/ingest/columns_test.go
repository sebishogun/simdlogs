package ingest

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// The fields a job supervisor writes for every line, the shape Columns is for.
var jobFields = []string{"_msg", "kind", "owner", "job", "attempt", "stream", "offset", "binary", "truncated"}

// frozenClock stops the time trigger, so where a group ends depends on rows and
// bytes alone and two writers given the same rows write the same groups.
func frozenClock(t *testing.T) {
	t.Helper()
	at := time.Unix(1_700_000_000, 0)
	old := nowFn
	nowFn = func() time.Time { return at }
	t.Cleanup(func() { nowFn = old })
}

// pairOfWriters opens two stores with one flush worker each, so group ids are
// handed out in order and the two directories can be compared file by file.
func pairOfWriters(t *testing.T, streamFields []string) (byMap, byCols *Writer, mapDir, colDir string) {
	t.Helper()
	open := func() (*Writer, string) {
		dir := t.TempDir()
		st, err := storage.OpenStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		w := NewWriterWorkers(st, 1)
		if streamFields != nil {
			w.SetStreamFields(streamFields)
		}
		t.Cleanup(func() { w.Close(); st.Close() })
		return w, dir
	}
	byMap, mapDir = open()
	byCols, colDir = open()
	return byMap, byCols, mapDir, colDir
}

// addAsMap adds one row through Add, leaving out the empty values as a host
// building a map would.
func addAsMap(w *Writer, names []string, at int64, row []string) {
	m := make(map[string]string, len(names))
	for i, v := range row {
		if v != "" {
			m[names[i]] = v
		}
	}
	w.Add(at, m)
}

// sameGroups fails unless both directories hold the same group files, byte for
// byte.
func sameGroups(t *testing.T, a, b string) int {
	t.Helper()
	list := func(dir string) []string {
		names, err := filepath.Glob(filepath.Join(dir, "group-*.bin"))
		if err != nil {
			t.Fatal(err)
		}
		for i := range names {
			names[i] = filepath.Base(names[i])
		}
		slices.Sort(names)
		return names
	}
	na, nb := list(a), list(b)
	if !slices.Equal(na, nb) {
		t.Fatalf("groups differ: %v and %v", na, nb)
	}
	for _, n := range na {
		ba, err := os.ReadFile(filepath.Join(a, n))
		if err != nil {
			t.Fatal(err)
		}
		bb, err := os.ReadFile(filepath.Join(b, n))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(ba, bb) {
			t.Fatalf("%s differs: %d bytes by Add, %d by Columns", n, len(ba), len(bb))
		}
	}
	return len(na)
}

// jobRows makes n rows of a supervisor's shape. Owner and kind change often
// enough to pass through more label sets than the cache holds, some rows have no
// stream fields at all, binary and truncated are rare, and one field first
// appears half way through.
func jobRows(seed uint64, n int) ([]int64, [][]string) {
	rng := rand.New(rand.NewPCG(seed, 1))
	text := strings.Repeat("epoch 12 step 4096 loss 0.3141 lr 3e-4 tokens/s 81234 ", 8)
	ts := make([]int64, n)
	rows := make([][]string, n)
	owner, kind := "seb", "output"
	for r := range n {
		if rng.IntN(64) == 0 {
			owner = []string{"seb", "ana", "", "bo", "cy", "di", "ed", "flo", "gus", "hal", "ivy"}[rng.IntN(11)]
			kind = []string{"output", "event", ""}[rng.IntN(3)]
		}
		row := make([]string, len(jobFields))
		row[0] = text[:rng.IntN(len(text))]
		row[1], row[2] = kind, owner
		row[3] = "0123456789abcdef0123456789abcdef"[rng.IntN(4):]
		row[4] = strconv.Itoa(rng.IntN(3))
		row[5] = []string{"stdout", "stderr"}[rng.IntN(2)]
		row[6] = strconv.Itoa(r * 47)
		if rng.IntN(100) == 0 {
			row[7] = "true"
		}
		if r > n/2 && rng.IntN(50) == 0 {
			row[8] = "true"
		}
		ts[r] = 1_700_000_000_000_000_000 + int64(r)*1000
		rows[r] = row
	}
	return ts, rows
}

// columnsOf turns rows into the column-major slices Columns.Add takes.
func columnsOf(rows [][]string, width int) [][]string {
	cols := make([][]string, width)
	for i := range cols {
		cols[i] = make([]string, len(rows))
		for r, row := range rows {
			cols[i][r] = row[i]
		}
	}
	return cols
}

// The same rows through Add and through Columns, in calls of every size from
// one row to past FlushRows, with flushes at the same rows, write the same
// group files.
func TestColumnsStoreWhatAddStores(t *testing.T) {
	frozenClock(t)
	byMap, byCols, mapDir, colDir := pairOfWriters(t, []string{"owner", "kind"})
	cols, err := byCols.Columns(jobFields)
	if err != nil {
		t.Fatal(err)
	}
	n := FlushRows + FlushRows/2
	ts, rows := jobRows(1, n)
	rng := rand.New(rand.NewPCG(2, 2))
	crossed := false
	for r := 0; r < n; {
		var m int
		switch {
		case !crossed && r > n/4:
			// One call past the row trigger, starting part way into a group,
			// so the group ends inside the call.
			m, crossed = FlushRows+7, true
		case rng.IntN(3) == 0:
			m = 1
		case rng.IntN(2) == 0:
			m = 1 + rng.IntN(64)
		default:
			m = 1 + rng.IntN(5000)
		}
		m = min(m, n-r)
		for i := r; i < r+m; i++ {
			addAsMap(byMap, jobFields, ts[i], rows[i])
		}
		if m == 1 && rng.IntN(2) == 0 {
			err = cols.AddRow(ts[r], rows[r]...)
		} else {
			err = cols.Add(ts[r:r+m], columnsOf(rows[r:r+m], len(jobFields)))
		}
		if err != nil {
			t.Fatal(err)
		}
		r += m
		if rng.IntN(8) == 0 {
			if err := errors.Join(byMap.Flush(), byCols.Flush()); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := errors.Join(byMap.Flush(), byCols.Flush()); err != nil {
		t.Fatal(err)
	}
	if groups := sameGroups(t, mapDir, colDir); groups < 8 {
		t.Fatalf("only %d groups", groups)
	}
}

// Rows big enough to reach FlushBytes end their group at the row Add ends it at,
// whether they arrive in one call or many.
func TestColumnsEndAGroupAtTheByteTriggerWhereAddDoes(t *testing.T) {
	frozenClock(t)
	big := strings.Repeat("x", 3<<20)
	names := []string{"_msg", "owner", "kind"}
	var ts []int64
	var rows [][]string
	for r := range 70 {
		// Three lengths, so the dictionary holds three values and the groups
		// stay small on disk while their byte counts cross 64 MiB.
		msg := big[:(2<<20)-7*(r%3)]
		ts = append(ts, int64(r))
		rows = append(rows, []string{msg, "seb", []string{"output", "event"}[r%2]})
	}
	for _, chunk := range []int{70, 9, 1} {
		t.Run(fmt.Sprint(chunk), func(t *testing.T) {
			byMap, byCols, mapDir, colDir := pairOfWriters(t, []string{"owner", "kind"})
			cols, err := byCols.Columns(names)
			if err != nil {
				t.Fatal(err)
			}
			for r := 0; r < len(rows); r += chunk {
				end := min(r+chunk, len(rows))
				for i := r; i < end; i++ {
					addAsMap(byMap, names, ts[i], rows[i])
				}
				if err := cols.Add(ts[r:end], columnsOf(rows[r:end], len(names))); err != nil {
					t.Fatal(err)
				}
			}
			if err := errors.Join(byMap.Flush(), byCols.Flush()); err != nil {
				t.Fatal(err)
			}
			if groups := sameGroups(t, mapDir, colDir); groups < 3 {
				t.Fatalf("%d groups: the byte trigger was not crossed twice", groups)
			}
		})
	}
}

// Add and Columns writing into the same group: a column either one creates is
// the other's too, and a column only Add knows is padded for Columns' rows.
func TestColumnsMixWithAdd(t *testing.T) {
	frozenClock(t)
	ref, mixed, refDir, mixedDir := pairOfWriters(t, []string{"owner", "kind"})
	names := []string{"_msg", "owner", "kind", "job"}
	cols, err := mixed.Columns(names)
	if err != nil {
		t.Fatal(err)
	}
	type step struct {
		byMap bool
		row   map[string]string
	}
	steps := []step{
		{true, map[string]string{"_msg": "a", "level": "info"}},
		{false, map[string]string{"_msg": "b", "owner": "seb"}},
		{true, map[string]string{"_msg": "c", "job": "j1", "kind": "output", "zone": "z"}},
		{false, map[string]string{"_msg": "d", "job": "j2", "owner": "seb", "kind": "output"}},
		{true, map[string]string{"_msg": "e", "_stream": "{forged}", "owner": "ana"}},
		{false, map[string]string{"kind": "event"}},
		{false, map[string]string{}},
	}
	for round := range 3 {
		for i, s := range steps {
			at := int64(round*100 + i)
			ref.Add(at, maps.Clone(s.row))
			if s.byMap {
				mixed.Add(at, maps.Clone(s.row))
				continue
			}
			row := make([]string, len(names))
			for j, n := range names {
				row[j] = s.row[n]
			}
			if err := cols.AddRow(at, row...); err != nil {
				t.Fatal(err)
			}
		}
		if err := errors.Join(ref.Flush(), mixed.Flush()); err != nil {
			t.Fatal(err)
		}
	}
	sameGroups(t, refDir, mixedDir)
}

// A declared _stream is dropped when the writer labels streams, as Add drops a
// record's own, and stored as given when it does not.
func TestColumnsTreatADeclaredStreamAsAddDoes(t *testing.T) {
	frozenClock(t)
	for _, fields := range [][]string{{"owner"}, nil} {
		byMap, byCols, mapDir, colDir := pairOfWriters(t, fields)
		names := []string{"_msg", "_stream", "owner"}
		cols, err := byCols.Columns(names)
		if err != nil {
			t.Fatal(err)
		}
		row := []string{"line", `{owner="mallory"}`, "seb"}
		addAsMap(byMap, names, 1, row)
		if err := cols.AddRow(1, row...); err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(byMap.Flush(), byCols.Flush()); err != nil {
			t.Fatal(err)
		}
		sameGroups(t, mapDir, colDir)
	}
}

func TestColumnsRefuseWhatTheyCannotStore(t *testing.T) {
	st, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w := NewWriterWorkers(st, 1)
	for _, names := range [][]string{{""}, {"_time"}, {".error"}, {"a", "b", "a"}} {
		if _, err := w.Columns(names); err == nil {
			t.Errorf("declared %q", names)
		}
	}
	c, err := w.Columns([]string{"_msg", "level"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Add([]int64{1, 2}, [][]string{{"a", "b"}}); err == nil {
		t.Error("one column for two names")
	}
	if err := c.Add([]int64{1, 2}, [][]string{{"a", "b"}, {"x"}}); err == nil {
		t.Error("a short column")
	}
	if err := c.AddRow(1, "a"); err == nil {
		t.Error("one value for two names")
	}
	w.SetRecordLimits(RecordLimits{MaxFields: 4})
	if err := c.AddRow(1, "a", "b"); !errors.Is(err, ErrColumnsUnsupported) {
		t.Errorf("with record limits: %v", err)
	}
	w.SetRecordLimits(RecordLimits{})
	w.SetVectorFields(VectorFields{"emb": 4})
	if err := c.AddRow(1, "a", "b"); !errors.Is(err, ErrColumnsUnsupported) {
		t.Errorf("with vector fields: %v", err)
	}
	w.SetVectorFields(nil)
	if err := c.AddRow(1, "a", "b"); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.AddRow(2, "late", "b"); err != nil {
		t.Fatalf("after Close, as Add: %v", err)
	}
	if err := w.Flush(); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("Flush after Close: %v", err)
	}
	if st.TotalRows() != 1 {
		t.Fatalf("%d rows stored, want the one added before Close", st.TotalRows())
	}
}
