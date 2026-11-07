package embed

import (
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// jobFields is what a job supervisor writes for every line it keeps.
var jobFields = []string{"_msg", "kind", "owner", "job", "attempt", "stream", "offset", "binary", "truncated"}

// jobLines is n lines as a supervisor holds them: one string of text and one of
// offsets for the whole batch, and every value a substring of one of them.
func jobLines(n int) (msgs, offsets []string) {
	var text, offs strings.Builder
	starts := make([]int, n+1)
	ends := make([]int, n+1)
	for r := range n {
		starts[r] = text.Len()
		fmt.Fprintf(&text, "epoch %d step %d loss 0.%04d lr 3e-4 ok", r/1000, r, r%9973)
		ends[r] = offs.Len()
		offs.WriteString(strconv.Itoa(r * 46))
	}
	starts[n], ends[n] = text.Len(), offs.Len()
	t, o := text.String(), offs.String()
	msgs, offsets = make([]string, n), make([]string, n)
	for r := range n {
		msgs[r] = t[starts[r]:starts[r+1]]
		offsets[r] = o[ends[r]:ends[r+1]]
	}
	return msgs, offsets
}

// jobRun is a stretch of lines with the same owner, kind, job, attempt and
// stream, which is how a supervisor's batch arrives.
type jobRun struct {
	owner, kind, job, attempt, stream string
	first, n                          int
	flags                             func(r int) (binary, truncated bool)
}

func jobRuns() []jobRun {
	none := func(int) (bool, bool) { return false, false }
	return []jobRun{
		{"seb", "output", "0123456789abcdef", "1", "stdout", 0, 700, none},
		{"seb", "output", "0123456789abcdef", "1", "stderr", 700, 300, func(r int) (bool, bool) { return r%97 == 0, false }},
		{"seb", "event", "0123456789abcdef", "1", "", 1000, 5, none},
		{"", "event", "", "", "", 1005, 3, none},
		{"seb", "output", "fedcba9876543210", "2", "stdout", 1008, 2000, func(r int) (bool, bool) { return false, r%501 == 0 }},
		{"ana", "output", "aaaabbbbccccdddd", "1", "stdout", 3008, 992, none},
	}
}

// addByMap adds a run the way a supervisor uses Add: one reused map.
func addByMap(db *DB, f map[string]string, run jobRun, at []int64, msgs, offsets []string) {
	clear(f)
	for k, v := range map[string]string{"kind": run.kind, "owner": run.owner, "job": run.job, "attempt": run.attempt, "stream": run.stream} {
		if v != "" {
			f[k] = v
		}
	}
	for r := run.first; r < run.first+run.n; r++ {
		delete(f, "binary")
		delete(f, "truncated")
		binary, truncated := run.flags(r)
		if binary {
			f["binary"] = "true"
		}
		if truncated {
			f["truncated"] = "true"
		}
		f["_msg"] = msgs[r]
		if run.kind == "output" {
			f["offset"] = offsets[r]
		}
		db.Add(at[r], f)
	}
}

// fillRun writes a run into reused column slices, the way a supervisor would
// use Columns, and returns them cut to the run's length.
func fillRun(cols [][]string, run jobRun, msgs, offsets []string) [][]string {
	for i := range cols {
		cols[i] = cols[i][:run.n]
	}
	for j := range run.n {
		r := run.first + j
		binary, truncated := run.flags(r)
		cols[0][j] = msgs[r]
		cols[1][j], cols[2][j], cols[3][j], cols[4][j], cols[5][j] = run.kind, run.owner, run.job, run.attempt, run.stream
		cols[6][j], cols[7][j], cols[8][j] = "", "", ""
		if run.kind == "output" {
			cols[6][j] = offsets[r]
		}
		if binary {
			cols[7][j] = "true"
		}
		if truncated {
			cols[8][j] = "true"
		}
	}
	return cols
}

func emptyColumns(n int) [][]string {
	cols := make([][]string, len(jobFields))
	for i := range cols {
		cols[i] = make([]string, n)
	}
	return cols
}

func allRows(t *testing.T, db *DB, q string) []string {
	t.Helper()
	var out []string
	err := db.Search(t.Context(), q, SearchOptions{}, func(rows []Row) error {
		for _, r := range rows {
			var b strings.Builder
			fmt.Fprintf(&b, "%d %v", r.Time, r.NoTime)
			for _, f := range r.Fields {
				fmt.Fprintf(&b, " %s=%q", f.Key, f.Value)
			}
			out = append(out, b.String())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// The same lines through Add and through Columns answer every query alike,
// field for field and in the same field order.
func TestColumnsAnswerAsAddDoes(t *testing.T) {
	byMap, byCols := open(t, nil), open(t, nil)
	c, err := byCols.Columns(jobFields...)
	if err != nil {
		t.Fatal(err)
	}
	runs := jobRuns()
	last := runs[len(runs)-1]
	n := last.first + last.n
	msgs, offsets := jobLines(n)
	at := make([]int64, n)
	base := time.Now().Add(-time.Hour).UnixNano()
	for r := range at {
		at[r] = base + int64(r)*1000
	}
	f := map[string]string{}
	cols := emptyColumns(n)
	for i, run := range runs {
		addByMap(byMap, f, run, at, msgs, offsets)
		if err := c.Add(at[run.first:run.first+run.n], fillRun(cols, run, msgs, offsets)...); err != nil {
			t.Fatal(err)
		}
		if i%2 == 1 {
			if err := byMap.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := byCols.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := byMap.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := byCols.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"*",
		"job:=fedcba9876543210",
		`_stream:{owner="seb",kind="output"}`,
		"binary:=true",
		"truncated:=true",
		"stream:=stderr",
		"epoch AND loss",
		"* | stats by (_stream) count() n",
		"* | stats by (kind, stream, attempt) count() n",
		"* | field_names",
	} {
		want, got := allRows(t, byMap, q), allRows(t, byCols, q)
		if len(want) == 0 {
			t.Fatalf("%s: no rows by Add", q)
		}
		if strings.HasPrefix(q, "* | ") {
			// An aggregate's rows come in no promised order.
			slices.Sort(want)
			slices.Sort(got)
		}
		if !slices.Equal(want, got) {
			t.Fatalf("%s: %d rows by Add and %d by Columns; first by Add %q", q, len(want), len(got), want[0])
		}
	}
	if byMap.Rows() != n || byCols.Rows() != n {
		t.Fatalf("%d and %d rows, want %d", byMap.Rows(), byCols.Rows(), n)
	}
}

// Once a store has flushed a batch about the size of the next, adding rows
// allocates nothing: not per row, and not per call either. Each AllocsPerRun
// below runs once after its warm-up, so the count is the exact total.
func TestColumnsAddAllocatesNothingPerRow(t *testing.T) {
	db := open(t, nil)
	c, err := db.Columns(jobFields...)
	if err != nil {
		t.Fatal(err)
	}
	const rows, calls = 256, 25
	msgs, offsets := jobLines(rows)
	at := make([]int64, rows)
	for r := range at {
		at[r] = time.Now().UnixNano() + int64(r)
	}
	run := jobRun{"seb", "output", "0123456789abcdef", "1", "stdout", 0, rows, func(r int) (bool, bool) { return r%64 == 0, false }}
	cols := fillRun(emptyColumns(rows), run, msgs, offsets)
	batch := func() {
		for range calls {
			if err := c.Add(at, cols...); err != nil {
				t.Fatal(err)
			}
		}
	}
	// One batch the size of the warm-up and the measured run together, so
	// the next group's buffers are sized for both.
	batch()
	batch()
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	// The warm-up creates the group's columns, which is once per group.
	if a := testing.AllocsPerRun(1, batch); a != 0 {
		t.Fatalf("%v allocations adding %d rows in %d calls", a, rows*calls, calls)
	}
	line := func() {
		for r := range 100 {
			if err := c.AddRow(at[r], msgs[r], "output", "seb", "0123456789abcdef", "1", "stdout", offsets[r], "", ""); err != nil {
				t.Fatal(err)
			}
		}
	}
	if a := testing.AllocsPerRun(1, line); a != 0 {
		t.Fatalf("%v allocations adding 100 rows one at a time", a)
	}
}

func TestColumnsRefuseNonsense(t *testing.T) {
	db := open(t, nil)
	for _, names := range [][]string{{""}, {"_time"}, {".error"}, {"a", "a"}} {
		if _, err := db.Columns(names...); err == nil {
			t.Errorf("declared %q", names)
		}
	}
	c, err := db.Columns("_msg", "owner")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Add([]int64{1}, []string{"x"}); err == nil {
		t.Error("one column for two names")
	}
	if err := c.Add([]int64{1}, []string{"x"}, nil); err == nil {
		t.Error("a column with no values")
	}
	if err := c.AddRow(1, "x"); err == nil {
		t.Error("one value for two names")
	}
}

// benchBatch is the lines added between flushes, which are not timed: a
// supervisor flushes once per batch and the flush is measured on its own.
const benchBatch = 8192

func benchOpen(b *testing.B) *DB {
	db, err := Open(Config{Dir: filepath.Join(b.TempDir(), "db"), StreamFields: []string{"owner", "kind"}, MaintainEvery: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	return db
}

func flushUntimed(b *testing.B, db *DB) {
	b.StopTimer()
	if err := db.Flush(); err != nil {
		b.Fatal(err)
	}
	b.StartTimer()
}

// BenchmarkRows is the cost of one line, added the way a supervisor adds them:
// through Add with one reused map, through Columns a batch at a time with the
// host filling its column slices inside the timing, and through AddRow.
func BenchmarkRows(b *testing.B) {
	msgs, offsets := jobLines(benchBatch)
	const job = "0123456789abcdef0123456789abcdef"
	b.Run("path=add", func(b *testing.B) {
		db := benchOpen(b)
		f := make(map[string]string, 10)
		now := time.Now().UnixNano()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; {
			n := min(benchBatch, b.N-i)
			clear(f)
			f["kind"], f["owner"], f["job"], f["attempt"], f["stream"] = "output", "seb", job, "1", "stdout"
			for r := range n {
				delete(f, "binary")
				delete(f, "truncated")
				f["_msg"] = msgs[r]
				f["offset"] = offsets[r]
				db.Add(now+int64(i+r), f)
			}
			i += n
			flushUntimed(b, db)
		}
	})
	b.Run("path=columns", func(b *testing.B) {
		db := benchOpen(b)
		c, err := db.Columns(jobFields...)
		if err != nil {
			b.Fatal(err)
		}
		cols := emptyColumns(benchBatch)
		at := make([]int64, benchBatch)
		now := time.Now().UnixNano()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; {
			n := min(benchBatch, b.N-i)
			for r := range n {
				at[r] = now + int64(i+r)
				cols[0][r], cols[1][r], cols[2][r], cols[3][r], cols[4][r] = msgs[r], "output", "seb", job, "1"
				cols[5][r], cols[6][r], cols[7][r], cols[8][r] = "stdout", offsets[r], "", ""
			}
			var view [9][]string
			for k := range view {
				view[k] = cols[k][:n]
			}
			if err := c.Add(at[:n], view[:]...); err != nil {
				b.Fatal(err)
			}
			i += n
			flushUntimed(b, db)
		}
	})
	b.Run("path=row", func(b *testing.B) {
		db := benchOpen(b)
		c, err := db.Columns(jobFields...)
		if err != nil {
			b.Fatal(err)
		}
		now := time.Now().UnixNano()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; {
			n := min(benchBatch, b.N-i)
			for r := range n {
				if err := c.AddRow(now+int64(i+r), msgs[r], "output", "seb", job, "1", "stdout", offsets[r], "", ""); err != nil {
					b.Fatal(err)
				}
			}
			i += n
			flushUntimed(b, db)
		}
	})
}

// BenchmarkFlush is one Flush of a batch added through Columns: the group's
// dictionaries, encoding, the write and its fsync, timed apart from the rows.
func BenchmarkFlush(b *testing.B) {
	const job = "0123456789abcdef0123456789abcdef"
	for _, n := range []int{512, benchBatch, 64 << 10} {
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			msgs, offsets := jobLines(n)
			db := benchOpen(b)
			c, err := db.Columns(jobFields...)
			if err != nil {
				b.Fatal(err)
			}
			run := jobRun{"seb", "output", job, "1", "stdout", 0, n, func(int) (bool, bool) { return false, false }}
			cols := fillRun(emptyColumns(n), run, msgs, offsets)
			at := make([]int64, n)
			now := time.Now().UnixNano()
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				b.StopTimer()
				for r := range at {
					at[r] = now + int64(i*n+r)
				}
				if err := c.Add(at, cols...); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				if err := db.Flush(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkColumnsDeclare is what declaring the fields costs, once per store.
func BenchmarkColumnsDeclare(b *testing.B) {
	db := benchOpen(b)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := db.Columns(jobFields...); err != nil {
			b.Fatal(err)
		}
	}
}
