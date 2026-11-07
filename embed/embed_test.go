package embed

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func open(t *testing.T, tune func(*Config)) *DB {
	t.Helper()
	c := Config{Dir: filepath.Join(t.TempDir(), "db"), StreamFields: []string{"owner", "kind"}, MaintainEvery: time.Hour}
	if tune != nil {
		tune(&c)
	}
	db, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func row(msg string, kv ...string) map[string]string {
	f := map[string]string{"_msg": msg, "owner": "seb", "kind": "output"}
	for i := 0; i+1 < len(kv); i += 2 {
		f[kv[i]] = kv[i+1]
	}
	return f
}

func search(t *testing.T, db *DB, q string, o SearchOptions) []string {
	t.Helper()
	var out []string
	err := db.Search(t.Context(), q, o, func(rows []Row) error {
		for _, r := range rows {
			for _, f := range r.Fields {
				if f.Key == "_msg" {
					out = append(out, f.Value)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestFindsWhatWasAdded(t *testing.T) {
	db := open(t, nil)
	now := time.Now().UnixNano()
	db.Add(now, row("epoch 1 done", "job", "aa"))
	db.Add(now+1, row("CUDA out of memory", "job", "bb"))
	db.Add(now+2, row("epoch 2 done", "job", "aa"))
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := search(t, db, "memory", SearchOptions{}); len(got) != 1 || got[0] != "CUDA out of memory" {
		t.Fatalf("%q", got)
	}
	if got := search(t, db, "job:=aa", SearchOptions{}); len(got) != 2 {
		t.Fatalf("%q", got)
	}
	if got := search(t, db, "*", SearchOptions{Newest: 1}); len(got) != 1 || got[0] != "epoch 2 done" {
		t.Fatalf("newest: %q", got)
	}
	if got := search(t, db, "*", SearchOptions{From: now + 1}); len(got) != 2 {
		t.Fatalf("from: %q", got)
	}
	if db.Rows() != 3 || db.DiskBytes() == 0 {
		t.Fatalf("%d rows, %d bytes", db.Rows(), db.DiskBytes())
	}
}

func TestPipesWork(t *testing.T) {
	db := open(t, nil)
	now := time.Now().UnixNano()
	for i := range 10 {
		db.Add(now+int64(i), row(fmt.Sprintf("line %d", i), "job", fmt.Sprint(i%2)))
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	var got []Row
	err := db.Search(t.Context(), "* | stats by (job) count() n", SearchOptions{}, func(rows []Row) error {
		got = append(got, rows...)
		return nil
	})
	if err != nil || len(got) != 2 {
		t.Fatalf("%v %+v", err, got)
	}
}

func TestFlushedRowsSurviveAReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	db.Add(time.Now().UnixNano(), row("kept"))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal("a second Close:", err)
	}
	db, err = Open(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := search(t, db, "kept", SearchOptions{}); len(got) != 1 {
		t.Fatalf("%q", got)
	}
}

func TestSearchStopsWhenAsked(t *testing.T) {
	db := open(t, nil)
	db.Add(time.Now().UnixNano(), row("x"))
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := db.Search(ctx, "*", SearchOptions{}, func([]Row) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled search: %v", err)
	}
	stop := errors.New("enough")
	err = db.Search(t.Context(), "*", SearchOptions{}, func([]Row) error { return stop })
	if !errors.Is(err, stop) {
		t.Fatalf("an error from the callback: %v", err)
	}
	if err := db.Search(t.Context(), "bad(((", SearchOptions{}, func([]Row) error { return nil }); err == nil {
		t.Fatal("a bad query parsed")
	}
}

func TestSearchRefusesAShortAnswer(t *testing.T) {
	db := open(t, nil)
	now := time.Now().UnixNano()
	for i := range 100 {
		db.Add(now+int64(i), row("line"))
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	err := db.Search(t.Context(), "*", SearchOptions{MaxRows: 10}, func([]Row) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "row") {
		t.Fatalf("over the limit: %v", err)
	}
}

func TestRetentionDropsOldRows(t *testing.T) {
	db := open(t, func(c *Config) { c.Retention = time.Hour })
	now := time.Now()
	db.Add(now.Add(-2*time.Hour).UnixNano(), row("old"))
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	db.Add(now.UnixNano(), row("new"))
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	db.enforce()
	if got := search(t, db, "*", SearchOptions{}); len(got) != 1 || got[0] != "new" {
		t.Fatalf("%q", got)
	}
}

func TestTheBudgetDropsTheOldestFirst(t *testing.T) {
	db := open(t, nil)
	now := time.Now().UnixNano()
	text := strings.Repeat("some text that does not compress to nothing 0123456789 ", 32)
	for i := range 20 {
		db.Add(now+int64(i)*int64(time.Second), row(fmt.Sprintf("group-%d %s", i, text)))
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	total := db.DiskBytes()
	for range 40 {
		db.trim(total / 2)
	}
	if got := db.DiskBytes(); got > total/2 {
		t.Fatalf("%d bytes after trimming to %d", got, total/2)
	}
	if got := search(t, db, "group-19", SearchOptions{}); len(got) != 1 {
		t.Fatal("the newest group went")
	}
	if got := search(t, db, "group-0", SearchOptions{}); len(got) != 0 {
		t.Fatal("the oldest group stayed")
	}
}

func TestSmallGroupsGetMerged(t *testing.T) {
	db := open(t, nil)
	old := time.Now().Add(-time.Hour).UnixNano()
	for i := range 32 {
		db.Add(old+int64(i), row(fmt.Sprintf("line-%d", i)))
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	before := db.store.Len()
	db.enforce()
	if after := db.store.Len(); after >= before {
		t.Fatalf("%d groups, then %d", before, after)
	}
	if got := search(t, db, "line-7", SearchOptions{}); len(got) != 1 {
		t.Fatalf("%q", got)
	}
}

func TestOpenRefusesNonsense(t *testing.T) {
	if _, err := Open(Config{}); err == nil {
		t.Fatal("no directory")
	}
	if _, err := Open(Config{Dir: t.TempDir(), MaxBytes: -1}); err == nil {
		t.Fatal("a negative budget")
	}
}

func BenchmarkAdd(b *testing.B) {
	db, err := Open(Config{Dir: filepath.Join(b.TempDir(), "db"), StreamFields: []string{"owner", "kind"}, MaintainEvery: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	fields := row("a line of ordinary length, as jobs print them", "job", "0123456789abcdef", "stream", "stdout", "offset", "12345")
	now := time.Now().UnixNano()
	b.ReportAllocs()
	for i := range b.N {
		db.Add(now+int64(i), fields)
	}
	if err := db.Flush(); err != nil {
		b.Fatal(err)
	}
}
