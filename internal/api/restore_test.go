package api

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// The staged restore, end to end: back up a live server over HTTP, validate the
// archive without writing anything, restore it into a fresh directory, and
// query the result through a real server.
//
// TestBackupRestore covers the same round trip through RestoreTar. What this
// adds is the two things a disaster-recovery tool is judged on and RestoreTar
// cannot do: a dry run that answers "is this archive good?" before an operator
// commits to it, and a destination that is either the whole store or holds no
// groups at all.
// Both are worth nothing unless the restored bytes still open and answer
// queries -- an atomic restore of an unqueryable store is a tidier failure, not
// a better one.
func TestStagedRestoreOpensAndQueries(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	// Two separate inserts -> two immutable group files.
	postBody(t, ts, `{"_time":1,"service":"a","_msg":"x"}`+"\n"+`{"_time":2,"service":"a","_msg":"y"}`+"\n")
	postBody(t, ts, `{"_time":3,"service":"b","_msg":"z"}`+"\n")

	r, err := http.Get(ts.URL + "/admin/backup")
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r.Body); err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	archive := buf.Bytes()

	// A per-tenant backup restores into that tenant's store dir; the default
	// tenant lives under tenant-0-0.
	dir := t.TempDir()
	dst := filepath.Join(dir, "tenant-0-0")

	// What `simdlogs restore -dry-run` does: full validation, nothing written.
	man, err := storage.Restore(bytes.NewReader(archive), dst, storage.RestoreOptions{DryRun: true})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if len(man.Groups) == 0 || man.TotalRows() != 3 {
		t.Fatalf("dry run manifest: %d groups, %d rows; want >0 groups and 3 rows",
			len(man.Groups), man.TotalRows())
	}
	if _, err := os.Stat(dst); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dry run created %s (stat err %v)", dst, err)
	}

	// The archive names the tenant it was taken from, and a restore can be made
	// to insist on it: dropping one tenant's groups into another's directory
	// produces a store that answers that tenant's queries with someone else's
	// logs, and nothing inside a group file records where it came from.
	man2, err := storage.Restore(bytes.NewReader(archive), dst, storage.RestoreOptions{
		RequireTenant: man.Tenant,
	})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if man2.TotalRows() != man.TotalRows() {
		t.Fatalf("restored %d rows, dry run validated %d", man2.TotalRows(), man.TotalRows())
	}

	srv2, err := NewServer(dir)
	if err != nil {
		t.Fatalf("open restored store: %v", err)
	}
	ts2 := httptest.NewServer(srv2.Handler())
	defer ts2.Close()
	if st := statsBy(t, ts2, "service"); st["a"] != 2 || st["b"] != 1 {
		t.Fatalf("restored stats by service = %v want a:2 b:1", st)
	}
}

// A restore into a live tenant's directory is refused, by the emptiness check
// -- a live store holds a MANIFEST and its groups. The LOCK plays no part: it
// is never counted, because the file outlives the process that made it and
// counting it made a killed restore unrecoverable. lockDir is what tells a
// running process from a file it left behind.
func TestStagedRestoreRefusesALiveStore(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	postBody(t, ts, `{"_time":1,"service":"a","_msg":"x"}`+"\n")

	r, err := http.Get(ts.URL + "/admin/backup")
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r.Body); err != nil {
		t.Fatal(err)
	}
	r.Body.Close()

	// The live server's own directory, which holds its LOCK and its groups.
	live := filepath.Join(srv.Dir(), "tenant-0-0")
	if _, err := os.Stat(filepath.Join(live, storage.LockFileName)); err != nil {
		t.Fatalf("expected a lock in the live store dir: %v", err)
	}
	_, err = storage.Restore(bytes.NewReader(buf.Bytes()), live, storage.RestoreOptions{})
	if !errors.Is(err, storage.ErrDestinationNotEmpty) {
		t.Fatalf("restore over a live store: %v, want ErrDestinationNotEmpty", err)
	}

	// And the live store still answers.
	if st := statsBy(t, ts, "service"); st["a"] != 1 {
		t.Fatalf("live stats after the refused restore = %v want a:1", st)
	}
}
