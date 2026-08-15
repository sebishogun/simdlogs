package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// Recovery drills: the procedures in docs/runbooks/, executed.
//
// A runbook nobody has run is a document, not a procedure. Each of these
// performs the documented steps against a real server and asserts the outcome
// the runbook promises -- so a change that breaks the procedure breaks a test
// rather than being discovered during an incident.

// Drill: restore a backup onto a clean directory and verify equality.
//
// Equality of three things, because any one alone can hold while the others do
// not: the ROW COUNT (a restore that dropped a group), the row CONTENT (a
// restore that reordered or corrupted), and the ANSWER to a real query (a
// restore whose files are intact but whose manifest lost a group, so the data
// is on disk and invisible).
func TestDrillARestoredBackupAnswersIdentically(t *testing.T) {
	origin := realShard(t, corpus(1)[0])

	// The documented capture: GET /admin/backup, streamed to a file.
	resp, err := http.Get(origin.URL + "/admin/backup")
	if err != nil {
		t.Fatal(err)
	}
	archive, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("backup: %d %.200s", resp.StatusCode, archive)
	}
	if len(archive) == 0 {
		t.Fatal("the backup is empty")
	}

	// The documented restore: onto a CLEAN directory, never over a live store.
	fresh := t.TempDir()
	if err := storage.RestoreTar(strings.NewReader(string(archive)), fresh); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// A server over the restored directory. The restore unpacks one tenant's
	// store, so it becomes that server's default tenant directory.
	restored := serverOverStore(t, fresh)

	for _, q := range []string{
		`*`,
		`level:=error`,
		`* | stats count() c`,
		`* | stats by (level) count() c`,
		`* | sort by (_msg) | limit 5`,
	} {
		t.Run(q, func(t *testing.T) {
			oCode, oRows, oRaw := queryRows(t, origin, q)
			rCode, rRows, rRaw := queryRows(t, restored, q)
			if oCode != rCode {
				t.Fatalf("origin %d, restored %d: %.200s", oCode, rCode, rRaw)
			}
			if len(oRows) == 0 {
				t.Fatalf("the ORIGIN answered nothing for %q, so comparing the "+
					"restore against it proves nothing", q)
			}
			if len(oRows) != len(rRows) {
				t.Fatalf("%d rows from the origin, %d from the restore", len(oRows), len(rRows))
			}
			a, b := append([]string(nil), oRows...), append([]string(nil), rRows...)
			sort.Strings(a)
			sort.Strings(b)
			for i := range a {
				if a[i] != b[i] {
					t.Fatalf("row %d differs:\n  origin:   %s\n  restored: %s", i, a[i], b[i])
				}
			}
			_ = oRaw
		})
	}
}

// serverOverStore starts a server whose default tenant is an existing store
// directory.
func serverOverStore(t *testing.T, storeDir string) *httptest.Server {
	t.Helper()
	// The restore wrote a store's files directly into storeDir. A server takes
	// a PARENT directory and puts each tenant in a subdirectory of it, so the
	// restored store is moved into the place the default tenant will look.
	parent := t.TempDir()
	target := filepath.Join(parent, "tenant-0-0")
	if err := os.Rename(storeDir, target); err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(parent)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

// Drill: one corrupt group.
//
// The documented outcome is that the store REFUSES rather than serving damaged
// rows, and that the operator's options are visible: acknowledge and continue
// without the group, or restore. What must never happen is the damaged bytes
// coming back as data.
func TestDrillACorruptGroupIsRefusedNotServed(t *testing.T) {
	dir := t.TempDir()
	srv, err := NewServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	postLines(t, ts.URL, line("2026-06-01T12:00:00Z", "before-corruption"))
	ts.Close()
	srv.Close()

	var damaged int
	filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() || !strings.HasPrefix(filepath.Base(p), "group-") {
			return nil
		}
		b, rerr := os.ReadFile(p)
		if rerr != nil || len(b) < 32 {
			return nil
		}
		b[len(b)/2] ^= 0xff
		if os.WriteFile(p, b, 0o600) == nil {
			damaged++
		}
		return nil
	})
	if damaged == 0 {
		// Not a skip. A skip here reads as "the drill ran and found nothing
		// wrong", when what happened is that the drill never ran -- and the
		// fixture not producing a group file is a fixture defect, not an
		// environment difference.
		t.Fatalf("no group file was written, so there was nothing to damage and " +
			"this drill exercised nothing; the write did not reach the store")
	}

	srv2, err := NewServer(dir)
	if err != nil {
		// The store refusing to open is the documented outcome for a corrupt
		// group under the strict policy, and it is an explicit failure.
		if !strings.Contains(err.Error(), "corrupt") && !strings.Contains(err.Error(), "checksum") {
			t.Fatalf("the store refused to open but not for corruption: %v", err)
		}
		return
	}
	defer srv2.Close()
	ts2 := httptest.NewServer(srv2.Handler())
	defer ts2.Close()

	_, rows, raw := queryRows(t, ts2, "*")
	for _, r := range rows {
		if strings.Contains(r, "before-corruption") {
			t.Fatalf("a damaged group's rows were served as data: %.300s", raw)
		}
	}
	// And readiness must say so, or an operator has no signal at all.
	resp, err := http.Get(ts2.URL + "/-/ready")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Errorf("readiness is green over a store with a damaged group: %.200s", b)
	}
}

// Drill: one lost replica.
//
// The documented procedure is: bring a replacement up empty, point the router
// at it, run /admin/cluster/repair, and confirm the shard is whole again. This
// runs exactly that.
func TestDrillALostReplicaIsRebuiltByRepair(t *testing.T) {
	srv, front, nodes := replicatedShard(t, 2)
	for _, n := range nodes {
		for k := 0; k < 3; k++ {
			postLines(t, n.URL, line(
				"2026-06-01T12:00:0"+string(rune('0'+k))+"Z", "row"))
		}
	}
	if got := rowCount(t, nodes[1]); got != 3 {
		t.Fatalf("replica 1 starts with %d rows, want 3", got)
	}

	// Replica 1 is lost. Its replacement comes up EMPTY -- which is the case
	// that matters: a replacement with old data would be a different procedure.
	nodes[1].Close()
	replacement := realShard(t, nil)
	srv.SetBackends([]string{nodes[0].URL, replacement.URL})
	if got := rowCount(t, replacement); got != 0 {
		t.Fatalf("the replacement is not empty: %d rows", got)
	}

	rep := runRepair(t, front)
	if !rep.Complete {
		t.Fatalf("the repair pass did not complete: %+v", rep)
	}
	if got := rowCount(t, replacement); got != 3 {
		t.Fatalf("the replacement holds %d rows after repair, want 3", got)
	}
	// The survivor is untouched: repair only ever adds.
	if got := rowCount(t, nodes[0]); got != 3 {
		t.Fatalf("the surviving replica holds %d rows after repair, want 3", got)
	}
	// And a second pass copies nothing, so the shard is genuinely in step.
	if again := runRepair(t, front); again.Copied != 0 {
		t.Fatalf("a second pass copied %d groups; the shard is not converged", again.Copied)
	}
}
