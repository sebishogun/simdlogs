package main

import (
	"archive/tar"
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// The command, end to end.
//
// `cmd/simdlogs` had no tests at all, which is how a usage message claiming
// "the destination is the whole store or is untouched" shipped over an
// implementation where a concurrent server could make it neither.
func backupFile(t *testing.T) (path string, tenant string) {
	t.Helper()
	store, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i := 0; i < 3; i++ {
		d := storage.BuildDict([]string{"alpha", "beta"})
		g := &storage.Group{Rows: 2, Columns: []storage.Column{
			{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{int64(i*10 + 1), int64(i*10 + 2)}},
			{Name: "_msg", Type: storage.ColDict, Dict: &d},
		}}
		if _, err := store.AppendGroup(g); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	if err := store.BackupTarWith(&buf, storage.BackupOptions{Tenant: "0:0"}); err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(t.TempDir(), "backup.tar")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return path, "0:0"
}

func run(t *testing.T, stdin string, args ...string) (code int, out, errOut string) {
	t.Helper()
	var o, e bytes.Buffer
	code = runRestore(args, strings.NewReader(stdin), &o, &e)
	return code, o.String(), e.String()
}

func TestRestoreCommand(t *testing.T) {
	archive, tenant := backupFile(t)

	t.Run("dry run needs no destination", func(t *testing.T) {
		code, out, errOut := run(t, "", "-src", archive, "-dry-run")
		if code != 0 {
			t.Fatalf("exit %d: %s", code, errOut)
		}
		if !strings.Contains(out, "validated:") || !strings.Contains(out, tenant) {
			t.Fatalf("output %q does not report the validated archive and its tenant", out)
		}
	})

	t.Run("restore and reopen", func(t *testing.T) {
		dst := filepath.Join(t.TempDir(), "tenant-0-0")
		code, out, errOut := run(t, "", "-src", archive, "-dst", dst, "-tenant", tenant)
		if code != 0 {
			t.Fatalf("exit %d: %s", code, errOut)
		}
		if !strings.Contains(out, "restored:") {
			t.Fatalf("output %q", out)
		}
		s, err := storage.OpenStore(dst)
		if err != nil {
			t.Fatalf("the restored store does not open: %v", err)
		}
		s.Close()
	})

	t.Run("stdin", func(t *testing.T) {
		b, err := os.ReadFile(archive)
		if err != nil {
			t.Fatal(err)
		}
		dst := filepath.Join(t.TempDir(), "store")
		code, _, errOut := run(t, string(b), "-src", "-", "-dst", dst)
		if code != 0 {
			t.Fatalf("exit %d: %s", code, errOut)
		}
	})

	t.Run("the wrong tenant is refused", func(t *testing.T) {
		dst := filepath.Join(t.TempDir(), "store")
		code, _, errOut := run(t, "", "-src", archive, "-dst", dst, "-tenant", "9:9")
		if code == 0 {
			t.Fatal("an archive from another tenant was restored")
		}
		if !strings.Contains(errOut, "different tenant") {
			t.Fatalf("stderr %q", errOut)
		}
		// A refused restore leaves at most an empty directory holding a lock
		// nobody holds: the lock file is never unlinked, because unlinking
		// one hands a lock to two processes. No groups, and the next restore
		// accepts it.
		ents, _ := os.ReadDir(dst)
		for _, e := range ents {
			if e.Name() != "LOCK" {
				t.Fatalf("the refused restore left %q behind", e.Name())
			}
		}
	})

	t.Run("a negative limit is a typo, not a default", func(t *testing.T) {
		dst := filepath.Join(t.TempDir(), "store")
		code, _, errOut := run(t, "", "-src", archive, "-dst", dst, "-max-files", "-1")
		if code != 2 {
			t.Fatalf("exit %d, want 2", code)
		}
		if !strings.Contains(errOut, "must not be negative") {
			t.Fatalf("stderr %q", errOut)
		}
	})

	t.Run("no destination without -dry-run is a usage error", func(t *testing.T) {
		code, _, errOut := run(t, "", "-src", archive)
		if code != 2 {
			t.Fatalf("exit %d, want 2 (usage), got stderr %q", code, errOut)
		}
		if !strings.Contains(errOut, "usage:") {
			t.Fatalf("stderr %q does not print the usage", errOut)
		}
	})

	t.Run("a landed but unsynced store says so", func(t *testing.T) {
		// The whole operator-facing reason ErrRestoredButUnsynced exists: the
		// store is there, and a retry would hit an occupied destination.
		dst := filepath.Join(t.TempDir(), "store")
		var total int
		stop := storage.SetFaultHookForTest(func(p storage.FaultPoint) error {
			if storage.FaultPointString(p) == "dir-sync" {
				total++
			}
			return nil
		})
		if code, _, e := run(t, "", "-src", archive, "-dst", dst); code != 0 {
			stop()
			t.Fatalf("the counting restore: exit %d, %s", code, e)
		}
		stop()

		dst2 := filepath.Join(t.TempDir(), "store")
		n := 0
		stop = storage.SetFaultHookForTest(func(p storage.FaultPoint) error {
			if storage.FaultPointString(p) != "dir-sync" {
				return nil
			}
			n++
			if n == total {
				return errors.New("injected dir-sync failure")
			}
			return nil
		})
		code, _, errOut := run(t, "", "-src", archive, "-dst", dst2)
		stop()
		if code != 1 {
			t.Fatalf("exit %d, want 1", code)
		}
		if !strings.Contains(errOut, "do not retry") {
			t.Fatalf("stderr %q does not tell the operator the store landed", errOut)
		}
		if _, err := os.Stat(filepath.Join(dst2, "group-0.bin")); err != nil {
			t.Fatalf("the store did not land: %v", err)
		}
	})

	t.Run("-h is a request, not a mistake", func(t *testing.T) {
		if code, _, _ := run(t, "", "-h"); code != 0 {
			t.Fatalf("exit %d, want 0", code)
		}
	})

	t.Run("no source is a usage error", func(t *testing.T) {
		if code, _, _ := run(t, "", "-dst", t.TempDir()); code != 2 {
			t.Fatalf("exit %d, want 2", code)
		}
	})

	t.Run("a missing source file is reported", func(t *testing.T) {
		code, _, errOut := run(t, "", "-src", filepath.Join(t.TempDir(), "nope.tar"), "-dry-run")
		if code != 1 {
			t.Fatalf("exit %d, want 1", code)
		}
		if !strings.Contains(errOut, "no such file") {
			t.Fatalf("stderr %q", errOut)
		}
	})
}

// A CLUSTER archive is named as one, not called unverifiable.
//
// It carries cluster.json and one tar per shard, and no BACKUP-MANIFEST -- so
// the node restore reported "the backup archive carries no manifest", which
// names -allow-unverified. That flag is for a pre-format-1 NODE archive and
// fails identically, so an operator holding a cluster backup was told their
// archive was unverifiable and pointed at the one option that cannot help.
//
// This is also ValidateClusterBackup's first production caller. Its doc says it
// is "called BEFORE anything is unpacked" because "a restore that discovers the
// mismatch halfway has already written some of it", and no shipped path called
// it -- the example the unwired-mechanism gate's own header cites.
func TestAClusterArchiveIsNamedRatherThanCalledUnverifiable(t *testing.T) {
	// A cluster archive by hand, in the shape clusterBackup writes: the
	// manifest first, then one tar per shard.
	man := `{"format":1,"createdUnix":1700000000,"protocol":1,` +
		`"shards":[{"shard":0,"archive":"shard-0.tar","rows":7},` +
		`{"shard":1,"archive":"shard-1.tar","rows":5}]}`
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	write := func(name string, body []byte) {
		t.Helper()
		if err := tw.WriteHeader(&tar.Header{
			Name: name, Mode: 0o600, Size: int64(len(body)),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	write("cluster.json", []byte(man))
	write("shard-0.tar", bytes.Repeat([]byte{0}, 1024))
	write("shard-1.tar", bytes.Repeat([]byte{0}, 1024))
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "cluster.tar")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	// Every way an operator would reach for it, including the flag the old
	// message sent them to.
	for _, args := range [][]string{
		{"-src", path, "-dst", filepath.Join(t.TempDir(), "into")},
		{"-src", path, "-dry-run"},
		{"-src", path, "-dst", filepath.Join(t.TempDir(), "into2"), "-allow-unverified"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			code, _, errOut := run(t, "", args...)
			if code == 0 {
				t.Fatalf("restoring a cluster archive into a node succeeded: %s", errOut)
			}
			if !strings.Contains(errOut, "cluster archive") {
				t.Errorf("the refusal does not say what the archive IS:\n%s", errOut)
			}
			if strings.Contains(errOut, "carries no manifest") {
				t.Errorf("the refusal still calls a cluster archive unmanifested, "+
					"which points at -allow-unverified and that cannot help:\n%s", errOut)
			}
			// The manifest's own facts, so the operator knows what they hold.
			for _, want := range []string{"2 shards", "shard-0.tar", "shard-1.tar", "7 rows"} {
				if !strings.Contains(errOut, want) {
					t.Errorf("the refusal does not mention %q:\n%s", want, errOut)
				}
			}
			if !strings.Contains(errOut, "one shard at a time") {
				t.Errorf("the refusal does not say what to do instead:\n%s", errOut)
			}
		})
	}
}

// A cluster archive whose manifest does not validate is reported as that.
//
// ValidateClusterBackup runs before the manifest's contents are quoted, so a
// malformed one is named rather than read out as fact -- two shards claiming
// the same index would otherwise be printed as a shard list an operator could
// act on.
func TestAMalformedClusterManifestIsNotQuotedAsFact(t *testing.T) {
	for _, tc := range []struct{ name, man, want string }{
		{"two archives claim one shard",
			`{"format":1,"protocol":1,"shards":[{"shard":0,"archive":"a.tar"},{"shard":0,"archive":"b.tar"}]}`,
			"appears twice"},
		{"a shard names no archive",
			`{"format":1,"protocol":1,"shards":[{"shard":0,"archive":""}]}`,
			"names no archive"},
		{"no shards at all",
			`{"format":1,"protocol":1,"shards":[]}`,
			"records no shards"},
		{"not JSON", `{`, "does not parse"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			tw := tar.NewWriter(&buf)
			if err := tw.WriteHeader(&tar.Header{
				Name: "cluster.json", Mode: 0o600, Size: int64(len(tc.man)),
			}); err != nil {
				t.Fatal(err)
			}
			tw.Write([]byte(tc.man))
			tw.Close()
			path := filepath.Join(t.TempDir(), "cluster.tar")
			if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
				t.Fatal(err)
			}
			code, _, errOut := run(t, "", "-src", path, "-dry-run")
			if code == 0 {
				t.Fatalf("a malformed cluster archive succeeded: %s", errOut)
			}
			if !strings.Contains(errOut, tc.want) {
				t.Errorf("the refusal does not say %q:\n%s", tc.want, errOut)
			}
		})
	}
}
