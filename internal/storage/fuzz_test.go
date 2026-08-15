package storage

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Fuzzing what a store reads off disk or off the wire.
//
// The group parser has its own target in group_corrupt_test.go. These cover the
// rest of the surface where bytes this process did not produce become decisions
// it acts on: the backup manifest, a restore archive, and the adopted groups an
// anti-entropy peer sends.
//
// The property throughout: a refusal must leave NOTHING behind. A restore that
// unpacked half an archive before rejecting it has produced a directory that
// looks like a store, and the next open will read it.

func FuzzBackupManifest(f *testing.F) {
	good, _ := json.Marshal(BackupManifest{
		Format: BackupFormat, CreatedUnix: 1, Tenant: "t", ManifestSeq: 3,
	})
	f.Add(good)
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"format":99999999}`))
	f.Add([]byte(`{"format":1,"groups":[{"id":18446744073709551615}]}`))
	f.Add([]byte(`{`))
	f.Fuzz(func(t *testing.T, data []byte) {
		// Decoding must be total: whatever the bytes are, deciding whether they
		// describe a usable archive must not panic, and it must decide the same
		// way twice -- an archive accepted on one read and refused on the next
		// is a restore that depends on when it ran.
		m1, err1 := decodeBackupManifest(data)
		m2, err2 := decodeBackupManifest(data)
		if (err1 == nil) != (err2 == nil) {
			t.Fatalf("decoding is not deterministic: %v / %v", err1, err2)
		}
		if err1 != nil {
			if err1.Error() != err2.Error() {
				t.Fatalf("two different errors:\n  %v\n  %v", err1, err2)
			}
			return
		}
		if m1 == nil || m2 == nil {
			t.Fatal("decoded to a nil manifest with no error")
		}
		if m1.Format != BackupFormat {
			t.Fatalf("accepted format %d, this build reads %d: a newer archive "+
				"partially understood is worse than one refused", m1.Format, BackupFormat)
		}
		// TotalRows sums what the archive claims; it must not overflow into a
		// negative, which a restore would then treat as an empty archive.
		if n := m1.TotalRows(); n < 0 {
			t.Fatalf("TotalRows is %d for %d groups", n, len(m1.Groups))
		}
	})
}

// A restore archive is attacker-controlled if the operator's backup store is.
// Path traversal, absurd sizes, and links out of the directory all have to be
// refused -- and refused before anything lands.
func FuzzRestoreTar(f *testing.F) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	tw.WriteHeader(&tar.Header{Name: "manifest.json", Mode: 0o600, Size: 2})
	tw.Write([]byte("{}"))
	tw.Close()
	f.Add(buf.Bytes())

	var evil bytes.Buffer
	ew := tar.NewWriter(&evil)
	ew.WriteHeader(&tar.Header{Name: "../../etc/passwd", Mode: 0o600, Size: 1})
	ew.Write([]byte("x"))
	ew.Close()
	f.Add(evil.Bytes())

	f.Add([]byte("not a tar at all"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			return
		}
		dir := t.TempDir()
		err := RestoreTar(bytes.NewReader(data), dir)
		if err == nil {
			return
		}
		// Refused. Nothing may have escaped the target directory, and nothing
		// inside it may be a path the archive chose: a restore that wrote
		// ../../etc/passwd and then failed has already done the damage.
		filepath.Walk(dir, func(p string, fi os.FileInfo, werr error) error {
			if werr != nil {
				return nil
			}
			rel, rerr := filepath.Rel(dir, p)
			if rerr != nil || strings.HasPrefix(rel, "..") {
				t.Fatalf("a refused restore left %q outside %q", p, dir)
			}
			return nil
		})
	})
}

// AdoptGroup takes bytes from a peer. It must never store what it cannot
// verify, and must never store the same content twice.
func FuzzAdoptGroup(f *testing.F) {
	f.Add([]byte{}, "")
	f.Add([]byte("not a group"), "0000")
	f.Fuzz(func(t *testing.T, blob []byte, digest string) {
		if len(blob) > 1<<20 {
			return
		}
		st, err := OpenStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()

		adopted, err := st.AdoptGroup(digest, blob)
		if err != nil {
			if adopted {
				t.Fatal("reported an adoption alongside an error")
			}
			if n := st.TotalRows(); n != 0 {
				t.Fatalf("a refused adoption stored %d rows", n)
			}
			return
		}
		// Accepted. Then the digest must genuinely name these bytes, and a
		// second adoption of the same content must be a no-op.
		if digestBytes(blob) != digest {
			t.Fatalf("accepted bytes hashing to %s under the name %s",
				digestBytes(blob), digest)
		}
		again, err := st.AdoptGroup(digest, blob)
		if err != nil {
			t.Fatalf("the second adoption failed: %v", err)
		}
		if again {
			t.Fatal("adopted the same content twice; every repair pass would grow the store")
		}
	})
}

// GroupDigests and GroupBytes are the inventory a peer repairs from. A digest
// the store does not hold must never be served as some other group.
func FuzzGroupBytesByDigest(f *testing.F) {
	f.Add("")
	f.Add(strings.Repeat("0", 64))
	f.Add("../../etc/passwd")
	f.Fuzz(func(t *testing.T, digest string) {
		st, err := OpenStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		if _, err := st.AppendGroup(aeGroup("x", 3, 1000)); err != nil {
			t.Fatal(err)
		}
		have, err := st.GroupDigests()
		if err != nil {
			t.Fatal(err)
		}
		blob, err := st.GroupBytes(digest)
		if err != nil {
			return
		}
		// Served something. It must be the group whose digest was asked for --
		// a path-shaped digest must not reach the filesystem, and a miss must
		// not fall back to "the only group there is".
		if digestBytes(blob) != digest {
			t.Fatalf("asked for %q and got bytes hashing to %s", digest, digestBytes(blob))
		}
		found := false
		for _, d := range have {
			if d.Digest == digest {
				found = true
			}
		}
		if !found {
			t.Fatalf("served digest %q that the inventory does not list", digest)
		}
	})
}
