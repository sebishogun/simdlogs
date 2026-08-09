package storage

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
)

// BackupTar streams a tar of the store's current group files to w. Because a
// group file is immutable once written (append is write-temp-fsync-rename,
// never mutate), the set captured under the lock is a consistent snapshot even
// as ingest continues -- the VictoriaLogs vmbackup shape, minus the object
// store. A group dropped by retention between snapshot and read is skipped.
func (s *Store) BackupTar(w io.Writer) error {
	s.mu.RLock()
	paths := make([]string, len(s.groups))
	for i, g := range s.groups {
		paths[i] = g.path
	}
	s.mu.RUnlock()

	tw := tar.NewWriter(w)
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			continue // removed by retention after the snapshot; not part of this backup
		}
		if err := tw.WriteHeader(&tar.Header{
			Name: filepath.Base(p),
			Mode: 0o644,
			Size: int64(len(b)),
		}); err != nil {
			return err
		}
		if _, err := tw.Write(b); err != nil {
			return err
		}
	}
	return tw.Close()
}

// RestoreTar unpacks a backup produced by BackupTar into dir (created if
// absent), so a fresh store can be opened over it. Entry names are flattened to
// their base, so a crafted archive cannot escape dir.
func RestoreTar(r io.Reader, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		out, err := os.Create(filepath.Join(dir, filepath.Base(hdr.Name)))
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, tr); err != nil { //nolint:gosec // sizes are our own group files
			out.Close()
			return err
		}
		if err := out.Close(); err != nil {
			return err
		}
	}
	return nil
}
