package storage

import (
	"os"
	"path/filepath"
	"sync"
)

// DataFileMode is the mode every file this package writes is created with.
// Log data is the payload, so it is owner-only rather than umask-dependent:
// a store directory that lands on a shared machine should not be readable by
// every local account because the process umask happened to be 022.
const DataFileMode os.FileMode = 0o600

// faultPoint names one step of the durable replacement so tests can fail
// exactly one of them. Production never sets a hook, and the whole mechanism
// costs one nil check per step.
type faultPoint int

const (
	faultCreate faultPoint = iota
	faultWrite
	faultSync
	faultClose
	faultRename
	faultDirOpen
	faultDirSync

	// The manifest's own two steps. They are the commit point: a record that
	// is written but not synced is not a commit, and a record that is synced
	// IS one even if the process dies immediately after. The crash matrix
	// needs to stop between them, which no fault point covered.
	faultManifestWrite
	faultManifestSync

	// Points outside the durable write entirely, so the matrix can also stop
	// with rows buffered and nothing on disk, and after a batch has been
	// acknowledged to its caller.
	faultBuffered
	faultPostAck
)

// faultPointName is for test output and for the crash matrix's subprocess
// protocol, which names the phase on the command line.
var faultPointName = map[faultPoint]string{
	faultCreate:        "temp-create",
	faultWrite:         "partial-write",
	faultSync:          "file-sync",
	faultClose:         "file-close",
	faultRename:        "rename",
	faultDirOpen:       "dir-open",
	faultDirSync:       "dir-sync",
	faultManifestWrite: "manifest-append",
	faultManifestSync:  "manifest-sync",
	faultBuffered:      "buffering",
	faultPostAck:       "post-ack",
}

var (
	faultMu   sync.RWMutex
	faultHook func(faultPoint) error
)

// setFaultHook installs a fault injector and returns a function restoring
// the previous one. Tests only.
func setFaultHook(h func(faultPoint) error) func() {
	faultMu.Lock()
	prev := faultHook
	faultHook = h
	faultMu.Unlock()
	return func() {
		faultMu.Lock()
		faultHook = prev
		faultMu.Unlock()
	}
}

func fault(p faultPoint) error {
	faultMu.RLock()
	h := faultHook
	faultMu.RUnlock()
	if h == nil {
		return nil
	}
	return h(p)
}

// writeFileAtomic replaces path with data durably, and is the only way this
// package creates a file that other processes or a later start will read.
//
// The sequence is: write the bytes to a temp file beside the destination,
// fsync the file, close it and check the close, rename over the destination,
// then fsync the parent directory.
//
// The last step is the one that was missing. A rename is atomic with respect
// to a concurrent reader -- that much was already true -- but atomicity is
// not durability: until the directory itself is synced, the entry pointing at
// the new inode lives only in the page cache, and a power loss can leave the
// directory naming the old file (or nothing) while the data blocks are safely
// on disk. Fsyncing the file and not its directory guarantees the contents of
// a file that may not be there.
//
// On failure the temp file is removed, so a partial write can never be picked
// up by the group-*.bin glob at the next open.
func writeFileAtomic(path string, data []byte, perm os.FileMode) (err error) {
	dir := filepath.Dir(path)
	tmp := path + ".tmp"

	if ferr := fault(faultCreate); ferr != nil {
		return ferr
	}
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	// Any exit before the rename removes the temp file. After a successful
	// rename the temp name no longer exists, so the remove is a no-op.
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(tmp)
		}
	}()

	if err = fault(faultWrite); err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = fault(faultSync); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = fault(faultClose); err != nil {
		return err
	}
	// Close is checked: on some filesystems a deferred write error is only
	// reported here, and an unchecked Close turns that into silent loss.
	if err = f.Close(); err != nil {
		return err
	}
	if err = fault(faultRename); err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	// Past this point the destination holds the new bytes, so the deferred
	// cleanup must not delete anything -- and it does not, since tmp is gone.
	if err = syncDir(dir); err != nil {
		return err
	}
	return nil
}

// syncDirNamed fsyncs the directory holding path. Callers that renamed or
// unlinked through some other route -- a rename this helper did not perform,
// or a removal -- use it to make that change durable.
func syncDirNamed(path string) error { return syncDir(filepath.Dir(path)) }
