package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Restoring a backup, atomically.
//
// RestoreTar writes each group into the destination as it reads it, which
// makes a failure halfway a destination holding half a store -- and a
// directory of valid group files opens as a store and answers queries with a
// silent subset. That is the shape a disaster-recovery tool must not have: the
// moment it is used is the moment there is nothing to compare against.
//
// So a restore stages. Every group lands in a sibling directory, is validated
// on the way in, and the whole thing is moved into place with one rename. A
// rename within a filesystem is atomic, so the destination is either the store
// that was in the archive or nothing at all -- never a prefix of it.
//
// The sibling matters: a rename across filesystems fails with EXDEV rather
// than doing anything, so staging on another device turns every restore into
// an error after all the work. Staging inside the destination's own parent
// keeps them on one device for every ordinary destination; a destination that
// is itself a mount point is the exception, and it fails loudly at the rename
// rather than corrupting anything.
//
// # The lock, and why the emptiness check alone was worthless
//
// The check that a destination is empty runs at the start; the rename runs at
// the end, an archive-read later. Between them a server can open a store at
// that path -- and the restore then removed the live store's LOCK, its
// manifest and every one of its groups, renamed the archive over the top, and
// returned success. The live writer, still running, allocated group ids from
// its own counter and overwrote the archive's files. The result opened clean
// and answered queries with a silent mixture of two stores.
//
// Two concurrent restores to one destination were the same defect in a
// different dress: both derive the same staging path, the second wipes the
// first's files mid-stream, and both write into it. Measured, that produced a
// destination holding two groups from each of two archives, with one call
// returning success and neither having asked for a tenant check.
//
// Both are closed by taking the store's own exclusive lock on the destination
// and holding it until the removal that ends that directory. A server cannot
// open the directory while it is held, a second restore cannot start WHILE IT
// IS HELD -- in the gap between the two renames one can, and the outcome is
// one of THREE, not one: this call aborts ENOENT, or aborts EEXIST, or, with
// both parked in their own gaps, returns nil over the other's destination.
// See the paragraph on the staging lock below for the measurement; the
// interleaving is not free either, because a kill inside it can leave a
// marker-less staging directory, which clearStaging refuses. And the
// emptiness re-check immediately before the removal is what makes the removal
// safe: a lock stops a process opening a STORE there, not one dropping a file
// in.
//
// One window is left and it is safe rather than closed: between the moment
// the destination leaves the namespace and the rename that refills it, the
// destination does not exist, so a process that opens a store there has to
// CREATE it -- and the rename then fails with EEXIST and this call aborts
// having touched nothing of theirs. Releasing the lock any earlier destroys
// that argument, because the directory is still there for them to open. Round
// six did exactly that, for a Windows requirement, and measured a restore
// returning nil with the archive's groups overwritten.
//
// # Why the old destination is renamed away and not removed in place
//
// That argument requires the destination to go from "exists, holding a lock
// this call holds" to "does not exist" with nothing in between. os.RemoveAll
// is not that: it unlinks dst/LOCK, then re-reads the directory until it reads
// empty, then rmdirs. In the middle of its own loop the directory is still
// there and its lock is gone, so a server's OpenStore creates a SECOND LOCK
// inode at the same path, flocks it unopposed, and writes a manifest and a
// group -- which the loop's next pass unlinks. The rename then succeeds,
// because the winner never had to create the directory and there is no EEXIST
// to abort on, and the ghost keeps writing into the restored store by path.
//
// Measured on the version that removed in place, thirty seconds against twelve
// ghosts: 24 of the archive's groups overwritten in one run and 3 in another,
// against 0 for the same harness with Restore never called; 2432 foreign LOCK
// files and 508 foreign MANIFEST and group files unlinked by a removal
// documented as touching nothing of anyone else's. Two inodes at one path,
// each flocked by a different process.
//
// So the destination is renamed to a sibling instead. rename(2) is one
// syscall: the directory and the lock inode inside it leave together, and
// there is no observable state where dst exists without the lock that guards
// it. The sibling is removed afterwards, off a path no opener can reach.
// os.Rename's own Lstat-then-rename is not a hole in this: for the raw rename
// to replace a directory a ghost created, that directory must still be empty,
// which means the ghost has not made dst/LOCK yet -- and the lock it then
// opens is the staging lock this call holds, so it gets ErrLocked. The other
// ordering leaves dst non-empty and the rename fails.

// Restore limits. Every one of them bounds something an untrusted archive
// chooses, and the defaults are sized for a real store rather than for a
// hostile one -- an operator restoring something larger passes their own.
const (
	// DefaultMaxRestoreFiles bounds the entries a restore will place. A store
	// of 100k groups at the 128Ki-row ceiling is over 12 billion rows.
	DefaultMaxRestoreFiles = 100_000
	// DefaultMaxRestoreBytes bounds the total. 1 TiB.
	DefaultMaxRestoreBytes = 1 << 40
	// DefaultMaxRestoreFileBytes bounds one group. A group is written at or
	// below the ingest flush ceiling; 1 GiB is far past it and still refuses an
	// archive that declares a single entry larger than any store holds.
	DefaultMaxRestoreFileBytes = 1 << 30
)

// RestoreOptions configures a staged restore. The zero value applies the
// defaults above and validates without a tenant requirement.
type RestoreOptions struct {
	MaxFiles     int   // 0 means DefaultMaxRestoreFiles
	MaxBytes     int64 // 0 means DefaultMaxRestoreBytes
	MaxFileBytes int64 // 0 means DefaultMaxRestoreFileBytes

	// MaxManifestBytes bounds the manifest entry, which is decoded before any
	// limit above can apply: it is what sizes everything after it, so a
	// ceiling that only takes effect per group takes effect too late. A 24 MiB
	// manifest decodes into roughly 340 MiB of live heap. 0 means the built-in
	// 64 MiB ceiling.
	MaxManifestBytes int64

	// RequireTenant refuses an archive whose manifest names a different
	// tenant. Empty accepts any.
	//
	// Restoring one tenant's groups into another's directory produces a store
	// that answers queries with someone else's logs, and nothing in a group
	// file records which tenant it came from -- the manifest is the only place
	// that fact exists. The check runs the moment the manifest is decoded, and
	// no group is written before it: readBackup enforces manifest-first
	// RETROACTIVELY -- a group arriving earlier is read, parsed and emitted,
	// and the ordering error is raised only when the manifest turns up -- so a
	// restore that names a tenant refuses any group that precedes it rather
	// than relying on that enforcement. See readRestore.
	RequireTenant string

	// AllowUnverified keeps a pre-format-1 archive, which carries no manifest
	// and so cannot be checked against one. ErrBackupUnverified is still
	// returned, so a caller requiring a verified restore fails; without this
	// flag the restore is also discarded, which left an operator holding an
	// old backup with no supported path.
	AllowUnverified bool

	// DryRun validates the archive and writes nothing. It never touches a
	// destination -- not even to look at one -- so a scheduled backup check
	// needs no directory, and a dry run against the store you intend to
	// overwrite is not refused for being occupied.
	DryRun bool
}

func (o RestoreOptions) maxFiles() int {
	if o.MaxFiles > 0 {
		return o.MaxFiles
	}
	return DefaultMaxRestoreFiles
}

func (o RestoreOptions) maxBytes() int64 {
	if o.MaxBytes > 0 {
		return o.MaxBytes
	}
	return DefaultMaxRestoreBytes
}

func (o RestoreOptions) maxFileBytes() int64 {
	if o.MaxFileBytes > 0 {
		return o.MaxFileBytes
	}
	return DefaultMaxRestoreFileBytes
}

func (o RestoreOptions) readLimits() backupReadLimits {
	return backupReadLimits{
		maxManifestBytes: o.MaxManifestBytes,
		maxGroups:        o.maxFiles(),
		// The per-entry ceiling goes DOWN into readBackup rather than staying
		// in the emit callback: readBackup reads a whole entry with io.ReadAll
		// sized from the manifest before emit sees it, so a limit checked in
		// the callback is checked after the allocation it exists to prevent.
		// Measured: 268.5 MiB of live heap for a 128 MiB group with
		// MaxFileBytes set to 1, dry run included.
		maxEntryBytes: o.maxFileBytes(),
	}
}

var (
	// ErrDestinationNotEmpty reports a destination that already holds
	// something. A restore never merges: the result would be a store whose
	// group set is neither the archive's nor the destination's.
	ErrDestinationNotEmpty = errors.New("storage: the restore destination is not empty")

	// ErrWrongTenant reports an archive taken from a different tenant than the
	// one the caller required.
	ErrWrongTenant = errors.New("storage: the archive was taken from a different tenant")

	// ErrRestoreDestination reports a destination path a restore cannot use
	// safely: "." or "..", or a symbolic link.
	ErrRestoreDestination = errors.New("storage: the restore destination cannot be used")

	// ErrRestoredButUnsynced reports a restore whose store IS in place and
	// whose final directory sync failed. The distinction is the whole point:
	// the data landed, so a caller must not report plain failure and must not
	// retry into what is now a non-empty destination -- but the rename may not
	// survive a power loss until something else syncs that directory.
	ErrRestoredButUnsynced = errors.New("storage: the restore landed but its directory sync failed")
)

// Restore unpacks an archive into dst atomically, returning the manifest it
// validated.
//
// The destination must not exist, or must exist and be empty, and must not be
// a store any process holds open -- which is enforced by taking that store's
// own lock and holding it, not by looking once. Nothing is written to dst
// until every entry has been read, checksummed and parsed into a staging
// sibling; then one rename puts the whole store in place.
//
// A DryRun validates and writes nothing, which is what an operator wants
// before committing to a restore and what a scheduled backup check wants
// instead of a restore. It ignores dst entirely.
func Restore(r io.Reader, dst string, opt RestoreOptions) (*BackupManifest, error) {
	if err := opt.check(); err != nil {
		return nil, err
	}
	if opt.DryRun {
		return dryRunRestore(r, opt)
	}

	dst = filepath.Clean(dst)
	if err := checkRestorePath(dst); err != nil {
		return nil, err
	}
	if err := checkRestoreDestination(dst); err != nil {
		return nil, err
	}

	// The destination's mode is preserved across the swap when it already
	// existed: an operator who made the data directory 0700 did so on purpose,
	// and a rename substitutes the staging directory's mode for it.
	mode := os.FileMode(0o755)
	madeDst := false
	if fi, err := os.Stat(dst); errors.Is(err, os.ErrNotExist) {
		// MkdirAll, not Mkdir: the destination named in the README is
		// `./simdlogs-data/tenant-0-0`, whose parent the SERVER creates. After
		// the disk loss a restore is recovering from, neither exists.
		if err := os.MkdirAll(dst, mode); err != nil {
			return nil, err
		}
		madeDst = true
	} else if err != nil {
		return nil, err
	} else {
		mode = fi.Mode().Perm()
	}

	lock, err := lockDir(dst)
	if err != nil {
		if madeDst {
			os.Remove(dst)
		}
		return nil, err
	}
	// Always released, never conditionally: release drops the flock and closes
	// the descriptor and does NOT unlink, so there is nothing here that could
	// belong to another process. The conditional version -- guarding an unlink
	// that no longer exists -- also skipped the close, leaking one descriptor
	// per restore and, when os.RemoveAll(dst) failed, leaving a held lock that
	// made the destination unopenable for the life of the process.
	defer func() { lock.release() }()
	// The destination this call created is NOT removed on the way out, and the
	// rmdir that used to be here could only ever have removed somebody else's.
	//
	// Its own residue it cannot touch: lockDir has put a LOCK in the directory
	// by the time the deferred cleanup runs, the file is never unlinked --
	// unlinking a lock file is what hands one lock to two holders -- and rmdir
	// refuses a directory that is not empty. Measured after a truncated restore
	// into a path that did not exist: dst holds [LOCK] and the rmdir fails
	// every time.
	//
	// Somebody else's it can. A server that creates the destination in the gap
	// between this call's two renames has an empty directory for as long as it
	// takes to open its lock file, and the rmdir removes it out from under
	// them. Measured deterministically, not inferred. The cost is a spurious
	// OpenStore failure rather than data, and it was the last thing making
	// "aborts without touching anything" false.
	//
	// So the residue is an empty directory holding a lock nobody holds, which
	// the next restore accepts because the destination check ignores exactly
	// that file for exactly this reason.
	staging := dst + ".restoring"
	// Where the destination goes while the staging directory takes its place.
	// A sibling, for the reason staging is one: rename(2) does not cross a
	// filesystem, and this one has to be a rename rather than a removal.
	displaced := dst + ".restoring.old"
	marker := dst + restoringMarkerSuffix
	if err := clearStaging(staging, displaced, marker); err != nil {
		return nil, err
	}
	// The marker goes down BEFORE the directory it marks. Written after, a
	// kill in between left an empty, marker-less staging directory that
	// clearStaging then refused forever -- the same non-retryable destination
	// this marker exists to prevent, two syscalls wide instead of an
	// fsync-and-readdir wide. A marker with no staging directory is harmless
	// and clearStaging removes it.
	if err := os.WriteFile(marker, nil, DataFileMode); err != nil {
		return nil, err
	}
	// Removed LAST, which is what registering it FIRST buys: deferred calls run
	// in reverse, so this one runs after the staging and displaced cleanups
	// below and the marker outlives both directories it vouches for.
	//
	// The version this replaced removed the marker inside those two cleanups,
	// and the displaced one runs first -- so for the length of an unwind there
	// was a staging directory with no marker, which clearStaging refuses
	// forever. Sampled from another goroutine while a restore unwound after an
	// abort between the renames: 690 of 9,323 samples for a 400-group archive,
	// 58 of 967 for 40 groups, 13 of 212 for 4. Around 6 to 7 per cent of the
	// unwind, on the exact invariant two documents state.
	defer func() { os.Remove(marker) }()
	// The staging directory gets the destination's mode from the start rather
	// than 0755 and a chmod later: for a 0700 destination the old order left
	// the staging directory world-listable for the whole restore, leaking
	// group names and sizes.
	if err := os.Mkdir(staging, mode); err != nil {
		return nil, err
	}
	stagingCommitted := false
	defer func() {
		if !stagingCommitted {
			os.RemoveAll(staging)
		}
	}()
	// A marker, so a later restore can tell ITS OWN crashed staging directory
	// from a directory that merely shares the name. A pre-MANIFEST store is a
	// directory of group files and nothing else, which is exactly what
	// crashed staging looks like -- and `-dst /srv/logs` derives
	// `/srv/logs.restoring`, so the collision is a path an operator can
	// stumble into. Measured: without this, restoring alongside a legacy
	// store at that path destroyed it silently.

	man, unverified, err := readRestore(r, func(name string, data []byte) error {
		return writeFileAtomic(filepath.Join(staging, name), data, DataFileMode)
	}, opt)
	if err != nil {
		return man, err
	}

	// The files are durable (writeFileAtomic fsyncs each one and its
	// directory). The rename is what makes them visible under the name a
	// reader will open, and the parent's fsync is what makes the rename
	// survive a power loss -- the same pair AppendGroup needs, one level up.
	// Only a destination that already existed keeps its mode. One this call
	// created gets what mkdir gives, umask included: chmodding it to 0755
	// unconditionally made a restore produce a WIDER directory than the
	// server would have -- 0755 where OpenStore under umask 077 gives 0700,
	// which is log data made world-readable by the fix for the opposite bug.
	if !madeDst {
		if err := os.Chmod(staging, mode); err != nil {
			return man, err
		}
	}
	if err := syncDir(staging); err != nil {
		return man, err
	}

	// The lock that survives the swap.
	//
	// os.RemoveAll(dst) unlinks the lock file this call has been holding, so
	// from that syscall to the rename there is nothing stopping a process
	// opening a store at the destination -- and if it wins, it is a live
	// writer over the restored groups, allocating ids from its own counter
	// and unlinking group files by path on a failed commit. Reproduced: a
	// restore returning nil with the archive's group-0.bin overwritten by the
	// writer, and a restored directory missing a group.
	//
	// Locking the STAGING directory closes it, because that lock file is the
	// one the rename installs as dst/LOCK. After the rename this process
	// holds the restored store's own lock, so an opener gets ErrLocked rather
	// than a store; before the rename, an opener that creates dst makes the
	// rename fail with EEXIST, which is a safe abort.
	//
	// That closes it against a SERVER. It does not close it against a second
	// restore, and an earlier version of this paragraph said "there is no
	// ordering left in which someone else writes into the result", which is
	// false. Two restores each parked in their own gap between the two renames
	// produce a first call returning nil with a manifest naming its own three
	// groups over a destination holding the other's six. Measured, using this
	// package's own restore-removed fault point, with two ordinary Restore
	// calls and no hand-made filesystem state.
	//
	// Reachability, measured: zero at production speed -- 20,000 barrier
	// rounds of six workers with six distinct archives, and 11.6 million
	// overlapping attempts with 104,479 successful restores, produced no
	// instance. Every run that DID observe it had something outside a restore
	// widening the window -- a concurrent os.RemoveAll of the destination,
	// 267 of 85,686 samples. That bounds how often it happens; it is not a
	// proof that nothing else can reach it, which is the same kind of claim
	// this paragraph was rewritten to stop making. A latent hole rather than
	// a loss path, recorded rather than closed, because closing it needs a
	// lock that outlives the swap on a path neither restore owns.
	staged, err := lockDir(staging)
	if err != nil {
		return man, err
	}
	defer func() { staged.release() }()

	// It has to happen: os.Rename is NOT raw rename(2) -- it Lstats the
	// destination and returns EEXIST for any existing directory, empty or not,
	// except that having found one it then Lstats OLDNAME and reports that
	// error first, so a staging directory another restore already deleted
	// yields ENOENT even though the destination exists. That is the ENOENT
	// branch of the three-outcome answer above
	// -- so this line is load-bearing on Linux rather than the portability
	// nicety an earlier comment called it. A destination that is a mount point
	// fails HERE, at the rename that displaces it with EBUSY, rather than at
	// the rename that refills it.
	//
	// Re-checked immediately before it, and not at the top of the call: the
	// lock stops another process opening a STORE at the destination, it does
	// not stop one dropping a file in, and the archive read between the two
	// is however long the archive is. A check placed before that read says the
	// directory was empty then, which is not what this removal needs to be
	// true.
	if err := checkRestoreDestinationLocked(dst); err != nil {
		return man, err
	}
	// Set before the destination is displaced, not after the rename: the
	// displacement is what takes this call's lock file out of the path, so
	// everything after it must treat dst/LOCK as somebody else's.
	// Per-platform, and a no-op on unix.
	//
	// Windows cannot move a directory holding an open handle, so the lock has
	// to go first there, and that leaves Windows a window this fix does not
	// close -- narrower than the removal walk it replaces, and the same class:
	// a ghost that flocks dst/LOCK in it opens a store which the displacement
	// then moves aside and this call's cleanup removes. Not measurable here;
	// no Windows machine runs these tests.
	//
	// Unix must NOT release: rename(2) moves a directory whose lock file is
	// held without complaint and the descriptor stays valid, so the lock
	// protects the destination right up to the syscall that ends it -- while
	// an early release, now that the file is never unlinked, leaves dst/LOCK
	// present and UNHELD. A server then flocks the file that is already there,
	// opens the store, and the rename SUCCEEDS, because the winner never had
	// to create the directory and there is no EEXIST to abort on. Measured: a
	// restore returning nil with the archive's group-0.bin overwritten by that
	// writer.
	//
	// Round six put a Windows-only requirement in shared code, and the shared
	// platform is the one that lost data.
	lock.releaseBeforeRemoval()
	if ferr := fault(faultRestoreReleasing); ferr != nil {
		return man, ferr
	}
	// One syscall, because two is a window. See the header: os.RemoveAll's
	// unlink-then-readdir loop leaves the destination present and lockless in
	// the middle of itself, and a store opened in that gap is deleted by the
	// rest of the same loop while the rename goes on to succeed.
	//
	// checkRestoreDestinationLocked ran two syscalls ago and requires the
	// destination to hold nothing but this call's own lock file, so what moves
	// aside here is an empty directory -- and removing it afterwards removes
	// exactly what os.RemoveAll(dst) used to remove, off a path no opener can
	// reach.
	if err := os.Rename(dst, displaced); err != nil {
		return man, err
	}
	// Registered after the rename that creates it, so it needs no flag: every
	// path out from here has a directory to clean up. It does NOT remove the
	// marker -- see below, and the defer registered with the marker's write.
	defer func() {
		os.RemoveAll(displaced)
		// The marker is not removed here. It is removed by the defer registered
		// with its own write, which runs after this one.
		if ferr := fault(faultRestoreCleanup); ferr != nil {
			return
		}
	}()
	if ferr := fault(faultRestoreRemoved); ferr != nil {
		return man, ferr
	}
	if err := os.Rename(staging, dst); err != nil {
		return man, err
	}
	// dst/LOCK is now the inode `staged` holds; the one `lock` held is gone
	// with the directory that contained it.
	// Tell the lock where its file went, or release removes nothing -- and on
	// Windows, where release is what removes the file at all, leaves a LOCK
	// that O_EXCL then refuses forever.
	staged.movedTo(dst)
	stagingCommitted = true
	// The one instant the old code left unguarded: the store is in place and
	// this call still holds its lock. A test opens the destination here.
	if ferr := fault(faultRestoreRenamed); ferr != nil {
		return man, ferr
	}

	// The marker is NOT removed here. It outlives the store's arrival on
	// purpose: the displaced destination is still on disk until this call
	// returns, and the marker is the only thing that tells a later restore
	// that directory is residue it may clear rather than something it must
	// refuse. The deferred cleanup removes the two together, in that order.
	serr := syncDirNamed(dst)
	if rerr := staged.release(); serr == nil {
		serr = rerr
	}
	if serr == nil {
		serr = syncDirNamed(dst)
	}
	if serr != nil {
		err := fmt.Errorf("%w: %v", ErrRestoredButUnsynced, serr)
		if unverified {
			// Joined, not replaced: an operator told only that the sync
			// failed is not told that nothing validated the groups, and both
			// facts change what they do next.
			return man, errors.Join(err, ErrBackupUnverified)
		}
		return man, err
	}
	if unverified {
		return man, ErrBackupUnverified
	}
	return man, nil
}

// restoringMarkerSuffix names the file beside a staging directory that marks
// it as one a restore made: `<dst>.restoring.marker` for `<dst>.restoring`.
const restoringMarkerSuffix = ".restoring.marker"

// check refuses options that cannot mean what they say.
//
// A negative limit is a typo, and reading it as "give me the default" means
// MaxFiles: -1 restores a hundred thousand files. The command line already
// refuses them; the library reading the same value differently is the kind of
// asymmetry that makes one of the two wrong later.
func (o RestoreOptions) check() error {
	for _, l := range []struct {
		name string
		v    int64
	}{
		{"MaxFiles", int64(o.MaxFiles)},
		{"MaxBytes", o.MaxBytes},
		{"MaxFileBytes", o.MaxFileBytes},
		{"MaxManifestBytes", o.MaxManifestBytes},
	} {
		if l.v < 0 {
			return fmt.Errorf("storage: RestoreOptions.%s is negative (%d); 0 means the default", l.name, l.v)
		}
	}
	return nil
}

// clearStaging removes a leftover staging directory, refusing one this restore
// did not make.
//
// The path is derived from the destination, so it can name a directory that
// has nothing to do with a restore -- `-dst /srv/data/logs` derives
// `/srv/data/logs.restoring`. Keying on "does it hold only group files" is not
// enough: a pre-MANIFEST store is a directory of group files and nothing else,
// so removing on that rule destroyed one, measured. The marker file is written
// by this function's caller and removed before the rename, so it is present in
// exactly one situation -- a restore that did not finish.
func clearStaging(staging, displaced, marker string) error {
	// Statted once, before either directory: the marker is what says both are
	// this call's kind of residue rather than something that merely shares the
	// name, and a `-dst /srv/logs` derives both names an operator can stumble
	// into.
	_, markerErr := os.Stat(marker)
	for _, dir := range [...]string{staging, displaced} {
		if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		if markerErr != nil {
			return fmt.Errorf("%w: %s exists and was not left by a restore; move it aside",
				ErrDestinationNotEmpty, dir)
		}
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("storage: clearing the staging directory: %w", err)
		}
	}
	// A marker on its own is residue of a restore that got no further, and
	// removing it costs nothing. Its absence is not an error: the common case
	// is that there was nothing to clear at all.
	if err := os.Remove(marker); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// readRestore walks the archive under every limit, refusing a wrong tenant
// before the first group is written, and hands each group to write.
//
// A nil write validates and writes nothing, which is what a dry run is. It is
// a nil FUNCTION rather than an empty path because the empty path was a
// sentinel whose failure mode was silent: filepath.Join("", name) is name, so
// a dry run that lost its guard wrote group files into the current working
// directory -- and no test looks at the working directory.
//
// It reports whether the archive was unverifiable (pre-format-1, no manifest)
// rather than only returning that as an error, so the caller can decide
// whether to keep what landed.
func readRestore(r io.Reader, write func(name string, data []byte) error, opt RestoreOptions) (*BackupManifest, bool, error) {
	var files int
	var total int64
	sawManifest := false
	man, _, err := readBackup(r, opt.readLimits(),
		func(m *BackupManifest) error {
			sawManifest = true
			return checkRestoreTenant(m, opt)
		},
		func(name string, data []byte) error {
			// readBackup enforces manifest-first RETROACTIVELY: a group that
			// arrives before the manifest is read, parsed and emitted, and
			// the ordering error is raised when the manifest finally turns
			// up. For a tenant-checked restore that is a whole archive
			// written to the destination's own volume before anything looks
			// at the tenant -- measured, all four groups landed in staging
			// from an archive whose manifest was moved to the end. A caller
			// that named a tenant gets nothing until the manifest has.
			if opt.RequireTenant != "" && !sawManifest {
				return fmt.Errorf("%w: %s arrived before the manifest, so its tenant is unknown",
					ErrWrongTenant, name)
			}
			files++
			if files > opt.maxFiles() {
				return fmt.Errorf("storage: the archive holds more than %d entries", opt.maxFiles())
			}
			if int64(len(data)) > opt.maxFileBytes() {
				return fmt.Errorf("storage: %s is %d bytes, over the %d-byte per-entry limit",
					name, len(data), opt.maxFileBytes())
			}
			total += int64(len(data))
			if total > opt.maxBytes() {
				return fmt.Errorf("storage: the archive's %d bytes exceed the %d-byte total limit",
					total, opt.maxBytes())
			}
			if write == nil {
				return nil // a dry run: every check above, no write
			}
			// The name is flattened by readBackup and refused unless it is a
			// group name, so a crafted entry cannot escape the staging
			// directory or place a MANIFEST that would make the restored
			// groups invisible.
			return write(name, data)
		})
	if errors.Is(err, ErrBackupUnverified) {
		// No manifest at all. The entries were still parsed and bounded; what
		// could not happen is checking them against sizes and checksums.
		if opt.RequireTenant != "" {
			return man, true, fmt.Errorf("%w: the archive carries no manifest, so it names no tenant", ErrWrongTenant)
		}
		if opt.AllowUnverified {
			return man, true, nil
		}
		return man, true, err
	}
	if err != nil {
		return man, false, err
	}
	return man, false, nil
}

// dryRunRestore validates an archive and writes nothing, applying exactly the
// limits a real restore applies.
//
// The first version called VerifyBackup, which is readBackup with no callback
// -- and every limit lived in that callback, so a dry run accepted an archive
// of any size while the command line offered three flags to bound it. The one
// mode an operator is told to point at an untrusted archive was the one mode
// that checked nothing.
func dryRunRestore(r io.Reader, opt RestoreOptions) (*BackupManifest, error) {
	man, unverified, err := readRestore(r, nil, opt)
	if err != nil {
		return man, err
	}
	if unverified {
		return man, ErrBackupUnverified
	}
	return man, nil
}

// checkRestoreTenant refuses an archive from the wrong tenant.
func checkRestoreTenant(man *BackupManifest, opt RestoreOptions) error {
	if opt.RequireTenant == "" || man == nil {
		return nil
	}
	if man.Tenant != opt.RequireTenant {
		return fmt.Errorf("%w: archive names %q, want %q",
			ErrWrongTenant, man.Tenant, opt.RequireTenant)
	}
	return nil
}

// checkRestorePath refuses a destination the rename cannot handle.
//
// "." because os.RemoveAll rejects a path ending in it (Go's endsWithDot) --
// which the staging and displaced cleanups still call, even though the
// destination is now renamed aside rather than removed --
// so the restore would fail after writing the entire archive to staging. ".."
// is refused for the same reason a level up: it names a directory a restore
// has no business replacing, and the kernel's refusal is a different errno
// arriving just as late. A symbolic
// link because os.ReadDir follows it while os.RemoveAll unlinks the link
// itself: the checks pass against the target, and the store lands in the
// LINK's parent while the target keeps whatever it had. Both were measured,
// both reported success or a late failure, and neither is worth supporting
// when refusing costs one Lstat.
func checkRestorePath(dst string) error {
	switch filepath.Base(dst) {
	case ".", "..", string(filepath.Separator):
		return fmt.Errorf("%w: %q is not a directory a restore can replace; name it explicitly",
			ErrRestoreDestination, dst)
	}
	fi, err := os.Lstat(dst)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: %s is a symbolic link; restore into the path it points at",
			ErrRestoreDestination, dst)
	}
	return nil
}

// checkRestoreDestination refuses anything a restore must not overwrite.
//
// The LOCK file is never counted. lockDir is what decides whether a process
// has the store open, and it runs next.
//
// On unix the file outlives the process that made it -- unlock releases the
// flock and closes the descriptor without unlinking -- so a cleanly stopped
// store leaves one, and so does a SIGKILLed restore. Counting it made a killed
// restore unrecoverable by the tool that produced it, which for a
// disaster-recovery command is the wrong end of the trade. On WINDOWS the lock
// is the open handle itself and unlock DOES remove the file, so a stale LOCK
// there means a crash and `lockDir` refuses it with O_EXCL whatever this
// listing does -- an operator must remove it by hand, as
// `docs/lld/storage.md` already says of the Windows lock generally.
func checkRestoreDestination(dst string) error { return checkRestoreDir(dst) }

// checkRestoreDestinationLocked is the same check, repeated once the lock is
// held. Separate names because the two answer different questions: the first
// is a cheap refusal before any side effect, the second is the one the removal
// at the end relies on.
func checkRestoreDestinationLocked(dst string) error { return checkRestoreDir(dst) }

func checkRestoreDir(dst string) error {
	ents, err := os.ReadDir(dst)
	if errors.Is(err, os.ErrNotExist) {
		return nil // it will be created by the rename
	}
	if err != nil {
		return err
	}
	var names []string
	for _, e := range ents {
		if e.Name() == LockFileName {
			continue
		}
		names = append(names, e.Name())
	}
	if len(names) == 0 {
		return nil
	}
	// Named, up to a few: an operator staring at this during a disaster wants
	// to know what is in the way, and a count alone does not say whether it is
	// their store or a stray file.
	shown := names
	more := ""
	if len(shown) > 4 {
		shown, more = shown[:4], fmt.Sprintf(" and %d more", len(names)-4)
	}
	return fmt.Errorf("%w: %s holds %s%s", ErrDestinationNotEmpty, dst, strings.Join(shown, ", "), more)
}
