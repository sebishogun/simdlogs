package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// The restore subcommand.
//
// A backup nobody has restored is a backup nobody has tested, and until now
// the only way to unpack one was to import the package. An operator holding a
// tar and a broken disk needs a command, and needs a way to check the tar
// BEFORE committing to it -- which is what -dry-run is: it validates every
// group's size, checksum and parse against the archive's own manifest, under
// every limit a real restore applies, and writes nothing. It needs no
// destination at all.
//
// It is a subcommand rather than a flag on the server because it is not the
// server: it takes a different set of arguments, it exits when it is done, and
// putting it behind `-restore` on a binary whose other flags all describe a
// long-running process invites running it against a live storage directory.
// Restore refuses that anyway -- it holds the store's own lock from before the
// archive is read until the old store is removed -- but the shape of the
// command should not suggest it.

// runRestore handles `simdlogs restore ...` and returns a process exit code.
//
// The work is here rather than in a function that calls os.Exit so that it can
// be tested: `cmd/simdlogs` had no tests at all, which is how a usage message
// claiming "the destination is the whole store or is untouched" shipped over
// an implementation where it was not.
func runRestore(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("simdlogs restore", flag.ContinueOnError)
	fs.SetOutput(stderr)
	src := fs.String("src", "", "backup archive to restore (a tar from /admin/backup); - reads stdin")
	dst := fs.String("dst", "", "destination directory; must be absent or empty, and must not be a store any process has open. Not used by -dry-run")
	tenant := fs.String("tenant", "",
		"refuse the archive unless its manifest names this tenant. This is the tenant KEY the "+
			"manifest carries (\"0:0\"), not the directory name (\"tenant-0-0\"); -dry-run prints it. "+
			"An archive restored into another tenant's directory produces a store that answers that "+
			"tenant's queries with someone else's logs, and the manifest is the only place that fact "+
			"is recorded")
	dryRun := fs.Bool("dry-run", false,
		"validate the archive and write nothing: every group's declared size, its CRC32C and a "+
			"full parse, against the manifest the archive carries, under every limit below. "+
			"Needs no -dst")
	allowUnverified := fs.Bool("allow-unverified", false,
		"restore a pre-format-1 archive, which carries no manifest and so cannot be checked "+
			"against one. The exit status is still non-zero")
	maxFiles := fs.Int("max-files", 0, "refuse an archive naming more groups than this (0 = the built-in default)")
	maxBytes := fs.Int64("max-bytes", 0, "refuse an archive larger than this in total (0 = the built-in default)")
	maxFileBytes := fs.Int64("max-entry-bytes", 0, "refuse any single entry larger than this (0 = the built-in default)")
	maxManifestBytes := fs.Int64("max-manifest-bytes", 0,
		"refuse a manifest larger than this. It is decoded before any other limit can apply, "+
			"because it is what sizes everything after it (0 = the built-in 64 MiB)")
	fs.Usage = func() {
		fmt.Fprintf(stderr, "usage: simdlogs restore -src FILE -dst DIR [-tenant KEY]\n")
		fmt.Fprintf(stderr, "       simdlogs restore -src FILE -dry-run\n\n")
		fmt.Fprintf(stderr, "Unpacks a backup atomically: every group is validated against the\n")
		fmt.Fprintf(stderr, "archive's manifest in a staging directory beside the destination, and\n")
		fmt.Fprintf(stderr, "the whole thing is moved into place with one rename. The store's own\n")
		fmt.Fprintf(stderr, "lock is held until the old store is removed, and the lock the rename\n")
		fmt.Fprintf(stderr, "installs is one this command already holds -- so a server that starts\n")
		fmt.Fprintf(stderr, "on that directory either finds it locked, or wins a race that makes\n")
		fmt.Fprintf(stderr, "this command abort without touching that server's store.\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		// -h and -help are a request, not a mistake. flag prints the usage
		// either way; only the exit status differs, and a command that exits
		// 2 on -h fails a script that checks it.
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *src == "" || (*dst == "" && !*dryRun) {
		fs.Usage()
		return 2
	}
	// A negative limit is a typo, not a request for the default, and silently
	// treating it as one means `-max-files -1` restores 100,000 files.
	for _, l := range []struct {
		name string
		v    int64
	}{
		{"-max-files", int64(*maxFiles)},
		{"-max-bytes", *maxBytes},
		{"-max-entry-bytes", *maxFileBytes},
		{"-max-manifest-bytes", *maxManifestBytes},
	} {
		if l.v < 0 {
			fmt.Fprintf(stderr, "simdlogs restore: %s must not be negative (got %d)\n", l.name, l.v)
			return 2
		}
	}

	in := stdin
	if *src != "-" {
		f, err := os.Open(*src)
		if err != nil {
			fmt.Fprintf(stderr, "simdlogs restore: %v\n", err)
			return 1
		}
		defer f.Close()
		in = f
	}

	man, err := storage.Restore(in, *dst, storage.RestoreOptions{
		MaxFiles:         *maxFiles,
		MaxBytes:         *maxBytes,
		MaxFileBytes:     *maxFileBytes,
		MaxManifestBytes: *maxManifestBytes,
		RequireTenant:    *tenant,
		AllowUnverified:  *allowUnverified,
		DryRun:           *dryRun,
	})

	// A failed directory sync AFTER the rename is not a failed restore: the
	// store is in place, and telling an operator otherwise sends them to retry
	// into a destination that is now occupied.
	if errors.Is(err, storage.ErrRestoredButUnsynced) {
		fmt.Fprintf(stderr, "simdlogs restore: %v\n", err)
		fmt.Fprintf(stderr, "the store IS in place at %s; do not retry. It may not survive a power\n", *dst)
		fmt.Fprintf(stderr, "loss until the directory is synced, so sync the filesystem now.\n")
		printManifest(stdout, "restored", man)
		return 1
	}
	if errors.Is(err, storage.ErrBackupUnverified) && *allowUnverified && !*dryRun {
		fmt.Fprintf(stderr, "simdlogs restore: %v\n", err)
		fmt.Fprintf(stderr, "the archive carries no manifest, so nothing checked its groups against\n")
		fmt.Fprintf(stderr, "declared sizes or checksums. They were parsed, and they are in place.\n")
		return 1
	}
	if err != nil {
		// The manifest is printed even on failure when there is one: an
		// operator diagnosing a bad archive wants to know what it claimed to
		// hold, and that is the first thing the archive says.
		if man != nil {
			fmt.Fprintf(stderr, "archive: tenant %q, %d groups, %d rows, manifest sequence %d\n",
				man.Tenant, len(man.Groups), man.TotalRows(), man.ManifestSeq)
		}
		fmt.Fprintf(stderr, "simdlogs restore: %v\n", err)
		return 1
	}

	what := "restored"
	if *dryRun {
		what = "validated"
	}
	printManifest(stdout, what, man)
	return 0
}

func printManifest(w io.Writer, what string, man *storage.BackupManifest) {
	if man == nil {
		fmt.Fprintf(w, "%s: an archive with no manifest\n", what)
		return
	}
	fmt.Fprintf(w, "%s: tenant %q, %d groups, %d rows, manifest sequence %d\n",
		what, man.Tenant, len(man.Groups), man.TotalRows(), man.ManifestSeq)
}
