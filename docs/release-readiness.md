# Release readiness

Where the production plan's exit criteria stand. Written as an assessment, not
as an announcement: the point of the list is the rows that are not green.

## Gates, run at `cf9bd3b`

| Gate | Result |
|---|---|
| `gofmt -l .` | clean |
| `go vet ./...` | clean |
| `go test ./...` | 9 packages ok |
| `go test -race ./...` | 9 packages ok, no data races |
| `go test -tags purego ./...` | 9 packages ok |
| Fuzz seed corpus (22 targets) | 9 packages ok |
| Crash / recovery / restart / drills, ×5 | 9 packages ok |
| Soak, 60 s with retention running | ok — groups peak 5,899 and fall to 5,677 |
| `scripts/release-check.sh` (the artifact, not the source) | passed |
| Cross-build arm64, ppc64le, s390x, riscv64 | ok |
| `git diff --check` | clean |

## Blockers

**The published benchmark table has no machine-checked provenance.** The
figures in README were taken under the stated discipline — load average under
1, two runs, agreement required, amd64/AVX-512 — but before `requireQuiet`
existed to enforce it, and they carry no record of which machine or commit
produced them. Plan step 10.3.3 requires confirming that current claims come
only from the corrected harness, and that cannot be confirmed without
re-measuring.

The gate is in place and refuses above load 1; what remains is to run the
harnesses on a quiet machine and replace the table with numbers that carry
their own provenance.

## Not blockers, but stated

These are limitations, not defects, and they are in `CHANGELOG.md` under known
limitations as well:

- No incremental backup. RPO is bounded by capture frequency.
- Repair is an operator action, not automatic, and only within a shard.
- Non-mergeable aggregates are refused across shards rather than answered.
- `/select/logsql/tail` and `/select/vector` answer 501 on a router.
- No single command restores a cluster archive.
- linux/386 does not build, because a dependency does not compile for a 32-bit
  int (upstream fix tracked separately).

## What has not been run here

- The GitHub workflows (`ci`, `cross`, `fuzz`, `release`). They are authored
  and their YAML parses; none has been observed running.
- The one-hour and 24-hour soak modes. The 60-second and 45-second runs pass.
- A fuzz campaign longer than ~10 s per target.
- Any measurement on a quiet machine.

## The state of the release

**Not tagged, and should not be** until the benchmark table is re-measured. The
README still describes the software rather than a release, and the module is
still pinned by commit rather than by version.

Everything else the plan asks for is in the code, the tests and the history.
The LICENSE (MIT, matching the other repositories in this family) is present,
which it was not — and a tag without one cannot be fixed afterwards, because
the Go module proxy caches versions immutably.
