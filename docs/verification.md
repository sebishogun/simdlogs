# Verification

How anything gets measured or trusted in this repository. The discipline is
the same one the sibling SIMD repos hold to; the numbers and commands here
are current for the rebased main.

## Gates (run always, bare or with `set -o pipefail`)

```sh
go test ./...
go test -race ./...
go vet ./...
```

The suite covers storage round trips and backward-compatible posting formats,
crash-safe append, retention, backup/restore, tenant isolation, ingest
protocols, LogsQL parsing and execution, SQL/vector surfaces, Elasticsearch
search, live tail, the replication/federation code paths, parser and regex
panic-safety, serial-versus-parallel query agreement, and router-merge answer
correctness. The shared-values, hits, stats-range, and ES merges are
fixture-tested in `internal/api/cluster_envelope_test.go`, including
cross-shard summation and post-merge limits.

`gofmt -l .` is clean and is a release gate. It was blocked for a long time on
pre-existing formatting in `internal/storage/group.go`, and the blocker
outlived the condition: five documents went on describing the gate as red after
the file was reformatted, one of them contradicting itself two hundred lines
apart.

This file is where a gate's status is maintained. Four other documents mention
this gate and say it is clean, which is true and is one sentence each; what
they must not carry again is a BLOCKER, because a blocker is a state that
changes and four copies of it will not change together.

Stale source comments that contradict shipped code are known
implementation-doc defects, listed in `docs/roadmap.md` ("Known
implementation-doc defects"): `es.go`'s terms/range/exists comments,
`scale_test.go`'s "no mmap yet", and the `-recompact-after` help's 17% vs
the measured -8.1%. They are a code task, not a docs task.

**AGENTS/CLAUDE body-sync check:** `CLAUDE.md` reproduces the body of
`AGENTS.md` in full so Claude Code runs are self-contained, and its header
declares AGENTS.md the source of truth (the headers agree on that). Any
change to `AGENTS.md`'s body must be mirrored into `CLAUDE.md`'s embedded
copy in the same commit, and `diff <(sed -n '/^## The goals, in one breath/,$p' AGENTS.md) <(sed -n '/^## The goals, in one breath/,$p' CLAUDE.md)` must be empty.

**Never pipe a gate through `tail`** (or anything else) without `pipefail`:
the pipe reports the last command's status and the failure vanishes. This has
laundered a red fuzz run, a red README gate, and two red bench-check runs
into green exits in this family of repositories. Run gates bare, or
`set -o pipefail` first.

## Report tests (env-gated, not unit gates)

These are reports: they print numbers and land in commit messages and the
README, and they are never CI gates. The committed tables are historical
baselines (code-state stamps in `docs/scale-curve.md`); a re-run is what
produces a current measurement, and the roadmap requires fresh realistic +
scale-vs-VL runs before any current footprint claim.

```sh
# realistic 1M-row query mix (default N; SIMDLOGS_REAL_N for the curve points)
SIMDLOGS_REAL=1 go test -run TestRealistic -v -timeout 90m ./internal/bench/

# single-node scale curve (sizes via SIMDLOGS_SCALE_SIZES, default 3M/30M/100M)
SIMDLOGS_SCALE=1 go test -run TestScale -v -timeout 30m ./internal/bench/

# disk-backed scale point against VictoriaLogs; stage internal/bench/victoria-logs first
SIMDLOGS_SCALEVL=1 SIMDLOGS_SCALEVL_N=100000000 \
  go test -run TestScaleVsVL -v -timeout 60m ./internal/bench/

# compact-mode tradeoff
SIMDLOGS_COMPACT=1 SIMDLOGS_REAL=1 go test -run TestRealistic -v -timeout 90m ./internal/bench/
```

### Staging the VictoriaLogs binary

The head-to-head runs VictoriaLogs as a subprocess from a prebuilt binary at
`internal/bench/victoria-logs` (gitignored), produced by building the
reference clone:

```sh
cd ../victorialogs-reference
go build -o /path/to/simdlogs/internal/bench/victoria-logs ./app/victoria-logs
```

Absent the binary, the VL half skips cleanly and the simdlogs half still
records a number — the harness says what it did not run. The binary is a
reference artifact only: the repo never imports VictoriaLogs code.

## The benchmark contract

`docs/benchmark-contract.md` is published before the implementation is
measured, so the numbers cannot be chosen to fit the code. It fixes the
corpus, the query classes, the metrics, and the method. The head-to-head
harness (`internal/bench/harness_test.go`) runs both engines as servers on
the same wire API, same machine, same corpus, interleaved in one process.

Method, the discipline:

- Deterministic corpus (fixed seed), reproducible by hashing two runs; both
  engines ingest byte-identical input.
- Both engines interleaved in one session — never an A/B across sessions.
- Each query class is the **minimum** of many samples after warmup (never a
  mean; the minimum is the least-perturbed run).
- Run the machine quiet. The release policy is **load average under 1**
  (the AGENTS rules and the benchmark contract say the same).
  `scripts/quiet-bench.sh` defaults to a looser **threshold of 1.5** as a
  convenience helper for routine runs — it waits bounded (180 minutes by
  default) and exits 3 rather than hanging; the strict <1 policy applies to
  anything published as a measurement.
- The instruction-set tier is named in every snapshot (the scale numbers were
  measured on amd64/AVX-512; **no wall-clock claim is made for another
  architecture**).
- Losses are published alongside wins (`docs/wrong.md`, the README, the scale
  curve). The 1M selective-window loss (0.8x) sits in the committed table.

## The noise floor

The code-layout noise floor here is **8.3%**. Anything smaller cannot be told
from nothing by wall-clock, and more samples do not help — layout noise is
per-build, not per-run. When a change is expected to be worth less than that:

- compare **instructions retired** and **cycles** with
  `perf stat -e instructions:u,cycles:u`, which are layout-independent;
- and read the disassembly, which is the only thing that explains *why*.

A/B builds must be interleaved in one session and compared on the minimum.
The wall clock has lied here before: it called an 11% instruction win a 24%
regression on a loaded box (`docs/wrong.md`); instructions retired settled
it.

## Disassemble first, always

Before proposing a cause for anything slow, before writing a variant, before
reading a profile delta — build it and read the instructions:

```
go test -c -o /tmp/x.test .
go tool objdump -s 'pkg\.functionName' /tmp/x.test | less
```

Go compiles in seconds; every guess that skips the disassembly costs a
build-measure-revert cycle and risks a wrong conclusion landing in
`docs/wrong.md` as fact. What the disassembly says that nothing else does:
register pressure (a spilled loop counter), eliminated bounds checks, index
multiply vs shift, inlined calls vs `memmove`, and which branch the compiler
laid out as fallthrough. Use gdb when a live register or breakpoint is
needed. Profile when a profile is the right tool (allocations, GC pressure,
cache misses) — but the disassembly comes first.

## Cross-architecture

Every simd kernel has a portable fallback through the `simd` package, so
missing architecture kernels affect speed, not availability. The query and
storage tests run the scalar paths as the conformance oracle and the SIMD
paths as the shipped path, differential-tested. Wall-clock claims are
amd64/AVX-512 only.

`.github/workflows/cross.yml` is where the arch lanes run, and the distinction
between its two jobs is the point:

| Lane | How | What it can show |
|---|---|---|
| linux/arm64 | native runner | a real run: test, race, purego |
| linux/s390x, ppc64le, riscv64 | qemu | correctness, slowly; **not** timing |

s390x is the one that earns its runtime. It is big-endian, and this store
writes little-endian lengths, maps files, and packs bits — every place a length
or a checksum is read back with the wrong byte order fails there and nowhere
else. `ci.yml` cross-**builds** the same targets on every push; a build proves
portability in the type system's sense and says nothing about endianness, so
the two are not substitutes.

**linux/386 is not supported, and its absence is the claim.** This module
depends on `github.com/sebishogun/simdjson`, whose `marshal.go` writes
`math.MaxUint32` as an untyped constant; on a 32-bit target that overflows an
int and the dependency does not compile, so `GOARCH=386 go build ./...` fails
before any code here is reached. `ci.yml` listed 386 in its cross matrix
anyway, which asserted a platform through a job that could not pass. Restoring
the lane needs the constant typed upstream and a release taken.

## Fuzzing

`.github/workflows/fuzz.yml`, nightly, plus `workflow_dispatch` with a
per-target duration.

The targets are **discovered** from the tree rather than listed. A
hand-maintained list is how a target gets written and never run — it sits in
the tree looking like coverage while nothing executes it — so the job greps for
`func Fuzz`, fails if the pattern matches nothing, and prints the count it
found.

22 targets across four packages:

| Package | Covers |
|---|---|
| `internal/ingest` | jsonline, logfmt, syslog, journald, Loki (JSON and protobuf), Datadog, OTLP (JSON and protobuf), vector field specs and values |
| `internal/query` | LogsQL, SQL, time-window resolution |
| `internal/storage` | group parser, backup manifest, restore tar, adopted groups, digest lookup |
| `internal/api` | peer envelope, Elasticsearch `_search`, cursors |

What they assert, beyond "no panic":

- **Determinism.** The same bytes twice must give the same counts and the same
  error. A retry of a rejected batch cannot be reasoned about otherwise.
- **The reported count is what landed.** Not "an error means nothing was
  stored" — journald deliberately keeps the entries that parsed before a
  truncation — but that `Accepted` equals the rows in the store either way. A
  client that cannot tell what landed re-sends it and duplicates it.
- **A refusal leaves nothing behind.** A restore that unpacked half an archive
  before rejecting it has produced a directory the next open will read.
- **Absent is not "yes".** A peer that sends no completeness header has not
  said the answer is complete.

The seed corpus runs in the ordinary `go test ./...`, so a seed that fails is a
normal red build. The fuzz job runs it as a separate step first, so a failure
is attributed rather than buried.

`crash-repetitions` in the same workflow repeats the crash/recovery/restart
tests 25 times, and 5 times under `-race`. One pass proves the mechanism
exists; the failures worth finding need a particular interleaving.

## Soak

`scripts/soak.sh` — `dev` (1 hour, the default), `release` (24 hours), or any
duration up to a 24-hour ceiling. Opt-in through `SIMDLOGS_SOAK`, and the
opt-in is guarded by tests that run in the ordinary suite: a change that made
the soak default-on would otherwise be found by whoever's machine fell over.

Concurrent ingest, queries, bounded tenant churn, backups (each holding a
snapshot for its whole stream), the ops surfaces, and listener restarts — all
overlapping, because each in isolation is already covered and what a soak adds
is the overlap.

**The bounds are ratios, and that is the design.** A store holding more data
maps more groups, uses more memory and occupies more disk; an absolute bound on
those numbers can only pass if nothing happened. What isolates a leak is the
relationship:

| Bound | Isolates |
|---|---|
| goroutines | the one quantity that does not grow with data: a goroutine per request that is never joined |
| mappings per 100 groups | a mapping that outlived its group — no resident pages, so invisible in RSS until the address space runs out |
| KB resident per MB stored | memory growing *faster* than the data it serves |
| manifest bytes per group | a manifest that never compacts, invisible in total disk use because the groups dwarf it |

Every bound reports, including ones it could not measure. A skipped bound reads
as a passing bound, and two of these were being skipped on every sample while
the summary said the run was clean.

Three of the harness's own defects are worth knowing about, because each made
the soak report success while measuring nothing:

- The load generator counted any status below 500 as a successful write. Every
  request was being refused 400 — `AccountID` must be numeric — and the run
  reported **172,037 writes** against a flat resource profile.
- Tenant churn drew a fresh tenant id from a 2^20 space every iteration, which
  created 92,589 tenants in twenty seconds. That is not a leak test; it is a
  memory-exhaustion test with no assertion. Churn now walks a ring of 64, so
  tenants recur and eviction and reopen actually happen.
- Unpaced, the generators ran at benchmark speed: 624 MB of store in twenty
  seconds, which is terabytes over the release mode. A soak is about duration,
  not throughput.

## Malformed input

- Parser panic-safety across malformed queries (`internal/query/hardening_test.go`):
  fuzz-style malformed LogsQL never panics the server (recoverPanic is the
  last line of defense; the parser is the first).
- Regex panic-safety: an invalid user pattern compiles to "matches nothing",
  never a panic; the parser validates up front for a clean 400.
- Ingest leniency: malformed lines are counted and skipped, never fatal; the
  batch boundary still flushes.
- `stream_context` is memory-bounded (`streamContextCap = 2,000,000`).

## Security and recovery drills

`docs/security.md` states what is defended and what is not, with the test
beside each claim. `docs/runbooks/` holds the procedures, and every procedure
is executed by a test — a runbook nobody has run is a document, not a
procedure.

| Drill | Test |
|---|---|
| A tenant cannot read another's data, implicitly or by claiming its id | `TestDrillATenantCannotReadAnother` |
| A role cannot reach another role's routes, including the internal ones | `TestDrillARoleCannotReachAnotherRolesRoutes` |
| A client cannot forge the internal protocol header into access | `TestDrillAClientCannotForgeTheInternalProtocolHeader` |
| A rejected credential is answered identically however close the guess | `TestDrillARejectedCredentialRevealsNothing` |
| A restored backup answers identically to its origin | `TestDrillARestoredBackupAnswersIdentically` |
| A corrupt group is refused, never served as data, and readiness says so | `TestDrillACorruptGroupIsRefusedNotServed` |
| A lost replica is rebuilt from a healthy one, and the survivor is untouched | `TestDrillALostReplicaIsRebuiltByRepair` |

Oversized bodies, decompression bombs and oversized syslog frames are covered
by the tests named in `docs/security.md` rather than duplicated as drills: a
second copy of a test is a second thing to keep in step and no extra coverage.

RPO and RTO, and the limitations behind them, are in
`docs/runbooks/backup-restore.md`. The short version: there is no incremental
backup, so RPO is bounded by how often a full capture is affordable; and the
spread between a cluster backup's shard archives is reported, not bounded,
because no threshold is right for every deployment.

## Crash recovery

- Append is write-temp-fsync-rename: a crash between temp and rename leaves a
  `.tmp` file that `OpenStore` ignores; a truncated partial flush is skipped
  by the index.
- Graceful shutdown (SIGINT/SIGTERM): stop accepting, drain in-flight (15 s),
  flush writers, unmap stores — no buffered rows lost, no mmap leaks.
- Restore: `storage.Restore`, behind the `simdlogs restore` command, unpacks a
  `/admin/backup` tar into a staging sibling and moves it into place with one
  rename, holding a lock on the destination until the syscall that takes that
  directory away, and arranging for the lock file the rename installs to be one
  it already holds -- so a server that opens the destination in the one gap
  where it exists (between the rename that takes the old store away and the one
  that puts the new store in place) has to CREATE it, which makes that second
  rename fail with EEXIST and the restore abort without touching that server's
  store. (One of two orderings: Go's os.Rename Lstats the destination and
  returns EEXIST for an existing directory, but a directory created between
  that Lstat and the raw rename(2) is empty and the rename replaces it. Safe
  either way -- the server then flocks the staging lock this call still holds
  and gets ErrLocked.) A server that arrives after the rename gets ErrLocked
  until the restore returns. A second RESTORE cannot start while the lock is
  held; in the gap between the two renames one can, and the outcome is one of
  three, not one: this call aborts ENOENT, or aborts EEXIST, or -- both parked
  in their own gaps -- returns nil over the other's destination. Measured at
  zero occurrences in 20,000 barrier rounds and 11.6 million overlapping
  attempts; every run that observed it had something outside a restore
  widening the window, which bounds the rate rather than proving the bound.
  `docs/wrong.md`, the entry on two restores parked in each other's gap.

  **The staging claim is asserted mid-stream, not after the fact:** a test
  that truncates an archive and finds the destination absent passes just as
  well against a build with no staging at all, because the error-path defer
  removes what it wrote.
  Entry names are flattened so an archive cannot escape, and only entries named
  as group files are written -- an archive can place group files and nothing
  else. A `MANIFEST` entry would otherwise be honoured by the legacy-directory
  gate and make every restored group invisible. `-dry-run` validates the
  archive against its own manifest under every limit and touches no
  destination; `-tenant` refuses an archive taken from a different tenant, the
  moment the manifest is decoded rather than after the groups are on disk.
  `.`, `..` and a symlinked destination are refused. `ErrRestoredButUnsynced`
  distinguishes a store that landed from a restore that failed.
  `storage.RestoreTar` is the older unstaged path and leaves a partial
  destination on failure.

### The process-kill matrix

`internal/storage/crash_test.go` re-execs the test binary as a child that
writes batches into a store and **SIGKILLs itself** at a named persistence
phase, then reopens the directory and asserts what survived. A subprocess and a
signal, not an injected error: an injected error unwinds every defer — the temp
file is removed, the manifest truncated back to a record boundary — which tests
the *error handling*. Only a kill shows what is actually on disk.

Three matrices, over the four callers of the durable write that they cover —
`AppendGroup`, `manifest.compact`, `Store.Recompact` and `Store.CompactGroups`.
The third is compaction's own, thirteen phases, and it exists because
compaction's commit is a TRANSACTION rather than an append: its record adds the
output and removes the inputs together, and only a kill can show whether that
is one record. Measured against a build that splits it in two, the rest of the
suite stays green and every batch appears twice.

| Matrix | Phases | Contract |
|---|---|---|
| `TestCrashRecoveryMatrix` — `AppendGroup` | 11: `buffering`, `temp-create`, `partial-write`, `file-sync`, `file-close`, `rename`, `dir-open`, `dir-sync`, `manifest-append`, `manifest-sync`, `post-ack` | no partial group adopted; every acknowledged batch present exactly once; no uncommitted batch visible; no duplicate rows |
| `TestCrashDuringRewriteIsVisibilityNeutral` — `manifest.compact` and `Store.Recompact` | the 7 `writeFileAtomic` phases | **visibility-neutral at every phase**: all batches present, each exactly once, no partial group, no duplicate rows |

The rewrite matrix has no per-phase expectation table on purpose. Neither
operation adds or removes a row — compaction folds the record log to one record
naming the same groups, and recompaction re-encodes a group file under the same
path and ID with **no manifest record at all**, so the rename is the whole
commit. A crash that changes what is readable is a defect wherever it lands.

Two tests exist to stop the matrix going vacuously green:

- `TestRewritePhaseCoverageIsComplete` runs the four `AppendGroup`-only phases
  against both rewrite paths and fails if one becomes reachable — that would
  mean the rewrite gained a commit step the matrix does not cover.
  `manifest-append` and `manifest-sync` are the two that carry signal, since
  they have call sites in the manifest commit; `buffering` and `post-ack` have
  no `fault()` call site at all and are markers the child checks.
- `TestCrashRecompactFixtureIsActuallyRecompacted` fails if the fixture stops
  qualifying for recompaction. `Recompact` skips any group whose flate rewrite
  is not smaller, so a too-small fixture makes every recompact subtest pass
  without the code under test writing a byte.

**What is not in the matrix.** Eight paths write durably or commit; four are
covered. `Store.Promote` (`cold.go`) is structurally identical to
`AppendGroup` — write the group, then commit an add. `Store.Demote` and
retention's `dropGroups` commit a REMOVE and then unlink, which is the
ordering the manifest was introduced for. `manifest.bootstrap` rewrites a
legacy directory's manifest from `OpenStore`. None of the four has a crash
matrix; extending the harness to them is a `crashEnvOp` per path and nothing
more.

The append matrix runs at **two batch counts**. Three is the default, so that
"present exactly once" has something to say. One is there because three hid a
live defect: with three batches the crash always lands on the last, two commit
first, and the store's visible set is never empty at recovery — which is
exactly the state in which `OpenStore` used to adopt uncommitted group files.
`TestCrashRecoveryMatrixFirstBatch` reaches it;
`TestUncommittedGroupIsInvisibleWhenNothingElseIsCommitted`,
`TestRemovedGroupStaysRemovedWhenItWasTheLastOne` and
`TestLegacyDirectoryWithNoManifestIsAdopted` pin the same boundary without a
crash. `docs/wrong.md` records the defect.

`partial-write` is named for the boundary, not for the artifact: the fault
fires *before* `f.Write`, so the temp file is zero bytes. No phase in either
matrix produces a torn file or a manifest with a partial record, so the
torn-tail replay path in `openManifest` is not exercised here either.

**What a process kill cannot establish.** SIGKILL destroys the process, not the
page cache. A `write()` that returned but was never fsynced is still held by the
kernel, so the next open reads it back — which is why `manifest-append` expects
the last batch *visible*. The matrix therefore proves no-partial-adoption,
no-lost-acknowledgement and no-duplication, and does **not** prove the fsync
boundary. That needs the unsynced writes actually dropped: `dm-flakey` with
`drop_writes`, a filesystem image discarded at the block layer, or an
`LD_PRELOAD` turning `fsync` into a no-op. Recorded in `docs/wrong.md` rather
than left as a passing test that appears to cover it.

## Compatibility corpus

The VictoriaLogs comparison is a report against the real binary, not a
unit-test gate. The suites that drive it: `compat_test.go` (the README's
machine-checked LogsQL parity count), `shapes_test.go` (wire-shape comparisons),
`apisurface_test.go` + `perops_test.go` (every query argument changes the
answer, on both engines), `realistic_test.go` (15-field Zipfian footprint and
query mix). The four findings of the API-surface work are in
`docs/wrong.md` — a status-code probe reported 0 gaps while seven endpoints
answered 200 with a body no client can read and five ingest paths 404'd.
That is why the surface is verified by shape and by answer, not by status.

## What is a gate vs a report

- Gates: `go test ./...`, `go test -race ./...`, `go vet ./...`, and any
  contract test with a hard assertion (crash recovery, backup/restore,
  tenant isolation, format round trips, parser safety).
- Reports: every head-to-head number, the scale curve, footprint, and the
  README tables. Reports are published with their method and machine; they
  are not gates. A report that argues against a change is the deliverable of
  the work that produced it (`docs/wrong.md`).
