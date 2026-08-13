# Roadmap: simdlogs production hardening

Status: **planned**. Nothing in this document ships today. Current behavior is
in the LLDs (`docs/lld/`) and the README; implemented compatibility is in
`docs/vl-parity.md` (status: complete, tiers 0–5, measured 40/40 against the
real VictoriaLogs binary).

The design document for this direction is
[`docs/plans/2026-08-13-simdlogs-production-design.md`](plans/2026-08-13-simdlogs-production-design.md);
the task-by-task plan is
[`docs/plans/2026-08-13-simdlogs-production.md`](plans/2026-08-13-simdlogs-production.md).

Each stage has measurable exits and no promises. A stage is "done" only when
its exits pass on a quiet machine with the gates run bare or under
`set -o pipefail`. Work that argues against a change lands in `docs/wrong.md`
whether or not code changed.

## Stage 0 — Compatibility hardening

Where we are: the LogsQL surface is 40/40 against the real VL binary on the
committed corpus (`internal/bench/compat_test.go`); the API-surface suite
proves each query argument changes the answer on both engines
(`apisurface_test.go`, `perops_test.go`); the four findings from that work
(status-code probe gaps, double timestamp storage, per-value dict inflation,
wall-clock-vs-instructions) are recorded in `docs/wrong.md`.

Exits:

- The compatibility corpus extends with malformed-query, empty-tenant, and
  streaming-response cases; every corpus case runs in CI as a gate.
- `go test -race ./...` clean; `go vet ./...` clean.
- A status-code probe plus a response-shape probe plus an answer-changes
  probe pass on every select and ingest endpoint, against the staged VL
  binary when present (skipped cleanly when absent — the suite records what
  it did not run).
- The ES surface documents its DSL subset as a fixed contract with a
  conformance test per clause.

## Stage 1 — Durability

Exits:

- Crash-recovery tests: kill the process (SIGKILL) at every phase of the
  write path (buffer, temp write, fsync, rename, post-rename) and assert the
  store reopens with every fsynced group present and no partial group
  indexed. The existing write-temp-fsync-rename path is the basis; the test
  is the deliverable.
- Backup/restore round-trip tested at scale (a store with hundreds of groups,
  including one mid-flush): restore into a fresh directory, open, query —
  byte-identical answers.
- Corrupt-group handling: a truncated or bit-flipped group file fails
  `OpenStore` without taking other groups down, and the failure is reported
  (log + metric), not silent.
- fsync policy documented: what is durable when (per-request flush boundary),
  and what is not (the OS page cache for mmap'd reads).

## Stage 2 — Ops

Exits:

- `/metrics` names documented as a stable contract in the LLD; every metric
  emitted has a test that asserts presence and sane monotonicity; no metric
  is fabricated as a zero.
- Retention and tiering run under fault injection (partial failure of a
  group's unlink, a group dropped mid-backup) with no crash and no lost
  index consistency.
- Upgrade policy written: what a storage-format version bump means for old
  data (the codec-flag design makes old groups readable; the policy
  documents the promise).
- `/flags` output, `/health`, `/-/ready` covered by contract tests; graceful
  shutdown proven under in-flight ingest and live tail.

## Stage 3 — Cluster protocol

The current router surface is experimental and not production-safe: the
`streams`, `stream_ids`, plain `stats_query`, and `hits` merges decode stale
envelopes and answer empty/bogus results, and `facets`, `tail`, `/select/sql`,
`/select/vector`, `/admin/backup`, `/metrics`, and `/alerts` are not
federated (router-local). The exact envelope mismatches are recorded in
[`lld/cluster.md`](lld/cluster.md); this stage fixes them tests-first.

Exits:

- Shard-shape fixture tests, written before the fixes: for each broken merge,
  a fixture shard answers the CURRENT backend envelope — the shared
  `{"values":[...]}` envelope for `streams`/`stream_ids`, the Prometheus
  stats vector for plain `stats_query`, the dense `timestamps`/`values` hits
  series for `hits` — and the test asserts the router's merged answer matches
  a single-node store with identical data. These tests fail on today's code;
  the fixes land only when the fixtures pass.
- The select-router behavior (LLD: cluster) becomes a documented wire
  contract with a multi-node integration test: N storage nodes + 1 router,
  writes replicated per shard, reads merged exactly (counts and value counts
  cross-checked against a single-node store with identical data).
- The non-federated endpoints (`facets`, `tail`, SQL, vector, backup,
  metrics) are either federated with the same fixture discipline or
  documented to error loudly in router mode — never a silent local answer.
- Failure injection: kill one replica mid-query and mid-write; assert reads
  still answer (next replica), writes still land (other replica), and the
  router never returns a partial write as success.
- avg/quantile stats merge implemented (sum+count / sketch) or removed from
  the router's claims; either way, documented.
- A multi-node scale point (3 nodes, e.g. 300M rows) measured against the
  single-node curve, published in `docs/scale-curve.md`.

## Stage 4 — Scale

The committed scale tables are historical baselines, not the current
footprint: the unique-hex curve predates the FOR postings and hex codecs,
and the realistic 2.62x predates hex (see `docs/scale-curve.md` for the
code-state stamps; `docs/wrong.md`'s hex nibble-pack entry is an estimate,
not a measurement). This stage re-measures before anything current-facing
claims a footprint number.

Exits:

- The 1B-row head-to-head point reproducible with the comparison harness
  (`SIMDLOGS_SCALEVL=1 SIMDLOGS_SCALEVL_N=1000000000 ...`, staged VL
  binary) plus a fresh realistic-corpus measurement
  (`SIMDLOGS_REAL=1 ...`), both published in the scale curve with the
  machine and SIMD tier named; `TestScale` (`SIMDLOGS_SCALE=1`) is a
  separate local scaling gate, not a substitute.
- Memory bounded: a documented peak-memory measurement at 1B rows (mmap keeps
  only the working set resident; the measurement is the exit).
- The selective-window 1M loss (0.8x in the curve) investigated: either fixed
  with a measured win published, or explained in `docs/wrong.md` with the
  measurement that says why it stays.
- Any current-facing footprint statement (README, storage LLD, scale curve
  headline) is rewritten from the fresh measurements — never from the
  historical tables.

## Known implementation-doc defects (source comments; code task, not docs)

Stale source comments contradict the shipped code and are recorded here for
a later code task that fixes the comments (no source edit comes from a docs
session):

- `internal/api/es.go`: the package comment lists "terms/range/exists" as
  part of the mapped DSL and the exists handler comment says exists clauses
  "become predicates" — neither is true (see `docs/wrong.md` entry 37).
- `internal/bench/scale_test.go`: the header comment says "there is no mmap
  yet" — mmap shipped long ago; the test builds readers in RAM, but the
  comment is stale.
- `cmd/simdlogs/main.go`: the `-recompact-after` help claims ~17% smaller
  for flate-only recompaction, disagreeing with the 8% in the
  `-recompact-drop-postings` help and the measured -8.1%
  (`docs/wrong.md` tiering entry).
- `docs/wrong.md` entry 37 and the gofmt blocker on
  `internal/storage/group.go` are likewise implementation-side work queued
  from documentation sessions.

The production plan maps them: `es.go`'s comment drift is owned by Task B.2
(the exists work rewrites those comments and lands entry 37), and the
scale-test comment, the recompact help, and the gofmt blocker are Task G.1 —
see `docs/plans/2026-08-13-simdlogs-production.md`.

## Stage 5 — Release

Exits:

- A tag with a changelog whose entries all reference merged commits.
- Storage-format version documented; the v7 groups plus codec-flag design
  stated as the compatibility surface.
- README status section flips from "no tagged release" to the tagged version
  with the pin-commit guidance retired.
- Binary artifacts build reproducibly from the tag (the committed
  `go.mod`/`go.sum`; no cgo).
- Stage 4's fresh realistic and scale-vs-VL measurements are in the curve
  and the README's footprint claims are rewritten from them — a release
  never ships with the historical tables standing in for the current
  footprint.
