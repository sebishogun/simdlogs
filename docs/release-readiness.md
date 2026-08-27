# Release readiness

Where the production plan's exit criteria stand. Written as an assessment, not
as an announcement: the point of the list is the rows that are not green.

## Gates

| Gate | Result |
|---|---|
| `gofmt -l .` | clean |
| `go vet ./...` | clean |
| `go test ./...` | 10 packages ok |
| `go test -race ./...` | 10 packages ok, no data races |
| `go test -tags purego ./...` | 10 packages ok |
| Fuzz seed corpus (23 targets) | 10 packages ok |
| Crash / recovery / restart / drills, ×5 | 10 packages ok |
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

The gate is in place and now covers `perops_test.go` too -- the harness that
produces the README table was the one it did NOT cover, while this document
said otherwise. What remains is to run the harnesses on a quiet machine and
replace the table with numbers that carry their own provenance.

The gate SKIPS rather than fails above load 1, and `SIMDLOGS_BENCH_NOISY=1`
overrides it (stamping the result unquotable). A platform without
`/proc/loadavg` only logs that the gate could not run.

## Not blockers, but stated

These are limitations, not defects, and they are in `CHANGELOG.md` under known
limitations as well:

- No incremental backup. RPO is bounded by capture frequency.
- Repair is an operator action, not automatic, and only within a shard.
- Non-mergeable aggregates (`quantile`, `avg`, `uniq`, `count_uniq`,
  `histogram`, `rate`) cost every matching row on the wire across shards: the
  router fetches the rows and aggregates once instead of merging per-shard
  numbers. The answer is exact and equal to a single node's.
- `/select/logsql/tail` and `/select/vector` answer 501 on a router.
- No single command restores a cluster archive.
- linux/386 does not build, because a dependency does not compile for a 32-bit
  int (upstream fix tracked separately).

## What has not been run here

- The GitHub workflows (`ci`, `cross`, `fuzz`, `release`). They are authored
  and their YAML parses; none has been observed running. That is how
  `release.yml`'s dry-run path stayed broken through several reviews: the
  `tag` input was read as `${GITHUB_REF_NAME:-inputs.tag}`, and
  `GITHUB_REF_NAME` is always set -- on a dispatch it is the BRANCH -- so a
  "dry run" would have built `simdlogs_main_linux_amd64` and then published a
  GitHub release named `main`. Corrected (the input is used on dispatch, and
  the publish step is gated on a tag ref), and still never executed.
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

## The v1 exit

The v1 exit is the **full production exit**: phases 0-10 of
`docs/plans/2026-08-13-simdlogs-production.md` complete (they are committed
locally), including the static-cluster contract and release artifacts, plus
the release gate set green. There is no phases-0-7 single-node exit in play
for v1.

## Task ledger

The work recorded between this document and the v1 exit, one row per task.
Each row carries its current state. A status transition is an edit in this
ledger, using the family vocabulary: open, staged, in-progress, blocked,
evidence-complete, shipped, rejected (terminal without a reopen condition in
`docs/wrong.md`).

| ID | state | work | evidence | exit |
|---|---|---|---|---|
| `LOGS-V1-01` | open | Quiet benchmark provenance: re-measure the published tables under `requireQuiet`, with no skip and no `SIMDLOGS_BENCH_NOISY` override, with a machine/commit record | corrected tables with provenance | the standing blocker cleared |
| `LOGS-V1-02` | open | Push and observed CI: the workflows have never been observed running; separate user push permission is required at operation time, so the task stops at the evidence checkpoint until that permission is given | observed CI result | CI observed |
| `LOGS-V1-03` | open | Long fuzz and dev/release soak beyond the short runs that pass | completed runs with durations | soak/fuzz evidence complete |
| `LOGS-V1-04` | evidence-complete | Stale docs/comments corrective: the `es.go` package comment, the `scale_test.go` header comment, the duplicated entry-37 heading in `docs/wrong.md`, and the stale `Task B.2` / `Task G.1` references in `docs/roadmap.md`'s known-implementation-doc-defects section (task IDs the production plan's numeric scheme does not use) reconciled; a code/record task, never a source drive-by from a docs session | dated historical corrections; `TestResolvedDocumentationDefectsDoNotReturn` and `TestTheStatedFactsAboutTheCodeAreTrueOfTheCode`; `go test`, `go test -race`, `go test -tags purego`, `go vet`, gofmt and diff gates green on 2026-08-27 | stale items fixed and entry references unambiguous |
| `LOGS-V1-05` | open | Bounded-ingest decision: taken with measurement - implemented, or rejected with a reopen condition in `docs/wrong.md` | the decision record | decision recorded |
| `LOGS-V1-06` | open | End-to-end release rehearsal without tagging (`scripts/release-check.sh` is the artifact under test) | the rehearsal run | rehearsal green |
| `LOGS-V1-07` | open | v1 preparation: the full production exit, phases 0-10 plus the release gate set green; commit, tag and publish operations stop at the evidence checkpoint until separately authorized | release gates green and release identity/docs agree | release evidence complete; operational v1 release only when separately authorized |
| `LOGS-V1-08` | open | Workload-backed ecosystem decisions: the parity and ecosystem documents reconciled against concrete ingest, query, storage, recovery, and operations workloads from VictoriaLogs, Loki, Elasticsearch/OpenSearch, ClickHouse, and Vector; every material gap classified as a v1 blocker, post-v1 work, or rejected with evidence; no feature is added merely for parity | the decision record | decisions recorded |
| `LOGS-IO-01` | open | Deferred, non-v1-blocking: queries are mmap-backed; durable ingest keeps file, directory, and manifest sync barriers. Instrument the ingest stages first; prototype an io_uring path only if explicit I/O waits are at least 30% of durable-ingest time; retain it only for a repeatable at least 20% end-to-end throughput or p99 gain while the durability and conformance gates stay green; without that evidence it stays deferred and no `docs/wrong.md` entry is written | the instrumented measurement or the deferred decision record | decision recorded with measurement |

Historical plans are not part of this ledger and are not edited:
`docs/plans/2026-08-13-simdlogs-production.md` is the completed implementation
record, and `docs/plans/2026-08-13-simd-family-production-documentation.md` is
the superseded historical family-documentation record, preserved unchanged -
family coordination is superseded by the GO_SIMD index
(`github.com/sebishogun/simd`,
`docs/plans/2026-08-24-simd-family-production-readiness.md`).

## Workload matrix

The workloads the ecosystem decisions (`LOGS-V1-08`) are measured against,
quoted from `docs/vl-parity.md`, the ecosystem documents, and this document's
limitation list. The matrix names workloads, not numbers.

| Axis | Workloads | Source |
|---|---|---|
| Ingest | jsonline, logfmt, ES `/_bulk`, Loki push (JSON and snappy-protobuf), syslog, journald, Datadog, OTLP (JSON and protobuf), vector field specs and values | `docs/verification.md` (ingest corpus), `docs/vl-parity.md` (ingestion protocols) |
| Query | The machine-checked LogsQL parity corpus; selective scan-heavy queries; SQL over logs; semantic/vector search; the ES DSL subset | README parity table, `docs/vl-parity.md`, `docs/roadmap.md` (Stage 0) |
| Storage | Retention and tiering; cold store (library-only interface); mmap reads; the writer emits v8 groups with v7 read compatibility; no incremental backup (RPO bounded by capture frequency) | `docs/lld/storage.md`, this document's limitation list |
| Recovery | Crash/recovery/restart drills; backup/restore round trip; repair only within a shard; no single command restores a cluster archive | this document's limitation list, `docs/runbooks/` |
| Ops | `/metrics`, `/flags`, `/health`, `/-/ready`; alerting rules; cluster route classification (federated / router-local / refused); non-mergeable aggregates across shards | `docs/lld/api.md`, `docs/lld/cluster.md` |

**Oracles and peers.** VictoriaLogs is the parity oracle only where
compatibility is promised (`docs/vl-parity.md`); Loki,
Elasticsearch/OpenSearch, ClickHouse, and Vector are workload and performance
peers, never behavioral oracles. A gap is a workload, an allocation, a
dispatch, or a measured result - not a feature count.

## Quiet-host protocol

Publishable performance measurements required by `LOGS-V1-01`, the measured
branch of `LOGS-V1-05`, `LOGS-V1-08`, and `LOGS-IO-01` run under the family
quiet-host protocol. Fuzz, soak, CI observation and release rehearsal remain
timed gates but do not consume a benchmark quiet window:

- The primary agent coordinates the machine: no parallel agents, builds,
  benchmarks, or other repository work during a window.
- A fixed source state is measured - a clean revision, or an immutable record
  of the exact dirty inputs; never a checkout that omits the candidate.
- The one-minute load average stays below 1 before and throughout every
  publishable run (`requireQuiet`); `SIMDLOGS_BENCH_NOISY` stays unset and the
  evidence shows the benchmark executed rather than skipped.
- Every wait is bounded and every command carries an explicit timeout.
- Provenance is recorded beside each result: source identity, CPU model and
  tier, Go version, kernel, logical CPU count, GOMAXPROCS, affinity, governor,
  and the one-, five-, and fifteen-minute load averages. This is the
  LOGS-V1-01 exit requirement; the current `machineFacts` output does not yet
  emit every field.
- A run contaminated by load, a competing workload, or a changed source state
  is discarded and rerun only after the host is quiet again.
- Services or settings paused for the window are restored afterwards, even
  after a timeout or a failure.
