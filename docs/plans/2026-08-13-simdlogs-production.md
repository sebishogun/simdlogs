# simdlogs — production hardening implementation plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to
> implement this plan task-by-task.

> **Status:** planned. Executes [`2026-08-13-simdlogs-production-design.md`](2026-08-13-simdlogs-production-design.md)
> and the stages of [`../roadmap.md`](../roadmap.md). The design and roadmap
> are approved direction; this file is the task-by-task plan. Work starts on
> its own branch from current main, tests-first, each task ends in a commit.

**Goal:** Turn the measured engine into a durable, contract-tested, released
log database, without giving back the selective-query wins or hiding the disk
trade.

**Architecture (unchanged by this plan):** immutable columnar row groups
(128K rows, v7 format, per-block codec flags), mmap reads, footer skip,
postings + bloom, write-temp-fsync-rename appends, static application-level
sharding/replication. This plan adds proofs, contracts, and policy around
that architecture; it does not redesign it.

**Tech Stack:** Go 1.26.5, `github.com/sebishogun/simd v1.20.0`,
`github.com/sebishogun/simdjson v0.6.0`. Reference: `../victorialogs-reference`
(read-only, cited by file:line). Staged binary at
`internal/bench/victoria-logs` (gitignored) for the head-to-head reports.

**Discipline:** disassemble first, gates bare or with `pipefail`, measurements
into `docs/wrong.md`, benchmark contract in `docs/benchmark-contract.md`,
noise floor 8.3% with `perf stat -e instructions:u,cycles:u` for smaller
deltas, quiet machine (load < 1), interleaved A/B minimums.

**Phases:** A crash recovery, B compatibility as gates, C ops contracts,
D cluster wire contract, E scale proofs, F release.

---

## Phase A: Crash recovery

### Task A.1: Kill-at-every-phase recovery test

**Files:**
- Create: `internal/storage/crash_test.go`

**Step 1 (test first):** Write `TestKillAtWritePhase` running a real store in
a subprocess that receives a phase signal: the parent starts the subprocess,
feeds an ingest batch, and SIGKILLs it at a named phase (buffering, temp
write, fsync, rename, post-rename). The subprocess reports its phase on a
pipe before the kill.

**Step 2:** Reopen the store in the parent and assert: every group the
subprocess fsynced is present and queryable; no partial group is indexed; any
`.tmp` files are ignored.

**Step 3:** `go test -race ./internal/storage/` bare; commit.

### Task A.2: Backup/restore round-trip at scale

**Files:**
- Create: `internal/storage/backup_scale_test.go`

**Step 1 (test first):** Ingest hundreds of groups through the writer while a
concurrent goroutine calls `BackupTar` mid-flush; restore into a fresh
directory; open; run a query mix and assert answers identical to the
original store.

**Step 2:** Assert `RestoreTar` rejects a crafted archive whose entry names
escape the target directory.

**Step 3:** Gates bare; commit.

### Task A.3: Corrupt-group handling and fsync policy doc

**Files:**
- Create: `internal/storage/corrupt_test.go`
- Modify: `docs/lld/storage.md`

**Step 1 (test first):** Bit-flip and truncate a group file; assert
`OpenStore` reports the failure, keeps every other group readable, and
`/metrics` records it (a log line + `vl_*` counter — add the counter if
absent, tested).

**Step 2:** Document the fsync policy in `docs/lld/storage.md`: durable at
the per-request flush boundary; the OS page cache is not part of the promise.

**Step 3:** Gates bare; commit.

---

## Phase B: Compatibility as gates

### Task B.1: Corpus extension and gate wiring

**Files:**
- Modify: `internal/bench/corpus/corpus.go`, `internal/bench/compat_test.go`

**Step 1 (test first):** Extend the corpus with malformed-query cases
(unbalanced parens, bad regex, empty pipes), empty-tenant requests, and
streaming-response cases (tail with a filter). Each new case asserts
simdlogs' answer shape and — when the staged binary is present — the VL
binary's, with the skip logged loudly.

**Step 2:** Make the corpus run in CI as a gate (a `-short`-safe invocation:
the corpus is small; the VL half skips when the binary is absent, and the
skip is a failure only if the binary was staged but not found).

**Step 3:** Gates bare; commit.

### Task B.2: ES DSL subset contract

**Files:**
- Create: `internal/api/es_contract_test.go`
- Modify: `docs/lld/api.md`

**Step 1 (test first):** One conformance test per committed ES clause
(`bool` must / filter, `term`, `range` incl. time-range feeding the group
skip, `exists`), asserting the response envelope (`hits.total.relation:
"eq"`) and the rows. `terms` is NOT committed — the DSL decoder handles
`bool`/`term`/`range`/`exists` only; adding it is a measurement-first
decision (a conformance fixture against the reference's behavior first,
then implementation), never a silent extension.

**Step 2:** Document the subset as the fixed contract in `docs/lld/api.md` —
the LLD becomes the spec the tests pin.

**Step 3:** Gates bare; commit.

### Task B.3: Probe sweep on every endpoint

**Files:**
- Modify: `internal/bench/apisurface_test.go`

**Step 1 (test first):** Extend the answer-changes probe to every select and
ingest endpoint, and add the missing `keep_const_fields` case. `facetKeep`
materially includes constant/single-distinct fields
(`internal/query/introspect.go`), and with no stream fields configured the
synthesized `_stream`/`_stream_id` hold single constant values, so
`keep_const_fields=1` must change the committed-corpus facets answer. No
committed probe currently varies or asserts this argument: the
`TestParamsHonoured` keep_const case is gated on the staged VL binary and
reports inconclusive when the reference's own answer does not change — which
is what the 2026-08-13 probe (commit f42cc8e) recorded, against the then
pre-facetable-synthesized-fields code (facets from stored columns alone);
the same commit made the synthesized fields facetable. The new probe must
assert the answer changes without depending on the VL binary, and the
finding's history (not in `docs/wrong.md`, which has no entry for it) goes
in the commit message.

**Step 2:** A status-code probe plus a response-shape probe per endpoint
(catch the class of defect where a 200 answers a body no client can read).

**Step 3:** Gates bare; commit.

---

## Phase C: Ops contracts

### Task C.1: Metrics contract test

**Files:**
- Create: `internal/api/metrics_contract_test.go`
- Modify: `docs/lld/api.md`

**Step 1 (test first):** Assert every emitted metric name exists in the LLD
table and vice versa; ingest N rows and assert
`simdlogs_rows`/`vl_storage_rows`/`vl_rows_ingested_total` move by N;
assert `vl_http_errors_total` counts a forced 400.

**Step 2:** Document the metric names as the stable contract in
`docs/lld/api.md`.

**Step 3:** Gates bare; commit.

### Task C.2: Retention and tiering under fault injection

**Files:**
- Create: `internal/storage/ops_fault_test.go`

**Step 1 (test first):** Run retention and recompaction while a concurrent
reader holds a group; inject a failed unlink; assert the index stays
consistent and no reader crashes (the 5-minute mmap retire grace is the
mechanism; the test pins it).

**Step 2:** Gates bare; commit.

### Task C.3: Shutdown and health contracts

**Files:**
- Create: `internal/api/shutdown_test.go`

**Step 1 (test first):** Start a server, open a live tail, issue in-flight
ingest, SIGTERM; assert: in-flight requests drain, the writer flushes, stores
unmap, and `/health` + `/-/healthy` + `/-/ready` answer during and after.

**Step 2:** Gates bare; commit.

---

## Phase D: Cluster wire contract

The router surface is experimental, not production-safe: four merges decode
stale envelopes and answer empty/bogus results, and several select surfaces
are not federated. The exact mismatches are recorded in
`docs/lld/cluster.md`. Phase D fixes them tests-first, in order of urgency.

### Task D.0: Fix the stale-envelope merges, fixture-first

**Files:**
- Create: `internal/api/cluster_envelope_test.go`
- Modify: `internal/api/cluster.go`

**Step 1 (test first, failing on today's code):** One shard-shape fixture
per broken merge. Each fixture is an in-process backend answering the
CURRENT envelope of its endpoint, plus a router pointed at it:

- `streams` / `stream_ids`: the backend answers the shared
  `{"values":[{"value":..,"hits":..}]}` envelope (what the local handlers
  send today); the test asserts the router's merged answer equals the
  single-node answer for identical data. Today the router reads the
  `"streams"`/`"stream_ids"` keys that do not exist and answers
  `{"streams":[]}` / `{"stream_ids":[]}`.
- plain `stats_query`: the backend answers the Prometheus vector envelope
  `{"status":"success","data":{"resultType":"vector","result":[...]}}`;
  the router must sum the sample values and answer the same envelope. Today
  it decodes a nonexistent `count` field and always answers `{"count":0}`.
- `hits`: the backend answers the dense series shape
  `{"hits":[{"fields":..,"timestamps":[..],"values":[..],"total":N}]}`;
  the router must sum per bucket across shards and return the same dense
  shape. Today it decodes the old `{"_time":..,"hits":..}` object shape, so
  every dense series object lands as one bogus zero entry and the router
  answers exactly `{"hits":[{"_time":"","hits":0}]}`.

**Step 2:** Fix each merge so its fixture passes; keep the single-node
byte-identical cross-check.

**Step 3:** Gates bare; commit per merge (four commits).

### Task D.1: Multi-node integration test

**Files:**
- Create: `internal/api/cluster_contract_test.go`

**Step 1 (test first):** N storage nodes (in-process servers on ephemeral
ports) + one router with `-select-backends` and `-replicas`. Ingest a corpus
through the router; query every merged endpoint; compare byte-identical
answers against a single-node store with the same data.

**Step 2:** Assert the avg/quantile gap: a stats query with `avg`/`quantile`
on the router either merges correctly (implement sum+count merge in
`internal/api/cluster.go` if the step's measurement says it is cheap) or
errors loudly with a documented message — never a silent wrong answer.

**Step 3:** Gates bare; commit.

### Task D.2: Failure injection

**Files:**
- Modify: `internal/api/cluster_contract_test.go`

**Step 1 (test first):** Kill one replica mid-query and mid-write; assert
reads answer from the surviving replica, writes land on the surviving
replica, and a write with all replicas of the chosen shard down returns 502
`all replicas unreachable`.

**Step 2:** Gates bare; commit.

### Task D.3: Document the wire contract

**Files:**
- Modify: `docs/lld/cluster.md`

**Step 1:** Move the router behavior (shard grouping, write replication,
merge table, failure rules) from prose to a numbered contract the D.1/D.2
tests cite.

**Step 2:** Commit.

---

## Phase E: Scale proofs

### Task E.1: Reproducible 1B point

**Files:**
- Modify: `docs/scale-curve.md`, `internal/bench/scale_test.go`

**Step 1:** Parameterize the scale test so the 1B unique-hex point runs from
the documented command (`SIMDLOGS_SCALE=1 SIMDLOGS_SCALE_SIZES=1000000000
SIMDLOGS_SCALE_DIR=/big/disk go test -run TestScale -v -timeout 0
./internal/bench/`).

**Step 2 (measure):** Reproduce the 1B numbers on a quiet machine; publish
with machine + SIMD tier. Measure peak RSS during the run.

**Step 3:** If the reproduction disagrees with the curve, investigate with
disassembly/perf first; the corrected numbers go to the curve, the loss (if
any) to `docs/wrong.md`. Commit.

### Task E.2: The 1M selective-window loss

**Files:**
- Modify: `internal/bench/realistic_test.go`, `docs/wrong.md`

**Step 1 (measure):** Profile the 0.8x selective-window case at 1M rows
(disassemble first). Hypothesis budget: group-skip granularity vs the
reference's block layout, and the parallel/serial crossover at 4 groups.

**Step 2:** Either land a measured fix (interleaved A/B, minimum, quiet
machine) or record the explanation with the measurement in `docs/wrong.md`.
Commit either way.

---

## Phase F: Release

### Task F.1: Upgrade policy and format statement

**Files:**
- Modify: `docs/lld/storage.md`, `docs/roadmap.md`

**Step 1:** Write the storage-format compatibility statement: v7 groups and
the per-block codec flags are the compatibility surface; a format bump
requires the A.1-style migration test.

**Step 2:** Roadmap Stage 5 exits updated with the statement's text. Commit.

### Task F.2: Tag and changelog

**Files:**
- Create: `CHANGELOG.md` (or adopt the repo's existing release artifact)
- Modify: `README.md` (Status section)

**Step 1:** Draft the changelog from merged commits; every entry references a
commit.

**Step 2:** Tag; verify the tag builds from a clean checkout
(`go build ./cmd/simdlogs`).

**Step 3:** Flip the README status section from "no tagged release; pin a
commit" to the tagged version; retire the pin-commit guidance. Commit.

---

## Gate checklist (every task)

- `go test ./...` bare.
- `go test -race ./...` bare.
- `go vet ./...` bare.
- Any changed behavior measured, not assumed; deltas under the 8.3% noise
  floor compared on instructions retired, not wall-clock.
- A measurement that argues against the plan's assumption goes to
  `docs/wrong.md` — the entry is the deliverable.
