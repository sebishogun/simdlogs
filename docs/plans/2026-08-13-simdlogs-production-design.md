# simdlogs — production hardening design

Status: approved. This design describes the direction documented in
[`../roadmap.md`](../roadmap.md); it is a plan for future work, not a
description of the current code. The current product is documented in
[`../architecture.md`](../architecture.md) and the LLDs. Claims below are
falsifiable by the verification discipline in
[`../verification.md`](../verification.md) — no promise is made that any
stage ships.

## Why this exists

The current codebase is fast and measurably compatible (LogsQL 40/40 against
the real VictoriaLogs binary; scale curve published through 1B rows) but it
is not a product yet: there is no tag, no storage-format stability promise,
no crash-recovery test suite, no documented cluster wire contract, and the
disk footprint of the inverted indexes is a known cost — the committed
numbers (realistic 2.62x of VL at `3f5a063`, unique-hex 19.4x at 1B measured
2026-08-10) are historical baselines predating the shipped hex codec, not
the current footprint. Production
hardening is the work of turning a measured engine into something an operator
can pin, back up, upgrade, and trust to survive a crash without losing data.

A source audit on 2026-08-14 found that list incomplete, and in a way that
changes the shape of the work rather than its length. The gaps above are
things the product does not *have*. The audit found things the product does
*wrong* while reporting success, which is worse, because no operator can
detect them from the outside:

- **Acknowledged writes that did not happen.** The parallel NDJSON path
  discards every shard writer's close error, so a store that cannot append a
  single group still answers `200` with a row count.
- **Success responses for malformed input.** Loki, Datadog and OTLP payloads
  that fail to parse return zero records and a success status.
- **Unauthenticated everything.** Any client can query, ingest, read flags
  and download `/admin/backup`; the tenant header *is* the identity, and an
  unparseable tenant ID silently becomes tenant 0.
- **Nothing bounded by default.** Request bodies are read with `io.ReadAll`,
  `search.maxRows` defaults to unlimited, queries carry no context and cannot
  be cancelled, and every numeric tenant ID creates a directory and worker
  goroutines.
- **Reads that outlive their memory.** Retention and cold demotion drop group
  entries while discarding their unmap callbacks; recompaction releases
  mappings on a five-minute timer that no query duration is bound by.
- **Renames that may not survive power loss.** Group files are fsynced before
  rename; the parent directory is not.
- **Corrupt files that reach the process as slices.** `ReadGroup` does direct
  slicing and integer reads after minimal length checking, with no checksum.
- **Cluster answers that are wrong rather than absent.** If every replica of
  one shard fails the merge omits that shard and still returns `200`, and a
  general LogsQL pipeline runs whole on each shard and is concatenated, so
  global `stats`, `top`, `uniq`, `sort` and joins are not merged at all.
- **A benchmark harness that measures something other than what it reports.**
  Ingest samples repeat full inserts so later query timings run over a corpus
  several times the stated size, and some VictoriaLogs ingest intervals
  include a post-ingest sleep.

The last one is why this section leads with it: every published number in
this repository was produced by that harness, so the harness is repaired and
re-run before any measurement in these documents is treated as current.

## Goals

1. **Nothing succeeds that did not happen.** An acknowledged write survives
   process death under the documented fsync policy; malformed or unsupported
   input never receives an unqualified success; a missing shard is never an
   unmarked partial result; a resource limit never silently changes a
   query's answer. This goal is first because every other goal is worth
   nothing while a caller cannot tell success from loss.
2. **Durability by proof, not by design.** The write path is already
   write-temp-fsync-rename, minus the parent-directory fsync it needs to be
   what it claims; the deliverable is that fsync, a checksummed
   bounds-checked group format, a manifest that makes group visibility a
   committed fact rather than a filename glob, and the crash-recovery test
   that proves all of it at every phase.
3. **Bounded by default, and authenticated.** Finite defaults for body size,
   decompressed size, rows, bytes, duration, concurrency and open tenants;
   context threaded through query execution so a disconnect stops the scan;
   authenticated tenancy where identity comes from a verified principal
   rather than a request header. Zero stops meaning unlimited.
4. **Memory owned by its readers.** Every mmap stays valid while a reader
   holds it and is unmapped after the last one releases it, through
   reference-counted snapshots rather than a timer.
5. **Compatibility as a contract.** The 40/40 corpus becomes a gate; the ES
   DSL subset and the select-router merge semantics become documented
   contracts with conformance tests; the API-surface probes (shape + answer
   changes, not status codes) extend to every endpoint.
6. **Ops surfaces that tell the truth.** `/metrics` emits only metrics whose
   meaning this server can honour; the LLD documents the names as the
   contract, with tests for presence and monotonicity. No fabricated zeros.
7. **A documented cluster protocol.** Application-level sharding and
   replication with no consensus stays the model (it is a feature: simple,
   statically configured, inspectable); the router's merge rules and failure
   behavior become a tested wire contract. The starting point is a defect
   list, not a clean slate: the `streams`, `stream_ids`, plain `stats_query`,
   and `hits` merges decode stale envelopes and answer empty/bogus results
   (exact mismatches in [`../lld/cluster.md`](../lld/cluster.md)), several
   select surfaces (`facets`, `tail`, SQL, vector, backup, metrics) are not
   federated, and the avg/quantile merge gap is
   closed or removed from the claims.
8. **Scale with numbers attached.** The 1B-row point reproducible from a
   documented command, with peak-memory bounds measured; the 1M
   selective-window loss investigated to a measured conclusion.
9. **A release.** A tag, a changelog that references merged commits, and the
   README status flipped from "pin a commit" to the tagged version.

## Design decisions (with the tradeoffs they imply)

- **The storage format stays versioned and codec-flagged, and old groups
  keep reading.** The v7 format plus per-block codec flags already makes a
  store hold LZ4, flate, and hex blocks at once. A format bump must come with
  a migration test that opens pre-bump data. Tradeoff: format-compatibility
  testing is permanent CI cost; the alternative — format freedom — is a
  promise this design refuses to make for a log store.
- **Crash-safety stays per-group atomic, and group visibility becomes a
  committed fact.** No group is ever appended to or mutated. What changes is
  what "leaves the index" means: a filename glob plus an in-memory slice is
  not a commit point, so retention that unlinks after dropping an entry can
  resurrect the group when the unlink fails, and a rename that is not
  followed by a parent-directory fsync is not guaranteed to survive power
  loss. A length-prefixed CRC32C manifest becomes the commit point, the
  atomic-replace helper always fsyncs the parent directory, and the recovery
  test pins the invariants with a SIGKILL at every phase.

- **mmap lifetime is owned by readers, not by a clock.** The previous design
  retired replaced mappings for five minutes and assumed no reader outlived
  that. Nothing bounded query duration to five minutes, so the assumption was
  unfounded rather than merely tight, and retention and cold demotion dropped
  entries while discarding the unmap callback entirely, which leaked the
  mapping instead. Each group version now owns a reference count, a retired
  flag and a one-shot unmap; a snapshot raises the count under the store lock
  and the final release unmaps a retired version. The tradeoff is that a
  pathological long-running query pins disk blocks until it finishes, which
  is a bounded and observable cost -- snapshot-lease count is a published
  metric -- where unmapping under a live reader is neither.
- **The router merges stay exact where counts are involved, and a shard that
  cannot be reached is an error rather than an omission.** Select merge is
  concatenate-sort-limit (exact). Sums are exact once the stale-envelope
  defects are fixed. The avg/quantile gap is either closed with
  sum+count/sketch merge or documented as out of scope — the design prefers
  closing it, because a stats endpoint that answers wrong on a cluster and
  right on one node is a trap for operators who scale out.

  Two failures found by the audit are the same trap in a worse form. A shard
  whose every replica fails is dropped from the merge and the router still
  answers `200`, so the result is silently short; completeness becomes
  explicit, with `allow_partial_response=1`, status `206` and named missing
  shards as the only way to get a partial answer. And a general LogsQL
  pipeline is currently run whole on every shard and concatenated, which is
  correct only for row-local pipes: `stats`, `top`, `uniq`, `sort` and joins
  need a shard-local plan and a global merge operator per aggregation, or an
  explicit rejection until one exists. Writes get a persisted random write ID
  so a retry cannot duplicate rows, and a configurable consistency level that
  defaults to `all` until repair is proven.
- **Disk footprint stays the known trade, not a hidden one.** The inverted
  index is the source of the selective-query wins and roughly 27% of a
  group. Roadmap work on footprint (the tiered-storage session `d846429`
  measured 2.40x → ~1.55x on the realistic 100K-row corpus, post-hex — the
  most recent realistic-corpus footprint measurement on record, still not a
  fresh scale/current-release number) must publish speed-vs-disk with both
  sides, and any change that shrinks disk at the cost of the needle
  (measured 90x slower when singleton postings were dropped) must be gated
  on a FRESH full curve, not on the historical numbers or on footprint
  alone.
- **Scale-out is measured against scale-up.** The cluster is statically
  configured; a multi-node point must be published against the single-node
  curve before the roadmap claims it (the one-node cluster measured as pure
  overhead; the multi-node point is the first number that can justify the
  complexity).

## Non-goals

- Consensus, leader election, transactions, automatic membership, shard
  rebalancing: the static application-level model is the design.
- Metrics ingestion: the product ingests logs; `/metrics` stays an export
  surface.
- Full Elasticsearch compatibility: the documented DSL subset is the
  contract.
- Performance parity on disk footprint: the trade is measured and published,
  not promised away.

## Two release exits

The work has two exits rather than one, because single-node correctness and
distributed correctness fail independently and an operator who runs one node
should not wait for the other.

**Single-node production exit** — phases 0 through 7 of
[the implementation plan](2026-08-13-simdlogs-production.md). Requires a
24-hour ingest/query/retention/recompaction soak with no race, mmap growth,
goroutine growth or acknowledged data loss; the repeated crash matrix and a
restore drill; a security review of auth, tenant isolation, TLS, backup and
admin routes; finite default limits documented in the README and LLDs. At
this exit **cluster mode stays explicitly experimental and disabled by
default** — it is not part of what the exit certifies.

**Full production exit** — phases 0 through 10, adding the static-cluster
contract (versioned wire protocol, idempotent writes, explicit consistency,
no silent partial reads, distributed LogsQL planning, repair and chaos
tests) and reproducible signed release artifacts.

Neither exit is reached by this document. Nothing in this design is shipped
until it is in the code, the tests, the changelog and the gates; the roadmap
records what is built and this file records what is intended. A reader
looking for current behavior wants `../architecture.md` and the LLDs.

## Falsifiable claims

Each stage of the roadmap has exits that can fail. The load-bearing ones:

- A SIGKILL at any phase of the write path never loses an fsynced group and
  never indexes a partial one.
- An acknowledged write is durable: every request that returned success has
  its rows present exactly once after a kill and reopen, and any request
  whose shard writer failed returned an error instead.
- No malformed or unsupported payload on any ingest protocol receives an
  unqualified success response.
- Every persisted structure survives arbitrary corruption without panicking:
  the group parser, the manifest, the backup manifest and the restore tar
  each fuzz to a typed error.
- A group's mapping stays valid for the whole life of any snapshot holding
  it, across retention, recompaction, cold demotion and store close, and is
  unmapped once the last snapshot closes. Soak measures the mmap count flat.
- Every query is cancellable at group pruning, scan, worker, materialization,
  aggregation, sort and subquery, and no limit changes a successful query's
  answer without an error.
- No unauthenticated request reaches tenant data, and no authenticated
  principal reaches a tenant it is not authorized for.
- The 40/40 compatibility corpus and the API-surface probes pass as gates
  with asserted body equivalence rather than logged differences (when the VL
  binary is staged; skipped loudly otherwise).
- A 3-node router test answers byte-identical results to a single node with
  identical data, for every merged endpoint, and a shard with no reachable
  replica produces an error or an explicitly-marked `206` — never a short
  `200`.
- The 1B-row scale point reproduces from the documented command with the
  machine and SIMD tier named, and peak memory is bounded and measured.
- Every published measurement comes from the corrected harness: one fixed
  corpus of the stated size for query timing, no sleep inside a timed
  interval, engines interleaved in one session on a quiet machine.

Where a claim fails, the measurement goes to `docs/wrong.md` and the roadmap
stage is re-opened — the deliverable is the entry, as always.
