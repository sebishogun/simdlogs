# simdlogs — production hardening design

Status: draft. This design describes the direction documented in
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
disk footprint of the inverted indexes is a known cost (2.62x of VL on the
realistic corpus; the unique-hex worst case is 19.4x at 1B). Production
hardening is the work of turning a measured engine into something an operator
can pin, back up, upgrade, and trust to survive a crash without losing data.

## Goals

1. **Durability by proof, not by design.** The write path is already
   write-temp-fsync-rename; the deliverable is the crash-recovery test that
   proves it at every phase, and the documented fsync policy.
2. **Compatibility as a contract.** The 40/40 corpus becomes a gate; the ES
   DSL subset and the select-router merge semantics become documented
   contracts with conformance tests; the API-surface probes (shape + answer
   changes, not status codes) extend to every endpoint.
3. **Ops surfaces that tell the truth.** `/metrics` emits only metrics whose
   meaning this server can honour; the LLD documents the names as the
   contract, with tests for presence and monotonicity. No fabricated zeros.
4. **A documented cluster protocol.** Application-level sharding and
   replication with no consensus stays the model (it is a feature: simple,
   statically configured, inspectable); the router's merge rules and failure
   behavior become a tested wire contract, and the avg/quantile merge gap is
   closed or removed from the claims.
5. **Scale with numbers attached.** The 1B-row point reproducible from a
   documented command, with peak-memory bounds measured; the 1M
   selective-window loss investigated to a measured conclusion.
6. **A release.** A tag, a changelog that references merged commits, and the
   README status flipped from "pin a commit" to the tagged version.

## Design decisions (with the tradeoffs they imply)

- **The storage format stays versioned and codec-flagged, and old groups
  keep reading.** The v7 format plus per-block codec flags already makes a
  store hold LZ4, flate, and hex blocks at once. A format bump must come with
  a migration test that opens pre-bump data. Tradeoff: format-compatibility
  testing is permanent CI cost; the alternative — format freedom — is a
  promise this design refuses to make for a log store.
- **Crash-safety stays per-group atomic.** No group is ever appended to or
  mutated; retention and recompaction unlink files only after they leave the
  index, and recompaction retires replaced mmaps for 5 minutes. The recovery
  test must pin these invariants (a SIGKILL at any phase, reopen, verify).
- **The router merges stay exact where counts are involved.** Select merge
  is concatenate-sort-limit (exact). Sums are exact. The avg/quantile gap is
  either closed with sum+count/sketch merge or documented as out of scope —
  the design prefers closing it, because a stats endpoint that answers wrong
  on a cluster and right on one node is a trap for operators who scale out.
- **Disk footprint stays the known trade, not a hidden one.** The inverted
  index is the source of the selective-query wins and roughly 27% of a
  group. Roadmap work on footprint (tiering already measured 2.40x → ~1.55x
  of VL) must publish speed-vs-disk with both sides, and any change that
  shrinks disk at the cost of the needle (measured 90x slower when singleton
  postings were dropped) must be gated on the full curve, not on footprint
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

## Falsifiable claims

Each stage of the roadmap has exits that can fail. The load-bearing ones:

- A SIGKILL at any phase of the write path never loses an fsynced group and
  never indexes a partial one.
- The 40/40 compatibility corpus and the API-surface probes pass as gates
  (when the VL binary is staged; skipped loudly otherwise).
- A 3-node router test answers byte-identical results to a single node with
  identical data, for every merged endpoint.
- The 1B-row scale point reproduces from the documented command with the
  machine and SIMD tier named, and peak memory is bounded and measured.

Where a claim fails, the measurement goes to `docs/wrong.md` and the roadmap
stage is re-opened — the deliverable is the entry, as always.
