# simdlogs

A log management database in Go, built on the simd library: VictoriaLogs'
API surface, plus the Elasticsearch search surface they do not have, at
orders-of-magnitude better numbers on the same machine.

Design: [docs/design.md](docs/design.md).

## The claim, and what measurement did to it

The design aimed for orders of magnitude over VictoriaLogs on selective
scan-heavy queries. The head-to-head harness refuted the premise it
rested on. Measured at 3M rows, both engines on one machine, identical
wire calls:

| query class | result |
|---|---|
| common-value equality | simdlogs **2.0x** faster |
| aggregation (hits) | simdlogs **1.8x** faster |
| **rare-value selective** (the headline case) | **VictoriaLogs 6.4x faster** |
| ingest | comparable, VL slightly ahead |

The design assumed VictoriaLogs scans up to 8M rows for a selective
query. It does not -- its per-block bloom and per-field indexing make the
needle query its strength, not its weakness. simdlogs is competitive on
common queries and behind on selective ones. Orders of magnitude is
unmet, and the class it was promised on is a loss. docs/wrong.md has the
full, unvarnished analysis; a real per-group posting index (the design's
deferred Phase 8) is the minimum for a competitive selective path, and
even then the evidence here does not support 10-100x against this
competitor. This README states what was measured, not what was hoped.

## Status

Under construction, phase by phase against the build plan.

**Landed:** the storage core (columnar row groups with dict+bitpacked
columns and delta+varint timestamps on the simd kernels; per-group skip
footers with time min/max and value blooms), the crash-safe group store,
and the selective query engine (bitset algebra, footer group-skip, dict-id
equality scans). All four "new kernels" the design called for -- varint
decode, bitpacked decode, SIMD hash, bitshuffle -- shipped in simd
v1.18-v1.20 and are consumed directly.

**Head-to-head vs VictoriaLogs** (the reference clone as a subprocess):
1.6-1.8x on the selective query, comparable ingest, at 200K-3M rows.
Not orders of magnitude -- see docs/wrong.md for the honest analysis of
what that would take. The self-relative skip works (a selective window is
50x a forced full scan of our own store); beating a well-tuned VL by
orders of magnitude is a larger-scale, cheaper-query-path problem.

**Next:** ingest pipeline (P2), the LogsQL and Elasticsearch API surfaces
(P4), and the head-to-head harness against VictoriaLogs (P6) -- the
build plan is docs/plans/2026-08-07-simdlogs-full-build.md.
