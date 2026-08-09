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
| common-value equality | simdlogs **1.9x** faster |
| aggregation (hits) | simdlogs **2.0x** faster |
| rare-value selective (needle) | **VictoriaLogs 2.0x faster** |
| ingest | comparable |

The design assumed VictoriaLogs scans up to 8M rows for a selective
query. It does not -- its per-block bloom and per-field indexing make the
needle query a strength. Building the design's deferred Phase 8 (a
per-group posting index) and binary-searching the dictionary took the
needle from a 6.4x loss to 2.0x -- real progress, still behind.
simdlogs is ~2x faster on common-value queries and aggregations, ~2x
slower on rare selective ones, comparable on ingest: a competitive,
correct, VL-wire-compatible engine, not a dominant one. Orders of
magnitude is unmet and, on all evidence here, not achievable against this
competitor at these scales. docs/wrong.md has the full analysis. This
README states what was measured, not what was hoped.

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
