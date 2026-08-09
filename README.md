# simdlogs

A log management database in Go, built on the simd library: VictoriaLogs'
API surface, plus the Elasticsearch search surface they do not have, at
orders-of-magnitude better numbers on the same machine.

Design: [docs/design.md](docs/design.md).

## The claim, and where it actually stands

The design aims for orders of magnitude over VictoriaLogs on selective
scan-heavy queries. Measured head-to-head so far (200K-3M rows, both
engines on one machine, identical wire calls): **1.6-1.8x on the
selective query, comparable on ingest** -- real but not yet the bar. The
honest reasons are in docs/wrong.md: VictoriaLogs is well engineered, its
8M-row blocks carry their own time index and blooms, and the granularity
advantage (128K groups) only bites above 8M rows and with a cheaper query
path (lazy per-lane decode). This README states the earned number, not
the aspiration, and moves as measurements do.

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
