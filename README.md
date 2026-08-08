# simdlogs

A log management database in Go, built on the simd library: VictoriaLogs'
API surface, plus the Elasticsearch search surface they do not have, at
orders-of-magnitude better numbers on the same machine.

Design: [docs/design.md](docs/design.md).

## The claim

Same corpus, same machine, same query set: 10–100× faster on selective
scan-heavy queries, 2–4× faster ingest, at a fraction of the memory and
allocations. The floor on every metric is "better than VictoriaLogs".

## Status

Under construction, phase by phase against the build plan.

**Landed:** the storage core (columnar row groups with dict+bitpacked
columns and delta+varint timestamps on the simd kernels; per-group skip
footers with time min/max and value blooms), the crash-safe group store,
and the selective query engine (bitset algebra, footer group-skip, dict-id
equality scans). All four "new kernels" the design called for -- varint
decode, bitpacked decode, SIMD hash, bitshuffle -- shipped in simd
v1.18-v1.20 and are consumed directly.

**First engine number:** a selective time-window query runs **50x** faster
than a forced full scan of the same corpus (131 ms vs 6.6 s over 1M rows),
the group-skip factor exactly. Absolute latency has a known next lever
(lazy per-lane decode); see docs/wrong.md.

**Next:** ingest pipeline (P2), the LogsQL and Elasticsearch API surfaces
(P4), and the head-to-head harness against VictoriaLogs (P6) -- the
build plan is docs/plans/2026-08-07-simdlogs-full-build.md.
