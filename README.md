# simdlogs

A log management database in Go, built on the simd library: VictoriaLogs'
API surface, plus the Elasticsearch search surface they do not have, at
orders-of-magnitude better numbers on the same machine.

Design: [docs/design.md](docs/design.md).

## The claim, and what measurement did to it

![simdlogs vs VictoriaLogs](docs/bench.svg)


The design aimed for orders of magnitude over VictoriaLogs on selective
scan-heavy queries. The head-to-head harness refuted the premise it
rested on. Measured at 3M rows, both engines on one machine, identical
wire calls:

| query class (3M rows) | result |
|---|---|
| selective query (returns rows) | simdlogs **4.4x** faster |
| aggregation (hits) | simdlogs **2.0x** faster |
| full-scan count (engine, parallel) | **3.7x** the serial path |
| rare-value needle | **VictoriaLogs ~2.8x faster** |
| ingest | comparable, VL slightly ahead |

The design's whole vectorized-execution half was built after an external
review found it missing: the residual scan is now vpcmpeqd + pack over
encoded indices (equality) and vector range compares (time), not the
scalar per-row loop that was VictoriaLogs' own anti-pattern; query
execution fans across cores over groups; and the result path is
hand-built NDJSON, no reflection. That took the selective row query from
1.9x to 4.4x. The rare needle is still VictoriaLogs' -- its per-field
index wins the one-in-a-billion lookup, and the named next step is a
global value->group index (Phase 9). Ingest is comparable; LZ4 dictionary
compression (1.43x here) is the footprint lever for the 100M+ scale where
groups stop being cache-resident.

Honest standing: competitive-to-strong on the classes it targets (2-4.4x),
behind on the rare needle, comparable on ingest. Orders of magnitude
against a well-engineered VictoriaLogs is not reached, and the needle
class is genuinely theirs. docs/wrong.md carries the full arc, including
the premise that measurement killed and the levers that recovered from it.

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

**Surface:** /insert/jsonline; /select/logsql/{query,hits,field_names,
field_values,facets,stats_query}; the Elasticsearch _search and _count
(bool/term/range/exists, time-range mapped to the partition skip) that
VictoriaLogs does not have; time-based retention. First-milestone scope
per docs/plans/2026-08-07-simdlogs-full-build.md; full LogsQL, the wider
ES DSL, and clustering are explicit later-phase non-goals.
