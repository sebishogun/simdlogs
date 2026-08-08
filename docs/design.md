# simdlogs — design

A log management database in Go, built on the simd library, aimed at every
metric VictoriaLogs publishes — and the API surface it does not have.

Status: draft for the first milestone. The numbers in the targets table are
claims, which means they are promises to be falsified by the benchmark
harness, not hopes.

## Why this exists

The reference codebase (`../victorialogs-reference`) leaves its own
performance on the table, measurably:

- **8M-row blocks** (`lib/logstorage/consts.go:29`) — the bloom-filter skip
  granularity is 8M rows, so a bloom "maybe" decompresses and scans up to 8M
  rows. Block sizes of 64–128K rows give 64–128× finer skip.
- **Scalar residual scans** (`lib/logstorage/filter_exact.go:184`) — the
  equality filter is a closure per row (`bm.forEachSetBit(func(idx int) bool {
  return values[idx] == value })`). No SIMD anywhere in the package.
- **64-step bit iteration** (`lib/logstorage/bitmap.go:128`) — `for j := range
  64` with a shift and a branch per bit, where tzcnt or SIMD popcount does the
  same word in one pass.
- **Whole-block decode** — a query hitting 3 rows in an 8M-row block pays for
  the whole block's zstd decompression.

Their strength is real and worth copying: schema-free columnar storage,
per-column dict encoding, per-column bloom filters, an honest single-binary
ops story, and a wire-compatible ingest surface. The design below takes the
surface and the architecture, and replaces the execution model.

## The claim

On the same corpus, same machine, same query set: **10–100× faster on
selective scan-heavy queries, 2–4× faster ingest, at a fraction of the memory
and allocations**. The floor on every metric is "better than theirs"; the
orders of magnitude are the goal, not the promise.

Where the speed comes from, in order of leverage:

1. **Skip structures** — time-partitioned row groups at 64–128K rows, per-group
   per-column min/max, dict, bloom and cardinality. Queries that would make
   them decompress 8M rows touch 128K.
2. **Vectorized execution** — every operator processes column batches through
   the simd kernels; no per-row Go loops in the hot path.
3. **Bitset algebra end-to-end** — filters produce bitsets; AND/OR/NOT of
   filters is word-wise vector ops.
4. **SIMD decode** — bitpacked columns decoded with the kernels, selected
   lanes only.
5. **Zero-allocation ingest** — simdjson's mask-pass parse + SIMD hash
   interning into per-group dicts, arena memory.

## Architecture

### Storage

- Time-ordered row groups, 64–128K rows each, merged like LSM segments.
- Per column per group: dictionary encoding (low cardinality), bitpacked
  indices; timestamps delta + SIMD varint; strings bitshuffle + zstd.
- Per group footer: min/max time, per-column min/max, dict fingerprints,
  bloom filters, cardinality.
- mmap + prefetch pipeline for the groups the planner selects.

### Query

- Planner: time range → candidate groups → per-group stats → dict membership
  → bitset intersection → row selection → decode selected lanes → residual
  predicates on survivors.
- Parallel over groups; SIMD within groups. Regex on survivors is the only
  scalar-Go spot and it is deliberately so.

### Ingest

- Parse lines with the simdjson stage-one architecture (mask pass → token
  walk), intern against the group dict via SIMD hash, append encoded indices.
- Batched, arena-allocated, zero per-line allocations.

### API

Match VictoriaLogs' surface 1:1, then supersede it:

- `/insert/*`: jsonline, elasticsearch (bulk format), loki, opentelemetry,
  datadog, splunk, journald, native.
- `/select/logsql/*`: query, stats_query, stats_query_range, hits, facets,
  field_names, field_values, streams, tail, query_time_range.
- **Extras**: the real ES search surface — `_search`, `_msearch`, `_count`,
  `_bulk` with the log-relevant DSL (bool, term, terms, range, match_phrase,
  exists, wildcard) and the Grafana-ES aggregations (terms, date_histogram,
  stats, percentiles, cardinality). This is the surface VictoriaLogs does not
  have at all: ELK clients and dashboards work against us and not against
  them.

### Benchmarks

The contract is published before the implementation is measured:

- Same corpus (committed, reproducible), same machine, warm and cold cache.
- Query classes: selective time-window, field equality with high selectivity,
  full scan-and-count, aggregations.
- Ingest rate and latency percentiles.
- Both engines as servers on the same wire API; VictoriaLogs runs from the
  reference clone as a subprocess.
- Methodology from the sibling repos: one process per benchmark, shuffled
  order, minima of eight, idle machine, tier named.

## Kernel work

Already available in the simd library: compare (eq/neq/range masks),
compress (filter-compact), reduce (sum/count), sort, nary (u64 bitset ops),
fastmath (exp/log/pow for stats), the scan/mask family, the JSON kernels.

New kernels this project needs (one C file each, generated for the six
architectures like the rest):

1. SIMD varint decode — timestamps and ids.
2. Bitpacked/bitplane decode — packed columns.
3. SIMD hash (xxhash-class) — dict interning.
4. Bitshuffle — byte↔bitplane transpose for compression and decode.

## Go toolchain

Go 1.26.2 now; switch to 1.27 on release without ceremony — nothing in this
design may depend on a toolchain quirk that would make that switch painful.

## Non-goals for the first milestone

- Full LogsQL parser (subset first: filter + time range + hits + stats +
  tail).
- Distributed/clustered mode.
- Full ES DSL beyond the log-relevant subset.
- Metrics ingestion (VictoriaMetrics' other product; not this repo's game).

## The record

`docs/wrong.md` holds measurements that argued against changes, as in the
sibling repos. A finding that cost a measurement belongs there whether or not
any code changed.
