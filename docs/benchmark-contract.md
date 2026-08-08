# Benchmark contract

Published before the implementation is measured, so the numbers cannot be
chosen to fit the code. The satisfaction bar for simdlogs is **orders of
magnitude faster than VictoriaLogs** on selective scan-heavy queries; the
floor on every other metric is "better than theirs."

## Corpus

`internal/bench/corpus` generates a deterministic realistic log stream at
a fixed seed (levels, services, hosts, trace ids, messages with repeated
and unique parts), verified reproducible by hashing two runs. The
head-to-head runs a committed slice; both engines ingest the identical
bytes.

## Query classes

1. **Selective time-window** — a few minutes out of the corpus span.
2. **Field equality, high selectivity** — `level:=error AND service:=auth`.
3. **Full scan-and-count** — the pathological no-skip case.
4. **Aggregations** — date_histogram + terms (the `hits`/`facets` shape).

## Metrics

Query latency p50/p99 (warm and cold cache), ingest throughput and
latency percentiles, resident memory, allocations per operation.

## Method

Both engines as servers on the same wire API, on the same machine, same
corpus; VictoriaLogs built from `../victorialogs-reference` and run as a
subprocess. One process per benchmark, shuffled order, minimum of eight,
idle machine (load < 1), the instruction-set tier named in every snapshot.
Losses are published alongside wins — the transparency rule from the
sibling repos.
