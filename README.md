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

Skeleton. The design document is the deliverable so far.
