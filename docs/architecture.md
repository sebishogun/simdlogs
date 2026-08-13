# simdlogs — architecture

A disk-backed, columnar log database in Go. It serves the VictoriaLogs ingest
and LogsQL wire surfaces (plus the Elasticsearch search subset VictoriaLogs
does not have), and executes filters over immutable columnar row groups with
the [simd](https://github.com/sebishogun/simd) kernels.

This document describes what ships today, from the current source. Where a
behavior was chosen against a measured alternative, the measurement is in
[`wrong.md`](wrong.md). Planned direction lives in
[`roadmap.md`](roadmap.md), not here.

## Product boundary

What simdlogs is:

- A single-binary HTTP log store: ingest protocols in, LogsQL/ES/SQL/vector
  queries out, data persisted as immutable group files on local disk.
- Built for selective queries and low-latency aggregations. That choice has a
  measured cost: the per-column inverted indexes use more disk than
  VictoriaLogs (realistic corpus 2.62x of VL; unique-hex 19.4x at 1B rows).
  Both sides are published in [`scale-curve.md`](scale-curve.md).
- Multi-tenant by directory: `AccountID`/`ProjectID` headers select an
  isolated store per tenant.
- Clusterable at the application layer: a node with `-select-backends` becomes
  a select router over peer storage nodes, with sharding and replication.

What it is not (current, unambiguously):

- **Not a metrics store.** There is no metrics ingestion path. `/metrics` and
  `/alerts` are exported from this server; metrics-from-logs rules evaluate
  LogsQL on a timer and expose the result, but no Prometheus/OTLP metric
  ingest exists.
- **Not a consensus system.** The cluster layer has no leader election, no
  automatic membership, no cross-node transactions, no consensus protocol.
  Shards and replicas are configured statically.
- **Not released.** There is no tag and no stable-version promise; the storage
  format and HTTP surface may change. Pin a commit to deploy.
- **Not full Elasticsearch.** `/_search` and `/_count` cover a log-relevant
  DSL subset (`bool`/`term`/`terms`/`range`/`exists`); `_msearch`, the
  complete Query DSL, and ES aggregation-response compatibility are out of
  scope.

## Components

```
cmd/simdlogs        flag parsing, server lifecycle, graceful shutdown
internal/api        HTTP surface: tenancy, routes, wire shapes, cluster routing
internal/ingest     line parsing (simdjson), field mapping, writer + flush pool
internal/storage    immutable columnar group format, store, mmap, ops (retention,
                    tiering, backup, cold)
internal/query      LogsQL/SQL parsers, filter tree, pipes, vectorized scan,
                    bitsets, parallelism, streams, stats
internal/bench      deterministic corpora, head-to-head harness, scale tests
scripts/            quiet-bench.sh (benchmark-gate discipline)
```

Dependencies: `github.com/sebishogun/simd v1.20.0` (SIMD kernels, pure Go with
committed assembly; every kernel has a portable fallback) and
`github.com/sebishogun/simdjson v0.6.0` (ingest parsing). Go 1.26.5, no cgo.

## Data flow

### Ingest

1. HTTP handler reads the body (`/insert/jsonline`, logfmt, ES `/_bulk`, Loki,
   Datadog, OTLP, journald, syslog).
2. The tenant's `AccountID`/`ProjectID` selects a `storage.Store` + `ingest.Writer`
   pair (`tenant-<acc>-<proj>` directory).
3. Per-request field mappings (`_time_field`, `_msg_field`, `_stream_fields`,
   `ignore_fields`, `extra_fields`) rewrite each record.
4. Bodies ≥ 1 MiB split at line boundaries and parse across `NumCPU/3` shard
   writers; smaller bodies go through the tenant's persistent writer. Each
   line's timestamp comes from `_time` (RFC3339 or unix units) or a monotonic
   fallback; malformed lines are counted and skipped, never fatal.
5. The writer buffers rows column-first and hands a full group (128K rows,
   64 MiB, or 2 s) to a flush pool: dictionary build, group marshal, and the
   store's crash-safe append (temp file, fsync, atomic rename, mmap).
6. Every ingest request is flushed before its response — the durability
   boundary. SIGINT/SIGTERM drains HTTP, flushes writers, unmaps stores.

### Query

1. `/select/logsql/query` (or hits/stats/tail/introspection/ES/SQL/vector)
   parses the query and the time window (`start`/`end`; `Now` is stamped from
   the request for relative `_time:` filters).
2. `store.Groups(from, to)` returns only the groups whose `[TimeMin,TimeMax)`
   overlaps the window — the first skip, from the in-memory index.
3. Footer pruning (`groupCanMatch`) rejects groups the per-column bloom +
   dictionary prove cannot match, without decoding any column.
4. Surviving groups scan serially (a selective needle touches one or two
   groups) or on a worker pool (≥ 4 groups, no `Limit`). Per group: time
   mask from block checkpoints, predicate bitsets (equality via posting lists
   or the vectorized residual scan), bitset AND, then materialization of only
   the referenced fields.
5. Pipes transform the row set (`stats` aggregates during the scan without
   materializing rows); results serialize as NDJSON by hand (no reflection).
6. In router mode, queries fan out to one live replica per shard and the
   router merges (select: sort by `_time` descending, apply `limit`; hits:
   sum buckets; stats: sum; values: sum per value).

### Ops loops

- `-retention` (hourly): drop groups whose whole time span is older than the
  age; per-stream retention drops by `_stream` label set. Files unlink only
  after leaving the index.
- `-recompact-after` (hourly): re-encode old groups' dictionaries with flate
  (or also drop postings with `-recompact-drop-postings`); replaced mmaps are
  retired for 5 minutes before unmapping.
- `/admin/backup`: tar of the current group files — a consistent snapshot
  because groups are immutable; `storage.RestoreTar` unpacks it, entry names
  flattened so an archive cannot escape the directory.

## Read order

1. `README.md` — shipped surface, measured numbers, status.
2. `docs/lld/storage.md` — the on-disk format everything else reads.
3. `docs/lld/ingest.md`, `docs/lld/query.md`, `docs/lld/api.md`,
   `docs/lld/cluster.md` — the other layers.
4. `docs/verification.md` — how anything gets measured or trusted.
5. `docs/roadmap.md` — planned direction; nothing there ships yet.
6. `docs/wrong.md` — what was measured and rejected; read before proposing a
   change that repeats a measured idea.
7. `docs/design.md` and `docs/plans/2026-08-07-simdlogs-full-build.md` —
   historical: the original claims and the plan that built the current code.
