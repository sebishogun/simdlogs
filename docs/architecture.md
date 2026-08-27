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
  VictoriaLogs. The committed numbers are historical baselines, not the
  current footprint — the unique-hex 19.4x table dates from 2026-08-10
  (before the FOR postings and hex codec shipped) and the realistic 2.62x
  from 2026-08-12 (`3f5a063`, after FOR, before hex) — see
  [`scale-curve.md`](scale-curve.md) and
  [`lld/storage.md`](lld/storage.md).
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
  Shards and replicas are configured statically. Every route is classified
  federated / router-local / refused, and the classification is checked
  against the mux, so an endpoint's status is derived rather than remembered
  (see [`lld/cluster.md`](lld/cluster.md)). The stale merges this paragraph
  used to list are fixed; what remains outstanding is in
  [`release-readiness.md`](release-readiness.md).
- **Not released.** There is no tag and no stable-version promise; the storage
  format and HTTP surface may change. Pin a commit to deploy.
- **Not full Elasticsearch.** `/_search` and `/_count` cover a log-relevant
  DSL subset: `bool` (`must`/`filter`/`must_not`/`should`), `term`, `terms`,
  `match`, `prefix`, `exists`, `match_all`, and timestamp `range`. `_msearch`,
  non-time ranges, the complete Query DSL, scoring, analyzed-text semantics,
  and ES aggregation-response compatibility are out of scope.

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
   router merges — where a merge exists. Only `/select/logsql/query` (sort by
   `_time` descending, apply `limit`), `stats_query_range`, `field_names`,
   `field_values`, `stream_field_names`, `stream_field_values`, `/_search`,
   `/_count`, `facets` and `/select/sql` all merge. `tail` and `/select/vector`
   answer 501 on a router rather than pretending. The per-endpoint status is
   the table in [`lld/cluster.md`](lld/cluster.md), which is checked against
   the mux by a test; the cluster surface is
   experimental, not production-safe.

### Ops loops

- `-retention` (hourly): drop groups whose whole time span is older than the
  age; per-stream retention drops by `_stream` label set. Files unlink only
  after leaving the index.
- `-recompact-after` (hourly): re-encode old groups' dictionaries with flate
  (or also drop postings with `-recompact-drop-postings`). A replaced mapping
  is retired and unmapped when its last reader releases it -- reference
  counting, not a five-minute timer, which is what this line described until
  `recompact.go` replaced it.
- `-compact-min-groups` (0, off): merge runs of small adjacent groups into
  fewer larger ones. The other axis from recompaction -- group COUNT rather
  than group size -- because every query walks the group list before it reads
  a column. `-compact-after` keeps a pass off the range ingest is still
  appending to, and `-compact-max-outputs` / `-compact-max-input-bytes` /
  `-compact-max-group-bytes` bound its I/O, per tenant.
- `/admin/backup`: a self-describing tar — `BACKUP-MANIFEST` first, the group
  files, `BACKUP-COMPLETE` last — taken from a leased snapshot so a group
  retention removes mid-stream is still in it. Admin-only, one at a time per
  tenant, and the tenant is flushed before the snapshot -- with a ten-second
  bound, and skipped entirely when another backup's pre-flush is already
  parked on a stalled writer. Both exceptions exist so a stalled writer cannot
  turn "take a backup" into "wait forever"; the consequence is an archive that
  stops at the last durable group rather than the last acknowledged row.
  `storage.Restore`, behind the `simdlogs restore` command, unpacks it,
  validating each group against the manifest's size, checksum and a full parse,
  with entry names flattened so an archive cannot escape the directory. It
  stages into a sibling and moves the result into place with one rename while
  holding a lock on the destination and arranging for the lock file the rename
  installs to be one it already holds, so the destination is the whole store or
  holds no groups at all -- a failed restore into a path that did not exist
  leaves an empty directory carrying a lock nobody holds, which the next
  restore accepts. A server that opens the directory in the one gap that
  exists --
  between the rename that takes the old store away and the one that puts the
  new store in place -- has to create it,
  which makes the second rename fail with `EEXIST` and the restore abort
  without touching that server's store. That is one of two orderings: Go's
  `os.Rename` `Lstat`s first and returns `EEXIST` for an existing directory,
  and a directory created between that `Lstat` and the raw `rename(2)` is
  empty, so the rename replaces it -- safe too, because the server then flocks
  the staging lock this call still holds and gets `ErrLocked`.
  `storage.RestoreTar` is the older unstaged path and leaves a partial
  destination on failure. Immutability is why a group's BYTES are stable,
  not why the archive is complete: the previous version copied paths out and
  silently skipped any that had gone. See `docs/lld/storage.md`.

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
