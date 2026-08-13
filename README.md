# simdlogs

`simdlogs` is a disk-backed log database in Go. It implements the VictoriaLogs
ingest and LogsQL surfaces used by the repository's compatibility suite, adds
an Elasticsearch-compatible search subset, and executes filters over columnar
row groups with [simd.go](https://github.com/sebishogun/simd) kernels.

It is built for selective queries and low-latency aggregations. That choice has
a measured cost: its inverted indexes use more disk than VictoriaLogs. The
[scale curve](docs/scale-curve.md) publishes both sides.

## Run it

Go 1.26.5 or later is required. The server uses published `simd v1.20.0` and
`simdjson v0.6.0`; neither dependency requires cgo.

```sh
go run ./cmd/simdlogs -storage ./simdlogs-data -addr :9428
```

The default port matches VictoriaLogs. On startup the server prints the SIMD
tier selected for the current CPU.

Ingest newline-delimited JSON:

```sh
curl -sS -X POST http://localhost:9428/insert/jsonline \
  --data-binary $'{"_time":"2026-08-13T00:00:00Z","service":"api","level":"error","_msg":"timeout"}\n'
```

Query it through LogsQL:

```sh
curl -G http://localhost:9428/select/logsql/query \
  --data-urlencode 'query=service:=api AND level:=error | fields _time, _msg'
```

Each successful ingest request is flushed before its response is returned.
Group files are written to a temporary file, synced, and atomically renamed;
readers mmap immutable groups and reopen them after restart.

## Query and ingest surfaces

### Ingest

| Workload | Endpoint or transport |
|---|---|
| NDJSON | `/insert/jsonline` |
| logfmt | `/insert/logfmt` |
| Elasticsearch bulk NDJSON | `/_bulk` |
| Loki push JSON | `/loki/api/v1/push` |
| Datadog logs JSON | `/api/v2/logs`, `/v1/input` |
| OpenTelemetry logs JSON | `/v1/logs` |
| journald export | `/insert/journald` |
| syslog | `/insert/syslog`, or UDP and TCP with `-syslog` |

Large NDJSON bodies split at line boundaries and parse across workers. Smaller
bodies reuse the tenant's persistent writer. Records without `_time` receive a
monotonic wall-clock timestamp.

### LogsQL

The compatibility inventory in [`docs/vl-parity.md`](docs/vl-parity.md) is
complete through tiers 0-5. It covers boolean and field-to-field filters,
numeric, string, sequence, IPv4 and time predicates; stats and conditional
aggregates; row transforms; subqueries and joins; stream context and stream-id
queries; field/facet introspection; rate series; and block introspection.

The main HTTP endpoints are:

- `/select/logsql/query`, `/select/logsql/tail`, `/select/logsql/hits`,
  `/select/logsql/stats_query`, and `/select/logsql/stats_query_range`;
- `/select/logsql/field_names`, `/select/logsql/field_values`, and
  `/select/logsql/facets`;
- `/select/logsql/streams`, `/select/logsql/stream_ids`,
  `/select/logsql/stream_field_names`, and
  `/select/logsql/stream_field_values`.

Queries return NDJSON. A bare select materializes whole records; pipe chains
materialize only fields needed by their predicates and transforms.

### Additional surfaces

- `/_search` and `/_count` support a log-oriented Elasticsearch subset:
  `bool` with `must`/`filter`, `term`, and timestamp `range`. An `exists`
  clause is accepted on the wire but currently changes no answer (decoded,
  not mapped to a predicate — see `docs/wrong.md` entry 37).
- `/select/sql` translates a SQL `SELECT` subset into the same LogsQL engine.
- `/select/vector` performs cosine k-nearest-neighbor search over an embedding
  field supplied by the ingested logs.
- `/metrics`, `/alerts`, `/admin/backup`, and `/vmui` provide operational
  endpoints. The backup endpoint streams immutable group files as a tar; the
  Go storage API restores that tar into a new store.

This is not full Elasticsearch compatibility. In particular, `_msearch`, the
complete Query DSL, and Elasticsearch aggregation response compatibility are
outside the implemented surface.

## Storage and execution

Rows are grouped into immutable, time-ordered files. Each group carries:

- delta/varint timestamps with block min/max checkpoints;
- sorted per-column dictionaries and bit-packed row ids;
- cardinality-sized bloom filters and exact dictionary checks;
- frame-of-reference bit-packed postings for value-to-row lookup;
- a footer used to reject groups and blocks before value decode.

The query path narrows work in layers: time range, footer bloom and dictionary,
posting rows or vector predicate scan, bitset algebra, then selected-field
materialization. Groups execute in parallel only after footer pruning; a rare
needle that survives in one group does not pay for a worker pool.

The SIMD dependency supplies the bit unpack, varint decode, hash, bitset,
compare, compression, formatting, and data-movement primitives. Every operation
also has a portable path through `simd`, so missing architecture kernels affect
speed rather than API availability.

## Measured against VictoriaLogs

The scale harness runs both engines through HTTP on the same deterministic
bytes and disk-backed stores. Query order is shuffled; each latency is the
minimum of 15 samples after three warmups. These figures were measured on
amd64/AVX-512. No wall-clock claim is made for another architecture.

The committed unique-hex scale curve is deliberately hostile to compression:
every trace value is distinct. **It is a historical baseline, not the current
footprint**: the numbers were measured on 2026-08-10, before the FOR postings
rewrite (`a5f9098`) and the hex nibble-pack codec (`d000ae3`) shipped, and no
current-facing claim should be drawn from its disk column.

| rows | rare needle | selective window | aggregation | ingest | disk vs VL |
|---:|---:|---:|---:|---:|---:|
| 1M | 12.3x | **0.8x** | 5.1x | 8.2x | unavailable; VL rounds below 0.005 GB |
| 10M | 19.7x | 1.1x | 8.4x | 2.56x | 19.6x |
| 100M | 25.0x | 2.0x | 19.2x | 1.11x | 20.9x |
| 1B | 12.9x | 1.9x | 3.4x | 1.56x | 19.4x |

Values above 1 mean `simdlogs` is faster. The 1M selective row is a loss, and
it remains in the table. At one billion rows, ingest took 8m42s here and 13m34s
in VictoriaLogs; the rare needle took 1.68 ms versus 21.7 ms; the selective
query 350 ms versus 664 ms; and the aggregation 5.6 ms versus 19.3 ms. Disk was
50.1 GB versus 2.58 GB.

The realistic 15-field Zipfian corpus is less extreme on disk, and the 2.62x
figure is likewise a **historical baseline, not the current footprint**: it
was measured at commit `3f5a063` (2026-08-12), after the v8 count-table and
frame-of-reference postings changes and before the hex codec (`d000ae3`)
shipped — which measured -9.8% disk on the realistic corpus in its commit.
`docs/wrong.md`'s hex nibble-pack entry estimates ~2.38x for the realistic
ratio; that is an estimate, not a measurement. The inverted index is also what
powers the large rare-value and group-by wins; removing singleton postings cut
disk and made the needle 90x slower, so that change was reverted.

`-compact` uses flate for dictionary blocks. On the measured realistic corpus
it made groups about 15% smaller and value-reading queries 2-10x slower. It is
an opt-in cold-archive trade, not the default. The current default remains the
SIMD-backed LZ4 path.

Read [`docs/scale-curve.md`](docs/scale-curve.md) for the full curve and
reproduction commands. [`docs/wrong.md`](docs/wrong.md) records the earlier
losses, retracted claims, and rejected storage variants rather than presenting
only the final wins.

## Deployment behavior

- `AccountID` and `ProjectID` headers select isolated tenant directories;
  absent or invalid identifiers use tenant `0:0`.
- `-stream-fields` synthesizes `_stream` from selected fields and enables the
  stream-id and per-stream retention model.
- `-retention` removes groups whose complete time span is older than the
  configured age. Removal drops the group from the index before unlinking it.
- `-select-backends` turns a node into a query router. Backends are grouped by
  `-replicas`; writes replicate within one selected shard and reads use one
  available replica per shard before merging results.
- SIGINT and SIGTERM stop HTTP intake, drain in-flight requests, flush writers,
  and unmap stores.

The cluster layer is application-level sharding and replication, not a
consensus system. There is no automatic membership, leader election, or
cross-node transaction protocol.

The router surface is **experimental, not production-safe**: the `streams`,
`stream_ids`, plain `stats_query`, and `hits` merges decode stale envelopes
and answer empty/bogus results, and `facets`, `tail`, `/alerts` and other
endpoints are router-local, not federated. The enumeration here is
representative, not complete — the full per-endpoint status is in
[`docs/lld/cluster.md`](docs/lld/cluster.md); do not read an endpoint's
absence from this list as federation.

## Verification

```sh
go test ./...
go test -race ./...
go vet ./...
```

The suite covers storage round trips and backward-compatible posting formats,
crash-safe append, retention, backup/restore, tenant isolation, ingest
protocols, LogsQL parsing and execution, SQL/vector surfaces, Elasticsearch
search, live tail, replication/federation, and serial-versus-parallel query
agreement.

The VictoriaLogs comparisons are reports rather than unit-test gates:

```sh
# realistic 1M-row query mix
SIMDLOGS_REAL=1 go test -run TestRealistic -v -timeout 90m ./internal/bench/

# disk-backed scale point; stage internal/bench/victoria-logs first
SIMDLOGS_SCALEVL=1 SIMDLOGS_SCALEVL_N=100000000 \
  go test -run TestScaleVsVL -v -timeout 60m ./internal/bench/
```

## Status

The LogsQL parity plan is implemented and tested, but this repository has no
tagged release yet. Storage format compatibility, operational upgrade policy,
and the supported public API are therefore not under a stable-version promise.
Pin a commit if you deploy it today. This README and the `docs/` tree describe
the current source only; the production-hardening direction (durability,
cluster contract, release) is planned, not shipped — see
[`docs/roadmap.md`](docs/roadmap.md).

Current open work is measured rather than implied: reduce the disk footprint
without giving back the indexed-query wins, widen the Elasticsearch surface,
and turn the current cluster primitives into a documented production protocol.

## Documentation

- [`docs/architecture.md`](docs/architecture.md): product boundary, components,
  data flow, read order.
- [`docs/lld/ingest.md`](docs/lld/ingest.md), [`docs/lld/storage.md`](docs/lld/storage.md),
  [`docs/lld/query.md`](docs/lld/query.md), [`docs/lld/api.md`](docs/lld/api.md),
  [`docs/lld/cluster.md`](docs/lld/cluster.md): current low-level design, one
  per layer.
- [`docs/roadmap.md`](docs/roadmap.md): planned hardening stages with
  measurable exits; nothing there ships yet.
- [`docs/verification.md`](docs/verification.md): gates, reports, benchmark
  discipline, crash recovery, cross-arch.
- [`docs/vl-parity.md`](docs/vl-parity.md): LogsQL parity inventory (status:
  complete, tiers 0-5).
- [`docs/scale-curve.md`](docs/scale-curve.md): disk-backed scale results.
- [`docs/benchmark-contract.md`](docs/benchmark-contract.md): benchmark rules
  published before implementation was measured.
- [`docs/design.md`](docs/design.md): original milestone design and hypotheses
  (historical).
- [`docs/plans/2026-08-07-simdlogs-full-build.md`](docs/plans/2026-08-07-simdlogs-full-build.md):
  the completed build plan that produced the current code (historical).
- [`docs/plans/2026-08-13-simdlogs-production-design.md`](docs/plans/2026-08-13-simdlogs-production-design.md)
  and [`docs/plans/2026-08-13-simdlogs-production.md`](docs/plans/2026-08-13-simdlogs-production.md):
  the approved production-hardening design and plan.
- [`docs/wrong.md`](docs/wrong.md): measurements that changed or rejected work.

The wider SIMD project inventory lives in the
[`simd` README](https://github.com/sebishogun/simd#built-on-this).
