# simdlogs

`simdlogs` is a disk-backed log database in Go. It implements the VictoriaLogs
ingest and LogsQL surfaces used by the repository's compatibility suite, adds
an Elasticsearch-compatible search subset, and executes filters over columnar
row groups with [simd.go](https://github.com/sebishogun/simd) kernels.

It is built for selective queries and low-latency aggregations. That choice has
a measured cost: its inverted indexes use more disk than VictoriaLogs — 110.08
bytes per row on the realistic corpus, 2.05x VictoriaLogs, or 1.55x with
cold-tier postings dropped. On a corpus built so every value is distinct the
ratio reaches ~19x; that is the design's worst case, published with the [scale
curve](docs/scale-curve.md), not a footprint to plan against.

## Run it

Go 1.26.5 or later is required. The server uses published `simd v1.20.0` and
`simdjson v0.6.0`; neither dependency requires cgo.

```sh
go run ./cmd/simdlogs -storage ./simdlogs-data -addr 127.0.0.1:9428
```

The default port matches VictoriaLogs. On startup the server prints the SIMD
tier selected for the current CPU.

**Binding a public address needs a decision about transport.** The server
refuses to serve plaintext on anything but loopback, because log data is
tenant data and a server that binds every interface in the clear should take a
deliberate flag rather than be what happens when the operator forgets. Pick
one:

```sh
# TLS (add -tls.clientCAFile for mTLS)
go run ./cmd/simdlogs -storage ./simdlogs-data -addr :9428 \
  -tls.certFile server.pem -tls.keyFile server-key.pem

# behind a terminating proxy: bind loopback, let the proxy hold the certificate
go run ./cmd/simdlogs -storage ./simdlogs-data -addr 127.0.0.1:9428

# plaintext on a public interface, accepted deliberately
go run ./cmd/simdlogs -storage ./simdlogs-data -addr :9428 -insecure-http
```

The same rule applies to `-syslog`: that listener is plaintext by construction
and unauthenticated, so a public syslog address needs `-insecure-http` too.

**The server is unauthenticated without `-auth.config`,** and says so at
startup. See [docs/lld/api.md](docs/lld/api.md) for the token file format,
roles, and the tenant-authorization rules.

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
| OpenTelemetry logs, JSON and protobuf | `/v1/logs` |
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
  endpoints. The backup endpoint streams immutable group files as a
  self-describing tar; `simdlogs restore` unpacks one into a new store:

  ```
  simdlogs restore -src backup.tar -dry-run
  simdlogs restore -src backup.tar -dst ./simdlogs-data/tenant-0-0
  ```

  The restore stages into a sibling directory and moves it into place with one
  rename, holding a lock on the destination until the syscall that takes that
  directory away, and arranging for the lock file the rename installs to be one
  it already holds -- so a server starting on that directory either finds it
  locked, or wins the one race there is and makes the restore abort without
  touching that server's store. A second RESTORE cannot start while the lock is
  held; in the gap between the two renames one can, and the outcome is one of
  three: this call aborts `ENOENT`, or aborts `EEXIST`, or -- with both parked
  in their own gaps -- returns success over the other's destination. Measured
  at zero occurrences in 20,000 barrier rounds and in 11.6 million overlapping
  attempts; every run that did observe it had something outside a restore
  widening the window. Zero observations bound how often it happens, not
  whether it can, so: do not run two restores at one `-dst`.

  `-dry-run` checks every group against the
  archive's own manifest, needs no destination at all, and writes nothing;
  `-tenant` refuses an archive taken from a different tenant. The destination
  and its parents are created if absent.

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

### Per-operation gate

Every operation both engines expose runs through HTTP against the same 200k-row
deterministic corpus. Query order is shuffled; each latency is the minimum of
the samples for that operation, taken with the load average under 1. The gate
was run twice in succession and a figure is reported only where both runs
agree. These figures were measured on amd64/AVX-512. No wall-clock claim is
made for another architecture.

| operation | simdlogs | VictoriaLogs | ratio |
|---|---:|---:|---:|
| stats/groupby | 628µs | 14.2ms | 22.6x |
| field_values | 598µs | 11.8ms | 19.8x |
| stats_query_range | 315µs | 2.5ms | 7.8x |
| stats/uniq | 601µs | 3.9ms | 6.5x |
| facets | 13.0ms | 56.3ms | 4.3x |
| insert/jsonline | 153ms | 538ms | 3.5x |
| insert/elasticsearch | 172ms | 538ms | 3.1x |
| stats_query | 215µs | 592µs | 2.8x |
| query/windowed | 10.1ms | 25.9ms | 2.6x |
| stats/count | 262µs | 601µs | 2.3x |
| query/and | 40ms | 74ms | 1.9x |
| query/limit | 992µs | 1.8ms | 1.9x |
| stats/topk | 7.1ms | 12.7ms | 1.8x |
| query/common | 300ms | 508ms | 1.7x |
| query/or | 599ms | 1.01s | 1.7x |
| query/range | 233ms | 392ms | 1.7x |
| query/substring | 511ms | 822ms | 1.6x |
| field_names | 433µs | 656µs | 1.5x |
| stream_field_names | 473µs | 613µs | 1.3x |
| hits | 2.3ms | 2.8ms | 1.2x |

Values above 1 mean `simdlogs` is faster; all 20 rows are above 1 in both runs.
[`internal/bench/perops_test.go`](internal/bench/perops_test.go) fails the build
if any row falls below 1. On the 3M-row harness ingest ran at 3.17M rec/s here
and 0.49M rec/s in VictoriaLogs, and the selective window took 7.0 ms versus
11.1 ms; the window figure was 20.6 ms before the bounded-decode change
(`5419c80`). Disk on the realistic corpus is 110.08 bytes/row, down from 127.4.
VictoriaLogs still writes fewer bytes per row there; the paragraphs below give
the magnitude.

### Parity gate

Six checks run against the reference binary on identical bytes: a response
differential, a route inventory, a body-shape comparison, a per-argument
sensitivity check, an ingest-format check, and a persistence round-trip. Status
codes alone do not distinguish a correct response from a `200` carrying
different content, so the last four compare bodies.

| check | result |
|---|---|
| LogsQL responses, byte for byte, against the reference binary | 40 of 40 identical |
| routes the reference answers and this does not | 0 of 24 (20 shared, 4 here only) |
| response bodies — field names, nesting, types | match; pinned by shape tests |
| documented query arguments that change the answer | every one, on both engines |
| ingest formats accepting the reference's payloads | 8 of 8 |
| write, reopen, query | no row lost |

Findings from these checks, recorded in [`docs/wrong.md`](docs/wrong.md)
entries 32–37:

- A status-code probe reported 0 route gaps while 27 behaviors differed.
- The argument-sensitivity check found 16 arguments accepted without effect,
  among them `extra_label`, `match[]`, `limit` and `keep_const_fields`.
- `field_names` counted `_time` twice; a row group carries two columns of that
  name.
- A bounded query decoded the entire timestamp column; the fix bounds the
  decode span to the matched rows (`1a85d8a`, `5419c80`).
- The point-read threshold was expressed as a fraction of the group when the
  cost is absolute.
- The Elasticsearch `exists` clause is accepted and changes no answer; it is
  decoded but not mapped to a predicate.

| category | implemented |
|---|---|
| ingest | NDJSON, logfmt, Elasticsearch `_bulk`, Loki push, Datadog logs, OpenTelemetry logs in JSON and protobuf, journald export, syslog over HTTP, UDP and TCP |
| pipes | stats, sort, limit/head, fields, uniq, top, tail, offset, rename, delete, filter, unpack_json, unpack_logfmt, unpack_syslog, unpack_words, extract, extract_regexp, format, rank, replace, replace_regexp, copy, len, drop_empty_fields, collapse_nums, math/eval, decolorize, pack_json, pack_logfmt, sample, first, last, unroll, json_array_len, join, union, stream_context, blocks_count, block_stats |
| stats | count, sum, avg, min, max, uniq, count_uniq, count_uniq_hash, quantile/median, values, uniq_values, sum_len, count_empty, row_any, histogram, row_min, row_max, rate, rate_sum |
| filters | exact, phrase, prefix, substring, regex, numeric comparison, `in()`, `range()`, `len_range()`, `string_range()`, `i()`, `seq`, `ipv4_range`, `{eq,ne,lt,le,gt,ge}_field`, `_time:` relative, absolute, `day_range` and `week_range`, `_stream_id:` |
| select endpoints | `query`, `hits`, `facets`, `field_names`, `field_values`, `stats_query`, `stats_query_range`, `streams`, `stream_ids`, `stream_field_names`, `stream_field_values`, `tail`, `/_search` |
| not in VictoriaLogs | SQL over logs (`/select/sql`), HLL cardinality, alerting rules (`/alerts`), vector search (`/select/vector`) |

Read [`docs/vl-parity.md`](docs/vl-parity.md) for the full inventory.

### Realistic corpus, 1M rows

The 15-field Zipfian corpus, which is what log data looks like: values repeat,
so the dictionary dedupes and the postings pack. Both engines take the same
bytes over HTTP into disk-backed stores; query order is shuffled.
`SIMDLOGS_REAL=1 go test -run TestRealistic -v ./internal/bench/` reproduces it.

| | simdlogs | VictoriaLogs | ratio |
|---|---:|---:|---:|
| ingest | 2.12 s (0.47M rec/s) | 8.96 s (0.11M rec/s) | 4.2x — **withdrawn, see below** |
| footprint | 0.11 GB (110.08 bytes/row) | 0.05 GB | **2.05x of VL** |
| groupby | 21 µs | 9.84 ms | 463x |
| topN | 33 µs | 13.3 ms | 407x |
| rare needle | 29 µs | 1.02 ms | 35x |
| histogram | 589 µs | 3.10 ms | 5.3x |
| and | 11.9 ms | 58.7 ms | 4.9x |
| common | 98.6 ms | 171.0 ms | 1.7x |
| or | 171.7 ms | 273.5 ms | 1.6x |

**The ingest row is withdrawn.** At the commit it was measured on, the harness
stamped VictoriaLogs' ingest duration *after* a hardcoded
`time.Sleep(5 * time.Second)` that existed to let VL flush, so the 8.96 s
contains five seconds VL did not spend. VL's accept was 8.96 − 5.00 = **3.96 s
(0.25M rec/s)**, an accept ratio of **1.87x**, not 4.2x. The other half — when
the written rows become *queryable*, which is the number that five seconds was
standing in for — was never measured, so the true queryable ratio is somewhere
in [1.87x, 4.2x] and this table cannot say where.

The harness now measures **accept** and **queryable** separately, by polling
each engine's row count rather than sleeping (`internal/bench/timing_test.go`).
The ingest row will be replaced when it has been re-measured on a quiet machine;
until then the number above is history, not a claim. The query rows are
unaffected — they were timed with `timeQuery`, which never contained the sleep.
The scale-curve `ingest` column below came from `TestScaleVsVL`, which slept the
same fixed five seconds inside its timed interval: at 1M rows that dominates, at
1B rows it is noise.
| substring | 170.6 ms | 249.1 ms | 1.5x |

Measured at `50d13df` on amd64/AVX-512, two consecutive runs. The footprint
line is deterministic and identical in both. The latencies were taken with a
load average near 3, so read the small ratios as approximate and the ordering
as sound; the per-operation gate above is the quiet-machine measurement.

Disk is the one axis VictoriaLogs wins, by 2.05x. That gap is the inverted
index, which is also what produces the 463x groupby and 35x needle in the same
table. Dropping postings in the cold tier measured 1.55x of VL on the 100k
corpus, at the cost of a decode-and-scan for those groups — which is what
VictoriaLogs does for every query. Removing singleton postings entirely cut
disk and made the needle 90x slower, so that change was reverted.

### Scale curve, unique-hex worst case

A second corpus exists to stress the design rather than describe it: every
trace value distinct. That defeats the dictionary and gives the inverted index
one posting list per row, so it is where footprint is worst. It is not a
footprint to plan against — the table above is.

| rows | rare needle | selective window | aggregation | ingest | disk vs VL |
|---:|---:|---:|---:|---:|---:|
| 1M | 12.3x | **0.8x** | 5.1x | 8.2x | unavailable; VL rounds below 0.005 GB |
| 10M | 19.7x | 1.1x | 8.4x | 2.56x | 19.6x |
| 100M | 25.0x | 2.0x | 19.2x | 1.11x | 20.9x |
| 1B | 12.9x | 1.9x | 3.4x | 1.56x | 19.4x |

Values above 1 mean `simdlogs` is faster. The 1M selective row is a loss and
remains in the table. At one billion rows, ingest took 8m42s here and 13m34s in
VictoriaLogs; the rare needle 1.68 ms versus 21.7 ms; the selective query
350 ms versus 664 ms; the aggregation 5.6 ms versus 19.3 ms. Disk was 50.1 GB
versus 2.58 GB.

VictoriaLogs has no per-value index — it decodes and scans for every query — so
its footprint barely moves with cardinality while ours is almost entirely
index. Each latency is the minimum of 15 samples after three warmups, on
amd64/AVX-512; no wall-clock claim is made for another architecture. This curve
was measured on 2026-08-10 and predates the FOR postings rewrite (`a5f9098`)
and the hex nibble-pack codec (`d000ae3`), which act on exactly the structures
that dominate it. It has not been re-measured since.

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

- [`AGENTS.md`](AGENTS.md) and [`CLAUDE.md`](CLAUDE.md): the working rules —
  boundary, read order, gates, benchmark discipline. CLAUDE.md embeds
  AGENTS.md's rules verbatim so Claude Code runs are self-contained.
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
