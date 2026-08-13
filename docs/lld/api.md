# LLD: API

Source: `internal/api/` (server, tenant, cluster, protocols, es, metrics,
retention, tiering, logrules, alerts, syslog_listen, ui).

## Tenancy

`tenant.go`. `AccountID`/`ProjectID` headers select a per-tenant
`storage.Store` + `ingest.Writer` pair under `dir/tenant-<acc>-<proj>`
(created on first use). IDs are sanitized to digits only, so a header cannot
direct the storage path outside the data directory; absent or non-numeric
headers select the default `0:0` tenant. The middleware chain resolves the
tenant into the request context and counts the request:

```
recoverPanic(routeWrites(withTenant(mux)))
```

`recoverPanic` is outermost: one bad request can never take the server down
(panic → 500, connection served). `routeWrites` intercepts ingest in router
mode. `withTenant` also wraps the response writer to record the status code
for the error counter (`vl_http_errors_total`) and forwards `Flush` — a
wrapper that swallowed it would break live tailing.

## Routes

### Ingest

| Path | Protocol |
|---|---|
| `/insert/jsonline` | NDJSON |
| `/insert/logfmt` | logfmt |
| `/_bulk`, `/insert/elasticsearch/_bulk` | ES bulk |
| `/loki/api/v1/push`, `/insert/loki/api/v1/push` | Loki push |
| `/api/v2/logs`, `/v1/input`, `/insert/datadog/api/v2/logs` | Datadog |
| `/v1/logs`, `/insert/opentelemetry/v1/logs` | OTLP/HTTP logs JSON |
| `/insert/journald` | journald export |
| `/insert/syslog` | syslog over HTTP (native transport: `-syslog` UDP+TCP) |
| `/insert/ready`, `/insert/datadog/api/v1/validate` | 200 probes |

`/insert/<vendor>/...` prefixed spellings exist so agents configured against
VictoriaLogs' layout work unchanged.

### Query

| Path | Shape |
|---|---|
| `/select/logsql/query` | NDJSON rows |
| `/select/logsql/tail` | streaming NDJSON (live) |
| `/select/logsql/hits` | `{"hits":[{"fields":{...},"timestamps":[...],"values":[...],"total":N}]}` |
| `/select/logsql/stats_query` | Prometheus vector envelope |
| `/select/logsql/stats_query_range` | Prometheus matrix envelope |
| `/select/logsql/field_names`, `field_values`, `facets` | introspection |
| `/select/logsql/streams`, `stream_ids`, `stream_field_names`, `stream_field_values` | introspection |
| `/select/sql` | SQL subset → LogsQL engine |
| `/select/vector` | cosine k-NN, `{"field","vector","k"}` body |

Six introspection endpoints share one envelope —
`{"values":[{"value":..,"hits":..}]}` (`streams`/`stream_ids` use
`{"streams":...}`/`{"stream_ids":...}` keys) — so a client decodes them all
with one type; that is why they must not each invent a key.

### Ops

`/admin/backup` (tar), `/metrics` (Prometheus text), `/alerts`, `/health`,
`/-/healthy`, `/-/ready`, `/flags` (non-default flags only, VL's
`-name="value"` line form), `/vmui`, `/select/vmui`, and `/` (the UI,
catch-all).

### ES

`/_search`, `/_count` — a log-relevant Query DSL subset: `bool`
(`must`/`filter`), `term`, `terms`, timestamp `range`, `exists`. Range on a
time field feeds the group skip directly. `_bulk` on the ingest side; the
response gives per-document `{"create":{"status":201}}` items (the shape ES
clients parse to decide retry).

Not implemented (documented, not accidental): `_msearch`, the complete Query
DSL, ES aggregation-response compatibility.

## Query parameters

`parseRequest` (server.go):

- `query` — required on every select endpoint; a request without one is a 400
  (`missing `query` arg`). Defaulting to match-all answered a client's bug
  with the entire store.
- `start`/`end` — unix seconds/ms/us/ns inferred from magnitude (VL's rule:
  seconds below 1e11, ms below 1e14, us below 1e17, else ns), or RFC3339
  (nano, second, minute, day, month, year granularities). `end` defaults to
  `1<<62` (open future).
- `limit` — the most RECENT n, newest first (`LastN`); deliberately not the
  `| limit N` pipe (first n). The reference draws the same distinction;
  conflating them returned the oldest rows to a client asking for the tail.
- `step` (hits/range), `field`/`fields` (hits split, one field), `fields_limit`
  (busiest N series, the rest folded into one unlabelled "other"), `time`
  (stats instant window end), `by` (stats group-by extension), `start_offset`
  (tail replay window, default 5 s), `keep_const_fields` (facets; measured
  against the reference: this implementation has no stored constant fields,
  so the argument is accepted and changes nothing).

`Now` is stamped from the request for relative `_time:<dur>` filters.

## Result shaping

- Rows serialize as NDJSON by hand (`appendRowJSON`) — no
  `map[string]any`, no reflection: the engine produces rows in ~1.5 ms and
  the reflective encoder was doubling the wire time. A full record carries
  `_time` (RFC3339Nano), all columns, `_stream`, and `_stream_id` (a
  hashed-once constant for the empty stream). Stats rows and projections
  without `_time` carry none — VictoriaLogs treats `_time` as an ordinary
  field subject to projection, and a zero timestamp printed 1970-01-01 on
  every stats row before.
- `hits` returns dense, ascending, gap-free timestamp/value arrays per series
  (a client indexes the two arrays together), buckets aligned to step
  multiples — the alignment the reference uses.
- ES `_search` returns the ES envelope with `hits.total.relation: "eq"`.

## Bounds and errors

- `-search.maxRows`: a bare (no-pipe) select over the cap errors with an
  explicit message (never silently truncates); `MaxRows` keeps the parallel
  scan, `Limit` would force the serial path. Pipes' input is never bounded.
- `maxHitsBuckets = 100K` caps a hits response (a one-second step over a year
  cannot be asked to allocate 31M buckets).
- Malformed queries, SQL outside the subset, and invalid stats are 400s.
  Panics anywhere become 500s without taking the server down.
- `ReadHeaderTimeout = 10 s` (slowloris). Graceful shutdown: drain (15 s),
  flush writers, unmap stores.

## Flags and environment

`cmd/simdlogs/main.go`:

| Flag | Meaning |
|---|---|
| `-addr` | listen address (default `:9428`, VL's port) |
| `-storage` | data directory (default `./simdlogs-data`) |
| `-retention` | drop data older than this (hourly); 0 disables |
| `-recompact-after` | re-encode old groups with flate dicts (hourly); 0 disables |
| `-recompact-drop-postings` | also drop the per-column inverted index when recompacting |
| `-compact` | compact mode: flate dicts, ~15% smaller groups, 2–10x slower value reads — cold archival only |
| `-stream-fields` | comma-separated fields identifying a log stream |
| `-syslog` | also listen for syslog on UDP/TCP |
| `-select-backends` | peer node URLs; sets select-router mode (vmselect role) |
| `-replicas` | replication factor for the backends |
| `-search.maxRows` | cap on a bare select's rows; 0 = unlimited |

Environment: `SIMDLOGS_STREAM_FIELDS` (stream-field default before the
default tenant opens, so a deployment can synthesize `_stream` without a
code change).

## Metrics

`metrics.go`. Own names (`simdlogs_tenants`, `simdlogs_groups`,
`simdlogs_rows`, `simdlogs_insert_requests_total`,
`simdlogs_query_requests_total`, `simdlogs_uptime_seconds`) plus the same
numbers under the reference's names (`vl_rows_ingested_total`,
`vl_bytes_ingested_total`, `vl_rows_dropped_total`, `vl_http_requests_total`,
`vl_http_errors_total`, `vl_live_tailing_requests`, `vl_partitions`,
`vl_storage_rows`, `vl_data_size_bytes`, `vl_compressed_data_size_bytes`,
`vl_free_disk_space_bytes`) so a dashboard written for VictoriaLogs graphs
this server unchanged. Only metrics whose meaning is real are emitted: a
metric named after a structure this server does not have (an index database,
background merges) would be a fabricated zero, worse than a panel with no
data. The store footprint is cached 15 s; free disk comes from the platform
layer. Metrics-from-logs rules (`AddMetricRule`) append their series.

## Alerts and rules

`logrules.go` / `alerts.go`: LogsQL evaluated on a timer (`AddMetricRule`,
`AddAlertRule` — count vs threshold with an operator). Rule state is served
at `/alerts`.

## Live tail

`tail` subscribes at the store's high-water group id, replays the recent
window (`start_offset`, default 5 s), then polls every 500 ms for groups
after the cursor and runs the LogsQL filter over only the new groups
(`readerStore` adapts them to the query surface). Headers flush before the
first payload; `X-Accel-Buffering: no` keeps proxies from buffering. The
connection lives until the client disconnects.
