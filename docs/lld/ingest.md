# LLD: ingest

Source: `internal/ingest/` (writer, jsonline, logfmt, options, time, the
protocol parsers in `internal/api/protocols.go` and `syslog_listen.go`).

## Goals

Turn wire bytes into sealed, immutable groups with no lost records on the
batch boundary, no fatal error on malformed input, and as much parsing
parallelism as the box has cores.

## Protocols

| Protocol | Handler | Notes |
|---|---|---|
| NDJSON | `/insert/jsonline` | simdjson parse; parallel ≥ 1 MiB |
| logfmt | `/insert/logfmt` | key=value lines |
| Elasticsearch bulk | `/_bulk`, `/insert/elasticsearch/_bulk` | action lines stripped in place; per-doc `{"create":{"status":201}}` items |
| Loki push | `/loki/api/v1/push`, `/insert/loki/api/v1/push` | |
| Datadog | `/api/v2/logs`, `/v1/input`, `/insert/datadog/api/v2/logs` | `validate` endpoint answers 200 |
| OTLP/HTTP logs | `/v1/logs`, `/insert/opentelemetry/v1/logs` | JSON |
| journald export | `/insert/journald` | systemd-journal-upload |
| syslog | `/insert/syslog`; `-syslog` listens UDP+TCP | RFC3164/5424 lines |

The `/insert/<vendor>/...` spellings exist so an agent configured against
VictoriaLogs (which serves every third-party protocol under `/insert/`) does
not 404; both spellings of each path are registered.

Every insert handler follows the same shape (`ingestBody`): read the body,
parse through the tenant's writer, `Flush()` (fsync + atomic rename of any
completed groups), reply with the protocol's expected status and body, and
count ingested/skipped rows and bytes for `/metrics`.

## Per-request field mappings

`internal/ingest/options.go`. A shipper sends these as query args; ignoring
them would land the agent's message in a field nothing searches and replace
its timestamp with ingest time.

- `_time_field=ts` — take the record's time from `ts`; the field is consumed
  by the timestamp, never stored.
- `_msg_field=message` — store the value under `_msg`.
- `_stream_fields=app,env` — synthesize `_stream` for this request (the
  writer's stream fields are the deployment-wide default).
- `ignore_fields=a,b` — drop before storing.
- `extra_fields=env=prod` — add to every record.

Options are read from `r.URL.Query()` only, never `r.FormValue`: form parsing
would consume a line-protocol body and answer 204 with nothing stored.

## Time

`internal/ingest/time.go`. Accepted layouts: RFC3339Nano, RFC3339,
`2006-01-02T15:04:05.000Z07:00`, `2006-01-02 15:04:05`; plus unix units via
`api.unixToNanos` (magnitude-based seconds/ms/us/ns inference, VL's rule).
Records without a parseable `_time` get `tenant.fallbackTS()`: wall clock plus
an atomic monotonic bump, so concurrent shards never collide. Missing clocks
never reject a line.

## Writer and flush pipeline

`internal/ingest/writer.go`.

- `Writer.Add(ts, fields)` buffers column-first: a slice of timestamps plus
  one `[]string` per column. Unknown fields create a column, backfilling prior
  rows with `""`; a row missing a known field gets `""` — schema-free, like
  the reference.
- Flush triggers, whichever comes first: `FlushRows = 128K` (`storage.MaxRows`),
  `FlushBytes = 64 MiB`, `FlushEvery = 2 s`.
- `flushLocked` hands the buffers to a `flushJob` on a channel and swaps in
  fresh ones; the parser never blocks on marshal. `flushWorkers =
  min(4, NumCPU)` workers each: build dictionaries (`storage.BuildDict`),
  marshal the group, `store.AppendGroup` (write temp, fsync, rename, mmap).
  A flush error is recorded atomically and reported by the next `Flush()`.
- `Flush()` enqueues the current buffers and waits for every in-flight group —
  the batch boundary where durability is promised. `Close()` flushes, closes
  the job channel, joins the workers.
- With `-stream-fields`, `Add` synthesizes `_stream` as the canonical label
  set `{k="v",...}` with keys sorted, so the same label set always yields the
  same dict id and the same `StreamID` (`internal/query/stream.go`).
  `SetCompact(true)` makes flushed groups use the flate dict codec.
- **The request override is per request, not per row.** A request naming its
  own `_stream_fields` goes through `AddStreamOverridden`, which skips the
  deployment synthesis for every row of that request. The flag is a parameter
  rather than a sniff of `fields["_stream"]`, because sniffing made the
  decision per row: a row whose override label came out empty fell back to the
  deployment fields, so one request could produce a column mixing
  `{host="h1"}` and `{service="api"}`.
- **A payload field named `_stream` is not authoritative.** When the
  deployment owns labelling and the request did not override it, any `_stream`
  in the record is dropped and the synthesized label replaces it. `_stream` is
  what stream-scoped retention groups on (`internal/api/retention.go`), so
  honouring a client-supplied value would let a client choose its own
  retention bucket.

## Parallel path

`internal/ingest/jsonline.go`. `IngestJSONLinesParallelCfg(store, data,
fallback, cfg ParallelConfig, opts) (ingested, skipped int, err error)`:

- Bodies below `MinParallelBytes = 1 MiB` (or a machine with < 2 useful
  shards) stay serial on the tenant's persistent writer — no goroutine or
  per-shard writer setup cost.
- Otherwise the body is cut into `cfg.shards()` chunks, each ending on a
  newline, and each chunk parses through its own `NewWriterWorkers(store, 2)`
  writer over the shared store (`AppendGroup` is concurrency-safe). Total
  goroutines stay near the core count rather than oversubscribing.
- **`ParallelConfig` carries the deployment writer settings** — `Compact` and
  `StreamFields` — because the shard writers are built here rather than handed
  in. A setting not repeated onto them changes the stored schema for large
  bodies only: `Compact` was copied and `StreamFields` was not, so the same
  records grew a `_stream` column under the small-body path and none here.
- **`Shards` overrides the derived count**, which is `NumCPU/3` and so below
  the 2-shard minimum on anything with fewer than six cores. Tests set it so
  the concurrent branch actually runs; without it they exercise the serial
  fallback on a CI runner and pass against broken concurrent code.
- **The error return is the durability contract.** Every shard writer's
  `Close` is checked — it flushes, drains the pool and reports the first
  `AppendGroup` failure, so discarding it turned a store that could not write
  a single group into a 200 with a row count. Failures aggregate into a
  `*ParallelWriteError{Shards, Failed, Err}`.
- **Counts follow durability.** `ingested` counts only shards whose rows
  landed; `skipped` counts every malformed line, since being malformed is a
  parse fact independent of whether the group was written. On a partial
  failure the handler returns 500 *and* reports the durable count, because a
  shipper retrying a bare 500 would otherwise duplicate rows already stored —
  deduplicating that retry needs write IDs (plan task 8.2).
- Parsing is simdjson over each line, field values taken as strings
  (`StringNoCopy` where unescaped); a number keeps its source text — a log
  store round-trips what it is given. Malformed lines are counted and
  skipped; one bad line never aborts the batch.

`ingest.MinParallelBytes` also gates `/_bulk` (after the in-place action-line
strip), so a large ES bulk gets the same sharded path.

## Failure behavior

- Malformed input: skipped and counted (`skipped` in the response body,
  `vl_rows_dropped_total` in `/metrics`). Ingest is never fatal on bad lines.
- Write failure (fsync/rename/disk): recorded in `flushErr`, surfaced as an
  HTTP 500 on the next flush boundary; the response reports it rather than
  pretending success.
- Crash between temp write and rename: the `.tmp` file is ignored by
  `OpenStore`; the group is not indexed.
- Shutdown: SIGINT/SIGTERM → HTTP drain (15 s) → `writer.Close()` (flush)
  → `store.Close()` (unmap). No buffered rows are lost, no mmap leaks.
