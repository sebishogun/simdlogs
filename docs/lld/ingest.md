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
| Elasticsearch bulk | `/_bulk`, `/insert/elasticsearch/_bulk` | one item per ACTION; `update`/`delete` rejected explicitly; see below |
| Loki push | `/loki/api/v1/push`, `/insert/loki/api/v1/push` | JSON and snappy-protobuf; see below |
| Datadog | `/api/v2/logs`, `/v1/input`, `/insert/datadog/api/v2/logs` | array or single object; `validate` answers 200 |
| OTLP/HTTP logs | `/v1/logs`, `/insert/opentelemetry/v1/logs` | JSON and protobuf; see below |
| journald export | `/insert/journald` | systemd-journal-upload |
| syslog | `/insert/syslog`; `-syslog` listens UDP+TCP | RFC3164/5424; both RFC6587 framings; see below |

The `/insert/<vendor>/...` spellings exist so an agent configured against
VictoriaLogs (which serves every third-party protocol under `/insert/`) does
not 404; both spellings of each path are registered.

Every insert handler follows the same shape (`ingestBody`): read the body,
parse through the tenant's writer, `Flush()` (fsync + atomic rename of any
completed groups), reply with the protocol's expected status and body, and
count ingested/skipped rows and bytes for `/metrics`.

## OTLP/HTTP logs

Both encodings, decoded by hand (`internal/ingest/otel.go` for JSON,
`otelproto.go` for protobuf) because the repository takes no protobuf
dependency. The collector's `otlphttp` exporter sends **protobuf by default**,
so the Content-Type picks the parser: `application/x-protobuf` or
`application/json`, and nothing else is accepted. An unknown `Content-Encoding`
is rejected rather than read as identity; `gzip` is decompressed under the same
decompressed-size limit as every other route.

**The two encodings must produce identical rows.** That is a contract, not an
aspiration: an operator who switches their exporter's encoding must not see
their columns change. `internal/ingest/otel_conformance_test.go` describes one
export once, renders it into both encodings, and compares the stored rows field
by field.

All seven `AnyValue` kinds are stored:

| kind | stored as |
|---|---|
| string, bool | as written |
| int | decimal text (OTLP's JSON mapping already writes int64 as a string) |
| double | shortest round-tripping decimal |
| bytes | base64 — the encoding OTLP's own JSON mapping uses, so the two wire formats agree |
| array | compact JSON array, elements rendered by the same rule |
| kvlist | compact JSON object, keys in wire order |

Composite values nest, and the nesting arrives from an untrusted exporter, so
the decoder is depth-bounded (`maxAnyValueDepth`); a value past the bound is
truncated rather than dropped, and never recurses far enough to exhaust the
stack.

Beyond the attributes, each record carries `severity`, `severity_number`,
`trace_id`, `span_id`, `event_name`, `flags` and `dropped_attributes_count`,
plus `scope_name`, `scope_version` and the scope's own attributes. `trace_id`
and `span_id` are hex in both encodings — JSON sends hex, protobuf sends raw
bytes, and the protobuf path encodes them so the two agree. `severityNumber`
accepts either JSON spelling, the integer or the enum name with or without its
`SEVERITY_NUMBER_` prefix; an unrecognized one means UNSPECIFIED rather than a
rejected export. Zero and empty values are not stored: OTLP's zero values all
mean "unset", and a column of `0` for every record is storage spent to say
nothing.

Rejected records are reported through OTLP's `partial_success`, in the request's
own encoding, with a **200**. Not a 4xx: that tells the exporter to drop the
whole batch including the records this store did accept. Not a 5xx: that makes
it resend them. An empty response body (`{}` in JSON, zero bytes in protobuf) is
OTLP's "everything was accepted", so an accepted export of zero records is still
a 200 with an empty body — an exporter is entitled to send an empty batch.

A body that does not decode is an error, not a 200 with zero records: OTLP
exporters retry 5xx and give up on 4xx, and answering 200 for an undecodable
body told them the data was delivered. A metrics or traces export posted to
`/v1/logs` is discriminated by wire type — `LogRecord.time_unix_nano` is a
fixed64 where `Metric.name` and `Span.trace_id` at the same field number are
length-delimited — and rejected per record rather than stored as bogus log rows.

## Native syslog

UDP is RFC 5426: one datagram, one message. TCP is RFC 6587, which defines
**two** framings, and a receiver is expected to handle both:

    octet-counting:   "123 <13>1 2024-05-01T00:00:00Z host app - - - msg"
    non-transparent:  "<13>1 2024-05-01T00:00:00Z host app - - - msg\n"

Only the newline form was read. rsyslog's `omfwd` and syslog-ng's `syslog()`
driver both send **octet-counted by default**, so the default configuration of
the two most common forwarders stored the byte count and the space as part of
the message. The two are distinguishable without ambiguity: a frame begins with
a decimal digit if and only if it is octet-counted, because the other form
begins with `<`.

An octet-counted frame is **one message even when it contains newlines** —
which is the entire reason the framing exists. A forwarded multi-line stack
trace arrives as one counted frame, and passing it through the line-splitting
ingest path turned it into one valid record plus a run of records that parse as
nothing. `IngestSyslogMessage` is the whole-frame entry point.

**Bounds** (`SyslogConfig`, all defaulted): concurrent connections, a read
deadline reset per frame, the largest frame accepted, and the flush batching.
Every one closes a hole the listener shipped with — no deadline, so a
connection that opened and sent nothing held a goroutine and its buffer
forever; no connection limit; and `bufio.Scanner`'s `Err()` was never checked,
so an oversized line ended the connection silently and the sender saw a healthy
close. An over-limit connection is closed immediately rather than queued:
holding an unbounded number of accepted sockets waiting for a slot is the same
exhaustion one level down. The octet count is bounded **as it is read**, so a
sender writing digits forever cannot overflow the accumulator into a small
positive number that then passes the size check. TLS wraps the TCP listener
when configured (RFC 5425 is TLS over TCP only; UDP is unaffected).

**Flushing is batched**, by count and by time. It used to flush once per LINE,
which is an fsync per syslog message — the difference between thousands of
messages a second and tens. The time half is not optional: batching by count
alone means a low-rate sender's lone message is never queryable, which the
count-only first version reproduced immediately. UDP gets its tick from a read
deadline on the socket; TCP from a ticker that signals the read loop rather
than flushing on its own goroutine, since two goroutines flushing one writer is
a race the writer does not promise to survive.

A parse failure and a framing error are both counted and logged. On UDP there
is no response to fail, so an unreported failure there is silent loss with no
signal anywhere; on TCP one bad frame does not end a good connection, but a
flush failure does — the server can at least stop reading from a sender whose
data is not landing.

## Elasticsearch bulk

A `_bulk` body alternates an ACTION line with a SOURCE line, except `delete`,
which carries none. The response is **one item per action, in order**, because
Elasticsearch clients match items to their requests BY POSITION — an action
that produces no item shifts every later status onto the wrong document, which
is worse than an error because it is a wrong answer that looks like a right
one.

This store is **append-only**. `index` and `create` are supported and answer
201 per item. `update` and `delete` are rejected per item with a 400 and a
reason, not silently dropped: an `update`'s source is a `{"doc":...}` or
`{"script":...}` instruction rather than a document, and it used to be stored
as an **empty row** carrying only a synthesized timestamp — the ingester drops
object-valued fields, so the wrapper's single field vanished; a `delete` used
to produce no item at all, so a client got a success for a deletion that never
happened.

Two divergences from Elasticsearch, stated rather than implied. ES answers 404
`document_missing_exception` for an update against a missing document, and 404
`result: not_found` for a delete — with `errors` staying **false**. This store
rejects both with 400 because it is append-only and neither operation has a
meaning here, which the plan requires. And the action cap is 2^20: a body with
more actions than that fails the request rather than truncating silently.

An action line that cannot be IDENTIFIED fails the whole request with a 400,
matching Elasticsearch. That is not a shortcut: the body's meaning is carried
entirely by the alternation, so once a line cannot be identified the parser no
longer knows whether the next one is a source or an action, and every item
after it would be a guess. The failures that stay per-item are the ones where
the alternation is still known — an unsupported operation, a missing source
line, a source that is not an object.

The action's operation is read as the object's single KEY, not by substring.
It used to be detected with `bytes.Contains(line, "\"delete\"")`, so indexing
into an index NAMED `delete` was read as a delete action and the document that
followed was then read as an action line, desynchronizing the rest of the body.

**Allocation.** `parseBulkAction` runs once per action, so a 200k-document bulk
runs it 200k times. It is a byte scan over the fixed shape rather than a
`json.Unmarshal` into a `map[string]json.RawMessage`, which would build 200k
maps. Measured: `{"create":{}}` parses with **0 allocations**, and an action
carrying `_index` and `_id` costs exactly **2** — the two strings that have to
reach the response. The source lines are compacted into the front of the body
buffer in place, since each is preceded by at least its own action line, so a
multi-megabyte bulk is not copied twice.

## Loki push

Both encodings. Promtail, Grafana Alloy and the Grafana Agent all send
**snappy-compressed protobuf by default** (`Content-Type:
application/x-protobuf`), so the Content-Type picks the parser exactly as it
does for OTLP. Before that was supported the default configuration was not
merely unsupported but actively broken twice over: `lokiSpec` did not list
`application/x-protobuf`, so the media-type gate rejected the request, and a
snappy body is not JSON, so anything getting past would have failed the decode.
An agent shipping correctly-formed data got a 4xx.

`github.com/golang/snappy` is the only new dependency — not Loki's server
module, which brings a distributed database with it. The PushRequest itself is
decoded by hand, like OTLP's.

The two encodings carry the stream's labels differently: JSON sends a map,
protobuf sends the whole set as ONE Prometheus-syntax string
(`{app="api", env="prod"}`), which `parseLokiLabels` reads — braces, commas,
quoted values and Go-style escapes. A label set that does not parse rejects
that stream's entries and records a warning; it does not fail the push, because
the other streams in the same body are unaffected and their data is still
wanted.

**Structured metadata** — the optional third element of a JSON entry, and
`EntryAdapter.structuredMetadata` in protobuf — is stored. It used to be
discarded with a comment saying so, which meant a trace id sent there was
answered 204 and then was not in the store. It is applied AFTER the stream's
labels, so on a key collision the entry's own metadata wins: it is the more
specific of the two, and both encodings agree on that.

A snappy body is refused on its DECLARED decompressed length, before the
allocation. snappy's ratio on log text is routinely 4-6x and far higher on
repetitive input, so a body that passed the wire-size limit can still expand to
gigabytes — the same amplification the gzip path already guards.

## Datadog logs intake

A JSON **array** of entries or a **single object**; agents send both, and the
two produce identical rows for the same entry. `message` becomes `_msg`,
`timestamp`/`date` set the time (a number is milliseconds since epoch, a string
goes through the shared parser), and every other attribute becomes a field.
gzip is handled by the shared body reader.

`ddtags` is a comma-separated list. A `key:value` tag becomes a field, split at
the FIRST colon so `url:https://example.com/a:b` keeps its value intact. A
**bare** tag with no colon is equally legal Datadog and used to be dropped
silently — `ddtags=env,prod-canary` stored nothing at all — and is now kept
verbatim in the `ddtags` field so no tag the sender wrote is lost.

A nested object or array attribute is stored **compacted**. Keeping the source
bytes meant the whitespace the sender happened to use was part of the stored
value, so a pretty-printing agent and a compact one wrote two different strings
for the same attribute and neither matched a query written against the other.
Compacting also matches what the OTLP path does with a composite attribute.

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
