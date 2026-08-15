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
| journald export | `/insert/journald` | systemd-journal-upload; truncation reported, see below |
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

UDP is RFC 5426: one datagram, one message — including when the datagram
contains a newline, which the line-splitting entry point used to turn into two
rows while this document claimed otherwise. TCP is RFC 6587, which defines
**two** framings, and a receiver is expected to handle both:

    octet-counting:   "123 <13>1 2024-05-01T00:00:00Z host app - - - msg"
    non-transparent:  "<13>1 2024-05-01T00:00:00Z host app - - - msg\n"

Only the newline form was read, so an octet-counted sender's messages were
stored wrong — the count became the `hostname` field and the priority became
the app name. **syslog-ng's `syslog()` driver defaults to octet-counting** over
TCP; **rsyslog's `omfwd` does not** (`TCP_Framing="traditional"`, newline
framing, because its documentation notes few implementations support
octet-counting). An earlier version of this section claimed both.

A frame begins with a decimal digit if and only if it is octet-counted, because
the other form begins with `<`. That rule is what the reference uses too, and
it means a PRI-less RFC 3164 line starting with a timestamp digit is rejected —
parity with VictoriaLogs, not a simdlogs limitation.

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
positive number that then passes the size check. TLS wraps the TCP listener when configured — RFC 5425 syslog-over-TLS,
reachable from the binary as `-syslog.tls`, which reuses `-tls.certFile`/
`-tls.keyFile`. UDP stays plaintext: RFC 5425 is TLS over TCP, and RFC 5426's
UDP transport has no TLS form. Without `-syslog.tls` a syslog listener on a
non-loopback address is refused for the same reason a plaintext HTTP one is —
it is unauthenticated and writes to the default tenant — and the flag is what
makes that refusal something an operator can satisfy rather than only work
around with `-tls.insecure`.

The UDP receive buffer is **one byte larger** than the largest datagram the
kernel will deliver (65507 = 65535 − 20 IP − 8 UDP), so a full buffer is proof
of truncation rather than a coincidence. It used to be 65536, larger than
anything the kernel accepts, so the truncation branch could never fire and the
test that claimed to exercise it sent only one good datagram.

**Flushing is batched**, by count and by time. It used to flush once per LINE,
which is an fsync per syslog message. Measured on one TCP connection, 2000
newline-framed messages: **7,965/s before, 453,203/s after** — 57x, on tmpfs,
where fsync is nearly free, so a real disk would show the old figure lower
still. (An earlier version of this section said "thousands and tens", which
understated the old path.)

The time half uses an **absolute next-flush instant**, on both transports, and
the deadline is shared with the idle timeout — whichever comes first. Two
earlier attempts got this wrong. Batching by count alone left a low-rate
sender's lone message unqueryable. Re-arming the deadline as `now+FlushEvery`
on every read then made the flush *idle-only*: any sender faster than
`FlushEvery` pushed it forward forever, so 65 messages sent over 1.3 seconds
became queryable only when the sender stopped. On TCP a per-connection ticker
plus a forwarding goroutine was worse than useless — the signal was drained
only after a frame arrived, and a frame does not arrive on a held-open idle
connection, so the flush waited for the five-minute read timeout; at the
1024-connection limit those tickers cost 131M extra instructions per five idle
seconds, 2.1% of a core and 3.4 MiB. The shared deadline needs no ticker and no
goroutine.

A parse failure and a framing error are both counted and logged. On UDP there
is no response to fail, so an unreported failure there is silent loss with no
signal anywhere; on TCP one bad frame does not end a good connection, but a
flush failure does — the server can at least stop reading from a sender whose
data is not landing.

## systemd journal export

The format systemd-journal-upload sends: entries are blocks of fields
separated by a blank line, and a field is either `NAME=value\n` or `NAME\n`
followed by a little-endian uint64 length and that many RAW bytes. The binary
form exists so a value can contain newlines, which is why this parses bytes
rather than lines.

A **malformed length discards the rest of the upload**, so it is reported. Both
the "fewer than 8 bytes of length prefix" and the "length exceeds what remains"
branches used to set the cursor to the end and fall out of the loop with no
rejection count and no warning: every entry after the bad field was lost and
the request answered 202. `IngestJournald` could not return a failure at all,
which is why the listener's error handling for it was unreachable. The entries
that parsed BEFORE the bad field are kept and counted; the caller is told the
remainder is unreadable.

The declared length is compared against the remaining bytes **as a uint64**, so
a length near 2^64 cannot wrap when it is narrowed to `int` on a 32-bit build.

An entry carrying a timestamp and no storable field is rejected and reported
rather than dropped, which used to give a sender a 202 for records that were
not there.

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
- Write failure (fsync/rename/disk): reported to the caller at its own flush
  boundary as `*ingest.WriteError`, with the retry semantics below. It is never
  a silent success.
- Crash between temp write and rename: the `.tmp` file is ignored by
  `OpenStore`; the group is not indexed.
- A write that fails *after* the group file is durable but before the manifest
  commits it removes the file and fsyncs the directory. That window includes the
  mmap, the group re-read, the commit itself, **and the last two steps of
  `writeFileAtomic`** -- opening the parent directory and fsyncing it both run
  after `os.Rename` has already landed, so a failure there returns an error with
  the file at its final name. The first version of this cleanup described its
  own window as "every step after `writeFileAtomic` returns", which is where the
  reasoning went wrong. The manifest never names it, so no reader
  would ever open it and no retention pass would ever remove it: without the
  removal it is disk that only a human deleting files by hand can reclaim. It
  matters most for the failure it most often follows. On a full disk the
  group's own bytes can fit while the manifest record does not, and each retry
  would otherwise leave another full-size file, consuming the disk faster than
  an operator can free it. The removal is skipped when the manifest DOES name
  the id, which is the one fault point that returns an error after its record
  is durable -- deleting there would leave a committed group with no bytes. It
  is skipped again when the commit reports `storage.ErrRollbackFailed`: a
  commit whose record was fully written, whose sync failed, and whose rollback
  then failed too leaves that record in the page cache, where the kernel can
  write it back with no crash involved. Memory says invisible, disk ends up
  saying committed, and deleting on the strength of memory produces a store
  that fails to open with "committed but its file is missing". A leaked file is
  recoverable; a store that refuses to start is not.
- Shutdown: SIGINT/SIGTERM → HTTP drain (15 s) → `writer.Close()` (flush)
  → `store.Close()` (unmap). No buffered rows are lost, no mmap leaks.

## Write failure and retry semantics

A flush failure reaches the caller as `*ingest.WriteError`, which answers the
two questions a log shipper actually has. `Flush` and `FlushMark` return it, so
`err != nil` keeps its old meaning for callers that only test for nil.

### Is retrying worth anything?

`WriteError.Class` classifies the underlying failure. The errno survives every
layer of wrapping, so `errors.Is` reaches it.

| Class | Cause | HTTP | `Retry-After` |
|---|---|---|---|
| `RetryAfterRepair` | `ENOSPC`, `EROFS`, `EACCES`, `EPERM`, `EIO`, `fs.ErrPermission`, `storage.ErrCorruptGroup` | 503 | 30 s |
| `RetrySoon` | `ErrWriterClosed` (shutdown), `EMFILE`, `ENFILE`, `ENOMEM`, `EINTR`, `EAGAIN`, and anything unrecognised | 503 | 1 s |

**There is no never-retry class, and the absence is deliberate.** There was
one, and a group that failed its own bounds and checksum checks the instant
after being written was put in it -- 500, `retryable: false`, no `Retry-After`
-- on the reasoning that the bytes are a pure function of the payload so every
attempt reproduces it.

That reasoning is not sound. `ReadGroup` validates a CRC32C over a blob handed
to the filesystem seconds earlier, and a mismatch there is at least as likely to
be the storage returning different bytes than the ones written -- which is not
deterministic, and is fixed by a retry or by replacing the disk. Answering it
500 told a shipper to drop data that a media error had corrupted. It also
inverted the classification's own bias: an unrecognised failure defaults to
retryable *because* telling a client to give up on something transient loses
data, while a needless retry only duplicates it. So the class is gone rather
than misapplied. If a genuinely deterministic write failure turns up, it comes
back with the evidence that made it deterministic.

### Does a retry duplicate?

`WriteError.DuplicatesOnRetry()` is true exactly when the failure was
**partial** -- some groups in the caller's window reached the store and some
did not. There is no idempotency key on the ingest path yet (that is Phase 8),
so resending a payload whose first half landed stores that half twice.

The accounting is per **flush job**, not per batch. One batch routinely holds
several groups, because a caller whose rows crossed the row or byte trigger
while it was adding them gets its rows split across jobs. A batch-level flag
cannot tell "all three groups failed" from "one of three failed", and that is
the whole distinction: the first means a retry is clean, the second means a
retry duplicates.

It cannot be narrowed to "your rows specifically". The row buffer is shared by
every request and every syslog connection on a tenant, so a caller does not own
the groups its rows land in. *Some of what you sent may be stored* is the
strongest true statement, and it is the one a client needs.

### The window a mark can be answered from

`FlushMark` answers from a ring of the last `batchHistory` (64) batches. On its
own that ring loses writes, and the way it lost them is why there is a second
structure behind it. Every `Flush` and `FlushMark` installed a new batch whether
or not the old one carried anything, so 64 flushes from *any* caller on the
tenant -- other requests, a syslog connection flushing per line, the
`FlushEvery` timer, `Close` at shutdown -- evicted the batch a marked caller's
rows were in. That batch was then never waited on and its error never seen: a
200 for rows that are not in the store. At `MaxConcurrentWrite` 32, 64 slots is
about two completed request cycles.

Six things close it:

- An outgoing batch that carried **no jobs** is dropped from the ring rather
  than aging a real one out of it, so an idle or rejected flush costs nothing.
- A batch that did carry jobs leaves a four-word `batchOutcome` behind when it
  is retired -- seq, jobs, failed, error. `FlushMark` folds those in, so a late
  caller still gets the right counts and the right `duplicateOnRetry`.
- A batch is retired **only at zero outstanding jobs**, and the ring may run
  over its nominal length until then -- bounded by `maxHistory`. Without that gate the outcome log
  reproduced the defect it was added to fix: `FlushMark` waits only on batches
  at or after its own mark, so a later caller never blocks on an older one, and
  enough later flushes retired a batch whose job was still in flight --
  snapshotting "one job, none failed" for a job that went on to fail with
  ENOSPC. The worker decrements `outstanding` after recording its error, so
  observing zero is observing counters that can no longer change.
- **`maxHistory` is a hard ceiling on the ring, and past it a stalled batch is
  dropped UNANSWERABLE and removed from `live` with it.** Leaving it in `live`
  kept its counters reachable by the next unrelated plain `Flush` -- final by
  then, and handed to a caller whose own rows had all landed -- and kept every
  `Flush` blocked on the stalled job, since `Flush` waits on all of `live`.

  It unblocks `Flush` and nothing else. `Writer.Close` runs `Flush` and then
  joins the flush workers, and a worker parked inside `AppendGroup` has not
  returned, so `Close` blocks on a stalled writer with the ring drained and
  `live` empty. Tenant eviction blocks for the same reason. Both are open; see
  `docs/wrong.md`. The zero-outstanding gate above is not itself a
  bound: the flush pool bounds outstanding *jobs*, not *batches*, and every
  later job-carrying flush appends one more while a stalled one pins the front.
  Measured, 5000 client requests with one job pinned left the ring at 5002
  entries, walked under the writer's lock on every request. Past the ceiling
  the stalled batch leaves with no outcome and `oldestAnswerable` advances past
  its seq, so every mark at or below it answers `ErrDurabilityUnknown`.
  Recording counters that can still change is the frozen-zero defect again;
  refusing to answer is not.
- **`oldestAnswerable` only ever rises.** Two paths move it -- the outcome
  log's overflow and the ceiling drop -- and the first assigned it
  unconditionally, so a normal retire following an unanswerable drop lowered it
  again and un-hid a batch that is in neither the ring nor the log. It is a
  watermark; a watermark that can go backwards is not one.
- Past even the outcome log (`outcomeHistory`, 1024 real batches), `FlushMark`
  returns `ErrDurabilityUnknown` with `Partial` set. The writer says it does
  not know, which a client must treat as a possible failure and a possible
  duplicate. A wrong nil is the thing this whole mechanism exists to prevent;
  refusing to answer is not.

### What the client sees

On a JSON route:

```json
{
  "error": "ingest: 1 of 2 groups failed to store (partial; a retry may duplicate records): ...",
  "status": 503,
  "retryable": true,
  "retryAfterSeconds": 30,
  "duplicateOnRetry": true,
  "groupsFailed": 1,
  "groupsTotal": 2,
  "unit": "groups"
}
```

`Retry-After` is set as a header on every retryable failure, on text and JSON
routes alike, and is written before the status line -- a header set after
`WriteHeader` never leaves. A text-envelope route would carry the same two
booleans in the message rather than losing them; no ingest route uses one
today -- all five pass `ndjsonSpec()` or `otlpSpec()`, both JSON -- so that
branch is a guarantee for a route that does not exist yet rather than a
property any client currently observes.

**The parallel path answers the same way.** A body at or above
`ingest.MinParallelBytes` goes to `IngestJSONLinesParallelCfg`, which shards it
across several writers when the configured shard count is at least 2 and
otherwise runs one writer serially -- `runtime.NumCPU()/3`, so every host with
fewer than six cores takes the serial branch. Both fail with
`*ingest.ParallelWriteError`, and both carry `Partial`; the serial branch
dropped it, which made `As` synthesize an answer strictly worse than the inner
error it replaced.

Both branches used to answer a flat 500 with no `Retry-After` and none of the
fields above, so the answer to one disk failure depended on how large the
request was. Two things fixed it: `failIngest` reads
the same metadata every other route does, and `ParallelWriteError` implements
`As` so `errors.As` sees the WHOLE write rather than the first failing shard.
Without that `As`, a request where shard 1 landed and shard 2 failed reported
`1 of 1 failed, duplicateOnRetry: false` -- the opposite of the truth, since
shard 1's rows are durable and resending the body stores them twice. On that
path the counts are shard writers rather than groups, which `WriteError.Unit`
says.

### Testing it

`storage.SetFaultHookForTest` exposes the durable-write fault injector to the
packages above storage, because "what does the client see when the disk fills"
is assembled out of the writer's batch accounting and the handler's status
code, and cannot be asked from inside the storage package.

It is guarded rather than build-tagged: it panics unless `flag.Lookup("test.v")`
resolves, which is true in every test binary and no production one. A build tag
would put the failure suite in a lane of its own, and a lane outside
`make verify` is a lane that goes stale -- this repository has already paid for
one vacuously-green tagged lane.

`TestEveryWriteFaultReachesTheCaller` enumerates `storage.FaultPointNames()`
rather than listing points, so a write step added later is covered the day it
exists. A write step whose failure is not surfaced is a write step that loses
data behind a 200.
