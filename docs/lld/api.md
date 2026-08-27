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
| `/-/ready` | the query-side readiness probe. It answers 503 with the degraded tenants named, and it MUTATES: it re-reads the quarantine directory of any degraded tenant that is not open and drops the record when the evidence is gone, which is how an operator's remediation takes effect. A GET with a write side effect is unusual enough to say out loud |
| `/insert/ready`, `/insert/datadog/api/v1/validate` | 200 probes. `/insert/ready` stays unconditional even for a degraded store: the degradation is read-side and writes still land, so failing it would take the node out of the ingest Service for a query-side problem. `/-/ready` is the probe that reflects storage health |

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
`{"values":[{"value":..,"hits":..}]}` — so a client decodes them all
with one type; that is why they must not each invent a key.

In router mode `/select/logsql/query`, `stats_query_range`, `/_search` and
`/_count`, and the `field_names`/`field_values`/`stream_field_names`/
`stream_field_values` value-count merges are wired (the `streams` and
`stream_ids` merges decode the wrong key and answer empty). The rest of the
introspection surface — `facets` and anything beyond the four value-count
endpoints — plus `tail`, `/select/sql`, `/select/vector`, `/admin/backup`,
`/metrics`, `/alerts` and others is router-local or not merged: those
endpoints answer from the router's own store and state. The enumeration
here is representative, not complete: the per-endpoint status table in
[`cluster.md`](cluster.md) is authoritative, and an endpoint's absence from
either list must not be read as federation. The cluster surface is
experimental, not production-safe.

### Ops

`/admin/backup` (tar), `/metrics` (Prometheus text), `/alerts`, `/health`,
`/-/healthy`, `/-/ready`, `/flags` (non-default flags only, VL's
`-name="value"` line form), `/vmui`, `/select/vmui`, and `/` (the UI,
catch-all).

All of these are router-local in router mode: backup covers the router's own
tenant stores, metrics/alerts count the router's own requests and rules, and
the health/flags/UI endpoints describe the router process — none is
cluster-wide.

### ES

`/_search`, `/_count` — a log-relevant Query DSL subset. `esClause`
(`internal/api/es.go`) names every field a client may send and the decoder is
strict, so anything not named is a 400 rather than a filter dropped on the
floor. What `esToQuery` walks and applies: `bool` (`must`, `filter`,
`must_not`, `should`, `minimum_should_match` 0 or 1), `term`, `terms`,
`match`, `prefix`, `exists`, `match_all`, and `range` on `@timestamp`/`_time`.
A range in positive conjunctive position becomes the scan window and feeds the
group skip directly; under `must_not`/`should` it becomes an ordinary time
predicate, because the window is an AND and cannot express a negation or a
union. A range on any other field is a 400, not a dropped filter. `_bulk` on
the ingest side; the response gives one item per action, in order, keyed by
that action's own name — `{"index":{…,"status":201}}` for a stored document,
`{"create":{…}}` for a create (the shape ES clients parse to decide retry).

Not implemented (documented, not accidental): `_msearch`, the complete Query
DSL, ES aggregation-response compatibility, scoring, and analyzed-text
semantics — `match` on this store is term equality, because there are no
analyzed text fields for it to mean anything else on. A
`minimum_should_match` that needs a counting operator ("2 of 3") is a 400
naming the value, and so is every spelling but the JSON integer: ES also takes
`"1"`, `"75%"` and `2<-25%`, which the strict decoder refuses.

This section described the opposite of the code for several rounds: it listed
`exists` among the clauses this surface decodes and then throws away, said an
`exists`-only search therefore returns the whole window, said `terms` was
absent, and cited the unnumbered source-reading note immediately before
`docs/wrong.md` entry 37 for all of it. Every part of that was true when the
note was written and none of it is true now — `exists` is NOT (field == "")
and `terms` is an `In` set, both applied.
README.md carried the same `exists` claim and was corrected one round earlier
while this file was not, which is why
`TestTheStatedFactsAboutTheCodeAreTrueOfTheCode` reads this document as well as
that one: the gate could only see the copy it had been pointed at.

## Query parameters

`parseRequest` (server.go):

- `query` — required on every select endpoint; a request without one is a 400
  (`missing `query` arg`). Defaulting to match-all answered a client's bug
  with the entire store.
- `start`/`end` — unix seconds/ms/us/ns inferred from magnitude (VL's rule:
  seconds below 1e11, ms below 1e14, us below 1e17, else ns), or RFC3339
  (nano, second, minute, day, month, year granularities). **Both defaults are
  named constants** (`defaultWindowFrom`, `defaultWindowTo` in `server.go`) and
  the Elasticsearch surface reads the same two: `start` defaults to
  `math.MinInt64` (open past) and `end` to `1<<62` (open future). `start`'s
  default was the EPOCH and only this bullet's silence about it kept that
  invisible — the ES window already began at `MinInt64`, so one binary answered
  1 row to `/select/logsql/query?query=*` and 3 to `/_search {"match_all":{}}`
  on a store holding 1900, 1969 and 2026, and `/_search` takes no `start`/`end`
  so a client could not bring the two onto one window. `docs/wrong.md` 131 and
  132. A row stamped after 2116 is still outside the `end` default, which is the
  same shape at the other end and is not fixed.
- `limit` — the most RECENT n, newest first (`LastN`); deliberately not the
  `| limit N` pipe (first n). The reference draws the same distinction.
  IGNORED on `/select/logsql/tail`, which has no last row to count back from
  — see "Live tail";
  conflating them returned the oldest rows to a client asking for the tail.
- `step` (hits/range), `field`/`fields` (hits split, one field), `fields_limit`
  (busiest N series, the rest folded into one unlabelled "other"), `time`
  (stats instant window end), `by` (stats group-by extension), `start_offset`
  (tail replay window, default 5 s), `keep_const_fields` (facets; when set,
  includes constant/single-distinct fields: `facetKeep` keeps a field with
  one distinct value, including the synthesized `_stream` / `_stream_id` —
  which, with no stream fields configured, hold the single constant values
  `{}` and its id — and a single-timestamp `_time` facet —
  `internal/query/introspect.go`). On the current committed corpus (no stream
  fields, so the constant fallback is a candidate), `keep_const_fields=1`
  therefore changes the facets answer. The 2026-08-13 answer-changes probe
  (commit f42cc8e) recorded this argument changing nothing, but that finding
  is historical: it was measured against the code before the synthesized
  fields were made facetable (facets from stored columns alone); the same
  commit added them as candidates. Committed tests pin the current behavior
  two ways: `TestFacetsFieldSelection` (`internal/api/shapes_test.go`)
  unconditionally asserts that `keep_const_fields=1` brings a constant
  stored field (`env`) back into the facets, and `TestParamsHonoured`
  (`internal/bench/shapes_test.go`) varies the argument in the
  VL-gated comparative probe, logging INCONCLUSIVE when the reference's own
  answer does not change. Neither pin is on the synthesized
  `_stream`/`_stream_id` constant fallback; the production plan (B.3)
  extends them.

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
  multiples — the alignment the reference uses, and the one `histoGroup` keys
  the counts with (`ts/step*step`), so a series timestamp and a bucket key are
  the same number. All three adjectives were false over a saturated window
  until round 19: `fillHits` derived its bucket count with an int64
  subtraction that wraps, so `?start=1970-01-01&end=9999-01-01&step=8760h`
  rendered 100,000 buckets whose timestamps ran off `MaxInt64` and came back
  at the far-past end, and the same wrap in the other direction answered a
  structurally valid EMPTY histogram. The count is `RangeWidthNs` now and the
  walk stops at `to`.
- ES `_search` returns the ES envelope with `hits.total.relation: "eq"`.

## Bounds and errors

- `-search.maxRows`: zero now means the built-in default rather than
  unlimited, which is what it used to mean. A bare (no-pipe) select over the cap errors with an
  explicit message (never silently truncates); `MaxRows` keeps the parallel
  scan, `Limit` would force the serial path. Pipes' input is never bounded.
- **Two constants are named `maxHitsBuckets`**, in different packages, and they
  hold different numbers. `internal/query`'s is **100K** — the engine's own
  ceiling on `query.Hits`, so a one-second step over a year cannot be asked to
  allocate 31M buckets (`query.md`). `internal/api`'s is **10,000**, the HTTP
  response ceiling, and it is the one a caller meets: an explicit window over it
  is a 413 (see the histogram section below). This line quoted the first
  number against the second constant.
  The **"one a caller meets"** half was itself false until round 19, through
  the coarse-step path: `?end=9999-01-01&step=8760h` passed the 413 at 292
  buckets and then rendered `internal/query`'s 100,000, because `fillHits` read
  its own wrapped count as "no buckets" and substituted its package's ceiling.
  `TestAHistogramOverASaturatedWindowRendersTheBucketsItPromised` holds the
  route to the 10,000 at any step, with or without a window.
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
| `-compact-min-groups` | merge runs of at least this many small adjacent groups into one; 0 disables |
| `-compact-max-rows` | cap an output group's rows (the time skip is per group, so bigger is coarser) |
| `-compact-after` | only merge groups older than this, so a pass never rewrites the live tail |
| `-compact-every` | how often to run a compaction pass |
| `-compact-max-outputs` | bound one pass to this many output groups per tenant |
| `-compact-max-input-bytes` | refuse to rewrite more than this per pass, per tenant; 0 = no bound |
| `-compact-max-group-bytes` | leave input groups larger than this alone; 0 = no bound |
| `-storage-reserve-warn-bytes` | free space at which readiness degrades, writes still accepted; 0 disables |
| `-storage-reserve-reject-bytes` | free space at which writes get 507; below the warn level when both are set |
| `-storage-max-tenant-bytes` | refuse writes once one tenant's groups reach this many bytes |
| `-compact` | compact mode: flate dicts, ~15% smaller groups, 2–10x slower value reads — cold archival only |
| `-stream-fields` | comma-separated fields identifying a log stream |
| `-syslog` | also listen for syslog on UDP/TCP |
| `-select-backends` | peer node URLs; sets select-router mode (vmselect role) |
| `-replicas` | replication factor for the backends |
| `-search.maxRows` | cap on rows a select may return; 0 = built-in default, -1 = unlimited. Over it the query errors 413 — for EVERY pipe shape, not only a bare select |
| `-search.maxGroupKeys` | cap on an aggregate's distinct `by` keys (stats/uniq/top); 0 = unbounded |
| `-search.maxPipeRows` | cap on the rows one pipe may produce (join fanout, union, stream_context); 0 = unbounded |

**Cursor pagination.** `/select/logsql/query` takes `page_size=N` and
optionally `direction=oldest|newest` and `cursor=<token>`. A page that has more
returns the next cursor in the **`X-Simdlogs-Cursor`** response header — not in
the body, because the body is NDJSON and a trailing object of a different shape
is a row as far as every client reading the stream is concerned. Page until the
header is absent and every row is seen exactly once.

Pagination is opt-in: without `page_size` the endpoint answers exactly as it
did. The total order it imposes — `(timestamp, group id, row index)` — is a
promise the unpaginated path never made, and imposing it on every select would
change answers.

The tuple exists because a timestamp is not an identity. Log timestamps collide
constantly, and "after time T" either repeats every row at T or drops all but
one of them. The group id is the manifest id, assigned once by `AppendGroup`
and never reused, so it survives compaction and restart; a group's *position*
in a snapshot does not.

The cursor is HMAC-SHA256 signed and carries the tenant, a hash of the query
text and its **resolved** window, the direction, and the tuple. Everything in
it is attacker-controlled on the way back, so all four are checked: a cursor
replayed against another tenant, another query, a relative window that has
since moved, or the other direction is **400**, not a wrong page. 400 rather
than 403 for the tenant case — the tenant middleware already made the
authorization decision, and 403 here would tell a prober that its forged cursor
was otherwise well-formed.

The signing key is random per process and never persisted. A cursor that
survived a restart would resume into a store that has since compacted, retired
and re-ingested, so "your cursor expired, start again" is the answer. A
select-router forwarding a cursor to a node that did not issue it gets a clean
rejection rather than a wrong page.

Each page is one snapshot, so rows appended mid-walk are not in it — including
rows that sort *before* the cursor, which is exactly what a timestamp-only walk
gets wrong.
| `-search.maxDuration` | wall-time cap for one query request (not the live tail); 0 = default, -1ns = unlimited |
| `-search.maxQueryBytes` | cap on the bytes one query may materialize; 0 = default (256 MiB), -1 = unlimited. Over it the query errors 504 rather than returning a short answer. |
| `-http.maxBodyBytes` | maximum request body; 0 = default, -1 = unlimited |
| `-tenants.max` | maximum tenants held open; 0 = default, -1 = unlimited |
| `-auth.config` | JSON auth file (token hashes, cert mappings, roles, tenants). Absent = **unauthenticated** |
| `-tls.certFile` / `-tls.keyFile` | PEM pair; serves HTTPS |
| `-tls.clientCAFile` | PEM CA bundle; requires and verifies a client certificate (mTLS) |
| `-tls.insecure` | serve plaintext on a non-loopback address, including `-syslog` (alias: `-insecure-http`) |

**Storage budget.** Two thresholds and a per-tenant cap. WARN degrades
`/-/ready` while the store still accepts everything, so an operator sees it
before a single write fails; REJECT refuses new writes with **507 Insufficient
Storage** — not 503, because the condition is about storage rather than the
server being down, and an agent that retries a 503 forever against a full disk
is a busy loop. Queries, `/metrics`, retention and the admin surface keep
working past both: the answer to a full disk is to read what is there and
delete some of it, and a store that refuses reads has taken away the only tool
the operator has.

Bytes, not a percentage. 5% of a 40 TB array is 2 TB of slack nobody needs; 5%
of a 20 GB volume is less than one large group plus the manifest rewrite that
follows it. What has to be protected is room for the RECOVERY — a retention
pass writes a manifest record before it can unlink anything.

The check runs in the middleware every insert route shares, before the body is
read: rows that reach the writer are the writer's, and refusing a request whose
rows are already buffered would either drop them silently or report a failure
for rows that will be written anyway. The mux registers fourteen ingest routes
reaching **nine** distinct handlers — the eight that write, plus the inline 200
on `/insert/datadog/api/v1/validate`, which resolves no tenant and has no
budget to check. A check written into each is a check that will be missing from
the tenth. (This paragraph has been wrong about its own numbers twice: "six
entry points reaching four functions", then "eight distinct handlers". That is
what a count nothing gates does.)

The HTTP mux is not every write path. The native syslog listeners take bytes
off a socket with no middleware anywhere near them; they call the budget
themselves (`syslogAdmits`). Neither transport can answer — RFC 5426 has no
reply and the TCP framing carries no ack — so a refused message is dropped,
counted by `simdlogs_writes_rejected_disk_total`/`_quota_total`, and logged at
most once every 30 seconds. It is deliberately **not** counted as an ingested
byte or a malformed row: an earlier version added it to
`vl_bytes_ingested_total` and `vl_rows_dropped_total`, which said bytes were
ingested that never were and called a well-formed message malformed — and the
HTTP path counts neither for the identical event.

A tenant whose store cannot be opened is classified by whether a retry could
survive it. Transient (`ENOSPC`, `EDQUOT`, `EMFILE`, `ENFILE`, `ENOMEM`,
`EAGAIN`, `EBUSY`, `EINTR`, `ESTALE`, `EIO`, and `storage.ErrLocked` — the
store lock held by another process) is **507**; permanent (`EACCES`, `EPERM`,
`EROFS`, `ENOTDIR` — a directory the process may never write to, a read-only
mount) is **500**, because no retry and no change by the client fixes it.
Anything else stays 400. The client is told the class; the server's own
message names the data directory's absolute path and goes to the log.

The free-space sample is cached for about two seconds, so a burst of small
writes is not a burst of `statfs` calls. That staleness is why the threshold is
a RESERVE: there is room to be wrong by one interval's worth of writes. A
filesystem that cannot be measured does NOT refuse writes — turning a `statfs`
failure into a write outage is the protection causing the harm it exists to
prevent. The per-tenant cap still applies there, because it is measured from
the store's own groups and needs no filesystem call. That was false when first
written: `QuotaState` returned at the `statfs` error before reaching the cap,
so a platform without `statfs` enforced neither budget.

`statfs` is used on **linux, darwin, freebsd and dragonfly**; everywhere else
only the tenant cap applies. The first fix said "the BSDs" and copied
`diskfree_unix.go`'s platform list, which was itself wrong — netbsd has no
`syscall.Statfs` and openbsd spells the fields `F_bsize`/`F_blocks`/`F_bavail`,
so neither compiled. Both files carry the corrected list now, and both
platforms build for the first time. A copied build tag is a claim, and that one
had never been compiled.

Metrics: `simdlogs_storage_capacity_bytes` (only when free space can be
measured, since it comes from the filesystem total),
`simdlogs_storage_warn_tenants`, `simdlogs_storage_reject_tenants`,
`simdlogs_storage_over_quota_tenants`, `simdlogs_writes_rejected_disk_total`,
`simdlogs_writes_rejected_quota_total`. The last two are separate counters
because "the machine is full" and "this tenant is over its share" are
different incidents with different fixes.

**Not covered.** Background writers — compaction, recompaction, the writer's
own flush, restore — do not consult the budget. A compaction pass writes its
output before unlinking its inputs, so one pass can transiently grow the store
while the reserve is exhausted; `-compact-max-input-bytes` bounds it and
defaults to 0. On a cluster, `forwardWrite` relays the LAST replica's status,
so one replica refusing on its own budget while another accepts is reported to
the client as the accepting one's answer.

Environment: `SIMDLOGS_STREAM_FIELDS` (stream-field default before the
default tenant opens, so a deployment can synthesize `_stream` without a
code change).

## The embedded UI

`ui.html`, `ui.go`. One self-contained page — inline CSS and script, no
external assets — driving the same JSON endpoints as any other client.

**The histogram never worked.** The page read `j.hits[i]._time` and `.hits`, a
bag of `{time, count}` objects; `/select/logsql/hits` returns the reference's
**dense** shape, one entry per series with parallel `timestamps` and `values`
arrays. Every value was `undefined`, `max` stayed 1, every bar computed a `NaN`
height, and the graph was permanently empty. It failed silently because the
fetch's `.catch(function () {})` swallowed everything — including the error
that would have said so. Both are fixed, and `ui_test.go` asserts the response
shape *and* that the page's code (comments stripped) reads those field names.

**The response is bounded.** Dense means its size is `window / step` and has
nothing to do with how much data matched: with no window and a one-minute step
that is a bucket per minute since 1970 — about 29 million, tens of megabytes,
from an empty store — which the UI requested on every page load. An
**unspecified** window is now defaulted to 240 steps ending now; an
**explicit** window over 10,000 buckets is a 413 that says to narrow the range
or raise the step. Unspecified is defaulted rather than refused because a
caller that named no range did not ask for all of history, it just did not say.

`query.Hits` also consults the budget before doing anything. Every read path
checks per group, which reports nothing when there are no groups — so a query
whose window held none finished without ever asking, and an already-blown
deadline produced a cheerful 200.

**No tenant selector.** It sent an arbitrary `AccountID` header, so without
`-auth.config` the UI was a free tenant switcher: pick "tenant 3" from a
dropdown, read someone else's logs. With auth configured the resolver refused
it, making the control a dropdown that produced 403s. Either way the tenant is
whatever the credential resolves to, decided by the server.

**Security headers**, because this page renders log content — arbitrary
attacker-influenced strings that arrived through an ingest endpoint — into a
table. The renderer escapes them; a CSP is what stands between an escaping bug
and script execution with the operator's session. `default-src 'none'`,
`connect-src 'self'`, `frame-ancestors 'none'`, `base-uri 'none'`,
`form-action 'none'`, plus `X-Frame-Options: DENY`, `X-Content-Type-Options:
nosniff`, `Referrer-Policy: no-referrer` (the query is in the URL, and a query
is a search over someone's logs) and an empty `Permissions-Policy`.
`'unsafe-inline'` is required by the inline blocks and is stated rather than
worked around: a nonce would mean the page could no longer be a static embedded
byte slice.

**Cancel and Load more.** The server already ends a scan whose client hangs up
— the scan's context is the request's — so an `AbortController` was the whole
mechanism and it simply was not wired to anything. Paging uses `page_size` and
the `X-Simdlogs-Cursor` header, appending to the table rather than replacing
it.

## Rules: metrics from logs, and alerts

`-rules.file` is a JSON file of metric and alert rules, loaded and **validated
at startup**. A bad rule is a startup failure, not a surprise an hour later: an
invalid metric name does not fail its own rule, it corrupts the whole
`/metrics` exposition, so one bad rule takes the scrape down for every series
the server publishes and the failure lands nowhere near its cause.

**Every rule needs a window.** Both rule kinds evaluated over `From, To = 0,
1<<62` — all history — on a timer. That is wrong twice. A gauge whose answer is
"how many errors, ever" only goes up, so it can never fall back below a
threshold and an alert built on it fires once and forever; the `!holds` branch
that clears an alert was unreachable for any monotonically counting query,
which is most of them. And the cost of each evaluation grows with the store
while the interval does not, so a rule that took 10 ms on Monday takes ten
seconds a month later and the ticker running it has no idea. The maximum window
is 24 h, because a recurring query over more than that is a scheduled outage.

| field | meaning |
| --- | --- |
| `window` | how far back each evaluation looks. Required, positive, ≤ 24 h |
| `interval` | how often it runs. Defaults to `window`; minimum 1 s |
| `tenant` | `account:project`; empty is the default tenant. Rules used to be hard-wired to it, so a multi-tenant deployment could not use them |
| `by` (metrics) | field to group by, one series per value, capped by `max_series` (default 1000, ceiling 10000) |
| `op` (alerts) | `>` `>=` `<` `<=` `==` `!=`, **validated** — unvalidated it fell through `crossed`'s default and the alert never fired: present, listed, permanently silent |
| `for` (alerts) | how long the condition must hold before firing. 0 fires on the first crossing, which is how a single spike pages someone |

Durations are strings (`"5m"`), never bare numbers: `300` is ambiguous between
seconds and nanoseconds and Go would read it as nanoseconds, which is not what
anyone writing it means. Unknown keys are refused — a misspelled key is a rule
that does not do what its author believes, and silence is how it stays that
way.

**Cardinality is capped and the cap is reported.** `by` on an arbitrary field
is one series per distinct value, unbounded by construction on a log server
(`by: request_id` is a series per request), and it falls over on the monitoring
system rather than here. A truncated rule keeps its largest series by count and
sets `simdlogs_rule_series_truncated{rule=...}` — a silently truncated set is a
gauge answering about a subset chosen by the store's internal ordering.

**Rules report their own health.** `simdlogs_rule_evaluations_total`,
`_last_evaluation_seconds`, `_failing`, `_series_truncated`, and the same in
`/alerts` JSON. A rule that has been failing for an hour used to look exactly
like a rule reporting zero: the error was swallowed and the previous series
left standing. A failed evaluation now keeps the last series rather than
dropping to zero — zero is a value an alert acts on — and says it is stale.

Evaluations run under the query budget (`ruleEvalTimeout`, the group-key and
byte ceilings). A rule is the one query nobody is watching: it runs on a timer
with no client to hang up, so an unbounded one is unbounded forever.

**Reload semantics: there are none.** Changing rules means restarting. There is
no management API and no reload endpoint, so there is no half-applied rule set
and no way for the running configuration to differ from the file.

## Observability: metrics, structured logs, audit

**The metric contract.** A metric name is an API — a dashboard, an alert rule
and a capacity model are all written against a name and a meaning, and changing
either silently is worse than deleting the series: a deleted series makes a
panel go blank, a redefined one makes it lie. This campaign did both. Refused
syslog bytes were added to `vl_bytes_ingested_total` ("Bytes of log data
ingested") for bytes never ingested, and three admission series were emitted
only when admission was configured while two documents listed them
unconditionally.

`metrics_contract_test.go` pins every series: name, type, and a one-line
meaning, asserted in **both directions** — every contracted series must be
present on a *default* server with its contracted type, and nothing may be
emitted that the contract does not name. Writing it immediately found five
emitted series no document mentioned (`simdlogs_tenants_open`,
`_tenants_evicted_total`, `_tenants_rejected_total`,
`simdlogs_retention_tombstones`, `_retention_failures_total`) and one that
disappeared when `statfs` was unavailable.

No metric carries an unbounded label. A tenant key or field name in a label is
one time series per value, and a log server sees unbounded numbers of both —
which falls over on the *monitoring system* rather than here, so it has to be
refused at the source. Every per-tenant number is an aggregate (how many
tenants are degraded, over quota, refusing writes) for exactly that reason, and
a test asserts the exposition is label-free.

**Structured logs.** `internal/observability`. Every operational line was a
formatted sentence — `tenant 7:3: flush on eviction failed: no space left on
device` — which a human reads fine and a machine cannot: alerting on "eviction
flush failures for tenant 7:3" needs a regex against wording that is not a
contract and changed three times in this campaign. The *fields* are the
contract: `tenant`, `route`, `request_id`, `shard`, `error_class`, `event`. A
query is `event=tenant.evict.flush_failed AND tenant=7:3`, and rewording a
message breaks nobody. `-log.format=json|text` (text by default, because that
is what the process wrote before and a silent format change breaks whatever was
parsing it) and `-log.level`.

`error_class` is coarse and closed — `client`, `storage`, `budget`, `upstream`,
`internal`, `cancelled`, `corruption` — because an operator alerts on "storage
errors rose" and cannot alert on a sentence.

**Audit.** A separate stream (`-audit.file`), because it answers to a different
reader with a different retention: an operator greps the operational log while
an incident is open and lets it roll; a security review reads the audit log
months later and needs it complete. Mixing them means either the operational
volume evicts the audit records or the audit retention pays for the operational
volume. Audit records are **never** filtered by `-log.level` — "we stopped
recording authentication failures because someone raised the log level" is not
a thing an audit trail may do.

A closed vocabulary: `auth.failed`, `auth.forbidden`, `admin.backup`,
`admin.restore`, `admin.rule_changed`, `admin.corruption_acknowledged`,
`admin.topology_reload`, `admin.retention`. Every record carries a subject; an
unauthenticated action records `unauthenticated`, which is a countable fact
rather than an absent field.

`auth.failed` and `auth.forbidden` are distinct events — no credential at all
versus a valid credential reaching for a role it does not hold — because the
response to them is different. Both are recorded at the **tenant resolver** as
well as in `requireAuth`: the resolver runs before the mux, so it is where most
refusals actually happen, and a trail that recorded only `requireAuth` missed
the common case entirely.

## Health: liveness and readiness

`health.go`. They are used for **opposite actions**: liveness failing means
kill the process, readiness failing means stop sending it traffic. Conflating
them turns a full disk into a crash loop — probe red, process killed, restarts
onto the same full disk, and the restart destroys the rows a graceful drain
would have flushed. So liveness is the **process and nothing else**, and every
store, disk and peer condition is readiness's.

`/health` and `/-/healthy` are liveness; `/-/ready` is readiness. The liveness
routes used to return a literal `OK` whatever the server was doing, draining
included — so an orchestrator kept routing to a process that was about to exit.

| state | ready | live | clears by itself |
| --- | --- | --- | --- |
| `ready` | ✅ | ✅ | — |
| `disk_low` | ❌ | ✅ | yes |
| `cluster_incomplete` | ❌ | ✅ | yes |
| `storage_degraded` | ❌ | ✅ | no |
| `disk_full` | ❌ | ✅ | yes |
| `shutting_down` | ❌ | ❌ | no |

`disk_low` is red on purpose. Writes still succeed through it, which is exactly
why: the warn reserve exists so readiness goes red BEFORE the first write
fails, and a probe that only went red once writes failed would report the
outage rather than prevent it.

There is deliberately **no `starting` state**. It was in the first draft and it
is not reachable: `NewServerConfig` opens the default tenant's store and runs
`scanDegradedTenants` before it returns, and the mux does not exist until it
has. A state that cannot occur is the dead readiness arm this campaign has
already removed twice.

Severities are distinct and ordered by what an operator does about them, so the
reported summary state is the most actionable one while the `conditions` list
keeps all of them — an operator fixing a full disk still needs to know two
peers are missing.

`?format=json` returns the machine-readable report. It names tenants, peer URLs
and byte counts, so it is gated on the **metrics or admin** role — the same
class of information as `/metrics`, which that role already scrapes. An
unauthenticated caller gets `state`/`ready`/`live` and nothing else, and the
probe itself never answers 401: a 401 on a readiness probe takes the server out
of rotation for having authentication configured. The plain-text body keeps the
shape it had, because it is what an existing probe parses and a health endpoint
whose body changes shape breaks the monitoring at the one moment you least want
it broken.

A select-router probes its peers in parallel with a 500 ms timeout. Sequential
probes at one second each would make a five-peer readiness check a five-second
probe, and an orchestrator whose probe times out kills the process — the
crash-loop failure again, reached through the probe rather than the disk.

## Elasticsearch and SQL contracts

Both surfaces had the same defect: **a clause the server did not implement was
silently dropped**. That is the worst failure a query surface can have — a
dropped filter returns MORE documents than the client asked for, in a response
that is structurally valid, and a client filtering `status:error` and getting
every log line back cannot tell that from a store where everything is an error.

**Elasticsearch.** The body is decoded with `DisallowUnknownFields`, so an
unknown field is a 400 rather than a filter on the floor. `term`, `terms`,
`match`, `prefix`, `exists`, `match_all`, `bool.must`, `bool.filter`,
`bool.must_not` and `bool.should` map onto the planner's `Expr` tree — must_not
and should are not expressible as an implicit AND of predicates, which is why
they were dropped. `exists` maps to `NOT (field == "")`: a column the store
does not hold reads as empty for every row, which is exactly what exists means
here. It was decoded and never read before, so `exists` matched every document.

A **non-time `range` is refused with 400**, not ignored; the comment that said
"Phase 7" documented the gap for a reader of the file and for nobody sending
the query.

**`minimum_should_match` defaults to 0 beside a `must` or `filter` and to 1
otherwise**, which is Elasticsearch's rule: `should` next to `must` is
OPTIONAL. It was ANDed in unconditionally, which is Kibana's own shape — the
filter pills go in `filter` and the search bar goes in `should` — so anything
typed into the search bar emptied the dashboard, and an explicit
`"minimum_should_match": 0` was a 400 for the exact value ES defaults to. Both
0 and 1 are answered; a value needing a counting operator ("at least 2 of 5")
is still a 400 naming it, because an AND/OR/NOT tree cannot express it without
enumerating the combinations. A `should` arm is still PARSED when
`minimum_should_match` is 0 and it will be discarded, so an unsupported clause
under an optional arm is refused rather than accepted by accident.

**A clause that translates to "no filter" means something under `must_not` and
`should`.** `{"match_all":{}}`, `{}` and `{"bool":{}}` all match every
document, so under `must_not` the negation matches NONE and under `should` the
union with it matches EVERY document. Those arms used to be dropped, which
means the opposite: `must_not: [{"match_all":{}}]` is Elasticsearch's canonical
"match nothing" and Kibana emits it whenever a negated filter pill empties out,
and it answered every document in the index at 200.

A time range on `@timestamp`/`_time` narrows the scan window — what makes an
ES time filter as cheap as a LogsQL one — **when it sits in positive
conjunctive position** (top level, `must`, `filter`). Under `must_not` or
`should` it becomes an ordinary time predicate instead, because the window is
an AND over the whole query: lifting a negated range applied it un-negated,
and lifting two `should` alternatives intersected a union — both measured as
wrong answers at HTTP 200 (docs/wrong.md, the entry closing 124). Bounds
combine by intersection: `gte` beside `gt` applies both, and a second range
clause on the same field narrows rather than overwrites.

**Every bound value is parsed or the query is a 400 naming it** — never a
bound dropped, which until entry 124 was the fate of every spelling but
RFC3339. Accepted: RFC3339 with or without fractional seconds; a zoneless
datetime, date, month or year (completed by `time_zone`, default UTC); bare
epoch numbers and numeric strings with the unit inferred from magnitude
exactly as the HTTP `start`/`end` params infer it (`unixToNanos`) unless a
`format` names the unit; `format` (`||`-separated) covering `epoch_millis`,
`epoch_second` and the `date_optional_time`/`date_time`/`date` families —
Grafana's ES datasource stamps `"format":"epoch_millis"` on every time range,
and the strict decoder used to answer 400 `unknown field "format"` to all of
it; and `now`-anchored date math (`now-5m`, `now+1d-1h`, calendar-aware
`M`/`y`). Refused with the reason: date-math rounding (`/d`), `||` anchors,
unknown `format` or `time_zone` names, and a range object with no bound at
all. `TestESTimeRangeSpellingsAllApplyTheBound` drives every accepted
spelling with a far-future case AND a far-past control, so a bound that
refuses everything cannot pass as one that works.

A STRING is tried against the date layouts BEFORE the epoch rule, which is
ES's own order for a date field (`strict_date_optional_time||epoch_millis`):
`{"gte":"2030"}` is the year 2030, not 2030 seconds since the epoch. The 10-
and 13-digit epoch spellings are unaffected, because a 4-digit-year layout
rejects their extra digits.

**Every bound SATURATES on the int64-nanosecond domain** (1677-09-21 through
2262-04-11) rather than wrapping. An instant past the domain behaves as the
infinity it stands for — `gte 9999-01-01` matches nothing and `lte 9999-01-01`
matches everything — and the same rule covers the HTTP `start`/`end` params and
the LogsQL `_time:` filter, from one definition (`query.SatNanos`,
`query.SatAdd`, `query.SatScale`). A wrapped bound is not a near miss: it turns
a far-future lower bound into a far-past one, so a filter meaning "nothing"
answers everything.

`hits.total.value` is the number of **matching** documents. It was `len(rows)`
after `size` had been pushed into the scan as a `Limit`, so `size=10` over a
hundred matches answered `"total": 10` — and every ES client renders that as
"10 results". The scan is unbounded by `size`; `from`/`size` page the whole
result afterwards. No `size` at all is Elasticsearch's default of **10**, not
every document. `_count` decodes strictly too; its decode error used to be
discarded entirely, so a malformed body counted the whole store.

**SQL.** The parser stopped after `LIMIT` and ignored whatever followed, so
`LIMIT 5 OFFSET 10` dropped the OFFSET, `HAVING count(*) > 1` dropped the
HAVING, and a `JOIN` answered about one table. A leftover token is now a 400
naming it, with the supported subset spelled out in the message.

SQL also had **no row cap at all** — `SELECT * FROM logs` materialized every
matching row, and `ORDER BY` over an unbounded input is the shape task 6.4 was
about. It takes `-search.maxRows` like the LogsQL select, and an explicit
`LIMIT` becomes a `| limit` pipe that the scan honours, so a bounded query is
answered rather than refused.

Both handlers now run the scan **before** setting `Content-Type` and taking a
writer, and both use the typed stop reason. They used to commit the response to
NDJSON/JSON first, so a budget stop wrote its status into a response already on
the wire, and reported the generic 504 whatever the cause.

## Metrics

`metrics.go`. Own names (`simdlogs_tenants`, `simdlogs_groups`,
`simdlogs_rows`, `simdlogs_insert_requests_total`,
`simdlogs_query_requests_total`, `simdlogs_uptime_seconds`,
`simdlogs_storage_corrupt_groups`, `simdlogs_storage_quarantined_groups`,
`simdlogs_storage_degraded_tenants`,
`simdlogs_storage_degraded_unacknowledged_tenants`,
`simdlogs_storage_capacity_bytes`, `simdlogs_storage_warn_tenants`,
`simdlogs_storage_reject_tenants`, `simdlogs_storage_over_quota_tenants`,
`simdlogs_writes_rejected_disk_total`, `simdlogs_writes_rejected_quota_total`,
`simdlogs_scan_workers_total`, `simdlogs_scan_workers_in_use`,
`simdlogs_query_admission_in_flight`, `simdlogs_query_admission_queued`,
`simdlogs_query_admission_rejected_total`, `simdlogs_query_streamed_total`) plus the same
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

**Pipes.** `tail` runs the ROW-LOCAL pipes of its query and REFUSES the rest
with 400. A row-local pipe is a function of one row, so it behaves the same on
a stream as on a batch; `query.ClassifyPipe` decides which those are, and
there are 22: `fields`, `rename`, `delete`, `copy`, `filter`, `format`,
`extract`, `extract_regexp`, `math`, `len`, `hash`, `unpack_json`,
`unpack_logfmt`, `unpack_syslog`, `unpack_words`, `replace`, `decolorize`,
`pack_json`/`pack_logfmt`, `drop_empty_fields`, `json_array_len`,
`collapse_nums` and `unroll`.

The other 17 are refused, and the 400 names the pipe and gives one of three
reasons. The message is chosen per pipe on the STREAMING axis, not from the
distribution class: `ClassifyPipe` splits `| stats` by whether a COORDINATOR
can merge its partial states, and a live tail has no coordinator, so that split
must not reach the message. The buckets below are the measured answers of
`nonStreamingPipe` over every pipe type in the language, and each bullet opens
with the exact text of the reason the endpoint sends
(`internal/api/tail_refusal_test.go` asserts them one by one, and requires every
refused pipe to be named under its own reason here and under no other):

- **It is computed over the whole result set, and a tail's input never ends, so
  there is no point at which its answer is final.** `| stats` (every aggregate
  — `count()` and `avg()` alike), `| sort`, `| uniq`, `| top`, `| rank`, and the
  introspection pipes `| field_values`, `| field_names`, `| facets`,
  `| blocks_count`, `| block_stats`.
- **A tail re-runs its pipeline once per poll**, over just the rows that
  arrived since the last one, so it would apply to each poll's rows rather than
  to the stream: `| limit`, `| offset`, `| tail`, `| sample`. These are not
  "needs the whole result set" pipes — `LimitPipe.apply` is `rows[:N]` and
  `OffsetPipe.apply` is `rows[N:]`, prefix operations a stream could carry.
  Their distribution classes do not say it either, and the four do not even
  share one: `| limit`, `| offset` and `| tail` are `PipeGlobalOrder` because a
  shard's first N is not the cluster's first N, while `| sample` is
  `PipeCoordinatorOnly` because a shard sampling its own rows gives a stratified
  sample of the cluster rather than a sample of the cluster. Both are sharding
  properties, and neither has anything to say about streaming; what stops all
  four here is the poll.
- **It needs a second result set** — a subquery's rows, or the rows around a
  match — and a tail has only the rows that have arrived: `| join`, `| union`,
  `| stream_context`.

The name in the message is the LogsQL token, so it can be pasted back into a
query. It used to be the Go type lowered, which spelled five of them as tokens
the language does not have (`streamcontext`, `fieldvalues`, `fieldnames`,
`blockscount`, `blockstats`).

It previously discarded every pipe, silently, at 200: `* | filter level:error`
delivered `level=info` rows and `* | fields _msg` returned whole records.

**`limit` is ignored.** A tail has no last row to count back from, so both row
caps are cleared. It previously cleared `Limit` and kept `LastN` -- which is the
field `limit=` sets -- so a tail with `limit=N` delivered N rows PER POLL and
dropped the rest, for as long as it stayed open.

**`tail` is not federated.** In router mode it answers 501 rather than tailing
the router's own store: a cluster tail is a long-lived stream from every shard
merged by arrival time, and that merge has no completeness signal -- a shard
that stops answering drops out with nothing to say so. (This section previously
said it tailed the router's local store, "empty when nothing was written
locally", which `docs/lld/cluster.md` had already corrected.)

## Request admission

`middleware.go`. Every ingest route is wrapped by `guard`, which is the only
place a request body becomes readable.

- **Method.** A wrong method is `405` with an `Allow` header. Unwrapped, the
  handlers took any method, so a `GET` to an ingest path was processed as an
  empty `POST` and answered `200` with zero records.
- **Media type.** Each route has an allowlist; a mismatch is `415`. An empty
  `Content-Type` is allowed because several vendor agents send none. journald
  has its own spec (`application/vnd.fdo.journal`) rather than that type being
  accepted on the NDJSON routes, where a journal blob parses as nothing.
- **Body size.** `http.MaxBytesReader` at `MaxBodyBytes`; over it is `413`.
  Every handler previously called `io.ReadAll(r.Body)` with no limit.
- **Compression.** `Content-Encoding: gzip` is decompressed with a *separate*
  `MaxDecompressed` bound, because a few hundred KB of gzip expands to
  gigabytes and a wire-only limit accepts that happily. Malformed gzip is
  `400`; an unknown encoding is `415` rather than being treated as identity,
  which would store compressed bytes as log lines.
- **Error envelope.** JSON for the JSON and OTLP protocols, plain text for the
  query surface, so a client is not handed a body it cannot parse.

Limits come from `internal/config`; `config.Unlimited` (-1) is the explicit
opt-out and zero means "use the default".

## Authentication and tenancy

`auth.go`, `internal/config/auth.go`. Off unless `-auth.config` names a file,
and the server logs a warning at startup when it is absent -- a server open to
everyone should say so rather than leaving an operator to infer it.

**Credentials** are bearer tokens. The auth file stores only the hex SHA-256
of each token, so a leaked file is not a set of working credentials. Lookup is
by hash, so a timing difference leaks nothing about the token itself. An
unknown field in the file is an error: `"disable": true` must not read as
authentication being on. An empty token list is an error too -- `disabled:
true` is the explicit opt-out, because a list that means "no auth" when empty
turns a truncated config file into an open server.

**Roles** are `ingest`, `query`, `admin`, `metrics`; `admin` implies the rest.
Every route names the role it needs. `TestRoutePermissionMatrix` walks the
registered endpoints and asserts 401 without a credential, 403 with the wrong
role, and acceptance with the right one. Liveness stays unauthenticated: a
load balancer's probe carries no credential, and 401 there takes the node out
of rotation.

**Tenancy.** `AccountID`/`ProjectID` are a *request*, checked against the
principal's allowed tenants. They used to be the identity: any client could
name any tenant and read or write its data. A malformed id is now rejected
rather than rewritten to `0`, which silently sent a typo's data into the
default tenant. A principal scoped to exactly one tenant gets it by default; a
multi-tenant principal must name one, since the server picking would be a
guess.

**Ordering matters.** `withPrincipal` is the outermost middleware, because
tenant resolution runs in the outer chain while the role check runs inside the
mux. With authentication applied only per route, the principal was not in the
context when the tenant headers were checked, and they were believed
unconditionally -- the exact defect being fixed. A test pins the cross-tenant
rejection.

## Transport security

`internal/config/tls.go`. `-tls.certFile` and `-tls.keyFile` serve HTTPS;
`-tls.clientCAFile` additionally requires a client certificate signed by that
bundle (mTLS), with `RequireAndVerifyClientCert` -- a certificate that is
requested and not verified is decoration. TLS 1.2 is the floor, 1.3 preferred,
and ALPN advertises h2 so an HTTP/2 client is not silently downgraded.

Half a pair is an error rather than a quiet fallback to plaintext: a cert with
no key would otherwise start the server in the clear while the operator
believes TLS is on. A client CA with no server certificate is an error too,
instead of a setting that does nothing.

**Plaintext on a public address is refused.** `CheckListen` allows it only on
loopback, or with `-insecure-http`. Log data is tenant data, and a server that
binds every interface in clear text should take a deliberate flag rather than
being what happens when the operator forgets one.

The `-syslog` address gets `CheckPlaintextListen`, not `CheckListen`. That
listener is plaintext by construction and unauthenticated, writing to the
default tenant, so exempting it would have made the guarantee a half-truth
with the larger hole left open -- and `CheckListen` would have exempted it,
because `CheckListen` returns early once a certificate is configured, which a
syslog port never uses.

**mTLS is an identity, not only a gate.** A verified client certificate is
mapped to a principal by its subject common name through the auth file's
`certs` entries. Without such an entry the certificate proves only that the CA
trusts the client: the request then falls through to the bearer-token path and
is refused like any other credential-less request. `certPrincipal` reads
`VerifiedChains`, never `PeerCertificates`, so an unverified chain is never
trusted.

**The alternative trust boundary** is a terminating proxy. Set
`"trustedProxy": true` in the auth file, together with **both** `proxyRoles`
and `proxyTenants` -- there is no default, and the configuration is rejected
without them.

They used to default to every role and every tenant, which made *omitting* a
credential strictly more powerful than presenting one: a least-privilege token
got 403 on `/admin/backup` while an anonymous request got 200 and a tar of the
whole store. A default that outranks every configured credential is not a
default, it is a bypass. For the same reason `proxyRoles` may not include
`admin` when tokens or certs are also configured.

Bind loopback when using it -- a trusted-proxy server reachable directly is an
unauthenticated server -- and have the proxy strip and re-set the
`AccountID`/`ProjectID` headers, because they are authorization inputs and a
proxy that forwards a client's copy unchanged has handed the client the tenant
selector back.

Certificate rotation needs a process restart: there is no `GetCertificate`
callback, so the pair is read once at startup.
