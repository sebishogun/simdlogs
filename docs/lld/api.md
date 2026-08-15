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

`/_search`, `/_count` — a log-relevant Query DSL subset: `bool`
(`must`/`filter`), `term`, and timestamp `range`. Range on a
time field feeds the group skip directly. `_bulk` on the ingest side; the
response gives per-document `{"create":{"status":201}}` items (the shape ES
clients parse to decide retry).

Not implemented (documented, not accidental): `_msearch`, the complete Query
DSL, and ES aggregation-response compatibility. `exists` is decoded but
ignored — `esToQuery` walks `Bool`, `Term`, and time `Range` only
(`internal/api/es.go`), so an exists-only search matches the whole window;
recorded as `docs/wrong.md` entry 37. `terms` is not supported either.

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
  multiples — the alignment the reference uses.
- ES `_search` returns the ES envelope with `hits.total.relation: "eq"`.

## Bounds and errors

- `-search.maxRows`: zero now means the built-in default rather than
  unlimited, which is what it used to mean. A bare (no-pipe) select over the cap errors with an
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
| `-search.maxRows` | cap on a bare select's rows; 0 = built-in default, -1 = unlimited |
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
reaching eight distinct handlers; a check written into each is a check that
will be missing from the ninth. (This paragraph said "six entry points reaching
four functions" — both numbers were wrong, which is what a count nothing
gates does.)

The HTTP mux is not every write path. The native syslog listeners take bytes
off a socket with no middleware anywhere near them; they call the budget
themselves (`syslogAdmits`). Neither transport can answer — RFC 5426 has no
reply and the TCP framing carries no ack — so a refused message is dropped,
counted as a skipped row, and logged at most once every 30 seconds. A tenant
whose store cannot even be opened (no space, no permission, EIO) answers 507
too: that failure happens in the resolver, before the budget middleware runs,
and used to fall through to a 400.

The free-space sample is cached for about two seconds, so a burst of small
writes is not a burst of `statfs` calls. That staleness is why the threshold is
a RESERVE: there is room to be wrong by one interval's worth of writes. A
filesystem that cannot be measured does NOT refuse writes — turning a `statfs`
failure into a write outage is the protection causing the harm it exists to
prevent. The per-tenant cap still applies there, because it is measured from
the store's own groups and needs no filesystem call. That was false when first
written: `QuotaState` returned at the `statfs` error before reaching the cap,
so a platform without `statfs` enforced neither budget. `statfs` is
implemented on linux, darwin and the BSDs; everywhere else — Windows,
illumos, plan9 — only the tenant cap applies.

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

`tail` is not federated: in router mode it tails the router's local store
(empty when nothing was written locally).

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
