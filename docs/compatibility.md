# Compatibility

What is frozen, what may change, and where each promise is checked. A promise
with no test beside it is a promise nobody is keeping.

## HTTP surface

47 routes, each classified federated / router-local / refused, and the
classification is checked against the mux itself
(`TestEverySurfaceRouteIsClassified`) — a handler added without being
classified fails a build.

### Response shapes

Frozen for ten routes as golden files in `internal/api/testdata/golden/`
(`TestFrozenResponseShapes`). What is pinned is the set of keys at each level
and the JSON type of each value, not the data: an array's length is data, a
response with three rows and one with four are the same contract.

A field **disappearing or changing type** breaks every client that reads it,
and that is what the goldens catch. A field **appearing** is compatible for a
client, but it still fails the build: the comparison is over the whole shape
file, so any change requires a deliberate regeneration. That is the intent --
the build asks whether the change was meant, and the answer for an added field
is yes. They are
regenerated with `SIMDLOGS_WRITE_GOLDEN=1`, which is a decision that the
contract changed — doing it to make a red build green is how a breaking change
ships.

### Range buckets

One convention for where a bucket STARTS and how far it REACHES, on both range
surfaces, and it is the reference's.

**The `step` parameter itself is still parsed by two different functions**, and
they disagree. `/select/logsql/hits` uses `hitsStepNs` (`internal/api/server.go`);
`/select/logsql/stats_query_range` uses `parseStepNs`. Measured on one node,
one window `start=2026-06-01T00:30:00Z end=02:30:00Z`:

| `step=` | `/hits` | `/stats_query_range` |
|---|---|---|
| `1800` | 120 buckets @ 1 min | 4 points @ 1800 s |
| *(absent)* | 120 buckets @ 1 min | @ 240 s (window/30) |
| `0s` | @ 1 min | @ 240 s |
| `-5m` | @ 1 min | @ 240 s |

Pre-existing at HEAD, and not covered by `TestTheTwoRangeSurfacesAgreeOnBuckets`
(one window, `step=1h`) or `TestRangeSurfaceCompat` (`1m`, `2m`, `90s` -- all
Go-parseable, all identical under both parsers). Unifying the two parsers is
its own change.

Also unmatched against the reference: `step=1d`/`1w`/`1y`, where VictoriaLogs
returns one wide bucket and this server returns 120 one-minute buckets with a
different total, both at 200; and `1800`, `abc`, `0`, `-1h` and empty, where
VictoriaLogs answers **400** and this server answers 200. Both pre-existing.

| Surface | Bucket starts | Bucket covers | Function |
|---|---|---|---|
| `/select/logsql/hits` | floored to a multiple of `step` | `[k*step, (k+1)*step)` | `query.fillHits` via `alignDown`, scan window from `query.BucketSpan` |
| `/select/logsql/stats_query_range` | floored to a multiple of `step` | `[k*step, (k+1)*step)` | `query.StatsQueryRange`, and `exactMatrix` on a router |

A bucket is a whole step whatever `start` and `end` fall on, so the first
bucket can begin **before** the requested `start` and the last can end
**after** the requested `end`. Both surfaces walk `bs < end`, so the bucket
COUNT is still the caller's.

This is measured against `internal/bench/victoria-logs`, not inferred. Six rows
at 00:15, 00:45, 01:15, 01:45, 02:15 and 02:45 on 2026-06-01Z over
`start=00:30Z&end=02:30Z&step=1h`, both binaries:

| Surface | VictoriaLogs | simdlogs |
|---|---|---|
| `/hits` | 00:00Z, 01:00Z, 02:00Z = 2, 2, 2, total 6 | identical |
| `stats_query_range` | 00:00Z, 01:00Z, 02:00Z = 2, 2, 2 | identical |

The `total` on `/hits` is **6**, which is more than the four rows inside
`[start, end)`: the two edge buckets count instants the caller did not name.
That is the reference's answer. The point query is not widened —
`/select/logsql/query` over the same window returns those four rows on both
engines.

`stats_query_range` omits a bucket in which a series has no rows, the
Prometheus matrix convention; `/hits` is dense and reports `0`. That is the one
remaining shape difference between the two surfaces and it is the reference's
too.

An explicit `start`/`end` is left untouched by `boundRangeBuckets`, so nothing
re-aligns downstream. Pinned by `TestTheTwoRangeSurfacesAgreeOnBuckets` (node,
both surfaces) and `TestTheRouterAndNodeAgreeOnRangeBuckets` (router against
node, through `exactMatrix`); either walk moving is a wire change.

An earlier build had the two surfaces disagreeing: `stats_query_range` anchored on the
request's own `start` and both surfaces clamped their edge buckets to
`[start, end)`. The same window gave a different bucket count, different labels
and different per-bucket values on the two routes, and neither matched the
reference. See `docs/wrong.md` entry 136.

### Error envelopes

Two of them, deliberately:

| Surface | Envelope | Why |
|---|---|---|
| read (`/select/...`, `/_search`, `/_count`) | `text/plain` | drop-in for VictoriaLogs, whose clients parse text |
| ingest (`/insert/...`, `/_bulk`, vendor paths) | JSON: `error`, `status`, and on a partial ingest `accepted`, `rejected`, `rejectedAt` | a shipper needs to know what landed |

Both pinned by `TestFrozenErrorEnvelopes`. The body's `status` always equals the
HTTP status; a client may branch on either.

### Authentication

Bearer token, or `X-Simdlogs-Token` for agents that send it bare. Tokens are
matched by SHA-256. Roles: `ingest`, `query`, `admin`, `metrics`. Tenancy is
`AccountID` / `ProjectID`, numeric, and a credential may only name tenants it
holds. See `docs/security.md`.

## Storage format

| Version | Read | Write |
|---|---|---|
| v7 | yes, and gated | no |
| v8 | yes | yes |

v8 is v7's body plus a trailing CRC32C over every preceding byte. v7 had no
integrity check, which is why v8 exists.

**v7 read compatibility is a gate, not a hope.** Five committed fixtures in
`internal/storage/testdata/v7/` are read on every test run and their VALUES
compared — not merely their row counts, which come from the header and would
pass with a decoder returning garbage for every field
(`TestV7FixturesStillRead`). A fixture regenerated by a newer writer fails
loudly, because that would destroy the evidence it exists to provide.

A store written by this build cannot be read by a v7-only binary. Downgrade is
not supported; restore from a backup taken by the older binary instead.

## Cluster protocol

`ProtocolVersion = 1`, in `X-Simdlogs-Protocol` on every internal request and
response.

**A peer on an unknown version is refused, never merged.** A router that spoke
two versions would have to merge results from both — every merge written twice
and tested once — and a field that moved would produce a wrong answer instead
of an error. During a rolling upgrade a node on the wrong version is reported
as an incomplete answer, which the caller can see.

The version is bumped when the meaning of any protocol header changes, and NOT
when a public response shape changes. Those are different promises: one between
versions of this binary, the other to clients.

## Rolling upgrade

Order: **storage nodes first, routers last.**

A router understands its own protocol version and refuses others. Upgrading
routers first would mean every storage node is on the old version and
unreachable, which takes reads down; upgrading storage nodes first means the
old routers keep talking to them until each is upgraded in turn.

During the roll, with mixed versions:

- Reads that reach an un-upgraded node get `version_mismatch` for that shard.
  The read is reported **incomplete** — 503 by default, or 206 with
  `allow_partial_response=1` — never a short answer with a 200.
- Writes at the default consistency level (`all`) fail while any replica of the
  shard is unreachable. That is the intended behaviour: the alternative is a
  replica silently missing data with no repair having run.
- `/admin/cluster/repair` reports the mismatch per replica rather than
  attempting a transfer.

A node is safe to restart at any point: a group is written, fsynced and renamed
before its manifest record is committed, so a crash leaves an uncommitted file
the next open ignores.

## What is not frozen

- Most things behind `SIMDLOGS_*` environment variables (bench, soak, fixture
  regeneration) are development controls. `SIMDLOGS_STREAM_FIELDS` is NOT: it
  sets which fields identify a log stream, so it changes the schema of what is
  ingested and is a production setting.
- The internal Go packages. `internal/` is not an API.
- Metric names are a contract, asserted in both directions by
  `TestEveryContractedMetricIsPresentAndTyped` and
  `TestNoMetricIsEmittedOutsideTheContract`, with
  `TestNoMetricCarriesAnUnboundedLabel` bounding cardinality,
  but metric VALUES and cardinality are not.
- Query performance. The numbers in README and `docs/scale-curve.md` are
  measurements of a machine at a moment, and carry the machine with them.
