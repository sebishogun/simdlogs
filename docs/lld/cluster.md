# LLD: cluster

Source: `internal/api/cluster.go`.

> **Status: experimental, not production-safe.** The router's merge code for
> four endpoints is stale — it decodes envelopes the backends no longer send
> and answers bogus/empty results (details in "Merges per endpoint" below).
> Several select surfaces are not federated at all and silently use the
> router's local store in router mode. Until the defects below are fixed and
> fixture-tested (planned in
> `docs/plans/2026-08-13-simdlogs-production.md`, Phase D), do not treat any
> router-mode answer as the cluster-wide answer.

## The internal protocol

`cluster_protocol.go`, `cluster_client.go`. Internal responses carry an
envelope in headers — version, shard, replica, completeness, high watermark,
error class, trace id — and the public response bodies are no longer the merge
state.

They were. A router called the same public endpoints a client calls and merged
the public bodies, which has three consequences and all of them are bad. A
public response has no field for "I skipped two groups because my disk is
degraded", because a client has no use for one, so a router merging public
bodies **reports a short answer as a whole one**. A public shape is a promise
to clients and an internal one is a promise between versions of this binary;
tying them together means neither can move. And there was no version at all, so
a router talking to a node from another release silently merged whatever came
back — a field that moved produced a plausible wrong answer rather than an
error.

An unknown version is **refused, not negotiated**, in both directions. A router
that spoke two versions would have to write every merge twice and test it once;
during a rolling upgrade the honest behaviour is that a node on an unknown
version is one this router cannot use, reported as incomplete where the caller
can see it.

| class | meaning | another replica? |
| --- | --- | --- |
| `unavailable` | refused, timed out, TLS failed, DNS | yes |
| `version_mismatch` | speaks a protocol this binary does not | yes — the rest of the shard may be upgraded |
| `overloaded` | refused for a budget reason | yes |
| `unauthorized` | this **router's** credential was refused | no — every replica refuses it identically, so retrying turns one 401 into N and delays the report |
| `malformed` | unparseable or oversized | no — it will return the same thing |
| `degraded` | answered from an incomplete store | — |

**The client.** Every peer call used `http.DefaultClient`, which has no timeout
at all: a peer that accepts the connection and never answers held a router
goroutine, a pool slot and the caller's request for as long as the caller
waited — times the number of shards. It also shares a process-wide transport
with any other outbound HTTP and cannot carry a client certificate, so mTLS
between nodes was not expressible. The peer client has its own transport with
dial, TLS-handshake and response-header timeouts, its own pool, and no client
timeout — the caller's context carries the deadline, and a second one here
would cut a query the caller was still waiting for.

The body is **bounded** (256 MiB) and an oversized one is **discarded, not
truncated**: a truncated JSON document is unparseable and a truncated NDJSON
stream is a partial answer indistinguishable from a complete one. Redirects are
not followed — a peer does not redirect, and following one would send the
router's credential to whatever host the response named.

**Headers are forwarded explicitly**, not copied wholesale: the resolved
`AccountID`/`ProjectID` and the tracing headers, and nothing else. Copying the
set would forward the client's `Authorization` and cookies to every storage
node, which is how one node's compromise becomes the cluster's — the router
authenticates to peers as *itself*.

Failures are **returned, not swallowed**. Every replica error was a bare
`continue` and the whole shard a `return nil, false`, so a caller could not
tell "this shard has no data" from "every replica is down" from "the credential
was refused" — and the merge treated all three as an empty contribution.
`fanOutPeers` returns one `PeerResponse` per shard including the failures;
`bodiesOf` keeps the old `[][]byte` shape for merges that have not yet learned
to report completeness (task 8.3).

## Read completeness

Every merge consumed a `[][]byte` with a nil entry for a shard that did not
answer, and merged the rest. A cluster read with one shard down returned the
other shards' rows, **HTTP 200**, with nothing anywhere in the response to say
a third of the data was missing — indistinguishable from a query that genuinely
matched fewer rows. Confident, plausible and silent.

**The rule: a read fails unless every shard contributed a complete answer.**
Not "every shard answered" — a shard serving from a degraded store answers, and
says so in the envelope, and that answer is missing data too. It counts as
missing, and the only difference is that it looks fine.

A refusal is **503** with the shards named:

```
X-Simdlogs-Shards-Total: 3
X-Simdlogs-Shards-Answered: 2
X-Simdlogs-Shards-Missing: 1(unavailable)
```

**Partial answers are opt-in.** `allow_partial_response=1` — the reference's own
parameter name — is answered **206** with `X-Simdlogs-Partial: true` and the
same missing-shard headers. A dashboard that would rather draw something than
nothing is a real use, and so is an operator triaging with a node down; but it
has to be asked for, because a caller who did not ask has no way to know, and a
monitoring system built on silently-partial answers alerts on the wrong thing
at the worst time.

206 rather than 200-with-a-header because a client that checks `resp.ok` sees
200 for both. The gate returns the `http.ResponseWriter` the handler must use,
and on a partial answer that writer forces 206 on the first write — the status
cannot be set in the gate itself, because every handler sets its own
Content-Type and then writes, so a `WriteHeader` there would be overtaken by
the handler's first `Write` sending 200. `federatedSelect` writes its status
explicitly for the same reason in reverse: an empty result writes no bytes at
all, and a handler that returns without writing sends 200.

`simdlogs_cluster_partial_reads_total` counts them, because a dashboard quietly
running on partial answers is exactly the state this makes visible.

All nine federated endpoints are covered by one-shard-down and all-shards-down
tests: select, hits, stats_query, stats_query_range, field_names, field_values,
streams, `_count` and `_search`.

## Replicated writes: idempotency and consistency

`forwardWrite` replicated to every member of a shard and relayed the **last**
response's status. Replica A refusing on its own quota and replica B accepting
answered whichever finished last, so the same write was reported stored or
refused by a coin flip — and in the other order, a retry duplicated into the
replica that had already taken it.

**Consistency is explicit.** `X-Simdlogs-Consistency: one | quorum | all`,
default **all**. Quorum is the usual production choice *because a repair
process reconciles the replicas that missed a write* — and this has none yet
(task 8.7). Without one, "quorum" means a replica silently missing data forever
and a read that lands on it returning a short answer with nothing to say so.
Defaulting to the strictest level is the honest position for a system that
cannot yet heal; the default moves when repair is proven, not before.

**Every write carries an id.** The router mints one (or accepts the client's,
so a retry can name the write it repeats) and sends the same id to every
replica. A replica that already committed it answers `X-Simdlogs-Duplicate:
true` and stores **nothing** — which counts as an acknowledgement, because the
data is there and that is the only thing the level asks about.

That is what makes a retry safe. The router cannot tell "did not commit" from
"committed and the answer was lost" — the connection drops while the response
comes back, the router times out, the process is killed between the fsync and
the reply — and those need opposite responses. Without receipts, retrying
duplicates every row on the replicas that did commit, *silently*, because a log
store has no primary key and a duplicated line looks exactly like a line that
happened twice; not retrying loses the rows on the ones that did not. A failed
write therefore returns its id, so the retry is safe by construction.

Ids are cryptographically random, not counters (which collide across routers —
the multi-writer case this exists for) and not content hashes (which would fold
two genuinely distinct identical batches into one, dropping data the client
sent on purpose). A client-supplied id is validated to 8–64 hex characters
before it reaches the manifest.

**Where the receipt is committed, and the window that leaves.**
`AppendGroupIdempotent` commits the id in the *same* manifest record as the
group — one record is one transaction, so the rows and the receipt become
durable together. The server's ingest path cannot use it: the writer batches
rows from many requests, so no single group is "this request's rows". It uses
`CommitReceipt` after the flush instead, which leaves a window — a crash
between the group commit and the receipt commit keeps the rows and loses the
receipt, so a retry stores them again. Given a choice between a duplicate and a
loss that window takes the duplicate; recording the receipt first would lose
the rows while claiming they were stored and refuse the retry that would have
saved them.

A replicated write therefore pays a **flush**. An ordinary client write carries
no id and pays nothing.

**Retention is bounded** at 65536 ids, and the bound is the honest limit: a
retry arriving after that many further writes is not recognised and will
duplicate. A count rather than a duration because the manifest has no clock —
records carry a sequence, not a timestamp.

**Per-replica detail is authorized.** Replica URLs and their individual
failures are the cluster's topology; an ingest client is told `acked`,
`required` and the write id, an operator is told which machine failed. On a
server with no `-auth.config` everything is open, as it is everywhere else.

## Roles

- **Storage node** — a default node: owns tenants' stores, serves ingest and
  queries locally.
- **Select router** — a node started with `-select-backends
  http://node1:9428,http://node2:9428,...`: the vmselect role. Ingest paths
  forward to storage nodes; select paths fan out and merge.

A node is one or the other by flag; there is no automatic membership, no
leader election, no consensus, and no cross-node transaction protocol. Shards
and replicas are the static configuration the operator passes at startup.
This is application-level sharding and replication, deliberately not a
consensus system — the README says the same thing.

## Sharding and replication

`SetReplicas(r)` groups the backends into shards of `max(1, r)` consecutive
URLs (`shards()`). `r = 1` is plain sharding; `r > 1` replicates each shard's
data to every member.

- **Write** (`routeWrites` → `forwardWrite`): a write path (`/insert*`,
  `/_bulk`, `/v1/logs`, `/v1/input`, `/api/*`, `/loki*`) is forwarded with a
  round-robin cursor (`s.rr`, atomic) to one shard, and to EVERY replica in
  that shard, with the request headers cloned (tenant headers ride along).
  The last response is relayed; if no replica answered at all the client gets
  a 502. A replica loss never loses data, and a burst of inserts spreads
  across shards.
- **Read** (`getFromShard`): each shard is asked via one replica at a time —
  a downed replica (connect error or ≥ 500) is skipped for the next in the
  shard, so replicated data is read exactly once per shard and never
  double-counted.

## Merges per endpoint

| Endpoint | Merge | Status |
|---|---|---|
| `/select/logsql/query` | `federatedSelect`: query all shards concurrently; rows merge in time order (newest first, parsed from each line's `_time`), `limit` applies across the merged set. Rows are kept as slices into each shard's response body (no per-row string copies — that was the router's dominant cost); the timestamp parse uses the fast RFC3339Nano path. | works |
| `/select/logsql/hits` | `federatedHits` decodes `{"hits":[{"_time":..,"hits":..}]}` — the OLD object shape — but the backend `selectHits` now answers the dense series shape `{"hits":[{"fields":..,"timestamps":[..],"values":[..],"total":N}]}`. Each dense series object decodes as `{"_time":"","hits":0}` and all collapse into one map entry: **the router answers exactly `{"hits":[{"_time":"","hits":0}]}` — one bogus zero bucket — for any non-empty store**. | **broken** (bogus zero bucket) |
| `/select/logsql/stats_query` (plain, no `by=` param) | `federatedStatsQuery` decodes `{"count":N}` from each backend, but the backend answers the Prometheus vector envelope `{"status":"success","data":{"resultType":"vector","result":[...]}}`. No `count` field exists: **the router always answers `{"count":0}`**. | **broken** (bogus zero) |
| `/select/logsql/stats_query` (`by=` param) | The backend's `by=` extension answers `{"stats":[{value,hits}]}`, which `federatedStatsQuery` decodes and sums per value. | works |
| `/select/logsql/stats_query_range` | `federatedMatrix`: each shard's series concatenated (shards hold disjoint groups, so a series is one shard's); decodes the backend's `{"data":{"result":[...]}}` envelope. | works |
| `/select/logsql/field_values`, `stream_field_values`, `field_names`, `stream_field_names` | All four route to `federatedValueCounts` (key `"values"`): backends answer `{"values":[...]}` and hits are SUMMED per value, sorted by count desc then value. (`federatedStrings` exists in `cluster.go` but is not wired to any route.) | works |
| `/select/logsql/streams` | Router sends key `"streams"`, but the backend `streams` handler answers the shared `{"values":[...]}` envelope — `v["streams"]` does not exist: **the router answers `{"streams":[]}` even when backends hold streams**. | **broken** (empty answer) |
| `/select/logsql/stream_ids` | Same key mismatch (`"stream_ids"` vs `{"values":[...]}`): **always `{"stream_ids":[]}`**. | **broken** (empty answer) |
| `/_search` | `federatedESSearch`: hits merged, `hits.total` summed, `relation: "eq"`. | works |
| `/_count` | `federatedESCount`: counts summed. | works |

Tenant headers (`AccountID`, `ProjectID`) propagate on every fan-out, so each
backend answers for the same tenant.

The merge for `select` is exact: rows are independent, so
concatenate-sort-limit is the correct distributed answer. Do not generalize
from it: the four broken rows above are stale-envelope defects, not design
limits, and until they are fixed the only correct statement is that **no
router-mode merge beyond `select` (and the working rows above) is verified**.

## Not federated in router mode

These endpoints have no router branch in `internal/api/server.go`: in router
mode they run against the router's OWN local store (which, with
`-select-backends`, holds only what was written locally — normally nothing),
or answer from router-local state. A client that points any of these at the
router silently gets local/empty answers, not a cluster-wide one:

- `/select/logsql/facets`
- `/select/logsql/tail`
- `/select/sql`
- `/select/vector`
- `/admin/backup` (backs up the router's local tenant stores only)
- `/metrics`, `/alerts` (router-local counters and rules)
- `/flags`, `/health`, `/-/healthy`, `/-/ready`, `/vmui`, `/` (UI)

All six introspection endpoints answer the shared `{"values":[...]}` envelope
locally; only `field_names`, `field_values`, `stream_field_names`,
`stream_field_values` currently have a working router merge for it.

## Failure behavior

- A downed replica: reads skip to the next in the shard; if all fail, that
  shard contributes nothing (a partial answer, not an error).
- A write with all replicas of the chosen shard unreachable: 502
  `all replicas unreachable`.
- Reads tolerate it by construction; writes are synchronous to every replica
  of the chosen shard (replication is write-path, not background).

## Measured state

`docs/wrong.md` ("Cluster scaling: at one-node scale the cluster is pure
overhead"): the federated path measured as pure overhead when the cluster is
a single node — expected, since a one-node cluster should run a storage
node. The cluster scaling benchmark (`internal/bench/cluster_test.go`)
exercises the router with real fan-out, and the zero-copy federated merge
(byte slices into response bodies) is the fix that kept the router off the
hot path for big results. None of that measures answer correctness — the
envelope defects above are exactly what such a benchmark cannot see.

## What this is not

No consensus, no leader election, no transactions, no automatic membership
discovery, no cross-node query planning beyond one-replica-per-shard fan-out,
no shard rebalancing when the backend list changes (the operator restarts the
router with the new list), no merge for avg/quantile stats, and — today — no
correct merge for `streams`, `stream_ids`, plain `stats_query`, or `hits`,
plus no federation at all for the endpoints listed above. Those are roadmap
items with measurable exits, not current behavior.
