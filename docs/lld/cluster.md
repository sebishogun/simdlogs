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
