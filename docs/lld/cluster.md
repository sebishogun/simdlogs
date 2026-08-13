# LLD: cluster

Source: `internal/api/cluster.go`.

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

| Endpoint | Merge |
|---|---|
| `/select/logsql/query` | `federatedSelect`: query all shards concurrently; rows merge in time order (newest first, parsed from each line's `_time`), `limit` applies across the merged set. Rows are kept as slices into each shard's response body (no per-row string copies — that was the router's dominant cost); the timestamp parse uses the fast RFC3339Nano path. |
| `/select/logsql/hits` | `federatedHits`: per-bucket counts summed by bucket start. |
| `/select/logsql/stats_query` | `federatedStatsQuery`: total count summed; `by=` sums each value's hits. avg/quantile across shards need sum+count or sketch merge — a documented follow-up, not implemented. |
| `/select/logsql/stats_query_range` | `federatedMatrix`: each shard's series concatenated (shards hold disjoint groups, so a series is one shard's). |
| `/select/logsql/field_values`, `streams`, `stream_ids`, `stream_field_values` | `federatedValueCounts`: hits summed per value, sorted by count desc then value. |
| `/select/logsql/field_names`, `stream_field_names` | `federatedStrings`: union, sorted. |
| `/_search` | `federatedESSearch`: hits merged, `hits.total` summed, `relation: "eq"`. |
| `/_count` | `federatedESCount`: counts summed. |

Tenant headers (`AccountID`, `ProjectID`) propagate on every fan-out, so each
backend answers for the same tenant.

The merge for `select` is exact: rows are independent, so
concatenate-sort-limit is the correct distributed answer. The hits/stats
merges are exact for counts; the avg/quantile caveat above is the one
documented inexactness.

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
hot path for big results.

## What this is not

No consensus, no leader election, no transactions, no automatic membership
discovery, no cross-node query planning beyond one-replica-per-shard fan-out,
no shard rebalancing when the backend list changes (the operator restarts the
router with the new list), and no merge for avg/quantile stats. Those are
roadmap items with measurable exits, not current behavior.
