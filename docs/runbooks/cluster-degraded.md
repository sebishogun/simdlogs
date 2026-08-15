# Runbook: a degraded cluster

## Read the completeness signal first

Every cluster read is either complete or explicitly not. A router that cannot
reach a shard answers **503**, not a smaller result set. If reads are
succeeding, the answer is whole.

    curl -fsS http://router:9428/-/ready

Readiness is the router's own: its store is empty and stays that way, so red
readiness on a router is about the router process, not about the data.

## Diagnose

    curl -fsS -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
      http://router:9428/admin/cluster/repair | jq '.shards[] | {shard, divergent, replicas: [.replicas[] | {replica, err, highWatermark, groups: (.groups|length)}]}'

A repair pass reports every replica's state whether or not it copies anything,
so it doubles as the diagnostic. What to look for:

| Reading | Means |
|---|---|
| `err` set on a replica | unreachable, wrong protocol version, or refusing this router's credential — the class says which |
| `divergent` above 0 | the shard's replicas do not hold the same groups |
| `highWatermark` far behind its peers | that replica stopped ingesting at that time |
| `complete: false` with no `errors` | the pass hit its bounds; run it again |

## Common states

**One replica behind.** Run repair until `copied` is 0. Reads were already
correct — a router reads one replica per shard, and the read completeness gate
does not know a replica is short — so this is about redundancy, not about
wrong answers.

**A replica on another protocol version.** Class `version_mismatch`. It is
refused rather than merged: a field that moved would otherwise produce a wrong
answer instead of an error. Finish the rolling upgrade.

**A replica refusing the router's credential.** Class `unauthorized`, and
retrying another replica is pointless — the credential is the router's, so
every replica refuses it identically. Fix the router's auth configuration.

**A shard with no complete replica.** Backups refuse (503). Repair first.

## What a degraded cluster still guarantees

- No read returns a partial answer as a whole one.
- No write is reported stored unless it reached the consistency level asked
  for; the default is `all`, and it stays `all` until repair is proven in the
  deployment.
- No repair pass destroys data: it only adds, and it validates at the receiver.

## What it does not

- There is no automatic repair. Repair is an operator action.
- Repair moves data between replicas of one shard, never between shards, so it
  cannot help a rebalance.
- A replica that is short and reachable will still be read from, and its answer
  will be short. The read completeness gate detects a shard that did not
  ANSWER, not one that answered with less than its peers hold.
