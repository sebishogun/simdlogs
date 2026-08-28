# Runbook: recovery

## One corrupt group

**Symptom.** Readiness is red; the store reports a corrupt or quarantined
group; a query returns fewer rows than expected, or the process refuses to
start.

**What the server does on its own.** Refuses. Damaged bytes are never served as
data — the group's checksum fails and the group is not readable. Under the
strict corruption policy the store refuses to open at all, which is an explicit
failure rather than a partial answer. Tested by
`TestDrillACorruptGroupIsRefusedNotServed`.

**Options, in order of preference:**

1. **Restore the tenant from backup.** Loses everything since the backup; keeps
   everything else exact.
2. **In a cluster, repair from a healthy replica.** The damaged group has a
   different digest from the good copy, so anti-entropy sees the good one as
   missing here and copies it in. Repair never deletes, so the damaged file
   stays until an operator removes it.
3. **Acknowledge and continue without the group:**

       curl -fsS -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
         -H "AccountID: $TENANT" \
         http://node:9428/admin/acknowledge-degraded

   This is a decision to serve short answers knowingly. It is refused on a
   router (501), because a router's own store is empty and acknowledging there
   clears nothing on the shards that are degraded.

**Do not** delete group files from a running store's directory. The manifest
names them; a file removed underneath is a store that will fail its next read
of that group rather than skip it.

## One lost replica

**Symptom.** One replica of a shard is unreachable or has been rebuilt empty.
Reads still succeed — the surviving replica serves — but the shard has no
redundancy.

**Procedure:**

1. Bring the replacement up **empty**, with its own data directory.
2. Point the router at it, in the same position in the backend list. Position
   decides shard membership; a replacement in the wrong position joins the
   wrong shard.
3. Run a repair pass:

       curl -fsS -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
         http://router:9428/admin/cluster/repair | jq .

4. Repeat until `copied` is 0 and `complete` is true. Each pass is bounded at
   64 groups and uses a 1 GiB accounting budget; `remaining` says what it left.
   Actual bytes are charged after each group, so a peer whose inventory
   understates a group can move that final group past the budget, but never
   beyond 2 GiB because each group is capped independently at 1 GiB. A group
   larger than 1 GiB is refused on every pass and requires the shard to be
   restored or reseeded outside this endpoint.

Tested end to end by `TestDrillALostReplicaIsRebuiltByRepair`, including that
the surviving replica is unchanged and a second pass copies nothing.

**Why repair is safe to run at any time.** It only ever adds; it validates every
transfer at the receiver; it reconciles by content, not by manifest id; and an
unreachable replica is never read as an empty one.

## A whole shard down

Reads **fail** with 503 rather than returning the surviving shards' rows. That
is deliberate: a partial answer with a 200 is indistinguishable from a smaller
result set. Tested by `TestChaosAWholeShardDownFailsTheRead`.

Bring a replica of that shard back, or accept the outage.

**There IS an opt-in for a partial answer**, and this runbook used to deny it in
the one section where an operator would want it:

    curl -sS 'http://router:9428/select/logsql/query?query=*&allow_partial_response=1'

With a shard fully down that answers `206 Partial Content` and
`X-Simdlogs-Partial: true`, carrying the surviving shards' rows. It is opt-in
because the default must not be a short answer with a 200; a caller that asks
for it has said it can handle one.

## Process killed mid-write

Nothing to do. A group is written to a temp file, fsynced, and renamed, and the
manifest record is committed after. A crash before the commit leaves an
uncommitted file the next open ignores. The crash matrix is repeated 25 times
nightly (`.github/workflows/fuzz.yml`).

An uncommitted file is **not** reclaimed automatically — it cannot be told from
a committed group whose record replay could not reach. It is disk that an
operator removes by hand, and it is named `group-N.bin` with no manifest entry.
