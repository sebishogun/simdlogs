# Runbook: backup and restore

Every procedure here is executed by a test. A runbook nobody has run is a
document, not a procedure.

## Single node

### Take a backup

    curl -fsS -H "Authorization: Bearer $ADMIN_TOKEN" \
      -H "AccountID: $TENANT" -H "ProjectID: 0" \
      http://node:9428/admin/backup > backup.tar

One tenant per archive. The capture is snapshot-consistent: it holds a snapshot
for the whole stream, so the archive is the store at one moment rather than a
walk of a directory that was changing underneath.

`-fsS` matters. Without `-f`, curl writes an error body into `backup.tar` and
exits 0, and the failure is discovered at restore time.

### Validate before restoring

    simdlogs restore -src backup.tar -dry-run

Writes nothing. Checks every group's declared size, its CRC32C and a full
parse, against the manifest the archive carries — and prints the tenant KEY the
manifest names (`0:0`), which is what the next step needs.

### Restore

Onto a **clean directory** — absent or empty, and not a store any process has
open:

    simdlogs restore -src backup.tar -dst /var/lib/simdlogs/tenant-0-0 -tenant 0:0

`-tenant` is not optional in practice. It refuses the archive unless the
manifest names that tenant, and it takes the manifest's tenant KEY (`0:0`), not
the directory name (`tenant-0-0`). An archive restored into another tenant's
directory produces a store that answers that tenant's queries with someone
else's logs, and the manifest is the only place that fact is recorded.

Then start a server over the PARENT directory (`/var/lib/simdlogs`), not over
the store directory itself.

A pre-format-1 archive carries no manifest and cannot be checked against one;
`-allow-unverified` restores it and still exits non-zero.

### Verify

Compare against the origin if it still exists — row counts alone are not
enough, because a manifest that lost a group leaves the data on disk and
invisible:

    for q in '*' 'level:=error' '* | stats count() c'; do
      diff <(query origin "$q") <(query restored "$q") || echo "DIFFERS: $q"
    done

Tested by `TestDrillARestoredBackupAnswersIdentically`, which compares row
count, row content and query answers across five query shapes.

## Cluster

### Take a coordinated backup

    curl -fsS -H "Authorization: Bearer $ADMIN_TOKEN" -X POST \
      http://router:9428/admin/cluster/backup > cluster.tar

One tar holding `cluster.json` and one archive per shard, each taken from a
replica that holds the **whole** shard. Completeness is checked, not assumed.

**If it answers 503**, some shard has no complete replica. That is the backup
refusing to capture a short shard silently. Run `/admin/cluster/repair` until
it reports `copied: 0` and `complete: true`, then retake.

### Read the manifest before restoring

`cluster.json` is the first entry, so it can be read without unpacking
gigabytes:

    tar -xOf cluster.tar cluster.json | jq .

Check `shards` against the topology you are restoring into. A mismatch is
refused — restoring an N-shard archive into an M-shard cluster puts rows where
no query looks for them — and the per-shard `highWatermark` values show how far
apart the archives were taken.

### Restore a cluster

Per shard, using the single-node procedure above — including `-dry-run` and
`-tenant` on each — then start the routers.
There is **no single command** that unpacks a cluster archive: the archive is
validated here and the unpacking is the operator's, per shard.

## RPO and RTO

| | Value | Why |
|---|---|---|
| RPO, single node | the age of the backup | there is no continuous shipping |
| RPO, cluster | the age of the backup, plus the spread between shard archives (`ClusterManifest.Spread()`) | the shard archives are taken sequentially |
| RTO | restore time, dominated by untarring the archive | no replay step |

**Unresolved:** there is no incremental backup, so RPO is bounded by how often
a full capture is affordable. `Spread()` is reported and not bounded — no
threshold is right for every deployment, and one invented here would either
refuse good backups or bless bad ones.
