# LLD: storage

Source: `internal/storage/` (store, group, column, dict, postings, footer,
cold, backup, retention, recompact, mmap_unix).

## The unit: a group

A group is an immutable columnar row group of up to `MaxRows = 128K` rows
(`internal/storage/group.go`). The 64–128K granularity is the point of the
design: skip structures at this size versus the reference's 8M-row blocks
(`lib/logstorage/consts.go:29`) are what make a selective query touch 128K
rows where VictoriaLogs touches 8M.

On-disk layout:

```
header:  magic u32 ("slog" 0x736C6F67), version u32 (8), rows u32, columns u32
columns: per column: name, type, width, rows, data
footer:  timeMin i64, timeMax i64, per-column meta records, footer-len u32
v8 only: crc32c u32 over every preceding byte
```

The footer is read first (its length is the four bytes before the checksum in
v8, the last four in v7), so a query consults skip metadata without decoding
any column.

**Versions.** The writer emits **v8**; `ReadGroup` reads v7 and v8 and rejects
anything else by version rather than by crashing on its layout. v7 is the
pre-checksum body and stays readable indefinitely -- an operator upgrading a
binary must not have to rewrite a disk of groups.
`internal/storage/testdata/v7/` holds five golden v7 blobs captured from the
last v7 writer, and `TestV7FixturesStillRead` fails if any stops parsing.
Those files are only regenerated under `SIMDLOGS_WRITE_V7_FIXTURES=1`, so a
normal run cannot silently rewrite the compatibility evidence with v8 bytes.

**v8 adds integrity.** A CRC32C (Castagnoli, hardware-accelerated on the
targeted architectures) over the whole body is verified before any structure
is parsed, so a flipped bit is an error rather than a wrong answer.

**Parsing is bounds-checked** (`group_read.go`). Every footer read goes
through a cursor that validates the remaining length, and every column's
data/postings/dict span is checked to lie inside the data region, with the
sum tested as `off > limit || length > limit-off` so a wrapping `off+len`
cannot pass. Row count, column count, bloom words and name length are capped
before any allocation, because those counts come from the file: a corrupt
column count of 4e9 otherwise becomes a 4-billion-element `make` and an OOM
kill before a single value is validated.

The previous parser advanced a raw offset and indexed the slice directly, so
any corrupt length panicked and took the process down. `FuzzReadGroup` pins
the new contract -- no panic, and a parse that succeeds has spans inside the
blob -- seeded with the v7 goldens and a fresh v8. A truncated or corrupt
file now fails `OpenStore` with a message naming what failed.

Column types (`column.go`):

- `ColDict` — sorted, deduped value table + bit-packed per-row indices.
- `ColTimestamp` — delta + zigzag varint, with checkpoints.
- `ColVector` — dense float32 embeddings: `dim u32` + `rows*dim` float32 LE,
  raw (high entropy, read whole for k-NN).

## Column encodings

### Dict columns

`BuildDict` interns a column's values with one hash map: a first pass assigns
provisional first-seen ids, the distinct set is sorted, and an array remap
rewrites provisional ids to sorted ones — one map and an array index, the
ingest hot path's largest single cost.

Indices bit-pack at `bitWidth(dictLen)` via the SIMD kernel
(`simd.BitPackInto`), in whole 32-value blocks with a scalar tail
(`encodeIndices`/`decodeIndices`).

The dictionary itself is block-compressed for random access from the mmap
(`dict.go`): values group into blocks of `dictBlock = 64`, each block
compressed, with an uncompressed sparse index of every block's first value. A
lookup binary-searches the first-value index (no decompression), decompresses
one block, and searches within it. Codecs, self-describing per block via flag
bits in the rawLen field:

- LZ4 (default) — fast SIMD decode.
- flate — the `-compact` and tiering codec: ~15% smaller groups, 2–10x slower
  value-reading queries.
- hex nibble-pack — for blocks whose values are all lowercase hex: 4 bits per
  char via the SIMD `HexEncode`/`HexDecode` convention, smaller than flate on
  hex and faster to decode. Trace/span ids are the target.

Whole-dictionary reads — `dictSectionAll`, and the `dictWalk` that
`ValueCounts` drives — convert a decompressed block's string region to ONE Go
string and slice each value out of it instead of converting each value
separately: a substring shares its backing array, so a 64-value block costs
one allocation rather than 64. The values a block yields therefore share one
array, which is the same set of bytes the caller was already holding. Those
two walkers also refill a single decompressed-block buffer instead of
allocating one per block (`dictSec.blockInto`). That is safe only because
each block's values are copied into their own string before the next block
overwrites it, and because every block decoder writes every byte it returns —
`flateDecompressInto` zeroes the tail that a truncated stream leaves
unwritten, which `flateDecompress` used to get for free from a fresh
allocation. The sparse-read paths (`dictSectionAt`, `dictSectionSearch`,
`dictSectionSome`) are unchanged: they touch a few values per block, where
converting the whole region would be waste.

`Reader.ValueCounts(name)` pairs each dict value with its posting count off
that walk, without materializing the dictionary as a `[]string` first.
`Reader.ValueCountsInto(dst, name)` is the same answer appended into a
caller-owned buffer, for a caller looping over groups (`FieldValues`,
`StatsByField`) that reads each group's counts before asking for the next.

### Timestamp columns

`encodeTimestamps` (column.go): zig-zag varint deltas, prefixed with a
checkpoint header. Every `tsBlock = 512` rows the header records the byte
offset in the varint stream, the running timestamp before the block (the seek
base), and the block's min/max. Consequences:

- a windowed query reads the header alone to get `timeWindowSpan` — the row
  range of blocks whose `[min,max)` overlaps the window — and decodes only
  that span;
- a single row's time is `decodeTsAt`, seeking to its checkpoint block and
  decoding at most 512 deltas (O(tsBlock), not O(rows));
- full scans decode via `simd.VarintDecode` with the scalar loop as the
  conformance oracle.

`Reader.TimestampsRange(name, lo, hi)` allocates the slice it returns and is
the default. `Reader.TimestampsRangeInto(dst, name, lo, hi)` writes into a
caller-owned buffer and is opt-in, because the two are NOT interchangeable:
`rebuild()` keeps the slice it gets as a `Column.Ts` for the lifetime of a
group, and refilling that buffer would rewrite a group being written out.
Only a caller that reads the times and drops them may pass a buffer — the
group scan (`appendMatches`) and the histogram, where each time is copied
into a row or a bucket. Those two draw theirs from a `sync.Pool`
(`internal/query/tsscratch.go`), which is safe because `decodeTsRangeInto`
writes every element it returns, zeroing the tail a short stream leaves
unwritten rather than exposing the previous group's timestamps there.

### Postings (the inverted index)

Per dict column, for each dictionary id the sorted row ids holding it
(`postings.go`). This is what turns an equality query from "decode every
row's index and compare" into "look up this value's rows" — the load-bearing
difference on selective queries, and roughly 27% of a group's bytes.

On-disk (v8 FOR, the default): a bit-packed per-value count table, then the
per-id row lists as frame-of-reference bit-packed d-gaps in blocks of
`postBlock = 64` ids. Each block packs at its own max-d-gap width, padded to a
32-value multiple so an aligned SIMD unpack needs no scalar tail. No
intra-block offset table: a run is located by summing the count table within
the block. `forBulkThreshold = 256`: runs above it decode with an aligned
`simd.BitUnpackInto`, below it with O(1) scalar bit reads.

Legacy formats still read: v7 (prefix-sum offsets + LZ4 varint lists) and the
superseded v8-LZ4, dispatched by sentinel words (`postV8Magic` /
`postV8ForMagic`) a v7 first word can never collide with. Old groups decode
without version plumbing; the format tests pin read-vs-write equivalence.

### Blooms

Each dict column's footer carries a cardinality-sized bloom over its dict
values (`colMeta.Bloom`), sized to the dict length. `DictContains` answers
"no" exactly and "maybe" with a bloom miss; the exact dict scan behind it
settles the maybes. A group whose bloom proves a required value absent is
rejected before any decode.

## The store

`store.go`. A directory of `group-<id>.bin` files, indexed in memory by
`timeMin` order.

- `AppendGroup`: marshal → `writeFileAtomic` → mmap the fresh file → append
  to the index. Crash between temp and rename leaves nothing: the helper
  removes its temp file on every failure path, so `OpenStore`'s glob has no
  partial to ignore. mmap-append is racy, so a group is written whole and
  never mutated.

**The fsync policy** (`atomicfile.go`). Every file this package publishes
goes through one helper: write the temp file beside the destination, `fsync`
the file, `close` and *check the close*, `rename`, then **`fsync` the parent
directory**.

That last step is a durability requirement, not a formality. A rename is
atomic with respect to a concurrent reader, but atomicity is not durability:
until the directory is synced, the entry naming the new inode lives only in
the page cache, and power loss can leave the directory naming the old file
while the new data blocks sit safely on disk. Syncing the file without its
directory guarantees the contents of a file that may not be there.

Every write/rename site goes through the helper — `AppendGroup`,
recompaction's `writeGroupFile`, cold `Put`, and `Promote`. Files are created
`0600`: the payload is log data, and a store directory should not become
world-readable because the process umask was `022`.

The helper carries fault points (create, write, sync, close, rename,
directory open, directory sync) that tests fail one at a time to assert the
directory afterwards holds either the complete new bytes or the previous
ones, and never a leftover temp file. On Windows `syncDir` is a documented
no-op — there is no directory handle to flush — so the durability guarantee
stated here is weaker there.
- `OpenStore` takes the directory lock, replays the manifest, and maps the
  groups the manifest names. A `group-*.bin` the manifest does not name never
  committed and is left on disk untouched; a group the manifest names whose
  file is missing is an error, not a silent short store.

**One writer per directory** (`lock_unix.go`, `lock_windows.go`). `OpenStore`
holds an exclusive `flock` on `LOCK` for the life of the store. Two processes
on one directory each allocate group ids from their own `nextID`, so both
write `group-7.bin` and one destroys the other's data with nothing to detect
it. flock is advisory but process-scoped and released by the kernel on exit,
so a crashed writer does not leave the directory permanently locked.

**The flocked inode is checked against the path afterwards, and that check is
load-bearing.** `open` and `flock` are two syscalls and a staged restore
replaces the whole directory in one, so a descriptor taken just before a swap
names an inode that has left the path -- unlinked with the directory it lived
in, contended by nobody, and therefore always flockable. The caller gets a lock
that excludes no one and writes group files BY PATH into the store that
replaced it, beside a second process holding the real lock. Traced under 24
concurrent writers: a store opened on lock inode 113001480 six directory
generations after that inode was released and deleted, appending `group-0.bin`
into the live directory whose lock was 113001501; 13 committed groups lost and
27 of a restored archive's groups overwritten. With the check, two runs at zero
and zero. A mismatch is retried rather than returned -- the caller is not wrong
to want the lock, it is holding the wrong file -- and retries are bounded at 8,
because a destination being replaced in a loop has no lock to give and an
unbounded wait there is a hang rather than an answer. The check is not itself a
race: replacing the directory requires holding this lock, so once a caller
holds the inode that IS at the path, no swap can start until it lets go.
Entry 87.

On Windows the held handle is the lock (Go opens without share-delete) and a
crash does leave a stale `LOCK` needing manual removal -- stated here rather
than papered over, since deleting a lock file whose owner may be alive is the
failure the lock exists to prevent.

**Retention order** (`retention.go`). Commit the removal to the manifest,
retire the group version, then unlink. Each step is in that order for a
reason: committing first means a failed unlink costs disk rather than
correctness, retiring means the mapping is released when the last snapshot
holding it closes, and unlinking last means no reader ever loses bytes it is
reading. The previous order dropped the in-memory entry, discarded the unmap
callback and unlinked ignoring the error, which both leaked the mapping (an
unlinked file's blocks stayed allocated until process exit) and resurrected
the group at the next start whenever the unlink failed.

A failed unlink leaves a tombstone retried on every later pass and counted in
`simdlogs_retention_tombstones`; `simdlogs_retention_failures_total` counts
the failures. If the manifest commit itself fails, nothing is removed --
dropping from the index anyway would make the store answer short until a
restart brought the groups back.

**The manifest is the commit point** (`manifest.go`). Group visibility used to
be whatever the `group-*.bin` glob returned, which is not a commit point, and
two failures came straight out of it: retention that unlinked after dropping
its in-memory entry resurrected the group when the unlink failed, and a group
was visible the instant its rename landed, before anything recorded that it
should be.

`MANIFEST` is an append-only log of length-prefixed, CRC32C-checked records,
each naming added ids, removed ids, a sequence number and an optional write
receipt (reserved for cluster idempotency, task 8.2). Replay applies records
in order and stops at the first that is incomplete or fails its checksum --
the shape a crash mid-append leaves. A torn tail is truncated at open, because
appending after garbage would make the next replay stop at the garbage and
lose everything written since.

`AppendGroup` commits before the group becomes visible, so a crash between
rename and commit leaves a file nothing reads. `CommitRemoval` commits first
and unlinks second, so a removal that is committed stays removed even if the
unlink fails. `compact()` rewrites the log as one record naming the live set
through the atomic helper, keeping replay proportional to the live group set
rather than to every change ever made. A legacy directory with groups and no
manifest is validated and bootstrapped with a single snapshot record.
- `Groups(from, to)` returns readers whose `[TimeMin, TimeMax)` overlaps the
  window — the first skip, before any column is touched. `TailCursor` /
  `GroupsAfterID` are the live-tail watermark.
- `Snapshot(from, to)` is how a reader gets groups: it returns the overlapping
  readers with a reference held on each, valid until `Close`. `Groups` still
  exists for callers that only inspect metadata, but anything that decodes a
  column takes a snapshot.
- `Close` stops new snapshots and retires every group. A mapping an open
  snapshot still holds is released when that snapshot closes, not at `Close`.

**mmap lifetime is reader-owned** (`snapshot.go`). Each group version carries
an atomic reference count, a retired flag and a one-shot unmap. A snapshot
raises the count under the store lock, so a retirement cannot land between the
overlap test and the acquire; retiring marks the version and unmaps it when
the last holder releases; a snapshot taken after retirement never sees it.

This replaces two ways of releasing memory a reader was still using.
Recompaction retired replaced mappings on a five-minute timer that no query
duration was bound by, and `Close` unmapped everything immediately -- shutdown
being exactly when in-flight queries are most likely to still be running.
Retention and cold demotion were worse than either: they dropped the index
entry while discarding the unmap callback, which leaked the mapping rather
than freeing it, so an unlinked file's blocks stayed allocated until exit.

The trade is that a pathological long-running query pins disk blocks until it
finishes. That is bounded and observable -- the lease count is a metric --
where unmapping under a live reader is a segfault.

Group files are mmap'd read-only (`mmap_unix.go`, `MAP_SHARED`): the OS pages
them in on access and evicts under pressure, so a large store keeps only its
working set resident. Non-unix builds use a plain read fallback
(`mmap_other.go`).

Decoded per-column indices are cached per reader with atomic pointers
(`footer.go`), bounded by `indexCacheBudget` (default 256 MiB, settable via
`storage.SetIndexCacheBudget`; `IndexCacheUsed` reports holdings). A column
whose indices are cached skips re-decoding on repeated queries (alerting
rules re-run on a timer); over budget the query is served and nothing cached.

## Ops

### Retention (`retention.go`)

- `DropGroupsBefore(cutoff)` — time-based retention: drops groups whose whole
  span is older than the cutoff, so no group a query could still see is
  removed.
- `DropGroupsWhere(drop)` — per-stream retention: the caller decides per
  group from its `_stream` value counts and newest timestamp.

Files unlink after leaving the index, so a concurrent reader holding a reader
still sees valid mmap'd bytes. The server loop runs hourly (`-retention`,
`api.StartRetention`).

### Tiering / recompaction (`recompact.go`)

`-recompact-after` re-encodes old groups' dict blocks with flate (smaller,
slower value reads on cold data); `-recompact-drop-postings` also drops the
per-column inverted index (flag help: 35% smaller total vs 8% for flate
alone, but cold equality queries fall back to a decode+scan — what
VictoriaLogs does for every query). The per-block codec flag makes the store
hold both kinds at once; `needsRecompact` makes the pass idempotent across
restarts with no marker file.

The size figures name different operations. `docs/wrong.md`'s tiering entry
measured flate-only recompaction at **-8.1%**; both recompaction flag help
strings round that to 8%. The measured full `-compact` mode (flate dict at
flush time, not recompaction) is **~15%** smaller, while dropping postings as
well as using flate is described as **35%** smaller total. Keep those shapes
separate: flate-only recompaction ~8%, full compact ~15%, and flate plus no
postings ~35%.

The subtlety is mmap lifetime: a query started before a swap still holds the
old reader, so a replaced mapping is retired and unmapped when its last reader
releases it — reference counting, which `recompact.go` uses and which this line
described as a five-minute grace period long after `retireGrace` stopped
existing. The same correction was made in `docs/architecture.md` and not here,
three lines above the section this round added; `docs/wrong.md` has now
recorded that shape fifteen times.

### Small-group compaction (`compact_groups.go`)

The other axis: recompaction makes a group smaller, compaction makes there be
fewer. A group is written per ingest request, so a client sending one row per
call writes one group per row, and every query walks the group list before it
touches a column — the time-window skip that makes a selective query cheap is
per GROUP.

`Store.CompactGroups` merges runs of small **adjacent** groups. Adjacent in the
store's own timeMin order, so every output's span stays inside the span its
inputs already covered and no output overlaps a group it did not come from;
merging arbitrary groups would widen spans and make every query read more.
Rows are concatenated in batch order and each input's rows keep their order, so
the output presents them in exactly the sequence a query walking those groups
would have seen. Sorting by timestamp instead would reorder rows sharing a
timestamp, which is a change no caller asked for.

**The column set is the union.** Groups written by different requests carry
different fields, and taking the first group's set would drop every column the
others had — a query that used to find a field would stop finding it, with a
successful exit code. A row from a group without a column gets that column's
zero value, which is what the store already returns for an absent field. A
name carrying two TYPES across the batch refuses the batch rather than picking
one.

**The commit is one manifest record.** `manifest.commit(add, remove, receipt)`
already writes an add-set and a remove-set in a single record, which is exactly
the transaction this needs and is why `manifest.go` did not change. One record
per OUTPUT, not per pass: a run of hundreds at the row cap is several outputs,
and holding every input visible until the last landed would make a crash in the
middle throw away all the work rather than the tail.

Crash residues. Two lanes, and the difference between them is the point:

`TestCompactionCrashMatrix` injects an ERROR at each of the four points. An
error unwinds every defer, so at the first three `discardUncommitted` removes
the output and only the inputs survive. The fourth is different and the
paragraph that said "nothing but the inputs survives" at all four was wrong
about it: `compact-unlink` fires AFTER the commit, so the output is committed,
visible and correct, and what is left over is the inputs' files. That tests the error paths and cannot test the
transaction, because a store that unwinds looks the same whatever the manifest
holds. An earlier version of this table claimed a per-point on-disk residue
from a test field that was never read, and got two of the four rows wrong.

`TestCrashDuringGroupCompactionIsVisibilityNeutral` SIGKILLs a child at
thirteen phases — the seven of the durable write, the two manifest phases that
only this rewrite reaches because its commit writes a record, and its own four
— and asserts every batch is visible exactly once after reopen. That is the
lane the transaction claim needs: measured against a build that commits the add
and the removes as two records, the whole rest of the suite stays green and
every batch appears **twice**.

A kill after the commit and before the unlink leaves the inputs on disk,
unreferenced. Unlinks that fail in-process become tombstones, the same path
retention uses; a restart drops that list, so `OpenStore` reclaims any group
file the manifest does not name. It is safe there and nowhere else: the process
holds the directory's exclusive lock and has not written a group yet. Without
it one kill leaked most of a store's bytes permanently — measured, 1 live group
and 8 orphan files after two reopens.

**Every threshold is a refusal, and the zero value compacts nothing**:
`MinGroups` — below 1 the pass is off entirely, and 1 is raised to 2 because
one group is not a run. The floor is not the switch; an earlier version had
only the floor and `CompactOptions{}` merged a 500-group store into one.
`MaxRowsPerOutput` (0 means the format's `MaxRows`; a
smaller value keeps the per-group skip fine-grained), `MaxInputBytes`,
`MaxGroupBytes`, `OlderThan`, `MaxOutputs`. The byte and output budgets are
checked before every batch, not only between runs — checked only between runs,
a single run of forty groups produced ten outputs against a one-byte ceiling.

`Server.StartCompactionAfter` resolves `OlderThan` at the start of every pass
rather than once: a cutoff computed at startup ages with the process, and after
a day of uptime "an hour ago" is "twenty-five hours ago", which stops excluding
the range ingest is still appending to. It walks the tenants **detached** —
snapshotted under the server lock and marked in-flight, then run without it.
`forEachTenant` holds the lock every request needs to resolve its tenant, and a
pass is file I/O measured in seconds: with it held, a query issued during a
50,000-group pass took 250.7 ms. Retention and recompaction still use the
locked walk; they are shorter, and that is a statement about this pass rather
than a claim about them.

The budgets are per TENANT, not per pass: each store enforces its own and there
is no cross-tenant total.

**Measured.** Two fixtures, kept apart because an earlier version of this
table mixed them — its group column was n=5,000, its byte column n=20,000, and
it said "1 group" where n=20,000 gives 3.

*Query side*, 5,000 one-row groups against the same rows in one group, six
interleaved runs at 2000x, load average 1.9 (above this repo's quiet-machine
bar, stated because it is):

| | min | spread |
|---|---|---|
| one row per request | 10,414 ns | 1.07x |
| compacted to 1 group | 25.33 ns | 1.63x |

**411x on the minimums, 252x on the least favourable pairing.** An independent
measurement at a different load gave 284x and 274x, so the durable claim is a
band of roughly 250–410x. The absolute nanoseconds are not reproducible across
sessions and are not claimed; the ratio is. A previous version of this section
said 582x, from three runs at a benchtime short enough that its spreads were
1.65x and 2.2x — meaningless at an 8.3% floor.

*Disk side*, deterministic and reproduced exactly by an independent
measurement: at n=20,000, 11,056,723 bytes in 20,000 groups become 324,965 in
3 — **34.0x**. One pass over 2,000 one-row groups costs about 10 ms.

Defaults were chosen after these numbers, not before, and
`-compact-min-groups` is 0: a store whose owner has not asked for a rewrite
does not get one, and `CompactOptions{}` is a genuine no-op rather than a
floor that reads like one.

### Cold tier (`cold.go`)

`ColdStore` is a tiny blob interface (`Put`/`Get`/`List`/`Delete`) for aging
group files onto S3/GCS/filesystem; `LocalCold` is the filesystem reference
implementation and a test stand-in. Demote uploads a group and drops it
locally; promote restores it to be queried (Glacier-style).

### Backup (`backup.go`, `backup_manifest.go`)

`BackupTar` streams a self-describing tar to `w`. The HTTP surface is
`/admin/backup` (application/x-tar), admin-only.

**The archive is bracketed.** Entries are, in order:

```
BACKUP-MANIFEST      JSON, always first
group-<id>.bin       one per group, in ascending id order
BACKUP-COMPLETE      empty, always last
```

A bare tar of group files is well-formed whether it holds every group, some of
them, or half of one, so `tar t` cannot tell a complete backup from a truncated
transfer. The manifest goes first so a reader knows what to expect while it
still has the whole stream ahead of it; the terminator goes last so a stream
that ends early is detectable without trusting a byte count. They answer
different questions — *is anything missing* and *did the transfer finish* — and
an archive can fail either one alone.

The manifest (`BackupFormat` 1) carries the format version, creation time,
tenant key, the store manifest's sequence at snapshot time (the high watermark
this backup represents), and per group: name, id, group format version, row
count, byte size, and CRC32C over the whole file.

**Every group, and the sequence, under one lock.** `BackupTarWith` takes a
`SnapshotAllWithSeq`, not a `Snapshot(MinInt64, MaxInt64)`. The time-range
overlap test is `TimeMin < to && TimeMax >= from` -- half-open at the top -- so
a group whose `TimeMin` is `MaxInt64` fails it and is absent from the archive
*and* from the manifest, since both are built from the same snapshot. The
backup then verifies clean while missing data, which is the one thing a
self-describing archive exists to make impossible, and a timestamp is a number
a client sends. The manifest sequence comes from the same lock acquisition for
the same class of reason: reading it afterwards is a different number, because
an `AppendGroup` in between advances it, and the archive would declare a
watermark covering a group it does not contain.

**A lease, not a path list.** The previous version copied group paths out under
the read lock and then read each with `os.ReadFile`, *skipping any that had
gone*. A group retention removed between the two steps was dropped from the
archive and the backup still reported success — a backup discovered to be
incomplete at restore time, which is the one moment there is nothing to fall
back to. `BackupTarWith` takes a `Snapshot` instead, which guarantees every
captured group stays mapped until Close whatever retention, recompaction or
cold demotion do. That removes the reason for the skip rather than handling it:
any failure past the snapshot is a real failure and is returned.

It also removes the copy. The bytes go from the mmap the store already holds
straight to the tar writer, instead of one `os.ReadFile` per group allocating
the whole file on the heap first — at the 64 MiB flush ceiling that was a
64 MiB allocation per group, live for the length of the write.

**Reading one back.** The manifest must be the FIRST entry, and that is
enforced rather than assumed: validation runs against the manifest, so a group
read before it is read with no size, checksum or parse check -- and the
completeness loop afterwards passes, because those groups *had* been seen. A
manifest-last archive carrying a wholly corrupt group verified clean and
restored. An archive with no manifest at all is a different thing, is still
restored, and returns `ErrBackupUnverified`; its entries are read under their
own ceiling, since nothing sizes them.

`VerifyBackup` walks an archive and checks it against its own manifest without
writing anything, which is what a dry-run restore and a
backup gate both need. Validation is streaming, not collect-then-check: a large
archive must not be buffered to be verified, and a failing entry is reported
before anything after it is written. Per group it checks the archive's declared
size, the manifest's size, the CRC32C, and then a full `ReadGroup` parse — a
checksum proves the bytes survived the transfer, not that they were ever a
readable group, which is what a store opened over them needs. It also refuses
an archive carrying a group the manifest does not name (a group nothing
checked), a duplicate entry, and two manifests.

Failure modes are distinct errors because they have distinct causes:
`ErrBackupTruncated` for a stream that ended before its terminator or that is
missing a group the manifest names, and `ErrBackupUnverified` for an archive
with no manifest at all.

`RestoreTar` unpacks into a directory, flattening entry names to their base so
a crafted archive cannot escape, and writing each group through
`writeFileAtomic`. Only `group-*` names are placed: an archive carrying a
`MANIFEST` entry used to be harmless, and since `OpenStore` decides "is this a
legacy directory" on whether MANIFEST exists, an empty or truncated one
restores a directory full of groups that the next open reports as EMPTY with no
error.

A pre-format-1 archive has nothing to validate against. It is restored and
`ErrBackupUnverified` is returned, so a caller requiring a verified restore
fails rather than being told success with a note.

### Staged restore (`restore.go`)

`Restore` is the supported one, and `RestoreTar` stays for the callers that
already use it. Two things separate them: where the bytes land while they are
being read, and whether anything stops a second writer.

**Stage, then rename.** `RestoreTar` writes each group into the destination as
it reads it, so a failure halfway leaves a destination holding half a store —
and a directory of valid group files opens as a store and answers queries with
a silent subset, no error anywhere. `Restore` writes into `<dst>.restoring`, a
sibling, and moves the whole thing into place with one `os.Rename`. The sibling
is not a detail: a rename across filesystems fails with `EXDEV` rather than
doing anything, so staging on another device turns every restore into an error
after all the work. A failed restore removes the staging directory, so a retry
is not blocked by the last attempt — but a *crash* does not, and the leftover
`<dst>.restoring` is how an operator can see where a killed restore got to. A
`<dst>.restoring.marker` file sits BESIDE it, written before the directory it
marks and removed after the rename: without a marker, "a directory of group
files at that path" cannot be told from a pre-MANIFEST store, and clearing on
that rule destroyed one. Beside rather than inside, and first rather than
second, so no process kill can leave a staging directory a later restore
refuses. Process kill, not power loss: the marker is written with no fsync of
either the file or its parent, so a power cut can lose it while the directory
it vouches for survives. Not measured -- this repository has no power-loss
harness. A
crashed restore also leaves `dst/LOCK`, which the destination check ignores
precisely so the retry is not blocked by it.

`os.Rename` is not raw `rename(2)`: it `Lstat`s the destination and returns
`EEXIST` for any existing directory, empty or not. So the destination is taken
out of the way first, and that line is load-bearing on Linux rather than a
portability nicety. A destination that is a mount point fails there, at the
rename that displaces it with `EBUSY`, rather than at the rename that refills
it.

**The lock, and why the emptiness check alone was worthless.** The check that
the destination is empty ran once, at the start; the removal ran at the end, an
archive-read later. A server that opened a store at that path in between had
its `LOCK`, its manifest and every group deleted, the archive renamed over the
top, and the call returned nil — while the still-running writer allocated group
ids from its own counter and overwrote the archive's files. The result opened
clean and answered queries with a silent mixture of two stores. Two concurrent
restores were the same defect in a different dress: both derive the same
staging path, the second's `RemoveAll` deletes the first's staged files
mid-stream, and both write into it; measured, that put two groups from each of
two archives into one destination with one call reporting success.

`Restore` takes the store's own exclusive lock on the destination and holds it
from before the archive is read until the syscall that ends that directory --
NOT until the rename that refills it, and the difference is measured. Releasing
it early leaves `dst/LOCK` present and unheld (the file is never unlinked), so a
server flocks what is already there, opens the store, and the rename then
SUCCEEDS because that server never had to create the directory: a restore
returning nil with the archive's groups overwritten. On unix a rename moves a
directory whose lock file is held without complaint, so holding costs nothing;
on Windows it cannot, which is why the early release exists there and only
there. A second restore cannot start while it is held -- in the gap between
the two renames one can, and the outcome is one of three, not one: `ENOENT`,
`EEXIST`, or, with both parked in their own gaps, a first call returning nil
over the other's destination. Both abort branches delete this call's staging
directory, since both restores derive their paths from `dst`. The three-way
answer and its reachability are below, under the staging lock. The emptiness
**re-check
immediately before the swap** is what makes the swap safe -- placed at the top
of the call it would only say the directory was empty an archive-read ago, and
a lock does not stop a process dropping a file in.

**The old destination is renamed away, not removed in place.** The safe-abort
argument needs the destination to go from "exists, holding a lock this call
holds" to "does not exist" with nothing in between, and `os.RemoveAll` is not
that: it unlinks `dst/LOCK`, then re-reads the directory until it reads empty,
then `rmdir`s, so in the middle of its own loop the destination is present and
lockless. A server opening it there creates a second `LOCK` inode, writes a
manifest and a group, and the loop's next pass deletes them before the rename
goes on to succeed. The re-check guarantees the destination holds nothing but
this call's lock file, so the walk is one unlink, one readdir and one rmdir --
a window nothing reaches at production speed, which is why measuring the shipped
walk finds no losses. Keep the shape and widen only the gap to 200us and it
opens: 56,434 foreign entries appeared inside an emptied destination in 25
seconds, 692 archive groups overwritten, 519 committed groups lost, 85 restores
landing incomplete; against 0, 0, 0 and 0 for the rename. One syscall takes the
directory and its lock inode away together, and the sibling it lands in is
removed afterwards off a path no opener can reach. Entry 87.

**The lock has to survive the swap, and it does so by being the one the rename
installs.** The staging directory is locked too, just before the swap, and that
lock file is the one the rename puts at `dst/LOCK`. After the rename this
process holds the restored store's own lock; between the two renames the
destination does not exist, so a server has to create it and the second rename
then fails with `EEXIST` — a safe abort. `os.Rename`'s own Lstat-then-rename is
not a hole in that: for the raw rename to replace a directory a server created,
that directory must still be empty, which means the server has not made
`dst/LOCK` yet -- and the lock it then opens is the staging lock this call
holds, so it gets `ErrLocked`. The other ordering leaves `dst` non-empty and the
rename fails. Those, plus holding the destination's own lock right up to the
first rename, leave no ordering in which a SERVER writes into the result.

Two restores are a different question and the answer is weaker. Both parked in
their own gap between the two renames, the first returns nil with a manifest
naming its own groups over a destination holding the other's -- measured with
two ordinary `Restore` calls and this package's own `restore-removed` fault
point. Zero occurrences at production speed (20,000 barrier rounds of six
workers with six archives; 11.6 million overlapping attempts, 104,479
successful restores), and reachable only when something outside a restore
widens the window. Recorded rather than closed: closing it needs a lock that
outlives the swap on a path neither restore owns. Do not run two restores at
one destination.

The lock file is never unlinked, on either side of the swap. Both orderings
have a gap -- unlink-then-unlock lets a competitor that already opened the fd
flock the dead inode after the unlock, unlock-then-unlink is the mirror -- and
measured under contention both produced 211 to 268 double-holds per thirty
seconds where not unlinking produced zero. A lock file whose inode can change
cannot be handed over by ordering. So `release` is `unlock`, exactly as
`Store.Close` has always been, and a restored store carries a `LOCK` like any
other.

Releasing the destination's own lock before the swap is a **Windows-only** step
(`releaseBeforeRemoval`, a no-op on unix). There the lock IS an open handle
without `FILE_SHARE_DELETE`, so a directory containing it cannot be moved or
removed. On unix that same release is what let a server flock the file that is
still there, open the store, and have the rename succeed over it -- measured at
4,364 overwritten archive groups in ten seconds, ending in a SIGBUS inside a
reader's mmap. The paragraph above says "there and only there"; this one used
to say the opposite, in the same document.

That release leaves Windows a window this design does not close, narrower than
the removal walk it replaces and in the same class: a server that flocks
`dst/LOCK` inside it opens a store which the displacement then moves aside and
the restore's cleanup removes. Stated rather than measured -- no Windows
machine runs these tests, and a claim of closure there would be one nobody
checked.

**The destination must be empty or absent, and must be a plain directory.** A
restore never merges — the result would be a store whose group set is neither
the archive's nor the destination's. The three refusals are checked up front,
before the archive is read, and each of the failures they prevent arrives at a
different place now that the destination is renamed aside rather than removed:
`.` fails the emptiness re-check (its own staging and marker siblings are
inside it) after the whole archive has been staged; `..` the same, a level up;
and a symbolic link SUCCEEDS with the check bypassed -- `os.Rename` replaces
the link with a real directory and leaves the target holding nothing but a
`LOCK`, which is a store written to the link's parent with success reported.
Measured with each refusal removed in turn. An earlier version of this
paragraph gave `os.RemoveAll`'s treatment of each as the reason; that is the
call this design replaced, and the refusals now stand on what actually
happens. A `LOCK` in the destination is never
counted and never named: `lockDir` is what decides whether a process has the
store open, and it runs next. On unix the file outlives the process that made
it -- `unlock` releases the flock and closes the descriptor without unlinking
-- so a cleanly stopped store and a SIGKILLed restore both leave one, and
counting it made a killed restore unrecoverable by the tool that produced it.
On WINDOWS the lock IS the open handle and `unlock` does remove the file, so a
stale `LOCK` there means a crash and `lockDir` refuses it with `O_EXCL`
whatever this listing does.

**Tenant.** `RequireTenant` refuses an archive whose manifest names a different
tenant, and refuses it the moment the manifest is decoded — and refuses any
group that PRECEDES the manifest, because `readBackup` enforces manifest-first
retroactively: a group arriving earlier is read, parsed and emitted, and the
ordering error is raised only when the manifest turns up. Measured, all four
groups of a manifest-last archive landed in staging before anything looked at
the tenant. The manifest is the archive's first entry and `readBackup`
enforces that, so there is no reason to fill a volume with the wrong tenant's
logs before refusing them. Nothing inside a group file records where it came
from, so the manifest is the only place that fact exists, and restoring one
tenant's groups into another's directory produces a store that answers that
tenant's queries with someone else's logs — with no error, at any layer, ever.
The flag wants the tenant KEY the manifest carries (`0:0`), not the directory
name (`tenant-0-0`); `-dry-run` prints it.

**Dry run.** `DryRun` runs the full `VerifyBackup` validation — declared size,
manifest size, CRC32C and a `ReadGroup` parse per group, plus the tenant check
— under every limit below, and writes nothing. It touches no destination at
all, so a scheduled backup check needs no directory and a dry run against the
store you intend to overwrite is not refused for being occupied. The first
version called `VerifyBackup` directly, which is `readBackup` with no callback,
and every limit lived in that callback: the one mode an operator is told to
point at an untrusted archive was the one mode that checked nothing.

**Limits.** `MaxFiles`, `MaxBytes` and `MaxFileBytes` bound what an archive can
make the process do; the defaults (100k entries, 1 TiB, 1 GiB per entry) are
sized for a real store and are all overridable. `MaxManifestBytes` is separate
because the manifest is decoded *before* any of the others can apply — it is
what sizes everything after it, and a 24 MiB manifest decodes into roughly
340 MiB of live heap. `MaxFiles` also bounds the group count the manifest
declares, which is where a verified archive is refused.

**Unverified archives.** A pre-format-1 archive carries no manifest and cannot
be checked against one. `AllowUnverified` keeps what landed and still returns
`ErrBackupUnverified`, so a caller requiring a verified restore fails; without
it the restore is discarded, which left an operator holding an old backup with
no supported path. `RequireTenant` with such an archive is always refused: it
names no tenant, and accepting it would place another tenant's logs under a
flag whose whole purpose is to stop that.

**A failed sync after the rename is not a failed restore.**
`ErrRestoredButUnsynced` says the store is in place and the final directory
sync failed, so the caller must not report plain failure and must not retry
into what is now an occupied destination — what is missing is the guarantee
that the rename survives a power loss.

The command is `simdlogs restore -src FILE -dst DIR`, with `-dry-run`,
`-tenant`, `-allow-unverified` and the four limits as flags; `-src -` reads
stdin. It is a subcommand rather than a flag on the server binary because it is
not the server, and a `-restore` flag on a process whose other flags all
configure a long-running listener invites pointing it at a live storage
directory.

## Disk

The published footprint numbers are **historical baselines, not the current
footprint**: the unique-hex scale table (`docs/scale-curve.md`) was measured
on 2026-08-10, before the FOR postings rewrite (`a5f9098`) and the hex
nibble-pack codec (`d000ae3`, measured -9.8% disk on the realistic corpus in
its commit) shipped; the realistic 2.62x figure dates from `3f5a063`
(2026-08-12, after FOR, before hex). Postings are roughly 27% of a group;
dropping near-unique postings shrinks disk and made the needle 90x slower,
so that change was reverted (`docs/wrong.md`).

The most recent realistic-corpus footprint measurement on record is the
tiered-storage session (`d846429`, 2026-08-13, POST-hex): 2.40x → ~1.55x of
VL on the realistic 100K-row corpus (10297KB hot, trace_id/span_id already
hex-packed), flate alone -8.1%, dropping postings -35.4%. It is a 100K-row
session measurement, not a fresh scale/current-release measurement — no
current-footprint claim exists beyond it, and the roadmap requires fresh
realistic and scale-vs-VL measurements before any current-facing footprint
statement.


## Corruption policy and storage health

One unreadable group used to make `OpenStore` return an error, which made the
whole tenant unopenable. That is the right default and was the wrong *only*
behaviour: an operator with one bad group out of ten thousand had no way to
read the other 9,999.

Two policies, chosen in `config.Config.CorruptionPolicy` (the
`-corruption-policy` flag) and parsed once at startup so a typo fails the
process rather than falling back silently:

| Policy | Behaviour |
|---|---|
| `fail` (default) | the first unreadable committed group is an error; nothing on disk is touched |
| `quarantine` | each unreadable group is moved to `<store>/quarantine/`, dropped from the manifest, and the store opens with what remains |

The policy applies to a **legacy directory** as well, and to a group that
cannot be MAPPED as much as one that cannot be parsed. Both loops used to treat
`mmapFile`'s error as a bare `continue` (legacy) or a hard error under both
policies (main) — so `fail` reported "healthy: 2 groups" for a legacy directory
with an unopenable group, and `quarantine` could not quarantine the one kind of
damage most likely to need it. The checksum in the record is best-effort for
the same reason: refusing to move a group because its bytes cannot be read
leaves the store unopenable under the policy chosen to keep it open.

**Quarantine ordering.** The record is written first, under the quarantine
name; then the file is renamed; then both directories are fsynced. Removals
are collected and committed to the manifest **once**, after the loop — one
commit per corrupt group is one fsync and one crash window per corrupt group.

The record goes first because the record is the point: a quarantined file
nobody can identify is evidence destroyed, where a record naming a file still
in the store is something an operator can act on and the next open re-does.

The window that leaves — file moved, manifest still naming it — is
**recovered**, not argued away. A committed group whose file is absent *and*
which has a record in `quarantine/` is treated as a completed quarantine and
removed from the manifest, under either policy. Without that, quarantine could
not recover from its own crash window: every later open returned "committed but
its file is missing" and the store never opened again. A missing group with no
quarantine record is still a hard error.

**Names carry the checksum.** Group ids are reused, so `quarantine/group-2.bin`
is not a unique name: a second quarantine of id 2 renamed over the first one's
file and wrote over the first one's record. Quarantined files are
`group-<id>-<crc32c>.bin`.

**A quarantined id is never reissued.** `nextID` starts above every id the
store has ever committed, which is not the same as every id the manifest still
names — the quarantining open REMOVES the id, so taking the maximum of
`visibleIDs()` regressed past it on the next open and the store handed that id
to new data. It is the maximum over the group files on disk (including
uncommitted ones) and the quarantine directory.

That is not every id ever issued — retention unlinks, so a store whose groups
have all been dropped restarts its ids from 0. The narrower property is the one
that matters and does hold: a quarantined id is never reissued, because a
quarantined file is always in `quarantine/`. A retention-removed id cannot be
laundered by the recovery gate either, since it leaves no record and the gate
requires one.

**The recovery gate reads the record, not the filename.** A committed group
whose file is absent is treated as a completed quarantine only when a record in
`quarantine/` parses, names that id, and names a file that is there. Matching
on the filename alone meant one empty `group-1-00000000.bin.json` made a
genuinely missing group open clean under the `fail` policy, reported as
"quarantined by an earlier open" with nothing quarantined and no record listed.

**The record** carries the group id, the original path, the quarantined name,
the reason, the byte count, and a CRC32C **of the bytes as they were at the
moment of the move** — or `-1` and `0` with the reason saying so, when the file
could not be read at all. The move still happens: refusing to quarantine a
group because its bytes are unreadable leaves the store unopenable under the
policy chosen to keep it open, and a moved file with a record beats a committed
group dropped with none. That checksum separates "already corrupt on disk" from
"changed after we moved it", which is the first question asked when another
copy of the same group reads fine.

**Health and readiness.** `Store.Health()` reports groups served, corrupt
count, quarantined count, the last error, the policy, and whether an operator
has acknowledged the state.

`Degraded()` is `Corrupt > 0 || Quarantined > 0`. Both, because `Corrupt` is
what *this* open found and is zero on the next one — the quarantining open
removed the group from the manifest, so the restart sees a consistent store.
The data is still gone. A signal that cleared on restart read healthy one
restart after permanent loss, with the alert metric at zero.

`Ready()` is `!Degraded() || Acknowledged`, because a degraded store *works* —
it opens, it serves, its queries return — and every query touching a
quarantined group comes back with fewer rows and nothing in the response says
so.

Acknowledgement **persists**, in `quarantine/ACKNOWLEDGED`, together with the
count it accepted: a restart with the same count stays acknowledged, and one
more quarantined group makes the counts differ and the store is unacknowledged
again. `POST /admin/acknowledge-degraded` is the operator surface; without it
the only way to clear a readiness failure was a restart.

**Probes.** `/-/ready` answers 503 with the degraded tenants named.
`/health`, `/-/healthy` and **`/insert/ready`** stay unconditional: restarting
the process fixes nothing a quarantined group suffers from, and the degradation
is read-side — the store takes writes normally, so failing the ingest probe
would convert a read-side loss into an ingest outage and take the node out of
the ingest Service.

Degradation is recorded on the **server**, keyed by tenant, and survives
eviction. Every tenant directory on disk is scanned at startup — a `ReadDir`
per tenant, no store opened — because a tenant is marked degraded when its
store OPENS and `NewServerConfig` opens only the default one: a replica
restarted onto a disk with a degraded tenant nobody had queried reported ready
until traffic arrived, which is the wrong way round for a probe whose job is
keeping traffic off. Walking the open tenants answered "no degraded tenant among those
currently open", so evicting an idle degraded tenant turned a 503 into a 200
while the data was still missing. An evicted tenant is acknowledged by writing
the marker into its directory rather than reopening it, which would evict
something else.

Four metrics, because they answer different questions:
`simdlogs_storage_corrupt_groups`, `_quarantined_groups`, `_degraded_tenants`,
`_degraded_unacknowledged_tenants`. The last is the one to alert on.

They and `/-/ready` derive from ONE snapshot, so the two endpoints cannot
disagree about a tenant. Their population is **not** `simdlogs_tenants`':
that counts open tenants, and these count open plus evicted plus scanned. A
dashboard dividing one by the other can exceed 1, which is a property of the
two denominators and not a bug in either.

`simdlogs_storage_corrupt_groups` counts groups. A quarantine directory that
cannot be READ sets a separate `Unreadable` marker rather than incrementing it
— a permissions problem on one directory is not one corrupt group, and the
gauge's help text says "committed groups that could not be read at open".

**Clearing the state.** Emptying `quarantine/` is the remediation: the snapshot
re-reads the directory for any tenant it names that is not open, so the next
probe sees the evidence gone and the record is dropped. Deleting the tenant
directory clears it the same way; a directory that merely cannot be READ does
not, because an error is a problem to report and never an absence.

The re-read is throttled to `DirRereadInterval` (default 250ms,
`-readiness-reread-interval`), which caps what an unauthenticated probe can
cost. It saves nothing at an ordinary probe cadence — every probe re-reads —
and what it closes is the amplification: 2000 probes at 19.4µs each rather than
2000 x N directory reads. A fresh read is written back into the record, so the
answer does not depend on which side of the window a probe lands. Without that re-read
the startup record kept a replica at 503 and the alert at 1 for an empty
directory, with no escape but a restart — three individually-correct fixes
interacting, and the only one of them a test could have caught in isolation was
none.

**What is and is not tested about the syncs.**
`TestQuarantineSyncsBothDirectories` counts the `faultDirSync` calls the
quarantine path makes and requires an error from one of them to reach the
caller — so deleting either `syncDirNamed` turns it red. What it does NOT prove
is durability: showing that the kernel actually drops unsynced entries needs a
power-loss rig this repository does not have, the same limit the crash matrix
has for the fsync boundary (`docs/wrong.md`). An earlier version of this
paragraph said the mutation could not be caught at all, which was wrong —
`syncDir` already carries a fault point.
