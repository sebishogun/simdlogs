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
so a crashed writer does not leave the directory permanently locked. On
Windows the held handle is the lock (Go opens without share-delete) and a
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

The size figures in the flag help are not one truth — they disagree with
each other and with the measurement. `docs/wrong.md`'s tiering entry
measured flate-only at **-8.1%** (consistent with the "8% for flate alone"
in the `-recompact-drop-postings` help), while the `-recompact-after` help
claims **~17%** for the same flate-only operation; the measured full
`-compact` mode (flate dict at flush time, not recompaction) is **~15%**
smaller. Distinguish them: flate-only recompaction ~8%, full compact ~15%,
and treat the help's 17% as an unmeasured source claim (a stale source
comment, recorded in `docs/roadmap.md`).

The subtlety is mmap lifetime: a query started before a swap still holds the
old reader, so replaced mappings are retired and unmapped only after
`retireGrace = 5 minutes` — longer than any request the server will serve.

### Cold tier (`cold.go`)

`ColdStore` is a tiny blob interface (`Put`/`Get`/`List`/`Delete`) for aging
group files onto S3/GCS/filesystem; `LocalCold` is the filesystem reference
implementation and a test stand-in. Demote uploads a group and drops it
locally; promote restores it to be queried (Glacier-style).

### Backup (`backup.go`)

`BackupTar` streams a tar of the current group files to `w`. Because groups
are immutable, the set captured under the lock is a consistent snapshot even
as ingest continues — the VictoriaLogs vmbackup shape minus the object store;
a group dropped by retention between snapshot and read is skipped.
`RestoreTar` unpacks into a directory, flattening entry names to their base so
a crafted archive cannot escape. The HTTP surface is `/admin/backup`
(application/x-tar).

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
