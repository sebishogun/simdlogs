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
header:  magic u32 ("slog" 0x736C6F67), version u32 (7), rows u32, columns u32
columns: per column: name, type, width, rows, data
footer:  timeMin i64, timeMax i64, per-column meta records, footer-len u32
```

The footer is read first (its length is the last four bytes), so a query
consults skip metadata without decoding any column. `ReadGroup` validates
magic/version and bounds; a truncated file fails `OpenStore` and is skipped.

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

- `AppendGroup`: marshal → temp file → `fsync` → `rename` into place
  (atomic) → mmap the fresh file → append to the index. Crash between temp
  and rename leaves a `.tmp` file `OpenStore` ignores. mmap-append is racy,
  so a group is written whole and never mutated.
- `OpenStore` globs `group-*.bin`, maps and parses each, and skips anything
  unreadable (a truncated partial flush); readers are safe for concurrent
  queries.
- `Groups(from, to)` returns readers whose `[TimeMin, TimeMax)` overlaps the
  window — the first skip, before any column is touched. `TailCursor` /
  `GroupsAfterID` are the live-tail watermark.
- `Close` unmaps every group; the store must not be used afterward.

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
so that change was reverted (`docs/wrong.md`). Tiered storage measured
2.40x → ~1.55x of VL on the realistic corpus (flate on old groups) — also a
pre-hex historical baseline. No current-footprint claim is committed; the
roadmap requires fresh realistic and scale-vs-VL measurements before any
current-facing footprint statement.
