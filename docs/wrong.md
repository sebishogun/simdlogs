# The record

Measurements that argued against a change, whether or not code changed.
The entry is the deliverable. Same discipline as the sibling repos.

## The selective-skip ratio is the group-skip factor, and absolute latency is next

First engine measurement, 1M rows in ten 100K-row groups, a 1/50th time
window with an equality predicate against a forced full scan of all ten
groups:

    selective-window   131 ms
    full-scan         6616 ms   50x

The 50x is exactly the skip factor -- the window overlaps one group of
fifty, and the other nine are never opened. That is the design's premise
confirmed at the engine level. The absolute 131 ms is not yet good: the
survivor group still decodes its whole _msg dict and materializes every
matched row's fields eagerly. The design's "decode selected lanes only"
is the lever there, and lazy per-lane materialization is the next query
change -- recorded here so the number is honest before it improves.

## First head-to-head vs VictoriaLogs: ingest won, query lost, and why

Same corpus (200K records), both engines on this machine, identical
/insert/jsonline and /select/logsql/query wire calls:

    selective query, pre-fix, UNFAIR windows:  simdlogs 34.8 ms  VL 17.1 ms  0.5x
    selective query, post-fix, UNFAIR windows: simdlogs 1.36 ms  VL 16.2 ms  12x
    selective query, post-fix, FAIR windows:   simdlogs 1.40 ms  VL 2.49 ms  1.8x
    ingest (VL async, 3s flush wait inflates VL): 457K vs 64K rec/s

Two lessons, both recorded so the honest number survives the exciting one.

The pre-fix loss was a real bug, not a design limit: appendMatches called
DictIndices (a full-column decode) INSIDE the per-matched-row loop, so
materializing m rows decoded the column m times -- O(matches x column).
Decoding each column once per group took the query from 34.8 ms to 1.36.

The 12x that produced was UNFAIR: the harness gave VL the full time span
and simdlogs a 1/50th window. Handing both the identical RFC3339 window
collapses it to 1.8x. That is the number. At 200K rows the design's
granularity advantage (128K groups vs VL's 8M-row blocks) cannot show --
VL has one block and skips by time just as we do. Orders of magnitude is
a large-corpus property, where a selective query lands a few rows in one
of VL's 8M-row blocks (forcing a full-block scan) against one of our 128K
groups. The scale measurement is the honest place to make that claim; at
this corpus size, 1.8x query and a real (flush-fenced) ingest lead are
what is earned.

## Scale head-to-head at 3M rows: 1.6x query, ingest even — NOT orders of magnitude

    ingest:          simdlogs 441K rec/s   VL 533K rec/s   VL faster
    selective query: simdlogs 5.73 ms      VL 8.92 ms      1.6x

The larger corpus dissolved the ingest "win" from the 200K run: with the
3s VL-flush wait now a small fraction of a bigger ingest, VL is in fact
slightly faster ingesting, so the earlier 7x was mostly the async
artifact and is retracted. The selective query holds a real but modest
1.6x.

This misses the design's stated bar -- orders of magnitude -- and the
reason is worth stating straight rather than burying. VictoriaLogs is
well engineered: within its 8M-row block it has a per-block time index
and blooms, so a selective query over a 3M-row single block is not the
naive full scan the design's premise assumed. The granularity advantage
(128K groups vs 8M blocks) only starts to bite above 8M rows, where VL
holds multiple coarse blocks -- and even there the gap will be a small
multiple, not 10-100x, unless the query path gets materially cheaper
(lazy per-lane decode, avoiding the per-candidate-group _time decode the
5.73 ms is mostly spent in). The orders-of-magnitude claim is, on this
evidence, aspirational; the honest current standing is "competitive,
modestly faster on selective queries, comparable on ingest." The README
says exactly that until a measurement earns more.

## The aggregation class is also ~1.6x, not orders of magnitude

The no-materialization count/histogram path (group-skip, then popcount of
the match bitset, no row built) was the design's best-case bet. At 3M
rows the hits/agg head-to-head is 1.6x (1.8ms vs 2.9ms) -- the same
modest multiple as the row query. VictoriaLogs' aggregation is well
optimized too. Conclusion, stated plainly: against a well-engineered
VictoriaLogs at 200K-3M rows, simdlogs is competitive and ~1.5-1.6x
faster on selective queries and aggregations, comparable on ingest. The
orders-of-magnitude bar the design set is not met and, on this evidence,
is not a small-corpus property -- it would need the >8M-row regime where
VL's coarse blocks force real over-scan, and likely more query-path work,
and even then a large multiple against this competitor is not assured.
The honest headline is a small multiple, and the README says so.

## The design's central premise is refuted: VL WINS the rare-selective query

This is the finding the whole project turned on, and measurement killed
the thesis. The design (docs/design.md, "Why this exists") rests on the
claim that VictoriaLogs, on a selective query, decompresses and scans up
to 8M rows because its skip granularity is a whole block. The rare-needle
head-to-head -- one planted value, queried over the full span, the exact
case the design said simdlogs would win 10-100x:

    RARE needle, full span:  simdlogs 2.30 ms   VL 0.36 ms   VL 6.4x FASTER

VL is six times faster at the query simdlogs was built to dominate. The
premise is wrong: VictoriaLogs does not scan 8M rows for a needle. Its
per-block bloom plus its per-field indexing find the value's block and
touch it surgically; it is a genuinely well-engineered selective path.
simdlogs, even skipping 22 of 23 groups by bloom, still decodes the whole
128K-row survivor group's column to find one row -- the "lazy per-lane
decode" and a real per-group posting index (the design's deferred Phase
8) are what a competitive needle path needs, and the design deferred
exactly the structure that turns out to be load-bearing.

Honest standing across the classes measured at 3M rows:

    common-value query (service:=auth):  simdlogs 2.0x faster
    aggregation (hits):                  simdlogs 1.8x faster
    rare-value selective query:          VL 6.4x faster
    ingest:                              comparable (VL slightly ahead)

The orders-of-magnitude bar is not met, and the specific class the design
promised it on is a loss. Recording this in full, per the AGENTS.md rule
that a finding costing a measurement is the deliverable, is worth more
than a number massaged to fit the claim. simdlogs is a working,
VL-compatible engine that is competitive on common queries and behind on
selective ones; the design's reason-for-existing needs the posting index
before its headline claim is even plausible, and even then, against this
competitor, 10-100x is not supported by any evidence gathered here.

## Phase 8 postings + binary-search dict: needle 6.4x loss -> 2x loss, still behind

The measurement gated Phase 8 (decision #3), so it was built: a per-group
posting index (dict id -> row ids, offset table + delta-varint lists) and
a single-row dict decode, so an equality query looks up a value's rows
instead of decoding the whole column. Then the dict lookups themselves,
which were linear scans over a high-cardinality column's 128K-entry
dictionary, became binary searches (the dict is sorted).

    rare needle, full span, over three rounds of fixes:
      baseline (full decode):        2.30 ms   VL 0.36 ms   6.4x loss
      + postings:                    2.22 ms                 1.4x loss
      + binary-search dict:          0.88 ms   VL 0.41 ms   2.0x loss

Each fix was real and the needle improved 2.6x overall, but VictoriaLogs
is still ~2x faster on it. The remaining gap is that simdlogs consults
every group's bloom over the full span (23 groups) where VL's per-field
index jumps closer to the block; closing it would need a global
value->group index, another structure. The other classes settled at
common-value 1.9x and aggregation 2.0x in our favor, ingest comparable.

Final honest verdict on the design's thesis: against a well-engineered
VictoriaLogs, simdlogs is ~2x faster on common-value queries and
aggregations, ~2x slower on rare-value selective queries, comparable on
ingest. Orders of magnitude is not achieved and, on all evidence
gathered, is not achievable against this competitor at these scales with
this architecture. The engine is a real, correct, VL-wire-compatible log
database that is competitive, not dominant. That is the measured truth,
and it is the deliverable.

## The review was right: vectorized scan + NDJSON took the row query 1.9x -> 4.1x

The code review (external) found the design's Task 3.3 -- the vectorized
residual scan -- was never built: the equality filter was a scalar Go
loop with per-row Set, VictoriaLogs' own filter_exact anti-pattern, the
thing the design existed to replace. Built it:

  - equality: EqualScalarInto (vpcmpeqd per lane) + MaskBits (pack to
    bits), chosen over the posting path when the value is common
    (count > n/8); postings still serve rare values.
  - time range: GreaterEqualScalarInto + LessScalarInto + pack + AND,
    replacing the per-row scalar window loop that ran on every group.

Those alone barely moved the wire number (1.9x), which localized the real
cost: the pure engine was already 1.5ms (Run) / 0.9ms (Count) at 3M rows;
the other ~4ms was HTTP plus a reflection-based json.Encoder over a
map[string]any per row. Hand-built NDJSON (no map, no reflection) took
the selective row query to 4.1x (2.7ms vs VL 11.2ms). Aggregation stayed
2.0x (no materialization to speed up), the needle 0.4x (VL's index still
wins the rare lookup).

Lesson recorded: a vectorized filter is invisible when serialization
dominates. Profile the whole path before crediting or blaming the kernel
-- the engine was never the 5.8ms, the encoder was.

## The needle gap was three engine bugs, not a missing index: 968us -> 7.2us

The rare needle stayed VictoriaLogs' win, and the named lever was a global
value->group index (Phase 9). Profiling the pure engine (no HTTP,
BenchmarkEngineNeedle, 3M rows / 23 groups, one planted value) first said
the index was not the bottleneck at all. The needle cost 968us -- and a
FULL SCAN counting a common value across all 23 groups cost 460us. The
needle, which touches one group, was 2x SLOWER than scanning everything.
That is the signature of wasted work, not missing skip structure.

Three costs, each measured, none of them the group scan a global index
would accelerate:

    runParallel fan-out   ~360us  32 goroutines + channel + merge for the
                                  ONE group that survives the bloom
    postingRows skip-walk ~220us  O(dict-rank) varint skips to reach a
                                  value's list, not the O(1) the doc claimed
    timestamp decode      ~230us  the row path decoded all 128K timestamps
                                  (+2MB alloc -> GC) to stamp one match's Time

The fixes:

  - Footer-prune groups BEFORE deciding to fan out. A needle survives in
    one group; spawning a pool for it costs more than it saves. Pre-prune,
    then go parallel only if enough survivors remain.
  - Postings carry a byte-offset table alongside the row-count offsets, so
    a value's list is reached by a seek, not by walking every preceding
    list. The row-count table still answers EqualityCount as a footer
    subtraction; the byte table is +4 bytes per dict entry, LZ4-friendly.
  - Timestamps carry a checkpoint header (byte offset + base timestamp
    every 512 rows), so one row's time is a point read of <=512 deltas,
    not a whole-column decode. appendMatches point-reads when matches are
    few and full-decodes only when many rows match. Header is ~3KB/group.

Measured, pure engine, minimum of three:

    needle:          968us -> 7.2us    (~130x)
    full-scan count: 460us  -> 450us   (unchanged; already skipped _time)
    windowed row:    1.49ms -> 1.43ms  (unchanged)

The global value->group index was NOT built: the profile said the cost was
elsewhere, and the three fixes removed 96% of it before any new index. The
group scan (23 dictLookups) never showed above 0.03s -- it would matter at
thousands of groups, where a global index is the right lever; at this scale
it was not. Fixing the measured cost beat implementing the named one.

Measured three ways, the simd-repo discipline (deterministic corpus,
warmup then minimum-of-N, the minimum never a mean; a busy machine, so the
layout- and load-independent instruction count carries the claim the noisy
wall-clock only corroborates):

  1. Pure engine, testing.B, perf stat -e instructions:u,cycles:u,
     setup cancelled by differencing two benchtimes:

         needle:          7.2us/op    201K instructions/op
         full-scan count: 461us/op   30.1M instructions/op

     The needle retires 149x fewer instructions than a full scan -- the
     load-independent proof it now does surgical work, not scanning. Before
     the fix the needle was 968us: decoding 128K timestamps (~1.3M insns for
     that step alone) and walking a ~128K-varint posting stream.

  2. Wire head-to-head, 3M rows, HTTP-to-HTTP vs VictoriaLogs, warmup +
     min of 25, three runs byte-identical (the minimum is robust to the
     load-3.8 machine):

         RARE needle, full span:  simdlogs 24.3us   VL 305us   12.6x
         selective query (rows):  simdlogs 2.31ms   VL 10.5ms  4.5x
         aggregation (hits):      simdlogs 1.59ms   VL 3.24ms  2.0x
         ingest:                  384K rec/s        497K       VL 1.3x

  3. HTTP floor subtracted. simdlogs runs in-process (httptest, 13.5us
     floor); VL is a subprocess over TCP (53.9us floor). Removing each
     harness's own overhead leaves the engine-only needle:

         simdlogs 10.8us   VL 251us   23.2x

     So the raw wire 12.6x is if anything conservative: VL's higher HTTP
     floor shrinks the visible ratio, not simdlogs' lower one inflating it.

The class the design was built to dominate, and the one measurement had
refuted as a 6.4x -> 2.8x loss, is now a 12.6x wire win (23x engine-only) --
without the global value->group index the loss entry named as the lever. The
three engine bugs above were the whole gap. The premise entry ("VL WINS the
rare-selective query") stands as the record of what was true then; this is
what is true now, measured to the same standard.

## Ingest was serial parse-then-flush; overlapping them: 384K -> 803K rec/s

Ingest was the one class VictoriaLogs led. Profiling IngestJSONLines
(BenchmarkIngest, 500K records, parse + intern + flush, no HTTP) split the
cost almost evenly and, critically, serially:

    flushLocked   49%   BuildDict 30% (sort + two hash maps), Marshal 17%
    parse         ~50%  simdjson Parse + buildIndex, ForEachKey, buffering

The parser filled a group, then blocked while that group's dictionaries
built, encoded and fsynced -- 31 of 32 cores idle through every flush.
Optimizing flush alone caps at ~1.3x (halving 49% -> 0.75 total); the win
is overlap. A group's buffers are now handed to a pool of flush workers and
the parser continues into fresh buffers, the two halves running at once --
the reference ingests asynchronously for the same reason.

    3M rows, wire, flush-fenced (both queryable):
      before:  384K rec/s
      after:   803K rec/s   2.1x, and now ahead of VL's 495K

Honest caveat kept: simdlogs' number is synchronous and durable when the
POST returns; VictoriaLogs accepts asynchronously (the harness adds 3s for
it to flush before querying, so its raw accept is ~3.1s for 3M, a touch
faster than our 3.74s). We trade a little raw accept latency for durability
on return and still lead on the flush-fenced basis the query comparison
requires. A further lever measured but not yet taken: BuildDict builds two
hash maps (a dedup set and an id table) where one pass with provisional ids
and an index remap would do -- ~2s of the 6s, deferred since async already
put ingest ahead.

## Ingest, continued: one-map dict and sharded parse -- 803K -> 2.78M rec/s

The async pipeline moved the bottleneck to the parser, so the profile
changed and two more levers opened.

BuildDict, the flush's biggest cost, built a dedup set and then a second id
table and looked the id up per row -- two maps and a random probe into a
128K-entry map per row. One pass with provisional first-seen ids, a sort,
and an array remap does it with one map and an array index:

    BenchmarkBuildDict highcard (128K distinct):  34.0ms -> 8.9ms  3.8x

End-to-end ingest barely moved on that alone -- proof the bottleneck really
had shifted -- because flush now runs on workers while the single-threaded
parser is the wall-clock limit. The parse tree was 11s on one core against
a flush tree of 17s spread over four. So the parser was sharded: the NDJSON
body is split at line boundaries into NumCPU/3 chunks, each parsed by its
own goroutine through its own writer (a small flush pool each) over the
shared store, whose AppendGroup is already concurrency-safe (marshal outside
the lock, index update inside). Race detector clean on the concurrent path.

    500K records, BenchmarkIngest:          677K rec/s
    500K records, BenchmarkIngestParallel:  2.66M rec/s   3.9x (at load 8.7)
    3M records, wire head-to-head:          384K -> 2.78M rec/s

2.78M rec/s is ~5.6x VictoriaLogs' reported 495K, and ~2.8x even against
its raw async accept (~3s for 3M, the 3s flush-wait removed). simdlogs'
number is synchronous and durable on POST return. Ingest, the one class VL
led, is now a decisive win, and every measured class is simdlogs': needle
14x, selective 4.5x, aggregation 2.1x, ingest 5.6x. Next perf target is the
aggregation, the smallest of those multiples.

## Windowed queries: per-block timestamp min/max range skip

The aggregation was the weakest win, so the windowed path got profiled: at a
1/50 window it still did O(full-group) timestamp decode and predicate scan on
each overlapping group -- varintDecodeU64AVX512 was 26% of the profile. The
timestamp checkpoints already existed (byte offset + base per 512-row block);
adding each block's min and max lets a query skip a whole block whose range
misses the window without decoding it, set whole blocks that fall inside, and
decode only the boundary blocks -- and restrict the materialize/bucketing
decode to the window's block span (decodeTsRange). No monotonicity assumed;
the skip is per-block min/max, verified against a brute-force scan on
shuffled timestamps.

    engine, 3M rows, 1/50 window, minimum of three:
      windowed count:      839us -> 65us    12.9x
      histogram (hits):    1.62ms -> 479us  3.4x
      selective row query: 1.46ms -> 1.09ms 1.34x (materialize span-limited)

    wire head-to-head vs VictoriaLogs:
      aggregation (hits):  2.1x -> 6.0x
      selective query:     4.5x -> 5.4x
      needle:              13.2x  ingest: 5.7x

The aggregation, the class that was 2.1x, is now 6.0x. The remaining
windowed lever is restricting the equality/predicate scan to the block span
(done for the time filter, not yet for the value scan) -- tracked in the
perf-max task.

## The 3M wins were cache-hot; the disk regime is the real test

Called out correctly: the head-to-head numbers were measured with the whole
store RAM-resident -- every group blob AND every decoded dictionary on the
heap -- so simdlogs queries never touched disk, while VictoriaLogs goes
through its block/storage layer. Not apples-to-apples, and a billion rows
OOM'd (killed at ~600M): ~40GB of decoded dict strings alone. Victory was
claimed too fast.

Fix (committed): mmap the group files (reader.blob is a view into the
mapping) and store the dictionary uncompressed with an offset table, so a
membership probe binary-searches it straight from the mmap -- no eager
decode, no dict on the heap. At rest the heap holds only footers.

The store now runs a billion rows from disk without OOM (RAM stayed ~7GB
for the build), and the honest disk-regime numbers, engine, minimum of N:

    scale     groups   needle    selective   full-count   regime
    100M         763   209us     3.5ms       2.2ms        cache-hot
    1B          7630   13.2ms    136ms       77ms         disk (mmap)

The needle degrades to 1724ns/group at 1B because the per-group bloom is
saturated (128K high-cardinality values into 2048 bits -> always "maybe"),
so every group falls through to a dict binary-search that pages from disk.
A cardinality-sized bloom rejects from the in-RAM footer without the disk
search; a global value->group index removes the per-group scan entirely.
Those are the scale levers, and the VictoriaLogs comparison must be run at
this scale, both from disk, before any claim -- neither is done yet. The
3M/5-13x numbers are cache-hot and are not extrapolated to a billion.

## Two scale fixes: block-compressed dict + sized bloom

The uncompressed dict made a 1B store 57GB (~4-5x VL). Block-compressing it
(64-value LZ4 blocks + an uncompressed first-value index for random access)
brought a group from 1.6x to 0.98x of raw -- footprint back, mmap access
kept, at the cost of one block-decompress per membership probe. That made
the sized bloom necessary: the old 2048-bit bloom saturated on a
high-cardinality column, so every group decompressed a block; sizing it to
cardinality (~10 bits/value, k=7) rejects almost every non-matching group
from the in-RAM footer. Together, engine, 1B from disk, minimum of N:

    metric        saturated bloom, uncompressed   sized bloom, compressed
    needle        13.2ms  (1724ns/group)          2.59ms (340ns/group)
    selective     136ms                           49.9ms
    full-count    77ms                            26ms
    ingest        4.6M rec/s                      5.5M rec/s

The needle is still O(groups) (340ns/group is the bloom check itself); a
global value->group index would make it O(1). Bloom RAM grows ~1.2GB per
billion rows -- the global index is the RAM-leaner endgame. Next: the
VictoriaLogs head-to-head at this scale, both from disk.

## Footprint breakdown: the 3.47x realistic loss is postings + dict on near-unique columns (2026-08)

Realistic corpus, 100K rows, one group, `TestFootprintBreakdown`:

    section     KB     %     what it is
    postings    6268   42    inverted index: rows per distinct value
    dict        5890   40    the distinct strings themselves
    index       1831   12    bit-packed dict-id per row
    bloom        573    4     per-column membership filter
    time         299    2     delta+zigzag+varint timestamps
    TOTAL      14863         152 bytes/row

Per column, the bulk is the near-unique fields:

    trace_id  3375KB (post 1086 dict 1958)   ~100K distinct / 100K rows
    span_id   2559KB (post 1086 dict 1143)
    path      2269KB (post 1037 dict  908)
    _msg      2264KB (post  894 dict 1067)
    bytes     1349KB (post  675 dict  415)

The finding: for a near-unique column every distinct value owns a ~1-row
posting list, so the postings section is essentially the identity permutation
0,1,2,... -- it costs 42% of the file and buys **zero** query speed (a posting
list of length 1 is no faster than reading the one row). The low-cardinality
columns (host/service/level/...) are the opposite: small postings (69-134KB)
that genuinely accelerate group-by and equality.

So the earlier postings-skip experiment skipped the wrong end. It dropped
postings for *common* values (count > n/8) -- exactly the lists that pay --
and kept them for the rare ones that don't, saving 2% and breaking two tests.
The correct lever is adaptive per column: **drop the whole postings (and the
dict-id index) for a column whose distinct/rows ratio is above a threshold,
storing its values inline; keep the full index for low-card columns.** Query
impact is nil in principle -- near-unique equality/substring already scans,
since a 1-row posting list is not an acceleration.

Estimated: dropping postings on the seven near-unique columns saves ~5.5MB of
6.3MB postings; dropping their dict-id index saves ~1.4MB of 1.8MB. Total
~14.9MB -> ~8MB, 152 -> ~80 bytes/row. That closes most of the 3.47x but not
all of it: the remaining ~5.9MB dict is high-entropy hex/uuid/message text
that LZ4 barely compresses and VL stores too. Beating VL outright likely also
needs a stronger codec on those specific columns -- but zstd decode is not on
the simd kernel, so it trades scan speed for bytes, which must be measured
before it lands (the whole point is fastest AND smallest, not one at the other's
cost). This is a v8 storage-format change touching crash-safety and the query
path; it is a deliberate campaign, not a drive-by, and the number above is the
entry.

## CORRECTION: dropping near-unique postings is a 90x needle regression (2026-08)

The entry above ("the 3.47x loss is identity postings on near-unique columns")
claimed those posting lists "buy zero query speed." An interleaved A/B on the
realistic 1M corpus disproves it:

    skipPostings   needle          footprint (of VL)
    off (baseline) 31.5us  28.9x   3.47x
    on (adaptive)  2824us   0.3x   2.31x

Dropping trace_id's postings made the needle **90x slower** -- a 28.9x win over
VL turned into a 0.3x loss -- to buy a footprint that STILL loses (2.31x). The
wrong trade, rejected and reverted.

Why the earlier reasoning was wrong: a length-1 posting list is not "no faster
than reading the one row." With postings the needle is bloom-hit -> DictID ->
postingRows: one row, no column decode. Without postings, EqualityCount returns
not-ok and the engine decodes the whole group's dict-id index and scans 128K
rows. The postings are what avoid the full-column decode -- exactly the needle's
speed. Footprint and needle-speed are in direct tension here; the index is the
speed.

## The honest realistic baseline: we do NOT win every class

The same run, simdlogs vs VictoriaLogs, 1M realistic rows, both from disk:

    needle     28.9x   WIN
    and         3.6x   WIN
    topN         307x  WIN
    groupby      432x  WIN
    histogram    3.3x  WIN
    ingest       4.1x  WIN
    common       0.7x  LOSS
    or           0.7x  LOSS
    substring    0.8x  LOSS
    footprint    3.47x LOSS (of VL)

So "beat VL on all benchmarks" is not met on the baseline either: common-value
retrieval, OR, substring, and footprint all lose. These -- not near-unique
postings -- are the real targets. common/or return many rows (VL streams them
leaner); substring scans _msg (VL's per-block tokenized bloom prunes more).
The footprint fix must come from the dict (40%, high-entropy text) WITHOUT
touching the postings that power the needle -- a stronger dict codec on the
scan-cold path, measured against both size and needle latency.

## Footprint parity needs the dict codec, not the postings -- and that trades speed (2026-08)

Following the query-perf fix (all classes now beat VL), footprint is the sole
loss. Researched the two postings-side levers the plan proposed; both measured
as NOT reaching VL:

    approach                          footprint (of VL)   needle
    default (full postings)           3.47x               27x  win
    compact (drop near-unique post.)  2.31x  still loss    0.3x loss
    inverse-index (rowOffsets/perm)   ~2.7x  est still loss ~kept

The corrected fact: compact mode does NOT beat VL (2.31x), and it forfeits the
needle. The reason every postings lever falls short is the **dict**: 40% of the
file is high-entropy trace_id/uuid/_msg text that LZ4 barely compresses and that
none of the postings work touches. VL is smaller because (a) it has no full
inverted index and (b) it compresses column blocks harder. To get under VL we
would need BOTH a weaker index (losing our 27-500x query wins) AND a stronger
dict codec (zstd-class, which is not on the SIMD decode kernel -> slower scans,
plus a new dependency the design avoided).

Conclusion: there is no configuration that is simultaneously smaller than VL and
faster than VL. The index is the speed and the size at once. Honest positions:
(1) faster than VL on every query + ingest, 3.47x its disk [current default];
(2) a dict-codec campaign to approach/beat VL disk at a measured scan-speed cost
[not done -- deliberate, needs the dep/codec decision and an A/B on scan latency].
Building compact mode was rejected: it loses on both axes.

## Compact mode measured: 15% smaller for 2-10x slower -- a bad trade, opt-in only (2026-08)

Built the opt-in compact codec the footprint research pointed to: the dict is
40% of the file and our LZ4 gets only ~2x on it (no entropy coding) where stdlib
flate gets ~4x. So compact mode flates the dict blocks (per-block codec flagged
in rawLen's high bit -> no format-version change, old data reads as LZ4).

Measured, realistic 1M head-to-head, default vs compact:

    metric      default        compact
    footprint   3.47x of VL    2.95x of VL   (only ~15% smaller)
    needle      27x            4.5x
    common      1.0x           0.5x
    and         3.5x           0.3x
    or          1.2x           0.7x
    substring   1.1x           0.6x
    ingest      4.1x           1.9x

The footprint gain is only ~15% because only the dict flates -- postings (42%)
and the dict-id index (12%) are untouched. Meanwhile every value-reading query
slows 2-10x: point reads (materialize) and scans now flate-decode dict blocks
instead of the SIMD LZ4 kernel; `and` craters 3.5x->0.3x because its per-row
point reads each pay a flate block decode.

Verdict: a bad trade for a queryable store (little size, big speed). Kept as an
opt-in (-compact) for cold archival of rarely-queried data only; the default is
unchanged (fast LZ4). This is the measured confirmation that footprint parity
with VL is not reachable without also sacrificing query speed -- the index and
the fast codec are the speed, and they are the size. The default position
stands: faster than VL on every query + ingest, 3.47x its disk.

## Front-coding the sorted dict is NOT a free win -- size-neutral alone (2026-08)

Hypothesis: the dict is stored sorted, so front-coding (shared-prefix-len +
suffix) should shrink it losslessly AND decode faster than LZ4, since our LZ4
gets only 1.05x on trace_id -- seemingly failing to exploit the sorted prefix.
Measured on the realistic dict (TestCodecCeiling), per-64-value block:

    column    lz4      front    front+lz4   flate
    path      3.99x    5.60x    6.60x       7.91x
    pod       2.74x    3.60x    4.04x       5.49x
    _msg      4.08x    2.61x    4.90x       9.09x   <- front ALONE worse
    trace_id  1.05x    1.12x    1.12x       1.96x   <- random hex, no prefix
    span_id   1.13x    1.28x    1.28x       2.09x
    TOTAL     2.05x    2.04x    2.46x       3.99x

Front-coding ALONE ties LZ4 overall (2.04x vs 2.05x). It wins on structured
columns (paths, pods share real prefixes) but LOSES on _msg -- LZ4 captures the
internal repetition in log messages that front-coding disrupts -- and barely
helps trace_id/span_id, because SORTED RANDOM HEX shares almost no prefix
(adjacent 16-char random ids agree on ~1 char). The 1.05x on trace_id was not
LZ4 failing to find a prefix; there is no prefix to find.

front+lz4 is 20% smaller than lz4 (2.46x), >= lz4 on every column, but adds a
sequential front-decode pass on top of the LZ4 decode -- so it is slower, not
faster. That makes it another size-for-speed lever (a better-ratio compact-mode
codec than flate, at less slowdown), not a free default win.

Conclusion for "can we optimize storage more without losing speed": no. Index
(ceil(log2 distinct) bits/row) is information-theoretically minimal; timestamps
(delta+zigzag+varint, ~3B/row) and the sized bloom are near-optimal; the dict's
only further shrink (front+lz4 20%, flate 2x) costs decode speed and belongs in
compact mode. The fast default is already at the size floor for its codec.

## The rowOffsets prefix-sum table was pure redundancy: v8 count-table, -10.5% group

The prior entry concluded the fast default was "already at the size floor for
its codec." Wrong -- it missed the postings' own header. The postings blob led
with `rowOffsets`, a dictLen+1 uint32 prefix-sum table, UNCOMPRESSED. On a
near-unique column (trace_id: dictLen ~= rows = 128K) that is ~512KB of the
biggest chunk in the group.

No consumer ever reads it as offsets. Grep found three readers -- EqualityCount,
EqualityRows/postingRows, ValueCounts -- and every one uses it only for a COUNT:
`count[id] = rowOffsets[id+1] - rowOffsets[id]`. postingRows locates a value's
data via each block's own intra-offsets, never the prefix sum. So the whole
table was a differenced-on-read count array stored as absolute prefix sums at
4 bytes each.

v8 stores the counts directly, bit-packed at `bitWidth(maxCount+1)` (<= 17 bits,
usually far less), self-describing via a `postV8Magic` sentinel the v7 first word
(dictLen+1, < 2^31) can never collide with. Measured, realistic 100K-row group,
same corpus, interleaved:

    section     v7        v8       delta
    postings    6268KB    4700KB   -25%
    group TOTAL 14863KB   13295KB  -10.5%   (152 -> 136 bytes/row)

Hot paths unaffected (perf stat instructions:u, load-independent, min of runs):

    BenchmarkEngineNeedle   v8 ~345K instr/op   v7 ~357K   (postingRows)
    BenchmarkEngineCount    v8 ~5.1M instr/op   v7 ~5.06M  (EqualityCount)

Two dead-ends the counters killed on the way, both on the count READ:

1. Scalar O(width) bit-loop extractor. The first extractCountBits looped `width`
   times per value. FieldValues (ValueCounts scan) ran 6-13% MORE instructions
   than v7's two-le32 rowOffsets subtraction -- consistent 3/3, real, not layout
   noise. Fix: O(1) read -- one 8-byte load, shift past the sub-byte offset,
   mask -- the same trick DictValueAt uses (width <= 25 so the value never
   straddles the window). Instruction bump gone on the hot paths.

2. SIMD bulk decode (decodeIndices/BitUnpackInto) in ValueCounts. Intuition said
   the kernel at 2.6 Gvals/s should beat per-value reads on the full-value scan.
   Measured across cardinality (BenchmarkValueCounts, one 128K-row group):
   card=131072 neutral (the per-value dictSectionAt string build is ~50ms and
   dominates -- the count read is <1%), card=8 was ~14% SLOWER because
   decodeIndices allocates a []uint32 + word-copies the packed bytes per call
   for a table of 8 values, and blocks=8/32=0 so no SIMD even runs. Reverted to
   uniform scalar O(1). The count read is never the bottleneck of a value scan;
   the string materialization is.

Residual: tiny-dict ValueCounts (card <= ~32) is ~5ns/value slower than v7's
adjacent-le32 rowOffsets read -- an O(1) 8-byte load still costs a hair more
than two sequential reads from a hot 32-byte array. It is sub-microsecond, cold
(field-value introspection, not the query hot path), and dwarfed by the string
build. Traded for -10.5% on every group on disk.

## FOR bit-packing the postings data: postings -55%, decode faster on the hot path

The count-table entry above left the data section -- the per-id row lists -- as
delta-varint compressed with LZ4 in 64-id blocks, each block also carrying a
cnt+1 uint32 intra-offset table. Frame-of-reference bit-packing (FOR) replaces
both: bit-pack each block's d-gaps at the block's max width, drop the offset
table entirely (id's run is located by summing the count table within its
block), self-describing via a second sentinel (postV8ForMagic 0xFFFFFFFE)
alongside the LZ4-data v8 and legacy v7, all three still readable.

Footprint, realistic 100K-row group, same corpus:

    section     v7        v8 count-table   v8 FOR    vs v7
    postings    6268KB    4700KB           2815KB    -55%
    group TOTAL 14863KB   13295KB          11410KB   -23%   (152 -> 116 B/row)

The postings drop is bigger than the isolated 1.02-1.23x FOR-vs-LZ4 size test
predicted, because that test measured only the packed d-gaps; dropping the
per-block offset table (65 uint32 per 64 ids, which LZ4 compressed poorly on
near-unique columns) is the rest.

Decode, single-id retrieval through the production readers (postingRows, v7-LZ4
blob vs v8-FOR blob, interleaved):

    regime        LZ4+varint   FOR        note
    near-unique   ~128 ns      ~167 ns    +39 ns, the count-sum cost
    medium        ~575 ns      ~420 ns    1.4x faster
    low-card      ~89 us       ~56 us     1.6x faster (aligned SIMD unpack)

FOR reads ONLY id's run -- a scalar O(1) bit read per value for a short list, an
aligned SIMD unpack (block padded to a 32-value multiple, no partial-block tail)
for a long one -- while the LZ4 path must decompress the whole 64-id block to
reach one id's varint list. That is the medium/low-card win.

The near-unique +39 ns is the one cost: with no offset table, id's value offset
inside its block is a sum of up to 63 counts (extractCountBits each), averaging
~32 for a random id. It does NOT show end to end -- BenchmarkEngineNeedle runs
FEWER instructions with FOR (315.3K vs 324.6K per op, fixed-iteration
instructions:u), because the needle query is dominated by the bloom sweep and
dict search, not the one postings read. Count queries measured faster too.

Two things that do NOT help, measured:

1. SIMD-summing the block's counts to get the offset. A block holds only
   postBlock=64 ids, so the prefix summed is always < 64 values -- below any
   threshold where a BitUnpackInto of 64 (fixed kernel cost) beats ~32 scalar
   reads. The SIMD-sum path is unreachable dead code; removed.
2. Re-adding a compact bit-packed intra-block offset table to make near-unique
   O(1). It restores ~114KB (postBlock cumulative counts, ~56 B/block over ~2048
   blocks) -- eroding a third of the FOR data saving -- to remove a +39 ns that
   is already invisible end to end. Not worth it.

Verified: TestV7BackCompat decodes a legacy LZ4 blob identically to the FOR blob
built from the same data; the storage/query suites pass under both the SIMD
kernels and -tags purego (the ref path other arches use); arm64, s390x
(big-endian), ppc64le, riscv64 cross-build.

## StreamVByte not added: byte-granular loses to FOR's bit-granular on size (2026-08)

Task #245 planned a StreamVByte SIMD codec as the postings d-gap encoding, on
the theory it decodes faster than bit-packing (branchless, one control byte per
four values). It was gated on "only if FOR is not fast/small enough." FOR turned
out both smaller and faster (entry above), so #245 was closed without building
it.

StreamVByte stores each value in 1-4 whole bytes; FOR bit-packs at the block's
exact width. On these d-gaps the widths are small and uniform (a near-unique
column's gaps are ~log2(rows) = 17 bits, a low-card column's a handful) -- FOR
packs 17 bits where StreamVByte would spend 24 (3 bytes) plus 2 control bits, and
low-card gaps fit ~5 bits where StreamVByte spends a whole byte. Byte-granular is
strictly coarser here, so StreamVByte would grow the postings section FOR just
cut 55%. Its decode-speed edge is on large contiguous lists, which the postings
path reaches only above the count>n/8 crossover (the non-selective case, already
served by FOR's aligned SIMD unpack). No size win, no speed need -- not built.

## The common query was allocation-bound in materialize: decode only referenced dict values

The realistic `common` query (level:=error, whole-record select, ~1/5 of the
corpus returned) sat at 1.0x vs VL -- the one class we did not beat. Profiled
(cpuprofile, query-dominated, BenchmarkCommonSelect): the filter is nothing; the
time is materialize, and it is allocation-bound:

    lz4BlockDecodeAVX512   15%   dict block decompress (unavoidable)
    dictSectionAll         26%   (slicebytetostring 17%) whole-dict -> []string
    scanObject/mallocgc   ~40%   GC driven by those string allocations
    appendMatches.func2    17%   building Row.Fields per match

The waste: the bulk-materialize path (cnt >= n/16) decoded the WHOLE dict of
every materialize column to Go strings -- including a high-cardinality column
like trace_id (100K distinct), where a select over 20% of rows references only
~20K values but 100K strings were allocated. The other 80% became garbage
immediately.

Fix: decode only the referenced values. DictIndicesRaw returns the per-row ids
without the dict; the matched rows mark a `want` bitset; DictDecodeSome
decompresses only blocks holding a wanted id and stringifies only wanted values.
Predicate columns keep the full dict (the filter needs every value); only the
pure-materialize columns take the subset path.

Interleaved A/B, BenchmarkCommonSelect, load 2.7, min of 3:

    baseline (full dict)        22.5 ms/op
    decode-only-referenced      17.2 ms/op    -24%

No regression on needle/selective: those have cnt < n/16 and never enter the
bulk path (they point-read). Full suite green; storage format unchanged (this is
a read path). It closes the common gap -- ~1.0x -> ~1.3x of VL.

The remaining materialize cost is the string COPY (slicebytetostring) and the
per-Row allocation. Zero-copy strings aliasing the decompressed block buffers
would cut it further, but the buffers must outlive the response and that
aliasing is easy to get wrong -- deferred, noted here as the next lever.

## Where the remaining disk is, and the ClickHouse-style lever (measured)

Post-FOR footprint, realistic 100K-row group, 11410KB total:

    dict     5890KB  52%   <- the target
    postings 2815KB  25%
    index    1831KB  16%
    bloom     573KB   5%
    time      299KB   2.6%

The dict is half the group, and it is dominated by high-cardinality columns.
Per-column dict, current LZ4 vs flate (entropy) vs a hex nibble-pack
(TestHexDictOpportunity):

    column    distinct  lz4(default)  flate     nibble
    trace_id  100000    1471KB        854KB     781KB   <- hex
    span_id    99998     723KB        425KB     390KB   <- hex
    _msg       78213     658KB        342KB     (text)
    path       94129     518KB        256KB     (text)

Two findings, both the ClickHouse specialized-codec idea:

1. **Hex columns don't compress under LZ4** (1.05x, entry above) because a
   random 16-char trace_id carries 4 bits of entropy per char in an 8-bit byte
   and LZ4 has no entropy coding. Nibble-packing (4 bits/char) halves them
   losslessly -- 1471->781KB (1.88x), 723->390KB (1.85x) -- and decodes FAST (a
   nibble unpack, no entropy decoder), so unlike flate it can be the DEFAULT.
   It even beats flate on hex (781 vs 854) because flate's Huffman approaches 4
   bits with overhead; nibbles are exactly 4 bits. Saving on the two hex columns
   ~= 1023KB, i.e. 11410 -> ~10390KB, realistic ratio 2.62x -> ~2.38x of VL.

2. **Text columns (_msg, path)** compress ~2x more under flate than LZ4 (entropy
   coding of natural-language tokens), but flate decode is slower than the SIMD
   LZ4 kernel -- so it stays a compact-mode lever (already available), not the
   default.

Recommendation: a self-describing hex nibble codec is the next default win. The
dict block format already carries a per-block codec flag (dictCodecFlate = the
rawLen high bit), so a dictCodecHex bit adds it with no format-version change
and full back-compat (old blocks read as before). It is a deliberate change to
the dict read path (blockValAt/dictSectionAt/All/Some/Search all decode blocks),
verified on its own, not folded into an unrelated commit.

## VictoriaLogs clustering is enterprise-only -- no OSS cluster head-to-head

Attempted the simdlogs-cluster vs VictoriaLogs-cluster head-to-head (the metrics
analog beat VM cluster on all three axes). Blocked: VictoriaLogs releases ship only
the single-node binary and an -enterprise.tar.gz; there are no OSS vlinsert /
vlselect / vlstorage binaries (checked v1.50-1.52 release assets). VL clustering is
an enterprise feature, so a fair OSS cluster-vs-cluster comparison is not possible
without a license.

What holds: simdlogs' own cluster (write-routing + replication + federation,
internal/api/cluster.go, tests green) is functional, and simdlogs already beats VL
SINGLE-NODE on the underlying metrics (needle 27.9x, groupby 481x, ingest 4.22x),
which is VL's best available OSS configuration. A cluster-vs-cluster number would
need the VL enterprise binary.

## Where the 2.4x-of-VL disk goes (and what this session's query work bought)

Re-measured the VL head-to-head this session (1M realistic rows, load ~9 so times
are noisy but ratios hold), then optimized the weakest queries.

    query        before -> after (ratio vs VL)
    common        1.7x  -> 1.9x   (99.1 -> 89.8ms)
    and           3.8x  -> 4.4x   (15.9 -> 13.5ms)
    or            1.6x  -> 1.7x   (170  -> 148ms)
    substring     1.6x  -> 1.7x   (168  -> 146ms)
    needle 34.5x, groupby 478.8x, topN 410.2x, histogram 3.4x, ingest 3.94x

The wins came from materialization, not the scan (BenchmarkCommonSelect
15.35 -> 10.37ms, -32%): a []Field allocation PER ROW became one arena per group,
a per-field-per-row map lookup became a positional resolve, and decoded per-row
dict indices are now cached on the (immutable, persistent) Reader under a byte
budget. GC was 23% of that profile.

Disk is the one axis where VL wins: 0.12GB vs 0.05GB. The breakdown at 100k rows
(105 bytes/row total) says why:

    dict     4776KB  46%   LZ4-compressed strings (VL uses ZSTD)
    postings 2815KB  27%   the inverted index -- what makes needle/groupby/topN 34-490x
    index    1831KB  18%   per-row dict ids
    bloom     573KB   6%
    time      299KB   3%

So a third of the footprint is index structures VL does not keep, and the largest
section is dictionaries where our LZ4 trades ratio for decode speed. The 2.4x is
mostly the PRICE of the 34-490x query wins, not waste -- but "better on all counts"
means closing it, and the honest route is tiered storage (recent data with full
postings; aged data re-compacted with stronger dictionary compression and merged
postings), not dropping the index. Tracked as a task.

## Tiered storage closes most of the disk gap (2.40x -> ~1.55x of VL)

The disk axis was the one place VictoriaLogs won. Two levers, both measured on the
realistic 100k corpus (10297KB hot):

    flate dictionaries only          9464KB   -8.1%
    flate + drop the inverted index  6648KB  -35.4%

Flate alone is modest here (the corpus's biggest dict columns, trace_id/span_id,
are already hex-packed, which beats both LZ4 and flate). The real weight is the
postings: ~27% of a group, and dropping them takes the footprint from 2.40x of VL
to about 1.55x.

That is a genuine trade, not free: without postings an equality query falls back to
a decode+scan -- which is what VictoriaLogs does for EVERY query, so cold-tier
queries land near VL's speed while hot groups keep the 34-490x wins. Hence the
tiering shape: recent groups untouched, groups older than -recompact-after
re-encoded, and -recompact-drop-postings opt-in on top.

Guards, because this is a data-rewriting background pass:
- storage: recompaction preserves every row exactly, shrinks in place, is
  idempotent (a group with no LZ4 blocks left is skipped), and survives reopen.
- query: a differential test runs six query shapes (common equality, rare
  equality, absent value, substring, two-predicate AND, time window) against the
  same data with and without postings and requires byte-identical rows. Dropping
  the index is a size/speed trade, never a correctness one.
- mmap lifetime: a query holds the old *Reader across the swap, so replaced
  mappings are retired and unmapped only after a 5-minute grace, not immediately.

## Cluster scaling: at one-node scale the cluster is pure overhead (measured)

Built the cluster benchmark (internal/bench/cluster_test.go, SIMDLOGS_CLUSTER=1):
400k realistic rows through a single node and through 3 storage nodes behind a
router, same queries both sides.

    ingest   single 1.06M/s  cluster-3 0.86M/s  0.82x
    needle   single  88.9us  cluster-3  134.2us 0.66x
    common   single  41.1ms  cluster-3   53.5ms 0.77x
    groupby  single  62.8us  cluster-3  120.3us 0.52x

The cluster is SLOWER on every axis at this size, and that is the correct result,
not a defect: 400k rows fit one node, so sharding only adds an HTTP hop plus a
merge. The sub-millisecond queries are dominated by fan-out RTT (~50us per hop),
which no merge optimization can recover. Clustering buys capacity past one node
and replica fault tolerance -- it does not buy latency on data that already fits.
Anyone quoting cluster numbers has to say at what scale.

Two real router costs were found and fixed while measuring:
- the merge parsed a timestamp per row with time.Parse (RFC3339Nano); a
  hand-rolled parser for the exact shape simdlogs emits replaced it, with a test
  asserting it agrees with time.Parse on every shape and REFUSES anything else so
  the caller falls back rather than mis-ordering. needle 0.28x -> 0.45x.
- the merge copied a string per row (Scanner.Text()); rows are now slices into
  the shard's response body, no copy. cluster common 58.8ms -> 53.5ms.

## The wall-clock gate lied; instructions retired settled it

Ran the full benchmark set interleaved against the pre-session baseline (33b4b12),
min of 3 rounds, on a machine at load 3-5. The wall-clock table reported NINE
regressions, including +94.9% on BenchmarkDictBlockDecode/lz4 -- a pure storage
decode this session never touched. That was the tell. The same benchmarks also
contradicted themselves across runs: BenchmarkEngineNeedle came out +47%, then
+10.4%, then +3.5%, then -2.0%; BenchmarkEngineCount +13.2%, then -1.9%, then
+57.0%.

Instructions retired (deterministic, load- and layout-independent) on the same
binaries:

    DictBlockDecode/lz4   627.6M -> 634.0M   +1.0%   (wall-clock said +94.9%)
    EngineCount            19.94B -> 19.82B  -0.6%   (said +57.0%)
    StatsByField           20.09B -> 20.28B  +0.9%   (said +33.1%)
    EngineNeedle           27.11B -> 27.12B  +0.05%  (said -2.0%)

So there is no regression -- the "regressions" were load noise on microsecond
benchmarks. And the wins are real, not noise in the other direction:

    CommonSelect           18.34B -> 14.50B  -20.9%
    EngineFullScanCount    28.17B -> 25.00B  -11.2%
    EngineWindowed         29.14B -> 27.39B   -6.0%
    SelectiveSkip/select   16.20B -> 15.68B   -3.2%

(The wall-clock wins are larger than the instruction wins -- -30% to -59% -- which
is the reduced allocation and GC pressure showing up in time but not proportionally
in instruction count.)

The 8.3% figure in CLAUDE.md is the LAYOUT noise floor for a quiet machine. Under
load, microsecond wall-clock benchmarks swing far past it in both directions, and
a gate run that way manufactures both false alarms and false wins. On a busy box,
gate on instructions:u. Three REAL regressions were still found this session, and
they were found because the first A/B pointed at them consistently across rounds
and instructions confirmed them -- not because one wall-clock run looked bad.

## Interning ingest field values -- reverted, +5.3% instructions

simdjson's alloc profile put unquote at 44.6% of simdlogs' ingest allocations (one
string per field value). Log fields repeat heavily -- `level` has four distinct
values across 200k rows -- so interning looked obvious: take simdjson's
StringNoCopy (a view into the line, no allocation), use it as a map key (Go
compiles a non-escaping map lookup without copying the key), and store one owned
copy per DISTINCT value. Repeated values would then cost a hash and nothing else.

It cut allocations 8.70M -> 6.85M per 200k rows (-21%) and made ingest SLOWER:

    instructions:u, BenchmarkIngestParallel, min of 2
    baseline (val.String())           153.4B
    intern every value                161.6B   +5.3%
    intern only values <= 32 bytes    162.1B   +5.7%

The map hash on every value costs more than Go's allocator does for a short
string, and restricting it to short values did not help -- most values in the
corpus are short, so almost nothing skipped the hash. Allocation count is not
the same thing as time: the allocator is fast and these strings die young, so the
GC never charged for them.

Reverted. The ingest profile's real weight is simdjson.Parse, BuildDict and
Group.Marshal, not the string allocations underneath them. (Wall-clock said
152 -> 174ms, agreeing with instructions here, but instructions are what settled
it -- the machine was at load 3-5.)

## Compatibility measured, not assumed: 17/40 -> 40/40 against the real VL binary

"simdlogs implements the LogsQL pipes" was a claim backed by a list of parser
cases. Running the same 40 queries against simdlogs AND victoria-logs over
identical data (internal/bench/compat_test.go, SIMDLOGS_COMPAT=1) said 17/40
identical. The gap was never the query engine -- it was result SHAPE and two
boundary semantics, none of which any unit test caught because the unit tests
asserted what the implementation already did:

    _time on every row            stats rows carried _time=1970-01-01, and
                                  `fields a, b` returned a, b AND _time. VL
                                  treats _time as an ordinary projectable field.
    MatAll cleared by any pipe    `* | limit 5` returned five TIMESTAMPS, and
                                  `| delete x` returned only _time. Only pipes
                                  that narrow (fields/stats/uniq/top/...) skip
                                  the full-record materialize; the rewriters and
                                  slicers still emit whole records.
    quantile interpolated         p50=290.5 where VL gives 291. VL uses
                                  nearest-rank -- the result is a real sample.
    uniq kept the whole row       VL emits just the distinct `by` combination.
    top named the column count    VL names it hits, and breaks ties by value asc.
    range(a, b) inclusive         LogsQL parentheses are an OPEN interval:
                                  range(100, 500) does NOT match 100. This cost
                                  exactly one row per query and would never have
                                  been noticed without a reference to diff.
    top N by (...) rejected       `by` was being eaten as the field name.
    math needed a quoted expr     VL takes `math a * 2 as b` unquoted.

Three unit tests had to be CORRECTED against the reference: they asserted
interpolated quantile, inclusive range, and a `count` column, i.e. they had
frozen the implementation's assumptions rather than VictoriaLogs' behaviour. A
test written from the implementation proves the implementation is what it is.

40/40 identical now. The lesson generalizes: for a drop-in replacement, the
compatibility suite has to run BOTH engines, or it measures nothing.

## 32. A status-code probe is not a compatibility test

The API-surface probe compared `resp.StatusCode < 400` on both engines and
reported 0 gaps. On that basis the surface was called complete. Then the same
endpoints were compared by BODY, and seven of them were answering 200 with
something no VictoriaLogs client can read:

    field_names        {"names":[...]}          VL: {"values":[{"value","hits"}]}
    streams            {"streams":[]}           VL: one entry for the empty stream
    stream_ids         {"stream_ids":[]}        VL: {"values":[{"value","hits"}]}
    stream_field_names {"names":[]}             VL: {"values":[...]}
    stats_query        {"count":500}            VL: a Prometheus vector
    hits               [{_time,hits}] unordered VL: dense parallel arrays, zeros kept
    facets             object keyed by field    VL: array of {field_name,values}

Five ingest paths 404'd outright, because VictoriaLogs prefixes every vendor
protocol (`/insert/elasticsearch/_bulk`, `/insert/loki/api/v1/push`,
`/insert/datadog/api/v2/logs`) and we served only the bare vendor path. A
Filebeat or Promtail configured against VictoriaLogs got a 404 on every write.

And `start=1700000000` was read as NANOSECONDS. VictoriaLogs infers the unit
from the magnitude, so a Grafana datasource's epoch-seconds window landed in
1970 and every query answered empty. The status-code probe saw 200 and a
well-formed empty result.

The lesson is the same as the "Compatibility measured, not assumed" entry
(the 17/40 -> 40/40 run) and cost the same twice: compare the ANSWER. A
probe that compares status codes tests that the server is running.

## 33. Two defects the benchmarks could not see

Found while fixing the above, both older than it.

`ValueCounts` asked `dictSectionAt` for one value at a time. That function
decompresses the whole dict block the value lives in, so each block was
inflated once per value it held -- 47% of a `top N by (host)`, and the same
tax on facets, field_values, uniq and stats-by. Decoding each block once:

    top N by (host), 200k rows, 1024 hosts:  12.35ms -> 1.32ms   9.3x

No benchmark measured it because every benchmark that would have exercised it
was measuring something else's total.

Ingest stored a parsed `_time` twice: once in the timestamp column and again as
a near-unique RFC3339 string in a dictionary, which is the worst thing a
dictionary can hold. Nothing read it back -- the wire format prints the row's
timestamp and skips a `_time` field, and whole-record materialization skips the
column by name.

    200k realistic rows:  127.43 -> 110.08 bytes/row   13.6% smaller

Disk was the one axis VictoriaLogs led on, and this was sitting in the writer
the whole time.

## 34. The wall clock called an 11% win a regression

`field_names` was the last operation the reference beat. Two costs were removed
-- a second full pass over every group for a row count the first pass had
summed, and asking for the empty value's ROW LIST when only its length was
used. Wall clock, on a box at load 11:

    before  38.9us/op        after  48.4us/op    "a 24% regression"

Instructions retired, min of three interleaved runs, same machine, same minute:

    before  837M             after  744M        -11.2%

The change is a plain win. The wall clock was reading the user's own workload.
This is the "The wall-clock gate lied; instructions retired settled it" entry's
rule restated: on a busy box, gate on instructions.

## 35. A bounded query decoded the whole timestamp column

`| limit N` and the endpoint's `limit` both trim the match bitset to the rows
they will return, and then `appendMatches` decoded the group's ENTIRE
timestamp column to read them, because the decode span was chosen before the
bound was applied. On the facets path -- which materializes at most a
thousand rows to decide whether `_time` is a facet -- that was a thousand
rows kept out of 131072 decoded:

    BenchmarkFacets   956us -> 220us   4.3x

Narrowing the span to the first and last surviving row fixes it for every
bounded query, not just that one. It was found by timing the PARTS of a
request rather than the request: the whole call was 956us and the piece
responsible was 860us of it, which no profile of the benchmark showed
because the benchmark's own 3M-row setup dominated every profile taken.

## 36. Four "regressions" and three more, all of them the machine

The full A/B against the pre-campaign commit, interleaved, minimum of two
rounds, flagged seven benchmarks outside the 8.3% floor. Instructions
retired, minimum of three interleaved runs, on the same benchmarks:

    EngineCount              wall +16.3%   instructions -0.84%
    EngineFullScanCount      wall +30.0%   instructions -0.85%
    EngineNeedle             wall +30.0%   instructions -0.28%
    Facets                   wall +470%    instructions +1.20%
    DictBlockDecode/hex      wall +39.7%   instructions +2.1%
    Postings medium/lz4      wall +12.3%   instructions -0.3%
    Postings near-uniq/lz4   wall +75.5%   instructions +1.0%

The last three are codec microbenchmarks this campaign never touched.
EngineNeedle read -19.7% in one round and +30.0% in the next, on the same
two binaries, twenty minutes apart -- the machine's load moved from 2.0 to
6.6 between them.

What the same A/B found that was real, and in the same direction on both
measures:

    ValueCounts card=131072   50.3ms -> 3.8ms    -92.4%
    ValueCounts card=1000     217us  -> 17.9us   -91.8%
    BuildDict highcard        15.9ms -> 12.5ms   -21.3%
    IngestParallel            162.6ms -> 145.9ms -10.3%
    Ingest                    685ms  -> 640ms     -6.6%

## 37. Accepted `exists` changes no answer -- decoded, not shipped (source reading)

`internal/api/es.go` decodes an `exists` clause (the `esClause.Exists` JSON
field) but `esToQuery` never walks it: only `Bool`, `Term`, and time
`Range` become predicates, so a search whose only clause is
`{"exists":{"field":"x"}}` matches the whole window, exactly as an empty
query would. It is not a probe result -- no committed test exercises it
(the ES contract tests cover bool/term/range only) -- so nothing measures
the gap. The wire accepts the clause; the answer ignores it, which is the
worst class of defect for a drop-in client: a 200 that reads as success.

The source comments disagree with the code: `es.go`'s package comment lists
"terms/range/exists" as part of the mapped DSL, and the exists handler's
own comment says exists "become predicates". Both are stale
implementation-doc defects (recorded in `docs/roadmap.md`); a future TDD
task either implements exists as a real predicate or rejects the clause
with an explicit error -- acceptance-without-effect is not a supported
state.

## 37. The point-read threshold was a fraction when the cost is absolute

The materialize path point-reads a row's fields below n/16 matches and
bulk-decodes above it. The guard is right at its edges -- one needle row
must not decode whole columns -- but a fraction of the group is the wrong
shape for it: at 4k matched rows in a 128K group it chose point reads, and
each point read decompresses the dict block its value lives in. Entry 33's
pathology again, on a different path: ~37k block inflations to materialize
7.5k rows.

Found by the agreed 3M harness, whose narrow-window selective query was
the one shape still losing to the reference (20.6ms vs 11.2ms, stable
across four runs, and NOT a campaign regression -- the pre-campaign
baseline measured the same 20ms). No full-window gate could see it: the
full-window queries take the bulk path already.

An absolute bound (512 rows) beside the fractional one:

    windowed selective, 3M corpus   21.8ms -> 7.0ms   VL 11.1ms   loss -> 1.6x
    per-op gate, new windowed shape           10.1ms   VL 25.9ms   2.56x
    needle                                     ~10us   unchanged

The shape is in the per-op gate now, so it cannot quietly regress again.

## Reference-counted mappings are not enough without referencing the candidates

**Believed.** Task 4.1 gave every group version a reference count, so
retention, recompaction and demotion could retire a version and let the
last reader release it. With that in place, tiering was thought safe.

**Actually.** It segfaulted within three concurrent runs. `Recompact`
collected its candidate entries under a read lock, released the lock, and
then called `needsRecompact()` on each raw `*Reader` -- doing IO and
parsing outside the lock, on purpose, so a rewrite does not stall
queries. Retention retired and unmapped one of those candidates in the
meantime, and the next `get32` read a freed page:

    unexpected fault address 0x7f18e902319d
    [signal SIGSEGV: segmentation violation]
    encoding/binary.littleEndian.Uint32(...)
    storage.parseDictSec(...)  dict.go:209
    storage.(*Reader).needsRecompact(...)  recompact.go:34
    storage.(*Store).Recompact(...)  recompact.go:106

`Demote` had the identical shape: candidates collected under the lock,
`os.ReadFile` and the cold upload outside it.

The reference count protects a reader that *holds* one. A candidate list
is a set of pointers with no reference taken, which is exactly what the
old raw-`*Reader` API was -- the bug moved rather than went away.
`acquire()` on each candidate with a deferred `release()` closes it.

**How it surfaced.** `TestTieringOperationsConcurrent`, written for task
4.3, running retention, recompaction, demotion and eight snapshot loops
against one store. It failed on the first run, before any of the code it
was written to test had been reviewed. A structural mutex serializing
recompaction against demotion would not have caught it: retention does
not take that mutex, and does not need to.

## Ten reviewed commits, eighteen findings: what self-assessment missed

**Believed.** Tasks 3.1-3.3, 4.1-4.4, 1.1, 1.2 and 1.4 were each written
test-first, passed their own tests, `-race`, vet and gofmt, and were
committed. Task 2.2 had been reviewed by a subagent and its fourteen
findings fixed; the ten after it were not reviewed.

**Actually.** An adversarial review of those ten commits returned
eighteen findings, most reproduced with working probes:

- `/_search` and `/_count` were registered with no auth wrapper. An
  anonymous POST returned another tenant's `_source` documents. The
  hand-written route matrix in `auth_test.go` listed routes by hand and
  did not enumerate the mux, so it could not see them.
- Router-mode write forwarding returns before the mux, so none of the
  per-route wrappers ran: an anonymous 4 MiB POST was relayed to a
  backend on a server configured with a 64 KiB body limit.
- `Recompact` never took `structMu`, although the field's own comment
  claimed it serialized recompaction. Its cleanup path then deleted a
  *live committed* group's file, after which `OpenStore` failed with
  "group N is committed but its file is missing" -- the tenant unopenable.
- The live tail still called `GroupsAfterID`, which hands out raw
  readers. `SnapshotAfterID`, written for task 4.1, had zero callers.
- The v8 "bounds-safe" parser validated the footer and nothing else.
  Column decodes are driven by `Rows`, which was never checked against
  the column's data span: a group claiming a million rows over a
  512-byte span sliced past the blob. `parseDictSec` had no bounds check
  at all, and `needsRecompact` reaches it from the tiering goroutine,
  which has no recover -- one corrupt file killed the process.
- `dropGroups` filtered with `kept := s.groups[:0]`, overwriting the
  backing array before the manifest commit was known to succeed. A failed
  commit turned index `[0 1 2]` into `[1 2 2]`: one group invisible until
  restart, another counted twice by every query.
- A write racing `Server.Close` sent on a closed channel. The panic
  unwound past a bare `w.mu.Unlock()`, so the mutex was never released
  and every later `Close` blocked forever.
- `manifest.commit` had no torn-tail recovery, so after a short write an
  acknowledged, fsynced append vanished at the next restart with no error
  anywhere.

**How it surfaced.** A reviewer that was told not to be agreeable, given
the task specs, and asked for `file:line` plus a reproduction. Nothing
here was visible from inside: every one of these commits was green.

The rule that follows is not "review before committing" -- task 2.2 was
reviewed. It is that the reviewer must not be the author, and after
fixing a reviewer's findings a *different* reviewer signs off, because a
reviewer grading its own feedback is grading its own work.

## An OTLP metrics payload whose Metric carries no name is stored as a log row

**Believed.** The wire-type discriminator in `internal/ingest/otelproto.go`
tells logs from metrics and traces by looking at each record's field 1:
`LogRecord.time_unix_nano` is field 1 wire type 1 (fixed64), while
`Metric.name` and `Span.trace_id` are field 1 wire type 2. A record whose
field 1 is length-delimited is the wrong signal and is rejected.

**Actually.** proto3 omits an empty `string`, so a `Metric` with no name has
no field 1 at all and the discriminator has nothing to key on. Measured:

```
NAMED metric            -> accepted=0 rejected=1 err=records carry no log timestamp...
UNNAMED metric          -> accepted=1 rejected=0 err=<nil>
description-only metric -> accepted=1 rejected=0 err=<nil>
traces payload          -> accepted=0 rejected=1 (Span.trace_id is always present)
```

The row lands with a fabricated fallback timestamp.

**The first version of this entry got the reasoning wrong**, and review
caught it. It claimed the only discriminator left was
`res.Accepted > 0 && !sawLogShape`, whose cost would be rejecting a legal
`LogRecord` batch where every record omits both timestamps. Three more
discriminators exist, each unambiguous, and none of them touches that case:

| field | Metric | LogRecord | wire types |
|---|---|---|---|
| 1 | `name` (string) | `time_unix_nano` (fixed64) | 2 vs 1 |
| 2 | `description` (string) | `severity_number` (enum) | 2 vs 0 |
| 7 | `sum` (message) | `dropped_attributes_count` (uint32) | 2 vs 0 |
| 11 | `summary` (message) | `observed_time_unix_nano` (fixed64) | 2 vs 1 |

A length-delimited value at any of those four field numbers cannot be a
`LogRecord`. Measured before the fix, each stored as a log row:

```
description-only metric (field 2 wire 2)  accepted=1  STORED
unit-only metric        (field 3 wire 2)  accepted=1  STORED as severity=ms
sum-only metric         (field 7 wire 2)  accepted=1  STORED
summary-only metric     (field 11 wire 2) accepted=1  STORED
timestampless LogRecord (sev 2 wire 0)    accepted=1  must keep working -- does
body-only LogRecord     (field 5 wire 2)  accepted=1  must keep working -- does
```

**Fix: all four rejected.** `wrongShape` now fires on
`fw == 2 && (fn == 1 || fn == 2 || fn == 7 || fn == 11)`.

**Residual risk, accepted -- and the first count of it was wrong.** This
entry said two field numbers stay ambiguous. It is five. Every `Metric`
field is wire type 2, and `Metric` intersects `LogRecord` at wire 2 on
`{3, 5, 9, 10, 12}`:

```
field 3  (unit)                   -> accepted=1   (was documented)
field 5  (gauge)                  -> accepted=1   (was documented)
field 9  (histogram)              -> accepted=1   UNDOCUMENTED
field 10 (exponential_histogram)  -> accepted=1   UNDOCUMENTED
field 12 (metadata)               -> accepted=1   UNDOCUMENTED
field 2 / 7 / 11                  -> rejected     correctly caught
```

`Metric.metadata` = 12 (OTLP v1.2.0) collides with `LogRecord.event_name`
= 12 (v1.5.0), which is why 12 cannot join the reject list without
rejecting a legal log record. Same for 3 and 5. Fields 9 and 10 CANNOT be added, and the first
version of this paragraph gave the opposite reason -- it said they had no
`LogRecord` counterpart at wire 2. They do: `LogRecord.trace_id` is field 9
and `LogRecord.span_id` is field 10, both `bytes`, both wire type 2, and
both present on every trace-correlated log line. `wrongShape` fires on
field number and wire type alone, before the record is stored, so acting on
the stated reason would have rejected exactly the logs an OpenTelemetry
deployment cares most about.

A metric posted to `/v1/logs` carrying only one of those five is still
stored as a log row. The four discriminators that ARE applied
(`fw == 2 && fn in {1, 2, 7, 11}`) reject no legal `LogRecord`: fields 1
and 11 are `fixed64`, 2 is an enum, 7 is `uint32`, and none is `repeated`
in any OTLP version from v0.7.0 on, so no packed encoding puts them at
wire 2.

## The allocation sweep: four measurements that pointed the wrong way

An allocation sweep of `internal/query` and `internal/storage` landed five
changes, each with both arms compiled into ONE binary and benchmarked
interleaved (`-count=6`, compared on the minimum). None was rejected. What
follows is the part worth keeping: the measurements that supported a
conclusion nobody should have drawn.

**A benchmark shape that measured a two-entry dictionary and called it
65536.** The first `BenchmarkDictSectionAllArena` built its high-cardinality
row from an LCG's LOW nibble:

    seed = seed*1664525 + 1013904223
    b[j] = hexChar(byte(seed & 0xf))

An LCG's low four bits have period 16. The 65536 "distinct" values
deduplicated to a two-value dictionary, and the row measured 56 B/op in 3
allocations on both arms — reported as "no difference at high cardinality",
which is exactly the shape the change was for. The high nibble
(`seed >> 28`) gives the shape its name claimed, and the arms differ by 32x:

    highcard-64k  arena       562,277 ns   3,538,945 B/op    2,049 allocs/op
    highcard-64k  per-value 1,133,369 ns   3,538,945 B/op   66,561 allocs/op

A benchmark shape is a claim about the data, and it needs checking like any
other claim. The allocs/op column was the tell: 3 allocations cannot decode
a thousand blocks.

**"The decoder fills the buffer" is not the same as "the decoder writes
every byte."** `flateDecompress` discards `io.ReadFull`'s error, so a
truncated block leaves the tail of its output unwritten. With a fresh
allocation per call that tail read as zero. Reusing one buffer across a
section's blocks returns the PREVIOUS block's bytes there instead —
characters from another dictionary value, at a place the offset table says
is a value. Disabling the added `clear(out[n:])` and re-running the
poisoning test returned a block whose tail was 0xDE repeated where the
allocating form returned zeros. The reuse is worth having:

    lz4/reuse   5,254 ns   22,784 B/op   12 allocs/op
    lz4/fresh   5,856 ns   30,208 B/op   17 allocs/op
    hex/reuse   3,348 ns   22,272 B/op   10 allocs/op
    hex/fresh   4,569 ns   36,096 B/op   17 allocs/op

but it is worth having only with the zeroing, and no wall-clock or
allocation number would ever have shown the difference.

**A pool removed 5.6 MB per operation and moved allocs/op by nothing.**
Pooling the per-group timestamp decode, on the ten-group full scan:

    pooled  min 2,329,783 ns   26,245,153 B/op   286 allocs/op
    make    min 2,407,044 ns   31,874,165 B/op   286 allocs/op

The 3.2% on wall-clock is inside the 8.3% layout noise floor, and allocs/op
is identical at the minimum: a `sync.Pool` miss allocates too, so the pool
trades many large allocations for fewer large allocations plus some small
ones, and the COUNT barely moves while the BYTES fall 17.7%. Read on
allocs/op alone this change looks like nothing. `perf stat` on the same
binary, 500 iterations each, is what settles it:

    pooled  127,578,211,079 instructions:u   32,858,150,947 cycles:u
    make    145,627,151,353 instructions:u   35,285,059,549 cycles:u

12.4% fewer instructions retired, 6.9% fewer cycles, layout- and
load-independent.

**A red `-race` gate that was not a regression.** `go test -race -short
./...` failed on `TestGetU32LengthAndReuse` — "a 600-element request did not
reuse the 600-element buffer just returned" — and kept failing with nothing
else running. Nothing in the pool had changed. `sync.Pool.Put` drops the
value at random one time in four when the race detector is enabled
(`go/src/sync/pool.go:103`, "Randomly drop x on floor"), so a test that
asserts reuse across a single round trip is red in a quarter of all race
runs, by design of the runtime. The test now takes reuse within a few
attempts. The assertion is the same; the flake is gone.

**Cross-build wall-clock disagreed with itself.** Comparing the sweep's
before and after end-to-end runs, three benchmarks looked worse and one
looked implausibly better:

    EngineNeedle         13,220 ns before    7,537 ns after   (identical 96 allocs, 40,360 B)
    EngineFullScanCount 339,525 ns before  445,964 ns after   (+1 alloc)

Five samples of the SAME build put both inside one distribution:

    EngineFullScanCount  269,631 .. 390,944 ns   3,457,371 .. 3,568,654 B/op   659 .. 671 allocs/op
    EngineCount           85,216 .. 107,873 ns     266,997 ..   276,338 B/op    34 .. 35 allocs/op

A parallel group scan's allocation count is not deterministic — which
worker gets which group decides which buffers grow — so ±1 alloc/op and a
30% ns spread are the floor of what a single end-to-end sample can say.
The per-change interleaved runs are the evidence; the end-to-end table is
read for the exact columns (B/op, allocs/op) and only where the change is
large.

## A process kill cannot test the fsync boundary: the page cache outlives it

Task 3.4's crash matrix SIGKILLs a child at eleven persistence phases and
reopens the store. The `manifest-append` phase — the commit record written but
deliberately **not** fsynced — was written expecting the batch to be
INVISIBLE after recovery. It is visible, at `-count=20`, every time.

That is not a defect. SIGKILL destroys the **process**; it does not destroy the
**page cache**. A `write()` that returned put the bytes in the kernel's cache,
and the next `open()` reads them straight back. Only a power loss or a kernel
crash discards those pages. The expectation was wrong, not the store.

What the matrix therefore does and does not establish:

| Claim | Established by a process kill? |
|---|---|
| No partial group adopted | yes |
| No leftover `.tmp` adopted | yes |
| No acknowledged batch lost | yes |
| No batch duplicated | yes |
| A group file with no commit record stays invisible | yes |
| **An unsynced commit record is discarded on replay** | **no** |

The last row needs the unsynced writes actually dropped: `dm-flakey` with
`drop_writes`, a filesystem image discarded at the block layer, or an
`LD_PRELOAD` that turns `fsync` into a no-op and then crashes the process.
None of those are in this repository, so the claim is not made.

The phase stays in the matrix with the corrected expectation, because the other
three clauses still apply to it and because a change that made this batch
invisible after a process kill WOULD be a regression — it would mean replay
discarded a record the kernel still held.

The cost of getting this wrong the other way is the reason it is recorded: a
matrix that asserted invisibility here would have failed for a correct store,
and the obvious "fix" — making replay drop unsynced-looking records — would
have been a data-loss bug shipped to satisfy a bad test.

## Recompaction has no commit record, so its crash contract is stronger

Extending the matrix to `manifest.compact` and `Store.Recompact` (task 3.4
step 4) started from the assumption that they would need the same per-phase
expectation table as `AppendGroup`: some phases before the commit point, some
after. They do not, and the reason is worth writing down.

`Store.Recompact` re-encodes a group file **under the same path and the same
ID** and writes **no manifest record at all**. There is no commit point to be
on either side of — the rename is the entire commit, and it swaps one encoding
of the same rows for another. `manifest.compact` is the same shape: it folds
the record log down to a single record naming exactly `visibleIDs()`.

So the contract is not per-phase, it is uniform: **visibility-neutral at every
phase**. All batches present, each exactly once, no partial group, no duplicate
rows, wherever the kill lands. That is a sharper assertion than the append
matrix's, and it is cheaper to state.

Four things nearly made it vacuous, and an adversarial review found the last
three after the first was already fixed:

- The hook closure reads the outer `crashOnBatch`, and the clean setup loop
  leaves `batch` past it — so the hook could never match during the operation
  under test. Every rewrite subtest would have passed without a single crash.
  Caught because the subtest fails when the child does not crash.

- "The child did not crash" was not the same as "the child ran to completion".
  `runCrashChildOp` classified the exit two ways, signalled or not, and
  discarded the code — so every `CHILD_*` error exit read as "did not crash",
  which is exactly what `TestRewritePhaseCoverageIsComplete` asserts. With
  `Recompact` stubbed to return an error immediately, all four of its subtests
  PASSED, with a `t.Logf` line as the only evidence. The test written to stop
  the matrix going vacuously green was itself vacuous. It now fails on any
  non-zero exit that is not a SIGKILL and reports the code and the child's
  last line.

- The append matrix's expectation was derived entirely from what the child
  acknowledged, which left it with no lower bound. With the crash moved to
  batch 0, `acked` is empty, the presence loop runs zero times, and **eight of
  the eleven subtests passed** — `buffering`, `temp-create`, `partial-write`,
  `file-sync`, `file-close`, `rename`, `dir-open`, `dir-sync`; only the three
  manifest-side phases failed. The acknowledged set is now asserted exactly,
  and the same break fails **eleven**.

  An earlier revision of this bullet said fifteen. Measured: 11 with the guard,
  8-of-11 passing without it. It also said the eight passed "against a
  directory holding a 0-byte MANIFEST and nothing else", which is true of
  `buffering` and `temp-create` only — four phases leave a `.tmp` alongside,
  and two leave a group file.

- A partial batch fired neither branch of the presence loop. `countOf` returns
  `-1` for a batch present in part, which is neither `0` nor `> 1`, so the
  `if n == 0 / else if n > 1` pair let a torn group adopted for an
  ACKNOWLEDGED batch pass in silence — clause one of the contract the file
  opens with. The interrupted-batch check had the mirror hole: `count(...) > 0`
  read `-1` as absent, which passes at the eight phases where absence is what
  is wanted. Both are three-way now, the way the rewrite matrix already was.

- Whether the fixture qualifies for recompaction at all. The first version of
  this entry said the 4-row fixture "never qualifies", so `writeFileAtomic` was
  never reached and no phase was live. That was written from reading
  `Recompact`, not from running it, and it is false: the child passes
  `dropPostings=true`, and a group carrying postings is a candidate on that
  alone, whatever its size. Measured, three batches, `Recompact(1<<62, drop)`:

  | rows | dropPostings | groups rewritten | bytes |
  |---|---|---|---|
  | 4 | false | 0 | — |
  | 4 | true | 3 | 1110 → 942 |
  | 256 | false | 3 | 13725 → 11948 |
  | 256 | true | 3 | 13725 → 10868 |

  The fixture stayed at 256 rows, for the reason the table shows rather than
  the one first claimed: at 4 rows only the postings check makes it a
  candidate, so the size test `Recompact` applies to every candidate is never
  reached; at 256 the flate rewrite is itself smaller and that test runs.
  `TestCrashRecompactFixtureIsActuallyRecompacted` fails if it stops
  qualifying. The byte figures are totals across three groups, not one fixture.

  An entry in this file recording a non-measurement is the exact failure the
  file exists to prevent. The correction is appended rather than edited over
  the original, because the original is the finding.

## The manifest's bootstrap gate adopted uncommitted groups, and no crash phase could reach it

`OpenStore` adopts every group file on disk when a directory predates the
manifest — a one-time migration for stores written before commit records
existed. The gate was:

```go
if len(man.visible) == 0 && len(onDisk) > 0 {   // -> bootstrap(everything on disk)
```

"The visible set is empty" is true of a legacy directory. It is also true of
two states that are its exact opposite, and both were adopted. Measured:

| Setup | MANIFEST | Result |
|---|---|---|
| `OpenStore`+`Close`, then a valid `group-0.bin` with no commit record | 0 bytes | **adopted** — an unacknowledged batch became readable |
| Append id 0, `CommitRemoval(0)`, file left on disk | 72 bytes, visible empty | **resurrected** — a committed removal came back |

The second is verbatim the failure `manifest.go`'s own header says the manifest
was introduced to prevent. It is reached by a crash between retention's
commit-remove and its unlink, or by an unlink that failed and left a tombstone,
whenever the victims are the last live groups.

**Why eleven crash phases could not see it.** `crashBatches` is 3 and the crash
always lands on the last batch, so batches 0 and 1 commit first and the visible
set is never empty at recovery. `TestCrashUncommittedGroupFileIsInvisible`
missed it for the same reason — it commits batch 0 before planting the orphan.
Set the batch count to 1 and `dir-open` and `dir-sync` fail immediately, with
no other change. A matrix can be green across every phase and still never
construct the state the defect needs.

**The fix, and the trap inside the fix.** The gate is now "the MANIFEST file
was not there", recorded by `openManifest` before it touches anything. That
needs the file NOT to be created during the decision, so the append handle
became lazy — and that first attempt was wrong in the other direction: with
nothing creating the file, a fresh store that wrote its first group and crashed
before committing it is byte-identical on disk to a legacy directory. Adopting
came straight back, and `TestUncommittedGroupIsInvisibleWhenNothingElseIsCommitted`
caught it.

The ordering that satisfies both: read the file's existence, decide, and only
then create it — last in `OpenStore`, but still before any group file can be
written. A legacy directory whose previous open died mid-validation still has
no manifest and is still adopted; a store that has been opened once always has
one, so its uncommitted groups are never mistaken for legacy data.

**And the fix's own second defect, found by the review of the fix.** `bootstrap`
reported failure through `m.reopen()`, and `reopen` opens with `O_CREATE`. On a
legacy directory the MANIFEST does not exist, so a FAILED bootstrap created a
0-byte one -- precisely the fact the new gate reads. Measured, two valid legacy
groups, an error injected at `faultWrite`:

```
first open failed as designed: injected ENOSPC
MANIFEST now EXISTS, 0 bytes, after the FAILED bootstrap
legacy batch 0 present 0 times, want 1
legacy batch 1 present 0 times, want 1
```

Every later open saw a manifest that existed, skipped the bootstrap, replayed
nothing, and returned a store with zero groups **and no error**, on a directory
holding the only copy of the data. Under the old gate the next open
re-bootstrapped and recovered; the fix turned a transient, self-healing disk
error into permanent silent loss. The `reopen()` call is deleted -- every
writer goes through `ensureOpen`, so it was redundant as well as harmful --
and `TestFailedBootstrapLeavesTheLegacyDirectoryRecoverable` injects the fault
and fails without it.

That is three consecutive wrong answers on one gate: the original
empty-visible-set test, the lazy-handle fix that made a fresh store
indistinguishable from a legacy directory, and the error path that created the
file the gate reads. Each was found by a reviewer who did not write it.

Three tests pin it, and all three fail against the old gate:
`TestUncommittedGroupIsInvisibleWhenNothingElseIsCommitted`,
`TestRemovedGroupStaysRemovedWhenItWasTheLastOne`, and
`TestCrashRecoveryMatrixFirstBatch` (at `dir-open` and `dir-sync`).
`TestLegacyDirectoryWithNoManifestIsAdopted` passes both ways and exists to
stop the fix from being an over-correction.

## A corruption policy whose own crash window made the store permanently unopenable

Task 3.5 added a `quarantine` policy: move an unreadable group aside, open with
what remains. The move writes the record, renames the file, then commits the
manifest removal — and the missing-file check ran BEFORE the policy check, so
a crash in that window left the manifest naming a group now in `quarantine/`
and every later open returned:

```
storage: group 1 is committed but its file is missing
```

Under **both** policies, forever. Quarantine could not recover from its own
crash window, which is the one failure it exists to survive. Reproduced with
the package's own fault injector at `faultManifestWrite`, which is exactly that
window. A committed group whose file is absent and which has a record in
`quarantine/` is now treated as a completed quarantine and removed from the
manifest; a missing group with no record is still a hard error.

The comment justifying the ordering was also false. It said the reverse order
would leave "the state OpenStore's legacy-directory gate has already been wrong
about twice" — but that gate is `!man.preexisted && len(onDisk) > 0`, and in
every quarantine scenario the manifest exists, so the gate cannot fire. The
order is right for a different reason, which is now the one written down: a
quarantined file with no record is evidence destroyed, and the record is the
entire point of quarantining rather than deleting.

## A degradation signal that read zero one restart after permanent data loss

`Degraded()` was `Corrupt > 0`. `Corrupt` is what *this* open found, and the
quarantining open removes the group from the manifest — so the next open sees a
consistent store, finds nothing corrupt, and reports healthy. Measured through
the API, after a restart following a quarantine:

```
simdlogs_storage_corrupt_groups 0
simdlogs_storage_quarantined_groups 1
simdlogs_storage_degraded_tenants 0
simdlogs_storage_degraded_unacknowledged_tenants 0
/-/ready = 200
```

`_degraded_unacknowledged_tenants` is documented as "the one to alert on", and
it read 0 with a group permanently gone. The degradation was process-lifetime
while the loss was durable.

Compounding it, acknowledgement was deliberately NOT persisted, on the
reasoning that a restart should re-ask the operator. It never re-asked: the
restart cleared the degradation too, so instead of being asked again the
operator was told the store was healthy.

`Degraded()` is `Corrupt > 0 || Quarantined > 0` now, and the acknowledgement
is a marker in the quarantine directory carrying the count it accepted — same
count, still acknowledged; one more quarantined group, unacknowledged again.

## Four more ways the surface was reachable only in tests

None of these is subtle, and all four survived writing the feature and its
thirteen tests:

- **No flag.** `config.Config.CorruptionPolicy` had no `-corruption-policy`
  flag, so an operator running the shipped binary could only get `fail`. The
  policy was configurable for embedders and tests.
- **No endpoint.** `Server.AcknowledgeDegraded` had no route. The only way to
  clear a 503 was a restart — which, per the entry above, also erased the
  degradation.
- **Eviction reset it.** Readiness walked the OPEN tenants, so evicting an idle
  degraded tenant turned 503 into 200 while the data was still missing.
  Measured with `MaxOpenTenants: 2`. Degradation is recorded on the server now,
  keyed by tenant, and survives eviction.
- **`/insert/ready` answered 503.** A quarantined group is old data and the
  store takes writes normally, so failing the ingest probe converted a
  read-side loss into an ingest outage and took the node out of the ingest
  Service. It also contradicted two documents that call it a 200 probe. Only
  `/-/ready` reflects storage health.

The pattern across all four: the feature was built and tested from the inside,
where every surface is reachable by calling the method.

## The `fail` policy did not fail on a legacy directory

`OpenStore`'s bootstrap loop — the one-time migration for directories written
before the manifest existed — excluded any group it could not read, silently.
The group never reached the loop that applies the policy, so `fail` neither
failed nor reported. Measured on a three-group directory with the MANIFEST
removed and one group corrupted:

```
OpenStore (default policy) succeeded
Health: "healthy: 2 groups"   Corrupt: 0   Ready: true
```

A silent drop with a clean health report, from the policy whose entire purpose
is to refuse rather than serve short.

## Three mutations that left the suite green, and one that still does

A reviewer broke the feature three ways and ran the full suite:

| Mutation | Before | Now |
|---|---|---|
| invert the quarantine order: rename, then write the record | green | caught |
| flip the server's default policy from `fail` to `quarantine` | green | caught |
| delete both `syncDirNamed` calls from the quarantine path | green | caught |

The first two are pinned by `TestQuarantineWritesTheRecordBeforeMovingTheFile`
and `TestServerDefaultPolicyRefusesACorruptTenant`.

The third was recorded here as uncatchable — "proving a directory sync needs
the unsynced entries actually dropped, which is a power-loss rig this
repository does not have". That conflated two claims. Proving the KERNEL drops
something does need a rig; proving the CALLS HAPPEN does not, and `syncDir`
already carries `fault(faultDirSync)`, the same injection point the crash
matrix uses one entry above. `TestQuarantineSyncsBothDirectories` counts the
calls and requires an error from one to reach the caller, and goes red with
either `syncDirNamed` deleted.

Recorded because it is the second time in this task that "cannot be tested"
turned out to mean "I did not look for the injection point that was already
there".

Also caught by the same review: group ids are reused, so two quarantines of one
id renamed onto one name and one record wrote over the other — destroying the
evidence quarantine exists to keep. Quarantined files carry the checksum in
their name now, and `nextID` advances past a quarantined id.

## Six more, four of them in the code that fixed the last four

The review of the round above found six new defects. Four are in the code
written to fix findings from the round before it, which is the part worth
recording: a fix made under time pressure to close a hole is written by someone
thinking about that hole, and it opens adjacent ones.

**A filename check authorized dropping a committed group.** The recovery gate
for an interrupted quarantine matched `group-<id>-*.json` and never opened the
file. One empty `quarantine/group-1-00000000.bin.json` dropped into a store,
with `group-1.bin` deleted:

```
degraded (unacknowledged, policy fail): 2 groups serving, 1 corrupt,
0 quarantined: group 1: quarantined by an earlier open
QuarantinedGroups lists 0 records
```

Under the **default** policy. A store reporting a completed quarantine with
nothing quarantined has laundered a missing group into a clean state, and the
token that authorized it is invisible to the operator listing. The gate reads
the record now, requires it to name that id, and requires the file it says it
moved to be there — which also makes it agree with `QuarantinedGroups` and
`countQuarantined`, which read the same directory and disagreed with it.

**A quarantined id was reissued, and the stale record then laundered a real
loss.** `nextID` advanced past every id in `visibleIDs()`, and the quarantining
open REMOVES the id from the manifest — so the next open regressed past it.
Entirely from the store's own behaviour:

```
quarantine the top id (2) -> restart -> AppendGroup returns id 2 again, real data
-> that file goes missing behind the manifest's back
-> OpenStore (DEFAULT policy): "degraded ... 1 corrupt, 1 quarantined:
   group 2: quarantined by an earlier open"
```

A genuine loss reported as an old quarantine. The LLD's claim that "`nextID`
advances past a quarantined id so the store does not reissue it" was true only
inside the open that quarantines. It is now the maximum over the group files on
disk and the quarantine directory, both of which carry every id ever issued.

**`fail` still did not fail when the file could not be MAPPED.** One line above
the `ReadGroup` branch that had just been fixed, `mmapFile`'s error was a bare
`continue`. Measured with mode 000 on one group of a three-group legacy
directory: `OpenStore` under `fail` returned `healthy: 2 groups`, `Corrupt: 0`.
The mirror in the main loop was a hard error under **both** policies, so
quarantine could not quarantine the one kind of damage most likely to need it.
Both take the policy now, and the record's checksum became best-effort —
refusing to move a group because its bytes cannot be read leaves the store
unopenable under the policy chosen to keep it open.

**Concurrent acknowledgement returned 500.** `writeFileAtomic` uses a fixed
`path + ".tmp"`, so two writers race on one temp name and the loser's rename
finds nothing. Through HTTP, three runs of 100 concurrent POSTs returned 9, 22
and 25 failures:

```
500: acknowledged 0 tenant(s), then failed: rename .../ACKNOWLEDGED.tmp
     .../ACKNOWLEDGED: no such file or directory
```

The contents were never wrong — every writer writes the same bytes — so this
was availability, on the one endpoint whose job is clearing a readiness
failure.

**Readiness still missed a tenant no request had touched.** Degradation is
recorded when a store OPENS, and `NewServerConfig` opens only the default
tenant. `/-/ready` answered 200 at startup with a degraded tenant on disk and
503 after one request to it — a probe whose job is keeping traffic off, going
green until traffic arrived. Every tenant directory is scanned at startup now,
one `ReadDir` each, no store opened.

**`Sscanf("%d")` ignored trailing input**, so an `ACKNOWLEDGED` marker reading
`"1 and whatever else"` acknowledged 1. Empty, `"yes"`, `"-1"` and a 23-digit
overflow were already refused. `strconv.Atoi` on the trimmed contents closes
it.

And the config check, wrong for the third time in three rounds: it duplicated
storage's policy set (round one), then rejected the surrounding whitespace the
parser deliberately trims (round two), so `-corruption-policy=" quarantine "`
was a startup error for a value storage considers valid. It is gone. The parser
owns the set, `NewServerConfig` calls it, and an unknown policy is still a
startup failure — from the one place that knows the answer.

## The readiness fix left /metrics behind, so the alert metric was blind to exactly the case it was added for

The round above moved readiness onto the server's own record of degraded
tenants, so an evicted or never-opened one still counts. `/metrics` was left
walking `forEachTenant`, which is open tenants only — the same walk that was
just replaced. Measured on a server whose `tenant-7-0` is degraded on disk and
untouched in this process:

```
/-/ready: 503
NOT READY: 1 degraded tenant(s)
7:0: degraded (unacknowledged, from the store directory): 1 quarantined

/metrics:
simdlogs_storage_corrupt_groups 0
simdlogs_storage_quarantined_groups 0
simdlogs_storage_degraded_tenants 0
simdlogs_storage_degraded_unacknowledged_tenants 0
```

The probe pulls the pod out of rotation and the alert never fires.
`docs/lld/storage.md` names `_degraded_unacknowledged_tenants` as "the one to
alert on", so an operator following the LLD is blind to the failure the whole
scan exists to surface — and two endpoints on one server disagree about one
tenant.

Both now read `s.degraded` through one function.
`TestMetricsAgreeWithReadinessAboutAnUntouchedTenant` asserts they agree before
and after acknowledgement, and fails against the old walk.

The shape is the one this task keeps producing: a fix that changes where a
fact comes from has to change **every** reader of that fact, and the one that
gets missed is the one nobody was looking at while fixing the bug.

### Three smaller ones from the same review

- **`HealthOfDir` reported unknowns as zeroes.** It reads a store directory
  without opening it, so Groups, Corrupt and Policy are unknown — and
  `Health.String()` printed them anyway: `0 groups serving, 0 corrupt, policy
  fail` for a store with several groups on a server running `quarantine`. It
  carries a `FromDirectory` flag now and prints only what it knows.

- **The `nextID` justification overclaimed.** The comment and the LLD said the
  group files and the quarantine directory "carry every id ever issued".
  Retention unlinks: three groups, `DropGroupsBefore` removes all three,
  reopen, and the first new id is 0. The property that matters is narrower and
  does hold — a *quarantined* id is never reissued, because a quarantined file
  is always in `quarantine/` — and a retention-removed id cannot be laundered
  either, since it leaves no record and the recovery gate requires one. The
  claim was wrong, not the code.

- **The best-effort checksum duplicated its own error.** A group that cannot be
  mapped usually cannot be read either, so the reason read `permission denied
  (checksum unavailable: permission denied)`. And the comment on `ackMu` said
  cross-process races are "prevented by the store lock", which is false in the
  one case that matters: `AcknowledgeDegradedDir` never takes the lock, and it
  exists for directories whose store is not open.

## The documented remediation stranded the replica, and "one function" was two

Two more from the review of the round above, both in the code that fixed it.

**Emptying the quarantine directory did not clear anything.** `docs/lld/storage.md`
says degradation "clears when the quarantine directory is emptied, which is an
operator deciding the evidence has been dealt with". Doing exactly that, for a
tenant not currently open:

```
after emptying the quarantine directory: AcknowledgeDegraded = 0, <nil>
/-/ready = 503
7:0: degraded (unacknowledged, from the store directory): 1 quarantined
metrics: degraded=1 unacked=1 quarantined=1
```

No escape but a process restart. It is the interaction of three fixes that are
each right on their own: the server's record was made to survive without an
open store, so eviction and restart could not hide a degraded tenant; "nothing
quarantined" was made a skip rather than an acknowledgement, so a store
degraded by Corrupt alone could not go ready with no marker; and the gauges
were pointed at that same record so they could not disagree with readiness.
Together they made a stale record unclearable.

The snapshot re-reads the directory for any tenant it names that is not open —
one ReadDir, the cost the startup scan already pays — and drops the record when
the evidence is gone. `AcknowledgeDegradedDir` returning "nothing to
acknowledge" now deletes the key rather than skipping it.

No test could have caught this from any one of the three changes. It needed the
question "what does the remediation this document promises actually do", asked
against all three at once.

**And the claim that readiness and /metrics "now read `s.degraded` through one
function" was false when I wrote it.** There were two implementations of the
snapshot — one inline in `readiness`, one in `storageHealthTotals` — differing
in their population. The difference was inert, because an open tenant absent
from the record is `Ready()` and readiness would ignore it either way. That is
the same "inert difference" the entry above this one calls the next drift
waiting to happen, written one entry later by the same hand.

One helper now, and both derive from it.

**Two smaller ones.** `simdlogs_tenants` counts open tenants while the degraded
gauges count open plus evicted plus scanned, so a dashboard dividing one by the
other can exceed 1 — recorded in the LLD rather than unified, because the two
denominators are both wanted. And `HealthOfDir` synthesised `Corrupt = 1` for a
quarantine directory it could not read, which `/metrics` summed into a gauge
documented as "committed groups that could not be read at open". A permissions
problem on one directory is not one corrupt group; it is an `Unreadable` marker
now, and `Degraded()` reads it.

## A flaky gate that measured the wrong thing, and the button that 429s under load

**The concurrency test was red one run in three, for a reason unrelated to what
it tested.** `TestConcurrentAcknowledgementDoesNotFail` fired 50 POSTs and
required all 200. `/admin/acknowledge-degraded` went through `adminSpec()`,
which is charged the QUERY semaphore, and the default budget is 32:

```
MaxConcurrentQuery = 32
200 requests -> map[200:184 429:16]
body: "too many concurrent requests; retry after a moment"
```

So it did not pin the fix it was written for: on a machine where 50 requests
never collided it passed whether or not the serialization existed, and where
they did it failed for the budget. It asserts no 500 now — the failure mode the
fix is about — and separately that nothing is throttled.

**And the endpoint really was charged the budget.** 32 in-flight queries is
enough to make the button that puts a replica back in rotation answer 429 —
the same class of failure as the temp-name race, from a different cause, on the
same endpoint. `/metrics` already carries `nosem` with the argument written
out: "a scraper that gets 429 under load takes away the telemetry that explains
the load". One word changes: an operator who gets 429 under load loses the
button that ENDS it, and the replica it would have restored is the one under
that load.

## Three more from the same review

**A deleted tenant directory stranded the probe.** The re-read added for the
emptied-quarantine case treats "not a store" as "keep the recorded answer", and
a deleted tenant has no MANIFEST:

```
after rm -rf tenant-7-0:
/-/ready = 503
7:0: degraded (unacknowledged, from the store directory): 1 quarantined
metrics: degraded=1 unacked=1 quarantined=1
```

`simdlogs_storage_quarantined_groups` reporting 1 for a directory that does not
exist. Deleting a tenant is a more ordinary operator action than emptying one
quarantine directory. "Absent" and "present but not a store" are distinguished
now.

**The probe did a ReadDir plus a ReadFile per degraded tenant, per request, on
an unauthenticated path with no concurrency budget.** Measured at 200 probes:
5.34 ms for one degraded tenant against 67.3 ms for eighty, 12.6x, on tmpfs —
a cold dentry cache is worse. The quarantine directory's mtime changes exactly
when the answer changes, so one `Stat` now decides whether the read is needed.

A time-based TTL was the other option and is worse here: the answer has to
change the moment the operator acts, and any TTL long enough to save work is
long enough to leave the replica out of rotation after the fix.

**The stale-record guard was not airtight.** It dropped a record when the
tenant was "not open", and between releasing the lock and reacquiring it a
tenant can open degraded — repopulating the record with fresh health — and then
be evicted, at which point a correct record is deleted on the strength of a
read that predates it. Not reproduced in 16 tenants racing three probe loops,
and one line to close: `storage.Health` is comparable, so the delete requires
the record to be the same value the re-read was taken against.

## An mtime cache that was unsound twice, and an error read as an absence

Three more, all in the two fixes from the round above.

**`os.Stat` failing is not "the tenant was deleted".** The check added to
distinguish a deleted tenant from a directory that is not a store was
`err == nil && fi.IsDir()`, and EACCES is an error. `chmod 000` on the DATA
directory:

```
with the data directory unreadable: /-/ready = 200 "OK"
metrics: degraded=0 unacked=0 quarantined=0
after permissions are restored:     /-/ready = 200 "OK"
```

The record was DELETED, so restoring the permissions did not bring the signal
back — only a restart rebuilds it. This is the third time in this task that an
error has been read as a clean answer (`countQuarantined` returning 0, then
`HealthOfDir` synthesising a corrupt count) and the first where it destroyed
state rather than misreporting it. Misreporting is recoverable.

Only `os.IsNotExist` may drop a record now. Worth noting for anyone writing the
test: `os.Stat` on a mode-000 DIRECTORY still succeeds — stat needs `+x` on the
parent, not read on the thing itself — so a test that chmods the tenant
directory exercises none of this, and passes against the broken version.

**The mtime cache was unsound twice.** It skipped the directory read when the
quarantine directory's modification time was unchanged.

Part of the cached answer is the *contents* of `quarantine/ACKNOWLEDGED`, an
exported, documented, operator-visible file holding a plain integer. Rewriting
it in place changes the answer and not the directory:

```
mtime before 22:33:50.492873315, after the in-place rewrite 22:33:50.492873315
/-/ready = 200, though the marker no longer accepts the quarantined count
```

And an equal mtime is not proof of an unchanged directory. One second is the
natural timestamp granularity on ext3, ext4 with 128-byte inodes, HFS+ and many
NFS servers; two on exFAT. Forcing the collision those produce on their own:

```
quarantine emptied, mtime unchanged: /-/ready = 503, quarantined=1
```

Permanently, because no probe ever re-reads — the same stranding the cache was
layered on top of a fix for, reintroduced conditionally on the filesystem.

Replaced by a time window on the whole snapshot: 250ms, no per-file staleness
and no dependence on any filesystem property. It is a `Server` field rather
than a constant, so the tests that assert the remediation takes effect set it
to zero and measure the semantics, and one test measures the throttle. A test
that sleeps out a window measures the clock.

The general point, which is the fourth time this task has produced it: **a
cache is a claim that two things are equivalent.** Here the claim was "the
directory's mtime determines the answer", and the answer depended on a file's
bytes and on a timestamp with coarser resolution than the events it was
distinguishing. Neither is visible from the call site.

## The throttle read the disk and never wrote it down

One defect, one line, and it broke an invariant this task already has a test
for.

The re-read that replaced the mtime cache fed its result to the response and
never wrote it back into the server's record, so the record stayed at whatever
startup found. The answer then depended on which side of the throttle window a
probe landed on. Same on-disk state, back to back:

```
re-read:   /-/ready = 200, unacknowledged = 0
throttled: /-/ready = 503, unacknowledged = 1
```

And the two endpoints disagreed with each other, which is the invariant the
one-snapshot change exists to hold:

```
/-/ready = 200 (ready), metrics unacknowledged = 1
```

It needs no setter to reach: with the 250ms default, any two snapshot calls
closer than that where the disk differs from the record — a Prometheus scrape
shortly after a kubelet probe, two load balancers probing. The trigger is a
marker written by something other than this process, which is the ordinary case
where the record and the disk diverge, because the acknowledge endpoint is the
only thing that updates the record.

Written back now, in the same second-lock pass that drops the stale keys and
guarded the same way: the record must be the one the read was taken against.

**And the knob was exported and unreachable.** `SetDirRereadInterval` had no
flag, no config field, and no caller but the tests — the shape the round-one
entry above already names ("the feature was built and tested from the inside,
where every surface is reachable by calling the method"), reproduced four
rounds later in the fix for something else. It runs through
`config.Config.DirRereadInterval` and `-readiness-reread-interval` now.

**What the throttle actually bought, stated plainly.** At any probe interval
above 250ms it saves nothing in steady state — every probe re-reads — so the
12.6x cost measured for the per-tenant read is unchanged for an ordinary
deployment. What it closes is the sharp half: the unauthenticated
amplification. 2000 unauthenticated `/-/ready` probes cost 19.4µs each instead
of scaling with the degraded-tenant count on every request. Worth not recording
the steady-state cost as solved.

## A failed write kept the file it could not commit

Task 3.6. `AppendGroup` writes `group-N.bin` durably, mmaps it, re-reads it,
then commits the id to the manifest. Every step after the first can fail with
the file already on disk, and none of them removed it.

The manifest never names it, so `OpenStore` ignores it and no retention pass
ever considers it. Invisible and undeletable is the whole shape of the bug: the
only way to reclaim that disk is a human deleting files by hand.

It is at its worst under exactly the failure it follows most often. On a full
disk the group's own bytes can fit while the manifest record does not, so a
retry loop leaves one full-size file per attempt — consuming the disk faster
than the operator frees it.

```
--- FAIL: TestFailedAppendLeavesNoOrphanFile (0.00s)
    orphan_test.go:68: a failed append left 1 group files behind, was 1:
        [group-0.bin group-1.bin]
```

The fix is not an unconditional remove. `manifest.commit` truncates its record
away on every failure **before** the sync, so the id is invisible and the file
is genuinely an orphan — but the fault point **after** the sync returns an
error with the record already durable and the id visible. Removing there would
leave a committed group with no bytes, which is worse than the leak. So the
removal is gated on `isVisible`, and the directory is fsynced so a crash cannot
resurrect the orphan.

## One 503 for every storage failure answered neither question a shipper has

Also 3.6. Every write failure produced `503` plus the underlying error's text.
A log shipper reading that cannot answer either of the two things it needs to
decide:

- **Is retrying worth anything?** A full disk clears when someone frees space.
  A group that fails its own checksum the instant after being written is a pure
  function of the payload and fails identically forever. Both were 503, so a
  shipper retried the second one until someone noticed.
- **Does a retry duplicate?** There is no idempotency key on the ingest path.
  If part of a payload landed and part did not, resending stores the landed
  part twice — and nothing said so.

The second one could not even be computed before this change. `flushBatch`
recorded the *first* error and nothing else, so "all three groups failed" and
"one of three failed" were the same state. They are the opposite answer: the
first means a retry is clean, the second means it duplicates. Counting per job
(`jobs`/`failed`) is what makes the distinction exist.

The classification's default is `RetrySoon`, not `RetryNever`, and that
direction is deliberate. An unrecognised failure is one this table has not met
yet; telling a client to give up on something transient loses data, while a
needless retry only duplicates it.

## "It cannot be tested from here" was again "the seam is one export away"

Third time in this repository (see the earlier two). The plan asked for fault
tests covering disk full, short write, sync failure, mmap-after-rename and
manifest failure at the **writer** level, and `setFaultHook` was package-private
to `internal/storage`.

The reflex is to reach for a build tag and a separate lane. That was rejected:
a lane outside `make verify` goes stale, and this repository has already paid
for one vacuously-green tagged lane. `SetFaultHookForTest` is exported and
guarded by `flag.Lookup("test.v") == nil` instead — true in every test binary
and no production one, one map lookup at arm time, and the failure suite runs
in the default lane.

The sweep enumerates `FaultPointNames()` rather than listing points. A
hand-written list is a list the next write step quietly falls out of, and a
write step whose failure is not surfaced is a write step that loses data behind
a 200.

## The batch a caller marked aged out, and FlushMark answered nil

Review round on 3.6. `FlushMark` answered from a 64-entry ring of batches, and
every `Flush`/`FlushMark` installed a new batch **whether or not the old one
carried anything**. So 64 flushes from any other caller on the tenant — other
requests, a syslog connection flushing per line, the `FlushEvery` timer, `Close`
at shutdown — evicted the batch a marked caller's rows were in. That batch was
then never waited on, its error never seen, and the caller was told success.

```
--- FAIL: TestEvictedBatchIsNotReportedAsSuccess
    FlushMark reported success; the store holds 0 groups
```

At `MaxConcurrentWrite` 32, 64 slots is about two completed request cycles.

The ring predates 3.6, but 3.6 made it the authority for two new affirmative
claims, and the second one fails in the worse direction:

```
--- FAIL: TestEvictedBatchStillReportsPartial
    a failed group reported success after its batch aged out
```

With the batch holding a *landed* group evicted, `failed == total` over what
remained, so `duplicateOnRetry` came back false and the client was told a retry
was clean. It resends and stores those rows twice — the exact outcome the field
exists to prevent. Before the change no such claim was made; the change added a
statement the window could not support.

Three things close it. An outgoing batch with **no jobs** is dropped rather than
aging a real one out, so an idle flush costs nothing. A batch that did carry
jobs leaves a four-word outcome behind — seq, jobs, failed, error — which
`FlushMark` folds in. And past even that log, `FlushMark` returns
`ErrDurabilityUnknown` with `Partial` set rather than nil.

The shape worth keeping: **a bound chosen against the expected interleaving,
where exceeding it is silent and reads as success.** "64 is far past any real
interleaving" was in the comment. It was not, and nothing said so when it was
exceeded.

## The orphan cleanup drew its own window one step too late

Same round. The fix for the orphaned-group leak said its window was "every step
of AppendGroup **after** `writeFileAtomic` returns". Two steps *inside*
`writeFileAtomic` are already past the point of no return: opening the parent
directory and fsyncing it both run after `os.Rename` has landed.

```
--- FAIL: TestReviewDirSyncFailureLeavesOrphan/dir-open
    a failed append left [group-0.bin] behind; the manifest names none of them
--- FAIL: TestReviewDirSyncFailureLeavesOrphan/dir-sync
    a failed append left [group-0.bin] behind; the manifest names none of them
```

Both errno shapes classify retryable, so the retry loop left one full-size file
per attempt — precisely the pathology the cleanup was written to stop.

The sweep over every fault point ran `dir-open` and `dir-sync` and passed: it
asserted the caller got an error, never that the directory was clean. A test
that covers a code path is not a test that covers the property.

## `isVisible` answered from memory a question about durability

Same round, found by inspection with no seam to test it. `discardUncommitted`
deletes an uncommitted group file, gated on `m.visible` not holding the id. That
gate is an in-memory fact standing in for a durability one.

`manifest.commit` rolls a failed record back with `truncateTo`, which swallowed
every error. If the record was fully written and the **sync** failed — a dying
disk returning EIO — and the rollback's own `Truncate` or `Sync` then failed
too, the record stays in the page cache and the kernel writes it back with no
crash involved. Memory says invisible; disk ends up saying committed; the file
is deleted; the next `OpenStore` fails with *group N is committed but its file
is missing* and the store never starts.

Before the orphan fix the file survived and the group was adopted intact. So the
fix converted a recoverable leak into an unopenable store, in a narrow case.
`truncateTo` now reports, `commit` wraps a failed rollback in
`ErrRollbackFailed`, and `discardUncommitted` keeps the file when it sees it. A
leaked file is recoverable; a store that refuses to start is not.

## A never-retry class that told shippers to drop data a disk had corrupted

Same round. 3.6 classified `storage.ErrCorruptGroup` — a group that fails its
own bounds and checksum checks the instant after being written — as never-retry:
HTTP 500, `retryable: false`, no `Retry-After`. The stated reason was that the
bytes are a pure function of the payload, so every attempt reproduces it.

`ReadGroup` validates a CRC32C over a blob handed to the filesystem seconds
earlier. A mismatch there is at least as likely to be the storage returning
different bytes than the ones written — not deterministic, and fixed by a retry
or by replacing the disk. The answer told a shipper to give up on data a media
error had corrupted.

It also inverted the classification's own stated bias, three functions higher in
the same file: an unrecognised failure defaults to retryable *because* telling a
client to give up on something transient loses data while a needless retry only
duplicates it. The class is gone rather than misapplied, and the table in
`docs/lld/ingest.md` said so as a fact, which made it a documentation defect as
well as a classification one.

## And a test that passed with the fix it appeared to guard fully reverted

*(Name correction, 2026-08-15: the test named here never reached a commit under
this name -- it was rewritten before the entry's own change landed, and is
`TestPostRenameFailureLeavesNothingReadable` in
`internal/ingest/writer_failure_test.go`. The entry below stands as written;
only the name was wrong.)*

`TestShortWriteLeavesNothingReadable` injected at `partial-write`, which fires
**before** the rename — so `writeFileAtomic`'s own pre-existing deferred
temp-file removal is what made it green. With all three `discardUncommitted`
calls commented out it still passed. It tested something real and nothing the
change added. It now injects at `dir-sync`, which is the post-rename case above,
and goes red without the fix.

Two others from the same file were checked the same way and are sound:
`TestFailedAppendLeavesNoOrphanFile` and
`TestPartialBatchFailureIsReportedAsDuplicating` both go red when their fix is
reverted, and the second does produce two real jobs in one mark window.

## The outcome log reproduced the defect it was added to fix

Second review round on 3.6. The first fix for "FlushMark returns nil once a
caller's batch ages out of the ring" was to leave a small outcome record behind
when a batch is retired. It froze the counters at the wrong moment.

`FlushMark` waits only on batches at or after its own mark, so a later caller
never blocks on an older one — and enough later flushes retire a batch whose
job is still in flight. The snapshot then said *one job, none failed* for a job
that went on to fail with ENOSPC:

```
b0 still in hist: false; outcomes: 10; b0's frozen outcome: jobs=1 failed=0 err=<nil>
b0 after its job finished: jobs=1 failed=1
FlushMark(mark) = nil for a row whose only group failed with ENOSPC
```

Same 200-for-lost-rows failure, moved from the ring into the log. A batch is
now retired only at zero outstanding jobs, and the ring is allowed to run over
its nominal length until then.

Two things about the round are worth more than the defect:

**Both tests written for the first fix were vacuous, and vacuous in the
direction that hid this.** They looped `w.Flush()` with an empty buffer — and
the same fix's empty-batch drop means an empty flush retires nothing, so
`len(hist)=2, len(outcomes)=0` after eighty of them. Nothing was evicted,
nothing was retired, and **nothing anywhere exercised the retired-outcome
fold-in**, which is precisely where the new defect lived. A test that cannot
reach the code it is named for is worse than no test: it is a claim of
coverage.

**A plain `Flush` was never exposed.** It waits on every live batch, so it
blocks on the stalled one. Only `FlushMark` — the function added to make
per-caller durability answerable — could skip it. The mechanism built to
answer the question precisely was the only way to get the wrong answer.

## The backup's time filter dropped a group at the top of the range

Task 5.1, found in the same round. `BackupTarWith` took
`Snapshot(math.MinInt64, math.MaxInt64)`, reading "the whole range". The
overlap test is `TimeMin < to && TimeMax >= from` — half-open at the top — so a
group whose `TimeMin` is `math.MaxInt64` fails it.

```
TestReviewBackupDropsMaxTimestampGroup: store holds 2 groups, the backup manifest names 1
TestReviewBackupDropsTimelessGroup:     store holds 1 groups, the backup manifest names 0
```

`VerifyBackup` passed, because the manifest is built from the same filtered
snapshot. A self-describing archive that describes itself as smaller than it
should be is worse than a bare tar: the manifest is what an operator would
trust. `parseTime` on the ingest path is `strconv.ParseInt(s, 10, 64)` with no
upper clamp, so the timestamp is a number a client sends.

The replacement is `SnapshotAll`, with a `SnapshotAllWithSeq` variant that also
reads the manifest sequence under the SAME lock acquisition — the second
acquisition it replaced could see an `AppendGroup` land in between, making the
archive declare a watermark covering a group it does not contain.

## One disk failure, two different answers, decided by request size

Also that round. A body at or above `ingest.MinParallelBytes` is sharded across
several writers, and that path answered a flat 500 with no `Retry-After` and
none of the retry metadata every other route reports:

```
body 1049666 bytes (>= MinParallelBytes 1048576)
status 500, Retry-After ""
{"durable":0,"error":"ingest: 10 of 10 shard writers failed to persist: ..."}
```

Underneath it, `errors.As` on a `ParallelWriteError` unwrapped to **one shard's**
`*WriteError`. A request where shard 1 landed and shard 2 failed reported
`1 of 1 failed, duplicateOnRetry: false` — the opposite of the truth, since
shard 1's rows are durable and resending the body stores them twice.

Fixed by an `As` method that aggregates the shards, and by `failIngest` reading
the same metadata as every other route. The counts on that path are shard
writers rather than groups, which `WriteError.Unit` now says rather than
leaving the reader to assume.

## Four more from the same round, each small and each the same shape

- **`joinRollback` used `%v` where it needed `%w`.** The commit error was
  flattened to text, so `errors.Is` could no longer reach its errno: a
  rollback-failed ENOSPC classified as unrecognised and was answered "retry in
  a second" instead of "someone has to free space". The wrapping was added in
  the *previous* round specifically so a caller could tell these apart.
- **`DisallowUnknownFields` ran before the format check**, so a format-2 backup
  manifest carrying a new field was reported as `json: unknown field "codec"`
  rather than as an unsupported format — while the function's own comment said
  "the format check comes first". The comment described the intent; the code
  did the other thing.
- **`ErrCorruptGroup`'s doc still carried the never-retry reasoning** that the
  same round had removed from `classify`, and `writeFlushErr`'s doc still
  promised "a 500 instead of a 503" that `HTTPStatus` no longer returns. A
  deleted behaviour leaves its justification behind in every comment that cited
  it.
- **`TestBackupIsCompleteUnderConcurrentRetention` was a measured no-op.** (Name
  correction, 2026-08-15: never committed under this name; the replacement
  described below is `TestBackupIsCompleteUnderConcurrentStoreChanges` in
  `internal/storage/backup_contract_test.go`.) Over
  40 runs it captured 12 groups every time: retention always won the start, the
  snapshot was always taken after the drop, and the streaming-under-retention
  path was never entered. A blocking writer that stalls the archive mid-stream
  replaces it, with retention, an append and a recompaction each run against it
  — the three the plan asked for and one of which existed. The no-op was
  *deleted*; an earlier version of this bullet said it had been replaced while
  it was still sitting in the same file beside its replacement.

## Round three: the same misreport, twice more, in the paths the first two rounds did not reach

Third review round on 3.6 and 5.1. The two data-loss defects round two found are
gone — an independent pass confirmed the counter pairing sound under `-race`
across 20 runs of 320 interleaved operations, and the backup's time filter with
it. What it found instead were two more instances of the same class, in code the
first two rounds had not looked at.

**A shard other than the first could land rows and go unreported.**
`ParallelWriteError` keeps only `firstErr`, so the aggregation could inspect
exactly one shard. With every shard failed and the *partial* one not the one
that won the mutex, `duplicateOnRetry` came back false:

```
--- FAIL: TestZZAggregationMissesNonFirstPartialShard
    aggregated: partial=false duplicateOnRetry=false (4 of 4 shard writers)
    a shard with rows on disk was reported as duplicateOnRetry=false
```

Partial-ness is now collected in the shard loop, from every shard, rather than
read back off one error. The comment claiming it already covered "any failing
shard that was itself partial" was false for more than one shard.

**A closed writer claimed a retry was clean while its rows were durable.**
`Close` flushes the shared buffer *before* it sets `closed`, so an in-flight
handler — the case `ErrWriterClosed`'s own doc names, `http.Server.Shutdown`
letting one request finish — got `Partial: false`:

```
--- FAIL: TestZZClosedWriterClaimsRetryIsClean
    store holds 1 group(s)/1 row(s); FlushMark says partial=false duplicateOnRetry=false
```

Before this work that path returned a bare error and made no claim. The change
turned "no claim" into a false one, which is the recurring shape: a new
affirmative statement is a new thing that can be wrong, and every path that
reaches it has to be checked, not the one it was written for.

## The gate that removed the unbounded freeze made the ring unbounded

Same round. Retiring only at zero outstanding jobs fixed the frozen counters and
made `hist` grow without limit: the pool bounds outstanding *jobs*, not
*batches*, and every later job-carrying flush appends one more while a stalled
one pins the front.

```
after 5000 client requests with one job pinned: len(hist)=5002 (batchHistory=64)
```

Per tenant, proportional to request rate times stall duration, and `FlushMark`
walks the whole slice under the writer's lock on every request. Per-request
latency did not move at 5002 entries — the fsync dominates — so this was memory
and lock-hold growth rather than a measured slowdown.

The comment was the worse half: *"bounded by the flush pool and its channel, not
by anything a client sends."* It is bounded by neither. There is a hard ceiling
now, and past it a stalled batch is dropped as **unanswerable** rather than
retired with counters that are not final — `ErrDurabilityUnknown` for any mark
at or below it. Refusing to answer is the only safe thing to do with a number
that can still change; recording it is the frozen-zero defect again.

## A test that stopped being vacuous in one way and stayed vacuous in another

Round two showed `TestEvictedBatchIsNotReportedAsSuccess` evicted nothing,
because its eighty flushes were empty and an empty batch is dropped rather than
retired. Fixed by giving them rows. Round three showed it *still* proved nothing:
the fault hook failed **every** `temp-create`, so all eighty later batches
carried the same ENOSPC and `FlushMark` found an error in a live ring entry
without ever consulting the outcome log. Deleting the fold-in entirely left it
green.

Two more things had to be true for it to bite, and both are behaviours worth
writing down. The marked row has to become its own flush job before anyone
else's traffic starts, or the shared buffer carries it into the first foreign
flush. And the foreign traffic has to use `FlushMark` with its own mark, not
`Flush` — a plain `Flush` waits on every live batch and would see the marked
caller's error, which is correct and is not what the test is about.

It now asserts, before the call it is testing, that the marked batch is out of
the ring. That is the difference between a test that exercises a mechanism and a
test that is merely in the same file as one.

## Four smaller ones, same round

- **The JSON body labelled shard counts as groups.** `groupsFailed`/`groupsTotal`
  went out with no unit, so a client parsing the body could not tell shard
  writers from groups; the distinction survived only in the message string,
  while `docs/lld/ingest.md` claimed `WriteError.Unit` carried it. It is a
  `unit` field now.
- **`/admin/backup`'s 429 and its pre-flush shipped with no test and no
  mention in any document.** A change that turns a request which used to
  succeed into a rejection is exactly the kind that needs one.
- **`BackupManifest.TotalBytes` had no caller and `Store.SnapshotAll` was a
  verbatim copy of `SnapshotAllWithSeq`** minus a return value, carrying a doc
  comment arguing a rationale for a use it did not have. The first is gone; the
  second delegates.
- **`docs/architecture.md` still said the archive was "a consistent snapshot
  because groups are immutable"** — the argument `docs/lld/storage.md` had
  already repudiated two sections earlier, since the old path copied paths out
  and skipped what had gone. Immutability is why a group's bytes are stable, not
  why the archive is complete.

## Round four: the watermark went backwards, and the fix for one branch skipped its sibling

Fourth review round. Both of round three's fixes were correct where applied and
both left a hole one step away.

**`oldestAnswerable` was assigned unconditionally.** Two paths move it — the
outcome log's overflow and the new ceiling drop — and only the second guarded
the assignment. A normal retire following an unanswerable drop lowered it again
and un-hid a batch that is in neither the ring nor the log:

```
--- FAIL: TestProbeOldestAnswerableNeverMovesBackwards
    oldestAnswerable 5001 -> 2
--- FAIL: TestProbeSecondStallIsUnhiddenEndToEnd
    markB=3501 oldestAnswerable=3114 inRing=false inLog=false
    FlushMark(markB=3501) -> <nil>
```

Nil for lost rows, reintroduced by the fix that was written to stop it. It
takes two stalled workers to reach through the real path — one pinning the
front, a second landing inside the newest slice of the drop window. It is a
watermark, and a watermark that can go backwards is not one.

**The serial fallback discarded `Partial`.** `IngestJSONLinesParallelCfg` takes
a one-writer branch whenever the shard count is below 2 — `runtime.NumCPU()/3`,
so **every host with fewer than six cores** — and that branch built its
`ParallelWriteError` without the field the loop eleven lines below it had just
been fixed to collect. `As` then replaced an accurate inner `*WriteError`
("1 of 3 groups, partial") with a synthesized one ("1 of 1 shard writers, not
partial"), so the client was told a retry was clean with a group on disk.

The record said "partial-ness is now collected in the shard loop, from every
shard". True, and the sentence stopped one branch short of the function it
described.

## Five fixes with no test, in a round whose subject was untested fixes

The same review reverted each of the previous round's fixes in turn and ran the
suite. Five stayed green: the shard-partial collection, `Partial: true` on a
closed writer, the `unit` field, the `maxHistory` ceiling, and
`ParallelWriteError.As` — the last of which both LLDs describe at length.

That is the failure mode this file has been recording for four rounds, arriving
as a property of the *process* rather than of any one change: every round fixed
what the previous round's reviewer found, and shipped the fix the way the
previous round had — without the test that would catch its regression.

All five now have one, and each was verified by reverting its fix:

```
hist grew to 4354 with a stalled job; the ceiling is 4096
the store holds 1 groups and the caller was told a retry is clean
parallel units "groups", want shard writers
duplicateOnRetry=false, want true (4 of 4 failed, shardPartial=true)
oldestAnswerable fell to 1 from 5000 at flush 1087
the writer's error was partial and the wrapper did not collect it
Flush on a closed writer said a retry is clean; Close flushed those rows
```

The last one needed two attempts to bite. Its first version drove
`outcomeHistory+8` flushes, and the first `batchHistory` of those only fill the
ring without retiring anything — so the overflow branch it was written for
never ran, and it passed against the unguarded assignment. A test that does not
reach its own mechanism is the thing this entry is about, written into the test
for it.

## The archive's ordering was documented and never enforced

Same round. `readBackup` validates a group only inside `if man != nil`, and
nothing required the manifest to come first. A manifest-LAST archive therefore
validated nothing — every group read with no size, checksum or parse check —
and the completeness loop afterwards passed, because those groups *had* been
seen:

```
--- FAIL: TestProbeManifestOrderIsNotEnforced
    VerifyBackup on a manifest-last archive with a wholly corrupt group: err=<nil>
    RestoreTar -> <nil>; wrote 2 files
```

Two properties had to be separated to fix it, and conflating them was the first
attempt: an archive with **no** manifest is a pre-format-1 backup and must
still restore, returning `ErrBackupUnverified`; an archive whose manifest
arrives **after** its groups is refused. The distinction is only decidable at
the end of the stream, so it is made there.

The unverified branch also read entries with an unbounded `io.ReadAll` on an
attacker-declared size — the allocation `maxBackupManifestBytes` exists to
prevent, one entry type over, which is the "bound that exists in two of three
places" shape this file has now recorded three times.

And `/admin/backup`'s pre-flush was unbounded. It waits on every live batch,
including one pinned by a stalled fsync — the scenario `maxHistory` exists for
— while holding `backupBusy`, so a stalled writer disabled that tenant's
backups entirely. It is bounded now, and a timeout takes the archive anyway,
which is the "stops at the last durable group" case the comment already
accepted.

## Round five: the ceiling bounded the ring and not the stall

Fifth review round. Round 4's `maxHistory` fix dropped a stalled batch out of
the ring and left it in `w.live`, which kept both of the problems it was added
to solve.

Its counters — final by the time anyone read them — were folded into the next
unrelated plain `Flush`, so a caller whose own rows all landed was handed
someone else's *"1 of 2 groups failed, partial"*. The owner got
`ErrDurabilityUnknown` on the stated grounds that those counters "can still
change"; the information existed and went to the caller not entitled to it.

And because `Flush` waits on all of `live`, every `Flush`, every
`Writer.Close` and every tenant eviction still blocked on the stalled job. The
ceiling bounded the slice; the thing it was written to survive was untouched.
Dropping the batch from `live` alongside the ring closes both — **past the
ceiling, and only there.**

That qualifier is the correction to the first version of this paragraph, which
said "closes both" flat. The drop is bounded by TRAFFIC, not by time: it needs
`maxHistory` job-carrying flushes on that tenant to fire at all. Below the
ceiling, and for a tenant that receives nothing further, `Flush` and `Close`
block exactly as before, and a `Flush` already parked is never released. Two of
those are recorded as open below.

## The archive's refusals arrived after the bytes did

Same round. Two orderings were documented and one of them was newly enforced;
the other was not, and the enforced one fired too late.

**The terminator.** `backup_manifest.go` says "a terminator goes LAST" and
`docs/lld/storage.md` says "always last". Only its PRESENCE was checked, so an
archive carrying `BACKUP-COMPLETE` as its FIRST entry verified clean and
restored. The manifest-ordering entry two above this one is the same sentence
about a different entry type, written in the round that missed this one.

**The manifest-last refusal.** It fires when the manifest finally arrives — by
which point every group before it is already on disk, and none of them had been
checked, because validation runs inside the manifest branch:

```
RestoreTar -> storage: the archive carries groups before its BACKUP-MANIFEST
  wrote group-0.bin (379 bytes, wholly 0xFF: true)
  wrote group-1.bin (381 bytes, wholly 0xFF: true)
  wrote group-2.bin (382 bytes, wholly 0xFF: true)
```

That is materially different from the truncation case already recorded, where
every written group had passed size, CRC and `ReadGroup`. Every group is parsed
on the way in now whether or not a manifest has been seen, so what lands is at
least a readable group. It is still not atomic — a refused restore leaves a
partial destination, and making it all-or-nothing is Task 5.2.

## A timeout that bounded the handler and not the goroutine

Round 4 bounded `/admin/backup`'s pre-flush with a ten-second timeout, in a
goroutine. `backupBusy` is released when the HANDLER returns, so polling the
endpoint against a stalled writer spawned one permanently parked goroutine per
request:

```
backup 0: status 200 after 10.004s
backup 1: status 200 after 20.013s
goroutines 7 -> 9; parked in the backup pre-flush: 2
```

Counted by nothing: `Server.Close` waits on the background loops and on
`inFlight`, and this is neither. At most one parked pre-flush per tenant now —
a second would wait on the same batches, so skipping it costs nothing.

## Thirty fixes with no test, and a record that said otherwise

The same review reverted every fix in the uncommitted diff one at a time.
**Thirty stayed green**, including both of round 4's own.

Worse than the number: the round-4 entry above says "All five now have one, and
each was verified by reverting its fix." Two of the five were not. The
shard-partial collection got a test that builds a `ParallelWriteError` by hand
and asserts the aggregation's `|| e.Partial` arm — deleting the loop that SETS
`Partial` left the suite green. And `Partial: true` on a closed writer was
covered at the `FlushMark` site while the `Flush` site stayed green.

Both now have a test that drives the real function, and the sharded loop has
one too. Each needed a body over `FlushRows` per shard, because a shard cannot
be partial with fewer: one group per shard means every shard is wholly failed
or wholly durable. Three earlier attempts passed for the wrong reason — a
four-shard version whose fault never landed and skipped; a version where one
shard survived, so `Failed < Shards` answered instead of the collection; and a
watermark test the outcome log's own advance covered. Each was found by
deleting the line it was named for and watching it stay green.

Three comments in the same sweep claimed coverage that does not exist, and each
is now corrected on the source side:

- `store.go`'s "the check is what keeps it that way under the fault matrix" —
  the matrix drives `manifest.commit` directly and never reaches
  `discardUncommitted`.
- `writer_failure_test.go`'s "And the file is still there" — followed by code
  that checks the error type and nothing about the file.
- `BackupManifest.Tenant`'s "so a restore can refuse an archive taken from a
  different one" — nothing reads the field; `RestoreTar` takes no tenant. A
  property described, not implemented, until Task 5.2.

And two branches of `writeFlushErr` are unreachable — every call site passes a
`*WriteError` and an `errJSON` spec — while `docs/lld/ingest.md` claimed the
second as a cross-protocol guarantee clients observe. Both are now labelled as
written for a route that does not exist yet.

## Two stalls the ceiling cannot reach, recorded rather than fixed

Round 6 measured what the ceiling actually buys, and it is less than the entry
above first claimed.

**The drop is bounded by traffic, not by time.** It fires only once the ring
exceeds `maxHistory`, which needs 4096 job-carrying flushes on that tenant. A
tenant with a stalled writer and no further requests never reaches it, so
`Flush` and `Close` block indefinitely there; and a `Flush` already parked on
the batch's WaitGroup is never released by the drop.

```
Flush is still blocked after 2s with one stalled job (the ceiling is not reached)
Close is still blocked after 2s with one stalled job and no further traffic
after 4352 FlushMarks: hist=64 live=0
Flush returned promptly past the ceiling: <nil>
```

**Tenant eviction wedges the server-wide lock.** `evictIdleLocked` calls
`victim.w.Close()` with `s.mu` held, so a stalled writer blocks every request
for every tenant:

```
tenant B is still blocked after 3s: the eviction is inside victim.w.Close(),
holding s.mu
a later tenant lookup for an ALREADY-OPEN tenant is blocked too
```

And because no request can pass `s.mu`, the stalled writer receives no further
flushes, the ring never reaches the ceiling, and the drop never runs. Permanent
and server-wide.

Not fixed here, and the reason is scope rather than difficulty: the Close is
inside the lock because the store must be shut before another `OpenStore` on
the same directory, so moving it out needs a per-key closing state that a
concurrent open waits on. That is a change to tenant lifecycle, which is Task
1.5's area and not this task's. It is pre-existing — this work did not
introduce it — but the ceiling's own entry claimed eviction no longer blocks,
which was true only past a threshold eviction cannot reach.

## Round six: the correction was itself wrong about Close

Sixth review round. Round 5's entry above says dropping the abandoned batch
from `live` closes the `Flush`, `Close` and eviction stalls "past the ceiling,
and only there". Measured, for `Close` the right qualifier is **never**:

```
past the ceiling: hist=64 live=0
Flush returned promptly past the ceiling: <nil>
Close is STILL BLOCKED 3s past the ceiling, with the ring drained
Close returned once the stalled worker was released: <nil>
```

`Writer.Close` runs `Flush` and then joins the flush workers. A worker parked
inside `AppendGroup` has not returned, so `Close` blocks on a stalled writer
with `live` empty and the ring drained. The drop unblocks `Flush` and nothing
else; eviction inherits `Close`'s behaviour one level up.

The stated CAUSE was wrong, not just the scope — "because Flush waits on all of
`live`" is true of `Flush` and irrelevant to a join — and it was wrong in three
places at once: the source comment, the LLD, and this record. Writing the same
sentence into three files is how one measurement error becomes three.

## Two more tests that passed for a reason they were not written for

Same round.

**`TestManifestMustComeFirst` did not test manifest ordering.** Its archive
carried wholly-corrupt groups, and the unverified branch's own `ReadGroup`
parse refused the first one before the manifest was ever read — so deleting the
ordering rule left it green. It was a second test of the parse wearing the
ordering rule's name. The groups are valid now, the terminator is last so the
terminator rule cannot answer either, and the assertion names the message.

**`TestTerminatorMustComeLast` tested the pair, not the line.** Placing the
terminator first puts both the manifest and the groups after it, and there are
two checks; green with either deleted, red only with both. It now runs two
placements, and the group check is individually covered. The manifest check is
not, and cannot be by this route: any archive with a manifest after the
terminator also has groups after it, unless the groups precede the manifest —
which trips the manifest-first rule instead. Redundant by construction, and
recorded as such rather than chased.

**And `writeFlushErr`'s `duplicateOnRetry` could be hardcoded `false`** on
every non-parallel ingest route — jsonline, logfmt, ES bulk, OTLP — with the
suite still green. The only test that read the field asserted it was FALSE,
which is the safe case. A test asserting TRUE existed nowhere, for the field
this whole task exists to make trustworthy.

Its first version posted to `/insert/jsonline`, which hands a body over
`MinParallelBytes` to the sharded path and answers through `failIngest` — the
other copy of the field, which was already covered. So it passed with
`writeFlushErr`'s copy hardcoded, which is exactly the hole it was written for.
It uses `/insert/logfmt` now, which has no parallel branch.

## Still open, and what it would take

- **`Writer.Close` and tenant eviction block on a stalled writer, always.**
  Close joins the workers; eviction calls Close with the server-wide `s.mu`
  held, so one stalled fsync blocks every request for every tenant. Fixing the
  second needs a per-key closing state a concurrent open waits on, because the
  Close is inside the lock so the store is shut before another `OpenStore` on
  the same directory. That is tenant lifecycle, Task 1.5's area.
- **The `ErrRollbackFailed` chain is tested only at its leaf.** `joinRollback`
  has a direct test; nothing drives `commit → truncateTo → discardUncommitted`,
  because there is no fault point on `Truncate`. Six single-line reverts across
  that chain each leave the suite green, and any one of them reintroduces the
  unopenable-store defect. It needs a fault seam inside `truncateTo`.
- **Neither archive size ceiling is tested** — the unverified branch's or the
  manifested one's. Both are DoS bounds rather than durability answers.
- **The backup pre-flush is entirely untested.** Removing it, unbounding its
  timeout, or removing the one-parked-goroutine guard each leaves the suite
  green. `TestBackupReleasesItsAdmission` looks like it covers the first and
  does not: the ingest request's own `FlushMark` already made the row durable.
- **The worker's `failed`-before-`outstanding` ordering is uncovered.**
  Decrementing `outstanding` before `failed.Add(1)` but after the error CAS
  lets a batch retire with `failed=0` and an error set, reporting "0 of N
  failed" with `Partial=false`. The `err` half of that ordering IS covered; the
  `failed` half needs a hook inside the worker.
- **`SnapshotAllWithSeq`'s single-acquisition property is uncovered** — an
  `AppendGroup` between two lock acquisitions would make the archive declare a
  watermark covering a group it does not hold, and nothing catches a regression
  to two acquisitions.
- **The manifest FORMAT refusal is uncovered.** A format-2 archive decoding as
  format 1 and restoring while ignoring whatever the new field required is the
  case `decodeBackupManifest`'s own comment calls "the only answer that cannot
  be wrong".
- **`TestCorruptGroupInBackupIsRejected` is answered by `ReadGroup`'s own v8
  CRC, not by the manifest checksum.** Removing the manifest's size check, its
  CRC check, or both leaves it green. That matters for one case: a **v7** group
  has no checksum of its own, so the manifest CRC is the only integrity check
  it gets in an archive, and nothing tests it.

## The seventh one-side-only claim, on the function the retraction was about

Round 8, and the last blocker in this task. `discardUncommitted`'s own doc
comment still opened with:

> The window is every step of AppendGroup after writeFileAtomic returns.

That is verbatim the sentence three other places in this repository record as
the reasoning error — the call site 43 lines above it, `docs/lld/ingest.md`, and
the entry "The orphan cleanup drew its own window one step too late". Corrected
on three sides, alive on the fourth: the one a reader of the function actually
reads, and the one whose wrongness produced the round-3 defect.

Seven rounds, seven instances, and the shape has not varied: a claim is
corrected where the reviewer pointed and left standing wherever else it was
copied. The lesson that finally sticks is not "check the other side" — it is
that a sentence worth writing twice is a sentence that will be wrong in one
place, so the second copy should be a pointer.

Alongside it, the round-5 finding reproduced in a file written after it:
`TestBackupAdmitsOneAtATime`'s comment said the 429 and the pre-flush "neither
had a test" and now both do. The 429 does. Deleting the entire pre-flush block
leaves both backup tests green — `TestBackupReleasesItsAdmission` asserts the
posted row is in the archive, and the ingest request's own `FlushMark` put it
there. Two sentences claiming coverage that a one-line deletion disproves, in a
file whose subject is claims of coverage.

## The restore that deleted the store it was restoring into

Task 5.2 shipped a staged restore whose whole claim was that the destination
ends up holding the archive's store or holding nothing. Review found the claim
false in three ways, two of them destructive, and found that not one of the
nine tests carrying "staged" or "atomic" in its name tested staging at all.

**The emptiness check and the removal are an archive-read apart.** The check
ran at the top of `Restore`; `os.RemoveAll(dst)` ran at the bottom, after every
group had been read. A server that opened a store at that path in between had
its `LOCK`, its manifest and every group deleted, the archive renamed over the
top, and the call returned nil. The measured aftermath is the part worth
keeping:

```
live AppendGroup after the swap SUCCEEDED
dst now holds: [group-0.bin group-1.bin]
re-open of the restored dir: err=<nil>
the reopened store reports 2 groups / 7 rows
```

The live writer's id counter is independent of the archive's, so its next group
**overwrote the archive's `group-1.bin`**. The result opened clean and answered
with 7 rows where the archive held 8 — a silent mixture of two stores, from the
tool whose entire reason to exist is that the moment it runs is the moment
there is nothing to compare against.

**Two concurrent restores were the same defect in a different dress.** Both
derive `<dst>.restoring`, so the second one's `os.RemoveAll(staging)` deleted
the first one's staged files mid-stream and both wrote into the same directory.
Measured with two archives from two tenants:

```
A err=<nil>  B err=open .../store.restoring/group-2.bin.tmp: no such file
dst holds 4 entries: [group-0.bin group-1.bin group-2.bin group-3.bin]
WRONG BYTES group-0.bin: 436 bytes, the manifest says 379
```

Two groups from each archive, one call reporting success, cross-tenant
contamination produced by the function `RequireTenant` exists to prevent.

Both are closed by one mechanism: take the store's own exclusive lock on the
destination before reading the archive and hold it through the rename, then
re-check emptiness **under the lock**. A server cannot open the directory while
it is held, a second restore cannot start, and the re-check is what makes the
removal safe — it proves the directory holds nothing but the lock this process
created.

**Every restore test passed against a build with no staging at all.** With
`staging := dst` and the rename pair deleted — i.e. exactly `RestoreTar` plus a
cleanup defer, the design the task exists to replace — all nine tests stayed
green. `TestRestoreIsAtomic` truncates an archive and finds the destination
absent, which the error-path `defer` produces just as well as staging does. The
difference between the two designs only shows on a **crash**, where no defer
runs, or **mid-stream**, where the destination can be looked at while the
archive is still being read. The replacement asserts mid-stream, through a
reader that runs a hook halfway:

```go
r := &pausingReader{r: bytes.NewReader(archive), at: int64(len(archive)/2),
    hook: func() { midDst = dirNames(dst); midStaging = dirNames(dst+".restoring") }}
```

and the lock's test opens a store at the destination from that same hook and
requires `ErrLocked`.

### The same mistake, made again, in the fix

The first replacement test for the lock was
`TestRestoreRefusesALiveStoreAndLeavesItIntact`: open a store at `dst`, restore
into it, assert the refusal and the store's survival. It passes with the lock
deleted **and** the under-lock re-check deleted, because the start-of-call
emptiness check refuses an already-occupied directory on its own. It was a test
of the check that was never the problem, written to guard the lock, and it took
a revert probe to see it — the same probe discipline that found the original
defect, applied to its fix, catching the fix's test one round later.

The lesson is not "write better tests". It is that a test for a
time-of-check-to-time-of-use defect **cannot** be written as a before-and-after
assertion, because the whole defect lives in the middle. It has to make its
assertion during the window or it is testing something else.

### The rest of the round

- **`DryRun` applied none of the three limits.** It called `VerifyBackup`,
  which is `readBackup` with no callback, and every limit lived in that
  callback. `-max-files 1 -max-bytes 1 -dry-run` accepted a four-group archive.
  The one mode an operator is told to point at an untrusted archive was the one
  mode that checked nothing.
- **The manifest is decoded before any limit can apply**, and it is what sizes
  everything after it: a 24 MiB manifest cost 339.6 MiB of live heap from an
  archive declaring `MaxFiles: 1, MaxBytes: 1`. Bounding it needed a limit
  inside `readBackup`, not in the callback — hence `backupReadLimits`.
- **The tenant check ran after every group was on disk.** The manifest is the
  archive's first entry and `readBackup` enforces that, so nothing forced it to
  be last; a 1 TiB wrong-tenant archive filled the volume before the refusal,
  and a SIGKILL in that window left the wrong tenant's groups in a
  `<dst>.restoring` sibling of the victim's directory.
- **`-dry-run` without `-dst`** cleaned `""` to `"."` and read the current
  directory, so a scheduled backup check succeeded or failed depending on where
  it was run from. A dry run now touches no destination at all.
- **`dst` as a symlink**: `os.ReadDir` follows it, `os.RemoveAll` unlinks the
  link itself, so the store landed in the link's parent and the target kept
  what it had — with success reported. `dst` as `"."`: `os.RemoveAll` rejects
  any path ending in one, so the restore failed *after* writing the whole
  archive. Both are refused up front now.
- **A post-rename `syncDirNamed` failure returned a plain error** with the
  store fully in place, sending an operator to retry into a destination that is
  now occupied. `ErrRestoredButUnsynced` says which it is.
- **The escape test proved nothing.** Its four crafted names are all rejected
  by `groupIDFromName` before the flattening matters, so deleting
  `filepath.Base` left the whole suite green while `../group-9.bin` landed in
  the destination's parent. The flattening is only reachable for an entry that
  gets as far as being written, which needs a manifest-less archive.
- **The `total bytes` limit subtest was a second per-entry test**: `MaxBytes:
  64` against 379-byte groups trips on the first entry, so `total +=` could be
  `total =` and it still passed. The accumulation is the only thing that
  distinguishes `MaxBytes` from `MaxFileBytes`.
- **A comment on the most dangerous line in the file was wrong about Go and
  argued the line was unnecessary.** It said `os.RemoveAll(dst)` was redundant
  on Linux because a rename onto an empty directory succeeds. Raw `rename(2)`
  does; `os.Rename` `Lstat`s the destination and returns `EEXIST` for any
  existing directory. The line is load-bearing on the platform CI runs, and it
  is also the line that turned the missing lock from a safe abort into
  destruction.
- **`cmd/simdlogs` had no tests at all**, which is how a usage message
  claiming "the destination is the whole store or is untouched" shipped over an
  implementation where it was not. The command now returns an exit code instead
  of calling `os.Exit`, and has seven.

Eight of the nine new guards were revert-probed; the ninth (the lock) needed
its test rewritten first, which is the entry above.

### Round two: that claim did not survive either

Reviewer B deleted the guards one at a time and found three of them left the
whole suite green:

- **the tenant-timing hook** -- `TestRestoreRefusesTheWrongTenant`'s
  `os.Stat(dst)` assertion passes whether the check runs before the first
  write or after the last, because the cleanup removes the destination either
  way. It tests the deferred cleanup, not the timing.
- **the under-lock re-check** -- deleting it changed nothing, and the reason
  turned out to be worse than a missing test: the re-check ran *before* the
  archive read, so it could not protect the `os.RemoveAll(dst)` that runs
  after it. A file appearing in the destination during the read -- which the
  lock does not prevent, since a lock stops a process opening a STORE, not one
  dropping a file in -- was deleted with no error. The check now sits against
  the removal it guards, and the test writes the file from inside a
  `pausingReader` hook.
- **the dry-run "writes nothing" sentinel** -- `staging == ""`, and
  `filepath.Join("", name)` is `name`. Deleting the guard left every package
  green while the dry runs wrote eight stray `group-*.bin` files into
  `internal/storage/`, `cmd/simdlogs/` and `internal/api/`. No test looked at
  the working directory. The signal is a nil function now, which has no such
  failure mode.

And seven more defects, of which two matter:

- **A SIGKILLed restore made the destination unrestorable.** `lockDir` creates
  `dst/LOCK` and nothing unlinks it when the process dies, so the next attempt
  counted it as an occupant and refused. A disaster-recovery tool that cannot
  recover from its own crash without a manual `rm` is the wrong end of that
  trade; `unlock` closes the descriptor and never removes the file, so a
  cleanly stopped store leaves one too. The listing no longer counts it, and
  `lockDir` -- the only thing that can tell a running process from a file it
  left behind -- decides.
- **`os.RemoveAll(dst)` unlinks the lock two syscalls before the rename.**
  From that instant another process can create the destination and take a
  fresh lock on a new inode; the rename then fails with EEXIST, and the
  deferred cleanup removed `dst/LOCK` -- *that* process's lock. Two writers
  with independent id counters, reached through the cleanup of the mechanism
  that exists to prevent them. The cleanup now skips the lock removal once the
  removal has run.

The rest: the README's own restore command failed on a fresh machine
(`os.Mkdir`, not `MkdirAll`, and the parent is created by the *server*);
`ErrBackupUnverified` was swallowed when the sync also failed, so an operator
was told the store landed and not that nothing had checked it (`errors.Join`);
`MaxManifestBytes` could only be lowered, so an operator whose own manifest
exceeded the built-in 64 MiB could not restore their own backup; a pre-created
destination's mode was replaced; the library read a negative limit as "give me
the default" where the CLI refused it; `os.RemoveAll(staging)` destroyed a
same-named directory that had nothing to do with a restore; and
`simdlogs restore -h` exited 2.

**And the worst of the round was mine and was not code at all.** The script
that added the "Staged restore" section to `docs/lld/storage.md` ended with
`s = s[:start] + new` -- it truncated the file. `## Disk` and the entire
`## Corruption policy and storage health` section, ~186 lines documenting a
subsystem shipped two commits earlier, were deleted: quarantine ordering, the
recovery gate, the record format, `quarantine/ACKNOWLEDGED` persistence, four
metrics, `DirRereadInterval`, the disk-footprint baselines. `go test ./...`
was green, `gofmt` was clean, `go vet` was clean, and this repository has no
link or count gate over the LLDs. Nothing but reading the diff would have
caught it, and I did not read the diff -- I read the section I had added.

The rule that follows is narrow and absolute: **a scripted edit to a document
is reviewed by its diffstat.** `+102 -2` is an insertion; `+89 -170` is not,
whatever the section you were looking at says. `git diff --stat` takes one
second and is the only thing that sees what a truncating splice did.

### Round three: the window was narrowed, not closed

Reviewer C reproduced the original catastrophe against the FIXED code.
`os.RemoveAll(dst)` unlinks the lock file the call has been holding, two
syscalls before `os.Rename` re-establishes anything. Hammering `OpenStore` +
`AppendGroup` against a loop of restores for fifteen seconds, four runs:

```
restores ok=63552 failed=179697  short-destinations=6  partial-visible=1065
restores ok=70483 failed=202267  short-destinations=9  partial-visible= 940
restores ok=82525 failed=265367  short-destinations=4  partial-visible= 582
restores ok=75717 failed=238429  short-destinations=6  partial-visible= 511
a successful restore left [group-1.bin group-2.bin group-3.bin];
  stat: group-0.bin=no such file or directory
```

`Restore` returned nil with a group missing, and `partial-visible` counts opens
that saw one to three group files -- a partial store visible mid-restore, which
staging exists to make impossible. Isolated deterministically, the mechanism is
plain:

```
AppendGroup err=<nil>
the restored group-0.bin was OVERWRITTEN by the live writer: 379 bytes -> 348
```

A writer that gets in allocates ids from its own counter, and
`discardUncommitted` unlinks `dst/group-<id>.bin` **by path** on a failed
commit -- after the rename, that path is the archive's file.

The window went from "the whole archive read" to "two syscalls", which is a
real improvement and is not what four documents asserted. The fix is to make
the lock survive the swap rather than to shrink the gap further: the STAGING
directory is locked too, just before the swap, and that lock file is the one
the rename installs as `dst/LOCK`. After the rename this process holds the
restored store's own lock; before it, a server that creates `dst` makes the
rename fail with `EEXIST`, which is a safe abort. There is no ordering left in
which someone else writes into the result.

Testing it needed a seam. The assertion is about the instant between the rename
and the return, so `restore-renamed` joins the fault points -- used as a
notification rather than an injection, with the test opening the destination
from the hook and requiring `ErrLocked`.

**And the fix for the lock introduced a lock defect of its own.** The deferred
cleanup ran `lock.unlock()` and then `os.Remove(dst/LOCK)`. Between them the
inode is linked and unheld; a process that flocks it in that gap has its lock
file unlinked out from under it, and a third process then creates a new inode
and also succeeds. Two holders of a lock whose entire purpose is that there is
one:

```
restores=4 competing lock acquisitions=5 DOUBLE-HELD=1
restores=2 competing lock acquisitions=3 DOUBLE-HELD=1
restores=5 competing lock acquisitions=8 DOUBLE-HELD=2
restores=3 competing lock acquisitions=4 DOUBLE-HELD=2
restores=2 LOCK-UNLINKED-UNDER-A-HOLDER=2
```

The unlink goes first, while the flock is still held. That ordering has no gap
to inject into, so it is recorded here rather than guarded by a test -- the
same placement as simdparquet's int64 wrap.

### The rest of round three

- **The tenant-timing guard was still untested**, and round two's own entry had
  named it as one of three that left the suite green. The other two got tests;
  this one did not. It has one now, and it counts `temp-create` faults rather
  than looking at what survives -- 0 writes for the wrong tenant, 4 for the
  right one, so the counter is measuring something.
- **"Before the first group is written" was false for a manifest-last
  archive.** `readBackup` enforces manifest-first *retroactively*: groups
  arriving first are read, parsed and emitted, and the ordering error is raised
  when the manifest turns up. All four groups landed on the destination's own
  volume from an archive whose manifest had been moved to the end, bounded only
  by `MaxBytes` -- a terabyte by default. A restore that names a tenant now
  refuses any group that precedes the manifest.
- **A fresh destination came out wider than the server would make it.** The fix
  for "a pre-created destination's mode was replaced" chmodded unconditionally
  to the hard-coded 0755, so under umask 077 a restore produced 0755 where
  `OpenStore` gives 0700: log data made world-readable by the fix for the
  opposite defect. The mode is applied only to a destination that already
  existed, and the test now runs at umask 077 -- 022, where it used to run, is
  the one umask at which the two branches agree and nothing shows.
- **`clearStaging` destroyed a legacy store at the staging path.** Keying on
  "does it hold only group files" cannot tell crashed staging from a
  pre-MANIFEST store, and `-dst /srv/logs` derives `/srv/logs.restoring`. A
  `.simdlogs-restoring` marker, written first and removed before the rename, is
  present in exactly one situation.
- **Two CLI branches had no test**: the `ErrRestoredButUnsynced` message, which
  is the entire operator-facing reason that error exists, and the usage
  refusal for a missing `-dst`.
- **Three stray `group-*.bin` files** were sitting in `internal/storage/`,
  residue of round two's dry-run-sentinel probe, one `git add -A` from being
  committed.
- Two stated rationales were wrong. "unlock closes the descriptor and never
  removes the file" is unix-only -- on Windows the lock IS the open handle and
  `unlock` removes it, so a stale LOCK there means a crash and `lockDir`
  refuses it with `O_EXCL` whatever the destination listing does. And
  `os.RemoveAll` rejects a path ending in `.`, not `..`; the kernel refuses
  that one, with a different errno arriving just as late.

Adding `restore-renamed` broke `TestEveryWriteFaultReachesTheCaller`, which
sweeps `FaultPointNames()` and requires each to fail a write. That test is
right and the fix is not a skip: the exclusion list moved into the storage
package as `NonWriteFaultPointNames`, inverted so that a new WRITE step is
covered the day it exists. A write-path list would have silently missed the
next one, which is the failure the sweep exists to prevent.

### Round five: the round that fixed it re-opened it

Round two closed a defect by skipping the lock-file unlink "once the removal
has run". Round four, rewriting the same function, moved that flag from
*before `os.RemoveAll(dst)`* to *after `os.Rename`*:

```go
if err := os.RemoveAll(dst); err != nil { return man, err }   // this call's dst/LOCK is gone
if err := os.Rename(staging, dst); err != nil { return man, err }  // lock still non-nil here
lock = nil                                                     // guard moved to here
```

On the rename-failure path the lock is still considered ours, and the deferred
cleanup unlinks `dst/LOCK`. But the ONLY way `os.Rename` returns `EEXIST` is
that another process created `dst` -- so on that path the lock file is theirs.
The failure condition and the ownership are the same event, not a race.

Measured with generation-counter attribution, 30-second runs under contention:

| build | LOCK-STOLEN | DOUBLE-HELD |
|---|---|---|
| as shipped | 38, 47 | 31, 37 |
| flag before the removal | 0, 0 | 0, 0 |

Identical EEXIST pressure in both. The failure it produces is two writers with
independent `nextID` counters in one directory -- the thing `lockDir` exists to
prevent, reached through the cleanup of the mechanism that prevents it. The
flag is back before the removal, where round two put it, and
`TestALostRenameDoesNotTakeTheWinnersLock` now creates the winning process from
a new `restore-removed` fault point and requires its lock to survive.

**On Windows every successful restore produced a store nobody could open.**
`dirLock` captures its path at lock time, and the rename moves the file from
the staging directory to `dst/LOCK`. The release then did `os.Remove(dst/LOCK)`
-- which on Windows *must* fail, since the lock IS the open handle and this
repo's own comment says an open file cannot be deleted -- and then `unlock`,
which removes the path captured earlier: `staging/LOCK`, long gone. Net:
`dst/LOCK` survives, and `lockDir`'s `O_EXCL` refuses that directory forever.
The unlink-before-unlock invariant this file recorded as universal one round
earlier is unix-only for exactly that reason. `dirLock` grew `movedTo` and a
per-platform `release`: unix unlinks while holding, Windows unlocks and lets
`unlock` do the removal.

**The dry-run guard was unguarded, and the test written to close it could not
fire.** Reverting the nil-function signal to the empty-path sentinel left
`go test ./...` at exit 0 while writing the same eight stray `group-*.bin`
files into the same three directories that round two's entry records --
reproduced against the fix for it. The test compared the working directory
before and after its own dry run, and any earlier dry run in the package put
the strays into the "before" snapshot: it failed alone and passed in the suite.
It runs in its own `t.Chdir(t.TempDir())` now. That is round one's recorded
lesson -- *a defect that lives in the middle cannot be tested by a
before-and-after assertion* -- made again on a different property.

**A kill in the marker-less window made the destination unrestorable.** The
marker was written inside the staging directory, so it had to be removed before
the rename or it would land in the restored store -- and between those two
points sit an fsync of the whole staged store, a lock, a readdir and the
removal of the old store. Seconds on a real store. A kill there left a
complete, marker-less staging directory that every later restore refused, and
if it landed after the removal the old store was gone too. The marker is a
sibling now, `<dst>.restoring.marker`, removed after the rename: it can never
reach a store and there is no window.

Also: the staging directory was created 0755 while the destination was 0700,
leaving group names and sizes world-listable for the length of the restore;
four documents and comments asserted properties the code did not have (the
"safe abort", "no server can open that directory", "a LOCK is named in the
error", and the manifest-first reasoning that round three had already corrected
360 lines below in the same file -- the eighth one-side-only claim, re-added in
the round that wrote the lesson down); and the umask helper justified itself
with "none calls t.Parallel" in a package with nine such call sites.

**What this round is really about.** Every defect above was introduced by the
fix for the defect before it, and two of them are the earlier defect returning
in the same function. Rewriting a function that a previous round hardened
re-opens what that round closed unless every guard it added is carried across
deliberately -- and a guard whose reason lives only in a comment, or only in a
`docs/wrong.md` entry, is one nobody carries. The three that survived five
rounds are the ones with a test that dies when they are deleted.

### Round six: a lock file cannot be handed over by unlinking it

Round five ordered the unlink before the unlock and recorded that ordering as
having "no gap to inject into". Review measured the gap on the other side: a
competitor whose `open(2)` precedes the unlink acquires the flock on the
now-unlinked inode after the unlock, while the next competitor creates a fresh
inode at the path and flocks that. One lock, two holders. Thirty-second hammer,
`OpenStore`+`AppendGroup` against a restore loop:

| build | restores | LOCK-STOLEN | DOUBLE-HELD |
|---|---|---|---|
| unlink-then-unlock (round five) | 321257 | 24 | 268 |
| same, second run | 315989 | 44 | 211 |
| `dstRemoved` late (round four control) | 305638 | 197 | 276 |
| **no unlink at all** | 315639 | **0** | **0** |

Both orderings have a gap; the mistake was thinking one of them did not. **A
lock file whose inode can change cannot be handed over safely by ordering.** So
the file is never unlinked -- which is what `unlock` and `Store.Close` have
always done, and what the destination check was already written to tolerate.
`release` is now `unlock` on unix, and a restored store carries a `LOCK` exactly
as a store the server created does.

That has a visible consequence and it is the right one: a failed restore into a
path that did not exist leaves an empty directory holding a lock nobody holds.
The next restore accepts it. Six tests that asserted "the destination does not
exist" as a proxy for "nothing landed" now assert the property itself -- no
group files -- which is what they were always about.

**Windows could never have completed a restore.** `lockDir(dst)` keeps
`dst/LOCK` open, and `os.RemoveAll(dst)` must then fail with a sharing
violation: Go opens without `FILE_SHARE_DELETE`, and `removeall_at.go`'s
`Deleteat` needs `DELETE` access. That is the premise `lock_windows.go` and the
LLD both state -- "a file opened without FILE_SHARE_DELETE cannot be deleted
while open… holding the handle is itself the lock" -- so the design was
internally inconsistent: either the rule holds and the removal cannot succeed,
or it does not and the Windows lock is not a lock. Every restore would have
failed after staging, validating and fsyncing the whole archive, leaving a
`LOCK` that `O_EXCL` refused forever. The destination's lock is released
explicitly before the removal now, which is what makes the removal possible
there at all. REASONED, not measured: there is no Windows host here, and
`GOOS=windows go vet ./...` does not even compile the test files (a pre-existing
`syscall.Kill` in a helper).

**The conditional release skipped the close, not just the unlink.** Round two's
rule was "do not remove the lock file once the removal has run"; round three
merged remove and unlock into one function, so the guard began skipping the
unlock too -- 75 leaked descriptors per 200 restores, and on an `os.RemoveAll`
failure a held lock that made the destination unopenable for the life of the
process. The release is unconditional now, and it has nothing left to guard
against.

**The marker-less window was moved, not closed.** The marker was written after
`os.Mkdir(staging)`, so a kill between them left an empty marker-less staging
directory that `clearStaging` refused forever -- the same non-retryable
destination, two syscalls wide instead of an fsync-and-readdir wide, in a round
whose entry said "there is no window". The marker goes down first now. Like the
two lock orderings, that is reasoning and not a test: the orderings differ only
on a kill, where no defer runs.

Three claims in three documents were also wrong: the LLD still described round
three's marker (wrong name, wrong location, wrong timing) in the document round
five rewrote; `docs/architecture.md` said "no server can open it mid-restore",
which is false and measured -- a server DID open the destination in the
removal-to-rename gap, appended a group, and the restore aborted with `EEXIST`,
which is safe but is not what the sentence says, and the accurate version is in
two other files; and the LLD restated the `..`/`os.RemoveAll` error that round
three had already corrected in the code beside it.

### Round seven: the Windows fix cost the unix platform its data

Round six released the destination's lock immediately before
`os.RemoveAll(dst)`, because Windows cannot remove a directory holding an open
handle. On unix that release is not merely unnecessary, it is the original
catastrophe again -- and it is round six's OTHER change that makes it so.

Since the lock file is never unlinked, after an early release `dst/LOCK` is
still there and is UNHELD. A server flocks the file that already exists, opens
the store, and then: `os.RemoveAll(dst)` deletes that live store, and
`os.Rename` **succeeds** -- because the winner never had to create the
directory, so there is no `EEXIST` to abort on. Instrumented, one run:

```
Restore returned nil; the manifest names 4 groups
a store OPENED at the destination inside the release-to-removal window
the ghost's AppendGroup SUCCEEDED, taking id 0
THE ARCHIVE'S group-0.bin WAS OVERWRITTEN: 353 bytes, want 379
the reopened store answers with 4 groups / 13 rows; the archive held 16
```

Round one's paragraph, word for word, six rounds later, on the fix for it.
Hammered, thirty seconds:

| build | restores ok | displaced holders | over a restored store | archive group overwritten |
|---|---|---|---|---|
| as shipped | 560 | 3739 | 7 | **1** |
| again, 60 s | 1055 | 7096 | 5 | **1** |
| pre-removal release deleted | 544 | 192 | **0** | **0** |

The control's 192 are `RemoveAll` racing a competitor that recreates `dst`;
none reached a restored store, because the rename fails `EEXIST`. That is the
safe abort the documents describe, and it is the argument the early release
destroys: **the abort depends on the winner having to CREATE the directory.**
Leave the directory there for them to open and the argument is gone.

The repair is `releaseBeforeRemoval`, per-platform: `release()` on Windows,
nothing on unix. Round six put a Windows-only requirement in shared code and
the shared platform is the one that lost data.

Round six's own table -- "no unlink at all: 0 steals, 0 double-holds" -- is
true of what it measured, two holders of the same INODE, and says nothing
about two holders of the same DIRECTORY. A metric that answers a narrower
question than the prose around it is how a round certifies its own regression.

### Three more, and one guard that turned out to be two

- **Nothing tested that the marker is written at all.** Two rounds hardened its
  timing and its location; deleting the `os.WriteFile` left the suite green,
  and `clearStaging` then refuses EVERY leftover staging directory -- the
  non-retryable destination both of those rounds wrote an entry about. Both
  crash tests wrote the marker by hand, and the fault-injected ones take the
  error path where the deferred cleanup makes it irrelevant. It is asserted
  mid-restore now, from a reader hook.
- **The per-entry `MaxFiles` counter was unguarded.** The subtest named "group
  count" trips the MANIFEST bound; the per-entry counter is the only
  entry-count bound an unverified archive can get, and nothing exercised it.
  Same shape as round one's "the total-bytes subtest was a second per-entry
  test", two rounds after that was recorded.
- **`RequireTenant` on an unverified archive is guarded by two rules, and the
  test only reached one.** An archive WITH groups is refused by the
  groups-before-manifest rule on its first entry, so the refusal at the end of
  `readRestore` never runs -- which is why deleting it changed nothing. It is
  reachable for an archive with no entries at all, and without it an empty
  manifest-less archive restores under a tenant it does not name. Both rules
  now have a case, and deleting either fails one.

And the ninth one-side-only claim, this time in the text an operator reads:
round six corrected "no server can open it mid-restore" in
`docs/architecture.md` while `simdlogs restore -h` went on printing "the
store's own lock is held for the whole run, so no server can open the
destination while it is in progress" -- false twice over. Six more sites said
"for the whole run"; the lock is held until the removal, and that is the
sentence that has to be everywhere, because the safe-abort argument depends on
it.

### Round eight: the protocol holds, and three residues

The swap was hammered with `OpenStore`+`AppendGroup` ghosts and with concurrent
restorers, and the harness proved sensitive by reproducing the round-seven
defect on demand:

| build | restores | ghost opens | displaced | archive group overwritten |
|---|---|---|---|---|
| as shipped, 30 s, 4 ghosts | 30636 | 173578 | 0 | **0** |
| as shipped, 30 s, 12 ghosts | 3825 | 132006 | 0 | **0** |
| control (unix `releaseBeforeRemoval` = `release()`), 10 s | 12314 | 89636 | 203 | **4364** |

The control's thirty-second run did not finish: `fatal error: fault / SIGBUS`
inside `binary.littleEndian.Uint32`, a reader's mmapped group replaced under
it. Restore-against-restore, 163k and 139k rounds at four and eight
concurrent restorers, destination verified byte for byte against the winning
archive: zero two-winner outcomes, zero mixtures, zero wrong bytes.

**The per-entry limits bounded the write, not the allocation.** `readBackup`
reads a whole entry with `io.ReadAll` sized from the manifest's declared size,
and every per-entry limit lived in the emit callback that runs after it:
measured, an archive declaring one 128 MiB group cost 268.5 MiB of live heap
with `MaxFileBytes` set to 1 -- in a DRY RUN, the mode the documents point an
operator at for an untrusted archive. That is the argument this file already
makes three times about the manifest, one entry type over, with the mechanism
to fix it (`backupReadLimits`) already in place. The ceiling is pushed down
now, and the test asserts the ERROR MESSAGE rather than the allocation: a
truncated tar is bounded by the bytes it carries, so a buffer sized from the
declaration and one sized from the stream look identical on that fixture, and
only the wording says the refusal came before any read.

**The descriptor-leak test's own rationale went false when round seven made
the release per-platform.** Its comment says "the success path releases the
lock explicitly before the removal, so only a failure exercises the deferred
release" -- true on Windows and false on unix since `releaseBeforeRemoval`
became a no-op there, where the deferred release is now the ONLY one. The test
drove twenty failing restores and no successful ones, so with the conditional
release restored, twenty successful restores leak twenty descriptors and the
suite stays green. Round six's own recorded number, uncovered again by the
round that fixed something else.

**And the eleventh one-side-only claim.** Round seven corrected "the lock is
held for the whole run" in eight places including the CLI help, and left
paragraph five of `docs/lld/storage.md` stating the round-six design outright:
"the destination's own lock is released explicitly before the removal". That is
the sentence whose wrongness produced the round-seven catastrophe, sitting
twenty-eight lines below the paragraph that says the early release exists "there
and only there". A reader who goes to the lock-protocol paragraph reads the
design that lost the data.

Two smaller ones. "A second restore cannot start" is false in the
removal-to-rename gap: one can, and it deletes this call's staging directory on
its way out, because both derive their paths from `dst`. Contained -- the
winner's next write fails `ENOENT` and it reports an error, zero mixtures in
302k rounds -- and the sentence now says so. And `FaultPointNamed`'s doc
enumerated eleven names, three behind, directly above the paragraph explaining
why `FaultPointNames()` exists so that a hand-written list cannot go stale.

## The lock that excluded nobody, and the fix that was aimed at the wrong window

Round nine came in as a data-loss finding against the uncommitted restore:
`os.RemoveAll(dst)` unlinks `dst/LOCK`, then re-reads the directory until it
reads empty, then `rmdir`s -- so in the middle of its own loop the destination
is present and lockless, a server opening it there creates a second `LOCK`
inode, and the loop's next pass deletes what that server wrote before the
rename goes on to succeed. Measured against twelve writers over thirty
seconds: 24 of the archive's groups overwritten in one run and 3 in another,
against 0 for the same harness with `Restore` never called.

The finding was right that data was being lost. It was wrong about which
window was losing it, and the fix aimed at the named window did nothing.

Replacing the removal walk with `os.Rename(dst, dst+".restoring.old")` -- one
syscall, so the directory and the lock inode inside it leave together, with
the sibling removed afterwards off a path no opener can reach -- and running
the same harness:

| build | ghost groups lost | archive groups overwritten |
|---|---|---|
| removal walk (as found) | 13 | 27 |
| one-syscall rename | 7 | 17 |

Within noise of each other. Both fail.

**What was actually firing** came out of a syscall trace rather than an
argument. `lockDir` opens the lock file and then flocks it, which is two
syscalls, and a staged restore replaces the whole directory in one. The
descriptor then names an inode that has left the path -- unlinked with the
directory it lived in, contended by nobody, so the flock ALWAYS succeeds:

```
077219 R renameIn ok dstDirIno=113001475 stagedLockIno=113001480
077220 R stagedRelease lockIno=113001480
077230 R rmDisplaced ino=113001475            <- inode 480's directory destroyed
...
077252 R lockDst ok  lockIno=113001501 dirIno=113001496
077256 G open        lockIno=113001480 dirIno=113001496   <- locked the corpse
077261 G append id=0 err=<nil>                <- writes into 496 BY PATH
```

Six directory generations after inode 113001480 was released and deleted, a
store opened on it and appended `group-0.bin` into the live directory whose
lock was 113001501. Two processes, one path, one of them holding a dead file.
That is not a restore defect at all -- every `OpenStore` in the product has it,
and the restore is merely the thing that swaps directories fast enough to
expose it.

Checking the flocked inode against the path afterwards, and retrying a bounded
number of times, closes it. Interleaved in one session:

| build | ghost groups lost | archive groups overwritten | |
|---|---|---|---|
| as found | 13 | 27 | FAIL |
| inode check + rename, 60s / 24 ghosts | 0 | 0 | PASS |
| inode check + rename, 45s / 16 ghosts | 0 | 0 | PASS |

**Then the rename had to justify itself, because the check alone might have
been enough.** Reverting to the removal walk with the inode check kept, 45
seconds and 16 ghosts: 0 lost, 0 overwritten, PASS. By that measurement the
rename is dead weight.

It is not, and the reason is that the walk's window is real but a great deal
narrower than the finding implied. `checkRestoreDestinationLocked` guarantees
the destination holds nothing but this call's own lock file, so the walk is
always exactly one unlink, one readdir, one rmdir -- a window no competitor
reaches at production speed. Widen only the width, keeping the shape and the
inode check, and it opens:

| build | foreign entries in the gap | archive groups overwritten | ghost groups lost | restores landing incomplete |
|---|---|---|---|---|
| removal walk, gap widened to 200us | **56,434** | 692 | 519 | 85 |
| one-syscall rename | 0 | 0 | 0 | 0 |

56,434 times in twenty-five seconds a file belonging to another process
appeared inside a destination the removal had already emptied. So both changes
ship: the inode check closes what fires at production speed, the rename closes
what only amplification reaches.

**The lesson is about the amplification.** The finding's evidence -- 2,432
foreign `LOCK` files and 508 foreign manifests and groups unlinked -- came from
an instrumented removal that was deliberately slower per entry. That
measurement proved a hole existed. It did not prove which hole was firing, and
the diagnosis attached to it named the window the instrument had widened rather
than the one the product was losing through. Slowing a suspect down finds holes
near the suspect; it cannot rank them against holes somewhere else. The trace
could, because it recorded what the losing writer actually held.

Recorded because none of it was reasoning: three rounds of argument about
rename orderings and `EEXIST` were all consistent with the code and all beside
the point, and the sixty-second trace answered it in one read.

## Four things the round-nine sign-off found, and the one measurement that was more useful than the argument

The sign-off reviewer could not lose data against the shipped build — 60 s,
24 ghosts, both windows widened to 200 µs, zero double-holds and zero groups
lost or changed. Four other things did not survive.

**The rename was independently load-bearing after all.** The round's own probe
B (removal walk restored, inode check kept) lost nothing at production speed,
which made the rename look like dead weight justified only by amplification.
Deleting the inode check while KEEPING the rename — the probe nobody ran —
produces 8 double-holds in 30 seconds at production speed. Both changes carry
their own weight; the earlier entry's framing of the rename as
"amplification-only" was an artefact of testing one direction.

**The marker died before the staging directory it vouches for.** Deferred
calls run in reverse, and the marker's removal was inside the cleanup
registered last, so it ran before the staging cleanup. Sampled from another
goroutine during the unwind after an abort between the renames:

| archive | samples | saw (marker gone, staging present) |
|---|---|---|
| 4 groups | 212 | 13 |
| 40 groups | 967 | 58 |
| 400 groups | 9,323 | 690 |

Six to seven per cent of every unwind, on an invariant `restore.go` and
`docs/lld/storage.md` both state in words — *"a kill between them would leave a
staging directory a later restore refuses"*. A kill in that window produces
exactly that, refused forever.

The end state is identical whichever order they go in, which is why nothing
caught it and why the fix needed a fault point rather than an assertion: the
marker's removal is registered with its WRITE now, so reverse order puts it
last, and `faultRestoreCleanup` lets a test stand inside the cleanup and check
that both directories are already gone.

**The `os.Remove(dst)` on the failure path could only ever remove somebody
else's directory.** Its own residue it cannot touch: `lockDir` has put a LOCK
in the directory, the file is never unlinked, and rmdir refuses a non-empty
directory — measured after a truncated restore into a path that did not exist,
`dst` holds `[LOCK]` and the rmdir fails every time. A competitor's it can: a
server creating the destination in the gap between the two renames has an empty
directory for as long as it takes to open its lock file, and the rmdir takes it.
Deleted. It was also the last thing making *"aborts without touching
anything"* false.

**Four guards were unguarded** — deletable with the whole suite green: the
rename itself, the marker ordering, `clearStaging`'s displaced arm, and that
rmdir. Now five test functions and six cases -- the marker ordering needs two,
one per path -- each verified to fail against its own reversion.

**And four doc claims did not match the code.** *"Two restores cannot
interleave"* in README.md and verification.md, which round eight had already
recorded as false and corrected in the LLD and in `restore.go` — the same
sentence left standing in the two documents an operator actually reads. The
`EEXIST` claim in architecture.md and verification.md, which is one of two
orderings: Go's `os.Rename` `Lstat`s and returns EEXIST for an existing
directory, but a directory created between that `Lstat` and the raw
`rename(2)` is empty and gets replaced — safe by the staging lock, not by
EEXIST. `lockedFileIsAtPath` claimed `os.SameFile` was stronger than comparing
device and inode; on unix it *is* that comparison, and what makes the check
sound is the held descriptor pinning the inode. And `movedTo`'s comment on the
unix file described the Windows consequence as if it were unix's, where
`release` never reads the path at all.

Five of these are the one-side-only shape this file has now recorded twelve
times. The new one is the third: **a correction that lands in the reference
document and not in the README.** The LLD was fixed in round eight, the entry
was written in round eight, and the false sentence survived in the two files
with the widest audience — because the round's diff touched the LLD and the
grep that would have found the others was never run.

## Two restores in each other's gap, and the sentence that said it could not happen

The round-ten sign-off could not lose data on any path a deployment reaches:
61,425 committed groups under four concurrent restore loops with none vanished
or changed, 20,000 barrier rounds of six workers with six distinct archives and
exactly one winner each, 11.6 million overlapping attempts with 104,479
successful restores and no foreign group in any of them. A ghost `OpenStore`
at every one of the five fault points loses nothing either.

It did find an ordering the code said did not exist. Two restores, each parked
in its own gap between `os.Rename(dst, displaced)` and `os.Rename(staging,
dst)`: the first returns **nil**, with a manifest naming its own three groups,
over a destination holding the other's six. Two ordinary `Restore` calls, no
hand-made filesystem state, parked with this package's own `restore-removed`
point. `simdlogs restore` would exit 0 and print the archive it read.

Reachability is the reason this is recorded rather than fixed:

| harness | occurrences |
|---|---|
| 20,000 barrier rounds, 6 workers, 6 archives | **0** |
| 11.6M overlapping attempts, 104,479 successful restores | **0** |
| same, re-run: 6.8M attempts, 95,600 restores | **0** |
| with a concurrent `os.RemoveAll(dst)` from outside any restore | 267 of 85,686 in-lock samples |

It needs something that is not a restore to widen the window. Closing it needs
a lock that outlives the swap on a path neither restore owns, which is a
different design; the documented answer is that two restores must not target
one destination, and the four places that said otherwise now say this.

**The claim is the finding.** `restore.go` said *"There is no ordering left in
which someone else writes into the result"*, and the LLD said *"leave no
ordering in which someone else writes into the result"*. Both were reasoned
from the server case, which is genuinely closed, and stated over all cases.
That is the thirteenth instance of the one-side-only shape this file has now
recorded twelve times — and the first where the untrue half is a claim of
completeness rather than a claim about a platform or a branch. "No ordering
left" is a quantifier, and a quantifier is exactly the kind of sentence that
cannot be true of the case you had in mind and false nowhere else.

Four smaller things from the same review, all text: a garbled sentence shipped
in `docs/verification.md` (a splice that duplicated half a clause and lost the
other half — the diffstat was insertion-shaped and the sentence was still
broken, so a numstat check does not catch this class); the pre-fix comment left
standing on the very cleanup this round rewrote, four lines above the line that
contradicts it; *"no crash can leave a staging directory a later restore
refuses"*, which holds for a process kill and is not established for a power
loss, because the marker is written with no fsync of the file or its parent;
and the refusal of `.`, `..` and symlinks justified by what `os.RemoveAll(dst)`
does, which is the call this round deleted.

And `TestTheMarkerOutlivesTheDirectoriesItVouchesFor` asserted on the success
path, where the staging directory has already been renamed away — so its
staging check could never fire, on a fix whose defect was on the abort path.
It is two subtests now, and the abort one fails against the pre-fix ordering.
A test that cannot fail is the shape entry 33 records; this one could fail, on
the half of the invariant that was never broken.

## A retraction that landed in one paragraph and left the claim standing twice in the same file

The previous entry retracted "there is no ordering left in which someone else
writes into the result" and rewrote the four places that stated it. The
verification round found the claim still standing in two more —
`docs/lld/storage.md` and `restore.go` — each of them **forty lines above the
paragraph retracting it**, so both files asserted the claim and its correction
at once. A third site, a test's own comment, had never been on the list.

The reason is worth more than the fix. The retracted sentence was reworded
where it appeared as a *conclusion*, and left where it appeared as a *passing
mention* inside a paragraph about something else — the lock's release timing in
one case, the emptiness re-check in the other. A grep for the sentence would
have found all six; the round searched for the paragraph instead, because that
is what it had just rewritten. Correcting a claim means finding every place it
is asserted, not every place it is argued.

Four smaller things from the same round, all text: `entry 44` cited for the
value-count bomb where the ordinal is 45; the `.`/`..`/symlink refusals still
justified by what `os.RemoveAll(dst)` does, which is the call this design
replaced — measured with each refusal removed, `.` now fails the emptiness
re-check and a symlinked destination SUCCEEDS, replacing the link with a real
directory and leaving the target holding a `LOCK`; "four guards in five tests"
against a file holding five functions and six cases; and a paragraph whose
"Asserted mid-stream, not after the fact" had drifted twenty lines from the
claim it qualifies.

And one the round introduced while fixing the first: **"reachable only when
something outside a restore widens the window"**. Four harnesses observed it
zero times without such a widener, which bounds how often it happens and does
not establish that nothing else reaches it. That is a universal negative
inferred from bounded runs — the same shape as the quantifier the previous
entry was written to retract, reappearing in the sentence retracting it. It now
says what was measured.

Also settled, because it is what makes one branch of the three-outcome answer
reachable: Go's `os.Rename` returns `EEXIST` for an existing directory, but
having found one it `Lstat`s OLDNAME and reports THAT error first — so a
staging directory another restore already deleted yields `ENOENT` even though
the destination exists.

## Compaction: a zero value that rewrote the store, and a transaction with no test

Fifteen findings from the compaction review. The ones that were not the code
being wrong are the ones worth keeping.

**`CompactOptions{}` merged a 500-group store into one group.** `minGroups()`
floored at 2, and three documents plus the field's own comment read that floor
as a refusal: *"the zero value compacts nothing"*. A floor is not a switch. The
zero value is off now, and the test that was supposed to cover this was named
`"the zero value compacts nothing beyond the floor"` and asserted only that the
rows survived — the name conceded the gap and nobody read it as one.

**The central transaction had no test, and the missing lane was the reason.**
The design says the output's add and the inputs' removes go in one manifest
record. Mutating that into two records left the entire suite green. It could
not have done otherwise: `TestCompactionCrashMatrix` injects an ERROR, an error
unwinds every defer, and a store that unwinds looks consistent whatever the
manifest holds. A SIGKILL does not unwind. Under a process-kill lane the same
mutant duplicates **every batch** at `manifest-append` and `manifest-sync`.
Thirteen phases now, and the error matrix stays for what it does test.

**Three thresholds were checked at the wrong granularity, twice over.** The
byte budget was first checked only between runs — forty groups produced ten
outputs against a one-byte ceiling. Fixed to check between batches, which the
shipped test then passed only because it set a row cap of 4: a batch runs to
`MaxRows` rows, so a store that fits in one batch was still read in full,
184,845 bytes against a limit of 1. It bounds the batch as it accumulates now.
And the group-count floor was applied per batch rather than per run, so
`MinGroups: 10` with a row cap of 8 compacted nothing, ever, with no error and
no stat.

**One unmergeable group refused its whole batch.** Discovering unmergeability
inside `mergeGroups` throws away everything alongside it: one vector column
among sixty left all sixty unmerged, permanently. It is a candidacy question,
so it belongs in `compactCandidate`, where such a group breaks the run like any
other ineligible one.

**Four data races for an early return.** An unlocked `len(s.groups)` pre-check,
to skip a mutex acquisition on a path about to do file I/O, raced `Close`,
`AppendGroup` and a second pass. Deleted.

**A pass blocked every HTTP request for its duration.** `forEachTenant` holds
the lock every request takes to resolve its tenant; a query during a
50,000-group pass took 250.7 ms, and a 200,000-group pass takes 1.33 s. The
tenants are snapshotted and marked in-flight now, and the pass runs outside the
lock.

**And a kill between the commit and the unlink leaked most of a store.**
Tombstones live in memory; a restart drops them, and `OpenStore` deliberately
ignored files the manifest does not name. For a failed append that residue is
one file. For compaction it is every input of the batch — measured, 1 live
group and 8 orphans after two reopens. `OpenStore` reclaims them now, which is
safe there and nowhere else: it holds the exclusive lock and has not written a
group yet.

Two smaller ones with a shape worth naming. A merge that GREW the store was
allowed, and the operator log printed "saved -0.0 MB" — `Recompact` has refused
that since it was written, and the new code did not inherit the rule because
nothing made it. And `mergeGroups` called the caching `DictIndices` on every
column of every group it was about to unlink, filling a 256 MB query cache that
is incremented in one place and decremented nowhere: ~32M merged rows retire
the cache for the life of the process.

**The measurements were wrong and the reason is the instrument again.** The
first version claimed 582x from three runs at a 200-300x benchtime, whose
spreads were 1.65x and 2.2x — at this repo's 8.3% floor a 2.2x spread makes a
minimum-of-three meaningless, and its small-groups figure was four times what a
2000x run measures. Six interleaved runs give 411x on the minimums and 252x on
the least favourable pairing; an independent measurement at another load gave
284x and 274x. What survives two sessions is a band of 250-410x, not a number.
The disk figure, being deterministic, reproduced exactly.

The table also mixed two fixtures: its group column was n=5,000, its byte
column n=20,000, and it said "1 group" where n=20,000 gives 3. One table, two
experiments, and the caption named only one of them.

## The fix for the leak deleted committed data, and the doc comment argued the wrong precondition

`reclaimOrphanGroups` was added last round to stop a killed compaction leaking
its whole input set. It unlinked every `group-*.bin` the manifest did not name.
The sign-off review measured what that does to a store whose manifest is torn:

| fixture | group files before | after one open | health reports |
|---|---|---|---|
| one flipped byte in the 5th of 10 records | 10 | **5** | `healthy: 5 groups` |
| MANIFEST truncated to zero, 20 groups | 20 | **0** | `healthy: 0 groups` |
| the first record's CRC corrupted, 12 groups | 12 | **0** | clean |

Replay stops at the first record that fails its checksum -- by design, and
documented one file over -- so every group committed after a torn record is
invisible. "Invisible" was read as "never committed", and the difference is
data. It also destroyed the documented recovery: remove the MANIFEST and let
the legacy path adopt the directory returns 20 of 20 rows without the reclaim
and 0 of 20 with it, because there was nothing left to adopt. The repo had
already identified this state as reachable -- `backup.go` carries a paragraph
about a truncated manifest making a full directory read as empty -- and the
reclaim upgraded that consequence from "invisible" to "gone".

**The doc comment argued the wrong precondition, at length.** It has a careful
paragraph about why the unlink is safe: the process holds the exclusive lock,
it has not written a group yet, no concurrent writer can be mid-way through a
file. All true, all verified, and none of it the precondition that fails. The
one it never stated is that **the manifest must be a complete record of what
was committed** -- which is exactly the assumption a checksummed append-only
log with a documented stop-at-first-bad-record replay does not give you.

The fix is to reclaim only ids a record explicitly REMOVED. An id in no record
at all -- an append that crashed before its commit, or anything past a torn
record -- is in neither the visible set nor the retired one, and only the
retired set licenses an unlink. `manifest.compact()` folds the log to one Add
record and drops the retired set, so a removal folded away stops being
reclaimable: a leak, not a loss, and stated rather than fixed because carrying
a forever-growing retired list in a file is its own problem.

**The shape.** A leak was traded for a loss, and the trade was invisible
because the change was tested against the failure it fixed and not against the
failures it created. Every test written for it -- the crash matrix, the orphan
count, the row oracles -- exercised a HEALTHY manifest, which is the one input
for which the two rules agree.

Three smaller ones from the same review. A batch was required to have *a*
timestamp column but not to agree on WHICH: two groups, one with `_time` and
one with `ts`, each got 0 in the other's column, so the output spanned from
zero and a window that matched nothing returned every row -- the same failure
the timestamp-less refusal exists for, reached through a second door. The
detached tenant walk marked every tenant in-flight for the whole pass, and
eviction skips in-flight tenants, so a long pass turned a would-be blocking
request into `503 all are in use` -- a 250 ms stall traded for a hard
rejection. And of the fifteen fixes the previous round claimed, five had no
test: re-adding the deleted race, marking in-flight after the unlock, dropping
the batch byte bound, restoring the caching decode, and taking the id before
the size decision all left the suite green.

Two findings are recorded and NOT fixed here, both outside this change's
surface and neither reachable from a shipped path: `Store.Promote` re-adds a
cold copy of a group compaction merged away (19 of 40 row shapes visible twice,
serial and deterministic), and `Promote` overwrites the file of a reissued id.
`Demote`/`Promote`/`ColdStore` have no non-test callers.

## A budget in the middleware is not a budget on the machine

The storage budget shipped with a check in the HTTP middleware and the argument
that a single shared check beats one per handler -- "a check written into each
is a check that will be missing from the seventh". A background review found
the seventh. The native syslog listeners take bytes off a socket with no
middleware anywhere near them: with the filesystem past the reject reserve and
every HTTP insert answering 507, one RFC 5424 frame over TCP and one datagram
over UDP each landed a row and the rejection counters stayed at zero. The
argument was right and was applied to one transport.

The same commit's `QuotaState` returned at the `statfs` error before it reached
`MaxTenantBytes`, so an unmeasurable filesystem disabled BOTH budgets. Measured
on a hook returning an error: 1776 bytes stored against `MaxTenantBytes: 1`
gave `OverQuota=false`, `Err=nil`, `CheckWrite()=nil`. `quota_windows.go`
always returns that error, so every platform it covered enforced nothing -- and
that file's own comment, `QuotaState`'s comment, and `docs/lld/api.md` all said
the tenant quota still applied there. Three statements of a claim, one code
path, no test.

**The cap is advisory at rate.** With `MaxTenantBytes` at 64 KiB, one client
over HTTP pushed **119-125 MB (1816x-1916x the cap)** before the first 507. The
store's own size is sampled at most once per 10 s and the free-space sample
once per 2 s; between samples the cap is a number nothing reads. That is the
same deliberate staleness the reserve is named for, applied to a budget that is
not expressed as a reserve. Also measured: a store with zero groups is under
any positive cap, so the first write is always accepted whatever its size
(349,669 bytes against a cap of 1024). Recorded, not fixed -- shortening the
interval puts a locked directory walk on the write path, which is what the
cache exists to avoid.

**Smaller ones from the same review.** `storagePressure`'s `case st.OverQuota`
arm was dead: `QuotaState` sets `Err` whenever it sets `OverQuota`, so the
`Err != nil` arm always won and "N bytes used, at its quota" could never print
-- deleting the arm left every test green. `diskUsageFn` was a plain global
read on the write path and written by `SetDiskUsageForTest`; `-race` reported
it three ways against the SHIPPED tests, whose `defer restore()` runs with an
httptest server still serving. `quota_unix.go` was tagged `!windows` against a
syscall that does not exist on illumos, so `GOOS=illumos go build ./...`
succeeded at the parent commit and failed at this one. A tenant whose store
could not be opened answered 400 with the server's absolute path in the body --
a permanent-looking code for a transient storage condition, which an agent
responds to by dropping the batch. And the LLD's new prose was inserted inside
the flags table, so twelve rows rendered as literal pipe text; the same
paragraph said "six HTTP write entry points reaching four functions" where the
mux registers fourteen routes reaching eight handlers. No docs gate covers that
file, so nothing caught either.

## The fix for a one-side-only shape reintroduced it, three times

The previous entry's fixes were reviewed adversarially. Four of them were
right; five created new defects, and each new defect is the SAME shape as the
one it fixed.

**Readiness counted findings, not tenants.** `storagePressure` had a `switch`
whose `OverQuota` arm was unreachable, so a store over its quota AND below the
reserve reported only the disk. Splitting it into three independent `if`s fixed
that. Its only caller prints `len(pressure)` as "N tenant(s) under storage
pressure", so one tenant tripping all three budgets became **"3 tenant(s)"** --
the operator-facing number the change set out to improve, now wrong by 3x. One
line per tenant, causes joined, is the answer; both halves of the original bug
were invisible because nothing asserted either number.

**The syslog refusal was counted in three wrong places.** `syslogAdmits` called
`countRows(0, 1, n)` on the argument that a refused message is data that did
not land and has to show up somewhere. It showed up as `vl_bytes_ingested_total
+n` for bytes never ingested, `vl_rows_dropped_total +1` ("rejected as
malformed") for a well-formed message, and `vl_http_errors_total +1` for a UDP
datagram. Measured, one refused TCP frame: `+68` bytes ingested, `+1` malformed.
The HTTP path counts none of those for the identical event -- so the transport
parity the fix existed to create was broken by the fix, in the metrics instead
of the writes. `NoteRejectedWrite` was already on the line above and is the
counter for this.

**The status classification was backwards in both directions.** `isStorageErr`
mapped `EACCES`, `EPERM` and `EROFS` to 507 -- a data directory the process may
never write to, or a read-only mount, told to retry forever -- and left
`EMFILE`, `ENFILE`, `ENOMEM`, `EAGAIN`, `EBUSY`, `EINTR` and `ESTALE` at 400,
which is the set by which a per-tenant store open actually fails under load and
the code on which an agent drops the batch. The defect being fixed was "a
transient condition reported as permanent"; the fix reported permanent ones as
transient and left the realistic transient ones alone. The new test enshrined
it: `chmod 0555` is permanent, and it asserted 507.

**A copied build tag is a claim that has never been compiled.**
`quota_unix.go` moved from `!windows` to `internal/api/diskfree_unix.go`'s
platform list, which was itself wrong: netbsd has no `syscall.Statfs` and
openbsd spells the fields `F_bsize`/`F_blocks`/`F_bavail`. Two files then
carried the broken list. Corrected to `linux || darwin || freebsd ||
dragonfly`, both platforms build for the first time -- and `lock_unix.go`, the
file with the same `!windows` shape two doors down, was split behind a
`flock`-narrow tag, which is what solaris was failing on. Nothing gates any of
this: CI's cross job is `GOOS=linux` with five GOARCHes.

**A failing `statfs` was never cached.** `cachedUsage` returned at the error
without stamping, so an unmeasurable filesystem was re-measured on every write
rather than every two seconds -- the syscall storm the cache exists to prevent,
reached through the one condition that makes the syscall slow.

**Claims in the previous entry that do not hold.** "Twelve rows rendered as
literal pipe text" is **fourteen**. "Three statements of a claim" is **two** --
`QuotaState`'s comment never mentioned the tenant cap. "`-race` reported it
three ways against the shipped tests" does not reproduce at the parent commit:
`-race -count=2` and `-count=4` on `./internal/api/` report zero races, and the
new race test only trips at `-count=50`, so it is a window rather than a guard
at CI's `-count=1`. "1776 bytes stored" is 1053 on the same code. The
`MaxTenantBytes` overshoot was measured again at **705,385,424 bytes accepted
on the wire against a 64 KiB cap (10,763x)** in 10 s -- the 119-125 MB figure
was the on-disk size, not what the server accepted.

**The shape.** Every one of these is the same failure as the bug it replaced,
committed while writing the fix for it: a count nobody counts, a claim nobody
compiles, a parity fixed on one side, a classification asserted in the
direction that was already believed. The review that found them is the only
reason any of them is in this file rather than in production.

## Two reviews of the same three commits, and the shape they share

Reviewer A took 6.2/6.3 and reviewer B refused sign-off on the storage-budget
fixes. Between them: one reproduced memory-unsafety, one regression that made a
failure worse than the thing it replaced, one whole feature that nothing ever
configured, and four claims in shipped source that are false.

**`wg.Wait()` was a statement, not a defer.** Its own comment says *"not
tidiness: the snapshot is unmapped when ScanEach returns, and a producer still
reading a group's blob after that is a use-after-unmap"* — and it was skipped
on the one return path that does not reach it, a panic or `runtime.Goexit` out
of the sink, while `defer sn.Close()` still ran. Reproduced at 64 groups ×
4000 rows: **SIGSEGV 5/5 runs** in `lz4BlockDecodeAVX512`, and 2/3 via
`t.Fatalf` inside a sink. Deleting the line entirely left the whole suite
green, so the invariant the comment describes had **zero** coverage. One
keyword.

**Streaming made the byte budget worse than materializing.** `emitter.bytes`
accumulated across groups while the walk held one. A scan holding 8,156 B at a
time was refused by a 32,624 B ceiling; over HTTP, where the same counter is
`-search.maxQueryBytes`, a 6.6 MB answer got **200, 4.5 MB and `unexpected
EOF`** where the materialized path returned a clean 413 with nothing on the
wire. `engine.go` had predicted it in a comment — *"when a streaming sink lands
this has to become the live figure rather than the running one"* — and the sink
landed without it.

**Half of `Admission` was never configured.** `NewAdmission` was called with
`MaxPerTenant` and `Wait`, never `MaxConcurrent`, so the global channel was
always nil: `Stats` reported `len(nil chan)` = 0 in flight however many queries
were admitted, `-query-queue-wait` was read only on the branch that returned at
`global == nil` (a refusal in **7.1 µs** with the wait set to an hour), and
`ErrQueueTimeout` could not be produced by the server at all. The
documentation described a path with no flag behind it. This is the repo's own
named failure — "a limit that is configuration nothing reads" — shipped in the
commit whose subject was governing limits.

**Four false claims in source, not just in commit messages.**
*"The other order deadlocks"*: `acquireKey` never blocked, so the deadlock
described could not occur, and swapping the order left the suite green.
*"lock_unix.go handles exactly this errno"* about EAGAIN: `lockDir` wraps it
into `storage.ErrLocked` and **drops** the errno, so the one case the errno
list was written for was the one it did not catch — `ErrLocked` answered 400,
the code an agent drops the batch on. *"the literal boxed six syscall.Errno
values to the heap on every call"*: measured **0.00 allocs/op** both ways, with
no `runtime.newobject` site in either disassembly — the constants are below 256
so the conversions resolve through `runtime.staticuint64s`. *"the server's
absolute path in the body"* written in the past tense while the body still
carried it: only the status code had changed.

**Caching a statfs failure opened a two-second hole in the reject reserve.**
`s.usage.Store(nil)` discarded the last good sample, and `QuotaState` treats an
unmeasurable filesystem as "do not refuse writes" — so one failed statfs turned
the reserve off for the whole interval on a full disk. Measured **2.0 s against
0 s** before the caching. Keeping the last reading gets the syscall fix without
the hole.

**Smaller, all measured:** an aborted stream was counted as a successful
streamed select and as no error (`panic(http.ErrAbortHandler)` bypasses the
`sw.code >= 400` accounting); three of five new metrics were absent from a
default server while two documents listed them unconditionally; a cancelled
client was counted as a rejection; live tails were exempt from admission on a
clause justified by the argument for exempting *writes*, which is the opposite
of true for the longest-lived read there is; the `if workers > len(groups)`
clamp was dead at all four sites; and `MaxQueriesPerTenant`, `QueryQueueWait`
and `MaxScanWorkers` were absent from `config.fields()` — whose own doc says
*"so a new limit cannot be added to the struct and forgotten by both"* — so
`-5` passed `Normalize()` for all three.

**The shape.** Every one of these is a claim that was written and never
executed: a comment that describes a guarantee the code does not make, a
config field nothing reads, a count nobody counts, a platform tag nobody
compiles. The tests were green throughout. What found them was two independent
adversaries running mutations — and the single most useful signal in both
reports is the revert-probe list, because a guard you can delete with the suite
still green is a guard that was never doing anything.

## 38. A test fixture that agreed with the defect, and column order decided by a map seed

Task 8.5 split LogsQL execution between shards and coordinator. Two things
turned up that were not the planner, and both were found by measurement rather
than by reading.

**The differential test passed one case for the wrong reason.** The fixture
built 30 rows, assigned `level` as `i%3`, and sent row `i` to shard `i%3`. Both
moduli are 3, so each level lived entirely on one shard — and a router that
aggregated per shard and concatenated therefore produced *exactly* the right
answer for `| stats by (level) count()`: three rows, ten each. The subtest was
green with the planner disabled. Changing the level to `(i/3+i)%3` puts all
three levels on all three shards and keeps ten rows per level; the same subtest
then reports **9 rows where a single node reports 3**. With the planner
disabled, **10 of 15 pipe shapes fail; before the fixture fix, 9 did**.

A group-by key that is a function of the shard index cannot test a cross-shard
merge. The fixture has to disagree with the defect, and this one agreed with it.

**A group's column order was Go's map iteration order.** `Writer.addVec`
registered a new column the first time it saw the name, iterating the record's
`map[string]string`. Go randomises that per process, so:

- two storage nodes given identical records built groups whose columns were in
  different orders;
- the same row read back from two shards came out with its fields permuted, and
  a client reading NDJSON from a router saw the shape change by shard;
- a group's bytes were not reproducible for identical input.

Measured directly: eight identical queries in one process returned a byte-identical
first row, and three separate `go test` processes over the same input returned
`{"_time","n","_msg","level","user"}` once and `{"_time","_msg","level","user","n"}`
twice. Stable within a process, a coin flip across processes — which is why no
test had ever seen it.

Fixed by registering only the names that are NEW, sorted, so column order is a
function of the data. Sorting the whole list per record would cost more and give
a worse order (a field first seen in record 900 ahead of one from record 1), so
the sort is per record and the order between records stays first-seen.
`BenchmarkAddSteadyState`: **0 allocs/op, 287–308 ns/op over three runs** — the
scratch slice is reused, and a row that introduces no column sorts nothing.

**The shape.** Both are the same failure as entry 37's, one level down: a
property nobody executed. The fixture asserted an equality that held for a
reason unrelated to the code under test, and the column order was never compared
across two processes because a test only ever runs in one.

## 39. Counting the handlers that federate cannot find the ones nobody wrote

Task 8.6 asked for every router surface to be complete or explicitly refused.
The obvious way to audit that is to count `len(s.backends) > 0` branches: 13 of
42 routes have one. That number is useless. It lists the handlers somebody
remembered to federate, which is the complement of what the audit is looking
for.

The test that works sends the same request to a router and to a storage node
that HAS the data, and fails when the storage node answers with something and
the router answers with nothing. It found three routes reading the router's own
empty store:

| Route | Storage node | Router |
|---|---|---|
| `/select/logsql/facets` | 30 faceted values | `{"facets":[]}` |
| `/select/logsql/stats_query` | `{"result":[{"value":[…,"30"]}]}` | `{"count":0}` |
| `/select/sql` | the matching rows | empty body |

The `stats_query` one is the sharpest: `federatedStatsQuery` decoded a
`{"count":N}` field, and no backend has ever emitted it — the handler answers
the Prometheus instant-vector envelope. So the router answered **`{"count":0}`
for every query against every cluster**, whatever the shards held. A confident
zero, in the response shape a client is least likely to question.

**Two more surfaces were worse than empty.** `/select/logsql/tail` tailed the
router's empty store and streamed **forever** without yielding a row — the first
run of this test took 240 s and reported nothing at all, because an unbounded
client on a streaming endpoint hangs the package until `go test -timeout` kills
it. `/select/vector` returned no neighbours. Both now answer 501 with the
reason.

**Two fixtures were skipping, and a skip reads as covered.** The hits request
used the default time window, which does not cover the corpus, so the storage
node answered empty too and the comparison was between two empty answers. The
`stream_field_*` endpoints ran against a node with no stream fields configured,
same result. Neither subtest could have failed. After fixing both, every read
subtest bites.

**One claim in the test itself was never executed:** the read test skipped
writes with a comment naming `TestARouterStoresNothingItself`, which did not
exist. It does now, per route — all 13 — under the name
`TestARouterForwardsWritesAndStoresNothing` (name correction, 2026-08-15: this
entry gave the intended name, not the one that landed) — because each ingest
handler decides for
itself whether to forward, and "kept nothing" is observed by removing the
backends and asking the same process again rather than by reading its store.

**The shape.** Entry 37 called this "a claim that was written and never
executed". This is the audit-level version: a metric that counts the wrong
population. If the question is "what did we forget", no count of what we
remembered can answer it — only a comparison against something that knows the
right answer.

## 40. A peer's 4xx was success, and a quota that measured a store's size from before the writes

Task 8.7 built anti-entropy between replicas. Three defects turned up, none of
them in the anti-entropy design; all three were found by a test failing rather
than by reading the code.

**A peer's 4xx was merged as an answer.** `clusterClient.do` classified 401/403
(unauthorized), 429/503 (overloaded) and 5xx (unavailable), and let every other
4xx fall through as success — so the peer's error body became part of the merged
result. The repair pass made it visible: the adopt POST was arriving without its
`?digest=`, the destination refused it with 400, and the router reported the
group as **copied**. Two rounds of "copied 2, still 1 row" before the cause was
the classification rather than the copy.

The missing query string was its own bug: `do` appended `r.URL.RawQuery` only
for GET, which is fine for a read fan-out and wrong for anything that addresses
a resource in the query and sends it in the body.

**The tenant quota measured every write against a size from before them.**
`DiskBytes()` caches for ten seconds so the check stays cheap on the write path.
Cached alone, it made the check wrong there. Measured directly:

| MaxTenantBytes | write 1 | write 2 | write 3 | write 4 |
|---|---|---|---|---|
| 1 (before) | 200 | 200 | 200 | 200 |
| 1 (after) | 200 | **507** | **507** | **507** |

The first 200 is correct — the store really was empty when it was checked, and a
bound smaller than one group cannot be enforced before the group exists. The
next three were the cache. The fix updates the cached size at the append, which
knows exactly how many bytes it committed, so the check stays cheap and becomes
correct for growth; shrink is left to the interval refresh, because a size
briefly too high refuses writes for a moment and a size briefly too low accepts
writes that should have been refused.

This is entry 37's "two-second reserve hole" again, five times longer and on the
other threshold.

**The LIFO cleanup deadlock, a second time.** A stalled-peer test registered
`close(block)` before `httptest.Close`. Cleanups run last-registered-first, and
Close waits for in-flight handlers — so Close ran first and waited forever on a
handler nothing would release. The package ran to its 450-second `-timeout` and
reported nothing about the code. The unblock must be registered LAST so it runs
FIRST, and the comment above it now says why, since the correct order reads
backwards.

The same run showed the stalled-peer bound is real: with a client patient enough
to outlast it, the router answers in **10.01 s**, which is
`ResponseHeaderTimeout`. The first version of the test used an 8-second client
and concluded the router never answered — measuring its own impatience.

**Two gates caught what nothing else would have.** The route-contract test and
the surface-classification test both failed on each newly added route until it
was classified and given a contract. That is the pair working exactly as
written: three anti-entropy routes and a cluster-backup route could not be
merged while invisible to either.

**The shape.** The peer-4xx one is the sharpest: a classification that handles
the interesting cases and lets the rest fall through to the success path. Every
status nobody thought about became "fine", and the failure only surfaced when
something downstream depended on the operation having actually happened.

## 41. A position field filled with a constant, and a 400 whose rows arrived later

Task 9.1 added 22 fuzz targets. Four of them failed on their seed corpus before
any generated input ran, which is the useful kind of failure: the seeds are
hand-written normal cases.

**`Result.RejectedAt` was populated by half the ingest envelopes.** There is a
`Result.Reject(ordinal)` helper that records the position and sets
`RejectedTruncated` past the bound; logfmt, journald, Loki, Datadog and the OTLP
protobuf path all bypassed it with a bare `res.Rejected++`. So a result reported
`Rejected: 1` with no position and `RejectedTruncated: false`, and the field's
own documentation says a caller must read that as "there were no more
positions", not "the positions are unknown.

**Five `res.Warn(0, ...)` calls passed a constant where an offset was meant**, so
every warning from those envelopes blamed byte 0. Datadog's was in the same
`if` as its missing rejection position.

Fixing this needed the two fields kept apart, and the first attempt got it
wrong: `RejectedAt` is a **record ordinal**, `Warning.Offset` is a **byte
offset**. Journald's three warnings already passed real byte offsets and were
correct; converting them to ordinals — which is what "make the units consistent"
suggests — would have replaced right with wrong. The units differ because the
questions differ, and the fix is per site.

**A 400 whose rows arrived under someone else's request.** The journald parser
keeps entries that parsed before a truncation, deliberately, with tests that say
so. But `ingestBody` returned on a parse error **without flushing**, and the
rows sit in the tenant's shared writer buffer. Measured:

| Step | Response | Rows in store |
|---|---|---|
| journald upload, 2 entries then a cut field | 400 | 0 |
| one unrelated jsonline write | 200 | **3** |

The client was told 400 and its two records were committed moments later, by a
request that had nothing to do with them. The 400 also said nothing about them,
so re-sending the upload — the only thing a client can do with a bare 400 —
duplicates exactly the part that landed, and a log store cannot tell a duplicate
from a line that happened twice. Now the error path flushes under its own mark
and the body carries `accepted`, `rejected` and `rejectedAt`.

**CI asserted a platform that cannot compile.** `ci.yml`'s cross matrix listed
`386`. `GOARCH=386 go build ./...` fails at `github.com/sebishogun/simdjson`,
whose `marshal.go` writes `math.MaxUint32` as an untyped constant that overflows
a 32-bit int. That job has never been able to pass. 386 removed from both
workflows with the reason recorded; the upstream fix is task #424.

**Three of the fuzz failures were the harness, not the code**, and saying so
matters as much as the real findings: a `From > To` window is the empty range
the query asked for and the scan returns zero rows (checked, not assumed); a
cursor spliced into a URL unescaped made the *client* refuse a control
character; and creating an `httptest` server per fuzz iteration exhausted the
ephemeral port range across 32 workers. A fuzz target that fails for its own
reasons is worse than none, because the next person reads it as a defect.

**The shape.** Entry 40's peer-4xx was a classification that let unhandled cases
fall through to success. This is the same thing in a data field: a position
nobody filled in, defaulting to a value that means something specific and wrong.

## 42. A soak that reported 172,037 writes while every request was refused

Task 9.2's soak found four defects, all of them in the soak.

**The load generator counted refusals as successes.** `post` treated any status
below 500 as fine. Every request was coming back 400 — `AccountID` must be
numeric and the generator was sending `t0` — so the first run reported:

    writes=172037 reads=57887 churns=45741 backups=13 restarts=3 failures=0
    baseline  goroutines=40 mappings=155 rss=33MB files=2 disk=0MB
    final     goroutines=10 mappings=155 rss=35MB files=2 disk=0MB

Flat, clean, and entirely meaningless. `files=2` was the only thing on the page
that said so: 172,037 writes and two files on disk.

**The bounds could only pass if nothing happened.** They compared raw numbers
against a warm-up baseline, so the first run with real load failed every one of
them — mappings 1060 to 6585 against a bound of 2184 — while the store had gone
from 1028 group files to 7602. That is the system working: a store holding more
data maps more groups. An absolute bound on a quantity that grows with the data
passes exactly when the test is broken, which is what had just happened.

Rewritten as ratios: mappings per 100 groups, KB resident per MB stored,
manifest bytes per group. Goroutines stay absolute, because they are the one
measurement here that does not grow with the data.

**Two of the four rewritten bounds were then skipped in silence.** The manifest
was measured in kilobytes and every manifest was under one, so it read as 0 on
every sample; and the mapping bound was a difference (`mappings - files`) that
went negative, because not every file is mapped. Both returned 0, both were
skipped by the `from == 0` guard, and the run printed PASS. A skipped bound
reads as a passing bound, so the summary now names every bound it could not
measure. In bytes, against group files only, the manifest bound reads **36 bytes
per group at both ends of a 45-second run** — flat, which is what it should be.

**Unbounded tenant churn.** A fresh tenant id from a 2^20 space every iteration
created 92,589 tenants, 92,601 files and 92,798 mappings in twenty seconds. That
is not a leak test. Tenants have to RECUR for eviction and reopen to happen at
all, so churn now walks a ring of 64.

**The shape.** Every one of these is the same failure as the fixture in entry 38
and the vacuous subtests in entry 39: a test that cannot fail. What is new here
is that three of them were passing *loudly* — printing six-figure counters and a
clean resource table — which is more convincing than silence and exactly as
empty.

## 43. A merge that skipped the shard it could not read, and a graph drawn out of order

**The claim under test.** `fanOutChecked` refuses a cluster read when a shard
did not answer or answered from an incomplete store — the completeness rule
that entry 30 exists for. So a clustered read is either complete, or refused,
or explicitly 206 with the missing shards named.

**What the code did.** Eight merges unmarshalled each shard's body and, on
failure, `continue`d. A shard that answered **200 with a complete envelope and
an unreadable body** was therefore dropped, and the read returned the other
shards' rows with HTTP 200 and no marker anywhere. The completeness rule cannot
see that case: as far as the fan-out is concerned, the shard answered fine. The
rule was one layer above the only layer that knew.

The reason the two were ever confusable is that `bodiesOf` returned a
`[][]byte` indexed by shard with `nil` for any shard that did not answer — and
`nil` is exactly what `json.Unmarshal` fails on. "Absent, and the caller opted
into a partial answer" and "present but unreadable" arrived at the merge as the
same value. Absent shards are now absent from the slice (`shardAnswer` carries
the shard number so a refusal can name the node), and anything in the slice is
an answer that has to parse.

**The second defect, in the same function.** `/select/logsql/hits` merged its
buckets into a `map[string]int` keyed by the shard's RFC3339Nano text and
emitted them `sort.Strings`-ordered. RFC3339Nano drops trailing zeros in the
fractional second, so the format is not fixed-width, and `'.'` (0x2E) sorts
before `'Z'` (0x5A):

    "2026-01-01T00:00:00.5Z"  <  "2026-01-01T00:00:00Z"     // lexicographic
     2026-01-01T00:00:00.5     >   2026-01-01T00:00:00      // actual

`step` is a `time.Duration` off the query string, so `step=500ms` produces
exactly that mix — whole-second buckets with no fraction interleaved with half
seconds that have one. The two arrays are indexed together by the client, so
the points were plotted in the wrong order. Buckets are now keyed by
nanoseconds and formatted only on the way out.

Two more shapes in the same merge were being absorbed rather than reported: a
series whose `timestamps` and `values` arrays are different lengths (the old
code truncated to the shorter one, dropping a bucket's count), and a bucket
timestamp that will not parse (unorderable against the other shards'). Both are
now refused.

**Also here.** `federatedEndpoints`, the table driving the one-shard-down and
all-shards-down suite, was hand-kept and had drifted to nine of the fourteen
federated reads — while `docs/lld/cluster.md` said "all nine federated
endpoints are covered". Both statements were true of the *table* and neither
was true of the *router*. The set is now derived from `surfaceRoutes()`, which
the mux-count test already ties to the real mux, and the gate fails in both
directions.

**The shape.** Entry 42 was tests that cannot fail. This is the same thing one
level up: a *rule* that cannot fire, because the layer that enforces it cannot
observe the case it is meant to catch. A completeness check above the merge is
not a completeness check — it is a check on the transport.

## 44. 147 allocations to decode one row, and the differential that found the bug in the replacement

**Measured, `internal/api`, ten-field NDJSON row (Zen 5, load average 26 — the
timing is indicative, the allocation counts are exact and layout-independent):**

| implementation | ns/op | B/op | allocs/op |
|---|---|---|---|
| `encoding/json` Decoder | 4881–5277 | 4448 | 147 |
| byte scanner | 383–450 | 576 | 2 |

`jsonLineToRow` is the coordinator's per-row cost for any clustered query with
a coordinator half: a `json.Decoder` over a `bytes.Reader` per row, its
512-byte read buffer, and an `any` box plus a string per token. The replacement
scans the line directly and allocates twice — one byte buffer that every key
and value aliases via `unsafe.String`, and one `Field` slice sized from a count
taken over the line rather than guessed.

**What the differential caught.** The scanner was tested against the Decoder as
the specification, the way `internal/ref` is used in the sibling repositories,
and the first run failed on *every ordinary row*: the `_time` lift `continue`d
without recording that a pair had been consumed, so the separator check used
`len(row.Fields) > 0` — which is still 0 after a lifted `_time` — and the comma
before the second key was read as the start of a key. Every row whose first
field is `_time`, which is every row a storage node emits, fell back to
`rawRow`. A hand-written test over a few lines would very likely have used a
row starting with `_msg`.

The fuzz differential then found two more in 28M executions: invalid UTF-8 in a
string, which `encoding/json` coerces to U+FFFD and the scanner was copying
raw; and `{"":{0}}`, where the brace-balancing composite scanner accepted a
nested value that is not JSON.

**Three places the Decoder was looser, which the scanner deliberately is not.**
A truncated object returned the fields read so far (`{` alone was an empty
row); trailing bytes were ignored (`{"a":1} garbage` and `{"a":1}{"b":2}` both
decoded to the first object); and a nested value was flattened by the token
stream, so `{"a":{"b":1}}` became a field `a` with value `"{"` followed by a
field `b` — a row that exists nowhere. The contract is now two-sided and
pinned: a valid JSON object decodes to exactly what the Decoder produced, and
anything else is the whole line as `_msg`.

## 45. Eight wrong answers a second reviewer reproduced against a live cluster

Reviewer B ran a router over two storage nodes and a single node holding the
same rows, and asked both the same questions. Every finding below came back
HTTP 200 and looked like a smaller right answer.

**`min()` and `max()` were SUMMED.** `mergeableAggs` has always listed
`AggMin` and `AggMax`, and the comment above it says "Additive **or extremal**
only" -- but all three federated stats merges added unconditionally. Only the
additive half was ever implemented.

| query, n = 100..111 split 100-105 / 106-111 | single node | router |
|---|---|---|
| `* \| stats min(n) m` | 100 | **206** |
| `* \| stats max(n) m` | 111 | **216** |
| `* \| stats count() c` (control) | 12 | 12 |

206 is not a plausible wrong answer, it is an impossible one: no row has
n = 206. And `/select/logsql/query | stats min(n)` answered 100 correctly on
the same binary, which is the one-aggregate-two-answers inconsistency
`rejectNonMergeableStats` exists to eliminate. Fixed with `MergeOps`, which
reads the operator per output name from the query's stats pipe; a series a
shard names that the pipe does not is refused rather than assigned one.

**`stats_query&by=` answered `{"stats":[]}` for every query with a stats
pipe.** A storage node emits that shape *only* in its error fallback -- when
`StatsQueryInstant` fails, which it does exactly when there is no stats pipe --
and the router switched on `by=` alone. The Prometheus vector envelope
unmarshals cleanly into `struct{ Stats []vc }` with a nil slice, so
`mergeDecode` cannot see it. A dashboard panel grouped by a label drew nothing.
The router now switches on `HasStatsPipe`, which is what actually decides the
shape.

**A backup was well-formed and short with a replica down.** `completeReplica`
builds its union from *reachable* replicas only, so an unreachable replica's
groups can never make the chosen source look incomplete, and the only check was
`reachable == 0`. One shard, two replicas, one row written only to replica 2:

    both up        HTTP 200  groups:3 rows:3
    replica 2 down HTTP 200  groups:2 rows:2

The second is a valid tar that passes `ValidateClusterBackup`. `repairCluster`
marks a shard incomplete on exactly this condition; the backup path did not.
Now refused, and `ShardBackup.ReplicasConsulted` records what was asked.

**One malformed unrelated parameter changed the answer.** `withoutLimits`
returned the request unchanged when `url.ParseQuery` failed, so `&x=%zz`
forwarded the caller's `limit` to every shard and the cluster's top-N became a
merge of shard-local top-Ns. The value that was #1 cluster-wide (6 hits) and
#11 on each shard vanished from a 200 response.

**`limit` meant two different things on facets, and "without limits" meant "at
the default".** On a node it truncates values within a field; at the
coordinator it truncated *fields*, so `?limit=2` gave five fields of top-2
values on one node and two fields of everything on a cluster. And facets reads
`intParam(r, "limit", DefaultFacetLimit)`, so DELETING the parameter means 10,
not unlimited -- `withoutLimits` bought nothing there and the merge still summed
shard-local top-10s. Both fixed; the shards are now sent `limit=0` explicitly.

**The federated ES `_search` accepted what a single node rejects.** It decoded
into `want` and discarded the error (`_ = dec0.Decode(&want)`), so
`{"from":-1,"size":3}` -- a 400 on one node -- came back 200 with the WRONG
DOCUMENTS: `need = from+size = 2` made each shard return two hits, rows 2-5
were never fetched, and `"total":12` said they existed.

**The primary read path took a shard's garbage as data.** `mergeDecode` covers
the eight envelope merges; `/select/logsql/query` and `/select/sql` go through
`mergeRows`, which had nothing. A proxy's HTML error page in a 200 body came
back to the caller as a log line.

**A hits bucket outside 1677-09-21 .. 2262-04-11 wrapped.** `time.Parse`
accepts any year and `UnixNano` is undefined outside that range:
`2600-01-01T00:00:00Z` became `2015-06-13T00:25:26.290448384Z`, and its count
was filed on -- and summed into -- a real 2015 bucket. The refusal added one
commit earlier caught only a parse failure, which is not this.

**Two more, found by tracing rather than reproduction.** A repair counted a
group as copied when the destination answered `{"adopted":false}` (it already
had it), so a pass could report `copied: N, complete: true` having moved
nothing. And `askReplicaState` treated any body that *parses* as a state, so
`null` or `{}` from a same-version peer read as an empty replica -- which
`ReplicaState.Err`'s own doc says must never happen, because it makes repair
copy the whole shard into a node that already holds it.

**The shape.** Entry 43 was a rule enforced above the layer that can see the
case. These are the same thing generalised: `mergeableAggs` said extremal
aggregates were mergeable and no merge implemented extremal; `withoutLimits`
documented a guarantee its error path abandoned; `ReplicaState.Err` documented
a distinction its guard did not draw. In each one the correct behaviour was
WRITTEN DOWN, in the same file, and the code did something else -- which is
why reading did not find them and asking the cluster did.

## 46. Round three: two of the fixes were the defect, and a gate its own reviewer bypassed on the first try

Three reviewers went over the round-two commits. Two ran live two-shard
clusters against a single node holding the same rows; one had reading only and
said so. Between them they found that **two of the fixes had introduced the
thing they were fixing**, and that one new guard was bypassable in one line.

**The facets fix made every cluster answer carry fields a node drops.** The
shards are sent `keep_const_fields=1` so a field constant on ONE shard survives
to be judged over the union — sound, and the coordinator then applied only the
cardinality half of `facetKeep` and never `distinct > 1 || keepConst`.
Measured, two nodes of six rows against one node of twelve, `?query=*`:

    cluster  _msg _stream _stream_id _time konst level n svc   8 fields
    single   _msg                    _time       level n svc   4 fields

`_stream_id` is a 48-hex-character value. Every field constant over the queried
window — `_stream` and `_stream_id` whenever the filter selects one stream,
`host` on a single-host cluster, the field just filtered on — appeared in a
cluster facet list and not a node's, and facets drives the UI.

**The three-replica repair fix made repair stop converging.** `overlapping`
uses a CLOSED interval deliberately: a group whose `TimeMax` equals the next
one's `TimeMin` shares a timestamp, and a duplicate row at the boundary is
still a duplicate. That is right for a compacted group against its pieces and
wrong for two ordinary pieces of ONE store, which by this file's own invariant
cannot hold the same rows. Growing `spans` mid-pass made it reachable, and the
block is recomputed identically every pass, so it never clears:

    source: 72e6653f[t0,t0]  0f0565fe[t1,t2]  b621a270[t2,t3]
    before  copied 3, blocked 0, complete true   -> destination 6 of 6 rows
    after   copied 2, blocked 1, complete false  -> destination 3 of 6 rows
            pass 2: copied 0, blocked 1          PERMANENTLY STUCK

Rows sharing one nanosecond split across a size-triggered flush produce exactly
that, on any client timestamping at second or millisecond granularity — syslog,
most of them. Not silent (`complete: false`), but a hard stall on an ordinary
shape, and the operator is told to investigate a compaction that never
happened. Fixed by provenance: two groups held by one replica are known
non-duplicates, so the guard skips them, and the union is walked in TIME order
so the narrowest spelling wins rather than whichever hash sorted first.

**The `_stream_id` fix turned a duplicate-key bug into a differing-value bug.**
The skip was put on the field loop, and a shard runs the bare `*` with
`withStream` true — so the ingested value was dropped AT THE SHARD and never
crossed the wire, while a node's projection (`withStream` false, nothing
synthesized) kept it. `| filter _stream_id:="CLIENT-SUPPLIED"` matched on a
node and could not match on a cluster. The skip belongs on the SYNTHESIS.

**The not-a-state guard was bypassed in one line.**
`bytes.Contains(body, []byte("\"groups\""))` matches the quoted token anywhere,
including inside a value: `{"note":"groups"}` passed it and read as an empty
replica — the exact state `ReplicaState.Err`'s doc says must never be inferred.
Its reviewer broke it on the first attempt. It unmarshals into
`map[string]json.RawMessage` and tests for the KEY now.

**And one that was fixed correctly but incompletely.** `release.yml` has FOUR
checkout steps; three were given the input ref and the one that COMPILES was
not, so a dry run of `v1.2.3` from `main` still built `main` and stamped it
`v1.2.3`. Counting is not reviewing.

**Measured, on the byte budget the same round made real.** Two routers over
the same two shards, 200,000 rows, four coordinator pipes, interleaved eight
times, minimum of each: 0.4009 s with the budget on, 0.3647 s off — 36.2 ms,
9.9% of the request, for a bound that genuinely did nothing before (the same
query at 1 MB answered 200 with 200,000 lines and now answers 413). The
measurement was two passes per pipe where one suffices, because the post-pipe
size of pipe k is the pre-pipe size of pipe k+1; carried forward now. And
`measure` was `MaxBytes > 0` where `exceeded` also checks `maxMemory` against
the same argument — the engine already has `countsBytes()` for that decision,
and using anything else is the same limit-nothing-reads shape one layer down.

**The shape.** Entry 45 said the correct behaviour was written down in the same
file and the code did something else. This is the next turn of that: the FIX
was written down correctly in its own commit message, and the code did
something else — three times, in one sitting, by the person who had just
written the rule. Reading did not find any of it. Two live clusters and an
adversarial reader did.

## 47. Round four: a fix that was only in the commit message, and an exemption broken by a retried ingest

**A claim with no code.** Entry 46 says the not-a-state guard "unmarshals to
`map[string]json.RawMessage` and tests the KEY now". It did not. The edit was
one half of a script whose first assertion failed; only the second half was
re-run, and the commit message described the whole thing. Three reviewers
found it independently — two by reading the file, one by running
`{"note":"groups"}` against a live router and getting `copied=2,
complete=true` on both binaries.

There is no interesting lesson about the guard. The lesson is that a commit
message is not evidence, and this one had been read twice by me before it
shipped.

**The exemption was broken by an ordinary retried ingest.** Entry 46's repair
fix rests on "two groups held by one replica came from one store and cannot
contain the same rows". False. Ingesting one time range twice without a write
id leaves a store holding, at once:

    fccec81c [T0,T0]  rows=1
    121191e6 [T0,T9]  rows=10      <- the re-ingest
    9e36b98a [T1,T9]  rows=9

With that replica in the union, `holdersShare` is true for every pair, the
guard's candidate list empties, and a CLEAN replica is copied onto:

    HEAD    R1=20 R2=10 -> copied 1, blocked 0, complete TRUE  -> R2=20,
                           10 distinct rows, all ten duplicated
    BEFORE  R1=20 R2=10 -> copied 0, blocked 1, complete false -> R2=10

Worse than what it replaced in the exact dimension that matters: the stall was
loud and left data intact; this was silent and destroyed it. Fixed by checking
the premise instead of assuming it — a replica may certify a pair only if its
own inventory is internally non-overlapping, verified once per pass, and one
that fails is reported and still used as a source and destination.

**And the same asymmetry, twice, four lines apart.** `_stream` tested the
VALUE and `_stream_id` tested PRESENCE, so a row carrying an empty `_stream`
got the key twice, and — because the store materializes a column for the whole
group — one client-supplied `_stream_id` anywhere in a flush blanked the field
for every other row in it. Entry 46 fixed one direction of this and shipped
the other. Both fields are emitted exactly once now, from the row's value when
it has one.

**Measured, and a cost that had to be put back.** `looksLikeJSONObject`'s
brace balancing is correct and costs 19× on 90-byte lines, 232× on 980-byte
ones, +186.6% instructions and +24.4% wall on a 60,000-row bare select — the
path whose whole point is not parsing. Truncation can only be at the END of a
response, so the balance check now runs on the last line of a shard body and
the O(1) shape check on every line, which is what catches an HTML error page.

`ApplyPipes` measured 9.9% → 6.2% after carrying the size forward: four pipes
cost five passes rather than eight, and 5/8 = 62.5% against 22.4/36.2 = 61.9%
measured. The remaining pass is the one that cannot be removed.

`max_values_per_field` was still DELETED rather than overridden, so each shard
fell back to its default of 1000 — a field with 1200 distinct values per shard
disappeared from a cluster answer that a single node answered with 2400, at
HTTP 200. That is the identical defect entry 45 documents for `limit`, left on
the sibling parameter by the commit that wrote the rule.

**The shape.** Entry 45: the correct behaviour was written down and the code
did something else. Entry 46: the FIX was written down and the code did
something else. This one: the fix was written down, the code did nothing at
all, and the fix that WAS written rested on an invariant a retried ingest
breaks. Four rounds, and every round's defects were found by running a cluster
or by an adversary reading with intent — never by the person who wrote them
reading them again.

## 48. Three ways to decide one thing, each broken by the reviewer who broke the last

Repair must not copy a group whose rows a destination already holds under
different bytes. The router decides that from `[TimeMin,TimeMax]` alone, and
two shapes are indistinguishable that way:

    two adjacent flushes        [T0,T1] [T1,T2]   different rows
    a re-ingest of one instant  [T1,T1] [T1,T1]   the same rows twice

Three variants shipped in one session. Each was broken by the reviewer who had
broken the previous one, and each break was a live reproduction:

| | duplication | convergence |
|---|---|---|
| no self-check (entry 46) | **silent**, a clean replica's every row doubled at `complete: true` | fine |
| closed self-check (entry 47) | none | **permanent stall** on ordinary adjacency |
| half-open self-check | **silent**, same as the first | fine |

The half-open attempt is the instructive one. It looked obviously right — a
shared boundary instant is what a flush produces, a real overlap is what
duplication produces — and it is wrong because a re-ingest of a SINGLE instant
produces two groups with identical `[T,T]` spans, which share exactly one
endpoint and are structurally identical to a legitimate adjacency. I checked
that against the reviewer's three-group reproduction, found it still caught by
a different pair, and shipped it. The two-group case defeats it and I had not
constructed one.

**Sufficiency, settled.** If two groups share a row at time *t* then *t* lies
in both spans, so the CLOSED test always fires: it is sufficient, and it is not
necessary. Every failure above is the unnecessary direction.

What ships is the closed test: no duplication, and a permanent stall that is
reported (`SelfOverlapping`, `complete: false`, an error naming the pair and
saying what to check). Repair's stated promise is that it never makes a replica
worse, so a loud stall beats silent duplication — but it is the least bad of
three, not an answer. The answer needs evidence the router does not have and
cannot be given by a peer's report: whether the rows AT the overlapping instant
are the same rows. `AdoptGroup` at the destination parses the group and holds
the store's index. Task 428.

**A fourth break, in the same pass.** `union[g.Digest] = g` was
last-writer-wins, so the guard compared a span taken on one peer's word. A peer
reporting a real digest with a fabricated far-future span made the check miss,
and a clean replica had every row duplicated — `selfOverlapping: 0`, complete,
because the fabricated spans were disjoint. The union now keeps the WIDEST span
any holder reports, which is fail-safe: a lie can only cause more blocking, and
more blocking is a reported stall.

**Two more measured this round.**

`looksLikeJSONObject` was restricted to a shard body's last line on the
reasoning that a response is truncated at its end. Both reviewers rejected it
independently, with the same constructed input: it is a WELL-FORMEDNESS check,
not a truncation check, and a middle line ending in a nested value's `}` passes
the cheap check and reaches the client as a row. One of them also showed the
restriction was defeated by a single trailing newline, since `last` was
computed on byte position. Truncation is caught a layer above anyway — a cut
response fails `io.ReadAll` and is classified `PeerUnavailable` — so the
restriction dropped the only case that could reach the check. Full check on
every line again, at a measured +188.5% instructions on a 60,000-row bare
select.

Two attempts to buy that back:

| | narrow (~90 B) | wide (~980 B) |
|---|---|---|
| byte at a time | 51,744 ns | 425,872 ns |
| `bytes.IndexAny` to jump between structural bytes | 171,182 ns | 478,139 ns |
| shipped: byte at a time, string skip inlined | 53,184 ns | 434,764 ns |

`IndexAny` is **3.9× slower** on the narrow shape — it decodes a rune and scans
the cutset per byte, which is worse than a five-way switch. Measured, reverted,
recorded. Inlining `skipStringRaw` into the walk (Go does not inline a function
containing a loop) closed the remaining 1.26× to within the 8.3% noise floor.

`max_values_per_field=0` meant "unlimited" to `facetKeep` and "1000" to
`timeFacet` — so the coordinator sending shards `0` to get their whole
distribution bounded each shard's `_time` scan to 1000 ROWS, not distinct
values, and `_time` vanished from every cluster facet answer on any tenant with
more than 1000 matching rows per shard. Fixing the parameter for one field
exposed the same trap one layer down, in the function that reads the same
value with the opposite convention.

## 49. The gate that stops the counts going stale ran nowhere but a developer's clone

`TestTheChangelogCommitCountsAreReal` (entry: added with the pinned-sha
changelog counts) resolves the sha `CHANGELOG.md` names and recomputes every
count from history. It has a skip branch for a source tarball, which genuinely
has no `.git`.

Every `actions/checkout@v4` in this repository — ten of them, across `ci.yml`,
`cross.yml`, `fuzz.yml` and `release.yml` — used the default `fetch-depth: 1`.
A depth-1 clone cannot resolve a sha that is not its tip, so the gate took the
tarball branch in **every CI run since it was written**. Proved directly:

```
$ git clone --depth 1 file:///…/simdlogs shallow
$ go test -run TestTheChangelogCommitCountsAreReal ./internal/tests/docs/
    changelog_test.go:42: 7efa127 is not in this repository (shallow clone or tarball)
--- SKIP
```

Two changes, and the second matters more.

`fetch-depth: 0` on the seven checkouts whose job runs the suite. That makes
the gate able to run.

And a shallow checkout now FAILS rather than skips. A tarball has no `.git` and
legitimately cannot run this; a shallow clone is a CI configuration that could
have had the history and was not asked for it, which is a misconfiguration
wearing a skip's clothing. The two are now distinguished by
`git rev-parse --is-shallow-repository`, and the failure says which flag to
set.

**The shape.** Every previous entry in this record is a test or a check that
could not fail. This one could — it simply never ran, and announced that fact
in a line nobody reads, in the one status (`SKIP`) that looks like success in
every summary view. A gate that skips is worth less than no gate, because it
occupies the place where somebody would otherwise notice one is missing.

## 50. Three ways to make the line check faster, all slower than the byte loop

`looksLikeJSONObject` runs on every line of every shard body on the
bare-select path — the one structural walk a clustered read cannot skip, and
the path whose whole design is to avoid parsing. It was measured at +188.5%
instructions against the O(1) shape check it replaced, so the question is
whether that can be bought back.

**The disassembly first.** `go tool objdump -s 'api\.scanComposite'`: the hot
loop is `MOVZX 0(AX)(DI*1), DX` with the length compared once at the top — no
bounds check inside, no spills, the switch compiled to a compare tree, five to
seven instructions per byte. There is nothing to reclaim in the scalar shape;
it is already at ~2.1 GB/s, which for a per-byte compare tree is close to what
the loads cost.

So the question becomes bulk, which is what this family is for. Three
candidates, measured interleaved in one session, minimum of four runs of 500,
1000 lines per operation:

| | narrow (~90 B) | wide (~980 B) | B/op |
|---|---|---|---|
| **byte loop, string skip inlined (shipped)** | **56,891 ns** | **469,180 ns** | **0** |
| `bytes.IndexAny` to jump between structural bytes | 171,182 ns | 478,139 ns | 0 |
| `simd.IndexAllAny` structural index, then walk positions | 77,200 ns | 572,165 ns | 0 |
| `simdjson.Valid` (whole-line validity) | 176,315 ns | 451,244 ns | 13 |

Every one loses on the narrow shape, which is the common one: 3.0×, 1.36× and
3.1× respectively. On the wide shape `simdjson.Valid` is 4% faster — inside the
8.3% floor, so not a difference — and it allocates 13 bytes per line, on a
per-line path, which the tenets rule out on its own.

**Why the SIMD kernels lose here.** They are not being used wrongly; the work
is wrong for them. A structural index has to WRITE every structural position to
memory, and the walk that follows still has to read them back; the byte loop
writes nothing and branches on a value already in a register. The lines are
also short — a per-call kernel entry has a floor (this family measured ~1.4 ns
for a non-inlinable call), and at 90 bytes that floor is a meaningful share of
the whole check. `simdjson.Valid` does strictly more than is needed: full
validity where balance-and-terminate is the question.

**What would change the answer.** Amortising the kernel over a whole shard
BODY rather than a line — one structural index for 60,000 rows, then a walk
that never re-reads the bytes — is the shape that would win, because it pays
the entry once and the write once. That is a different function with a
different interface, and it is not written.

Recorded rather than left as folklore: "we should use the SIMD kernels here" is
the obvious suggestion for this code, and on this shape, at this granularity,
it is measurably wrong three ways.

## 51. Five store-aware pipes answering cluster queries from the wrong set

`ApplyPipes` refuses `join`, `union` and `stream_context` at a coordinator,
with the reasoning that skipping a store-aware pipe "would answer the query as
if the pipe were not there, which is the silent-wrong-answer shape this whole
area is about". Five more pipes are store-aware and were not in that list.

On a storage node each is answered by a LEADING fast path over the store's own
footers and index — `runBlocksCount` and its siblings. `apply` is the fallback
for the non-leading case, and at a coordinator it is what runs, over the merged
WIRE rows. Measured against a single node holding the same twelve rows:

| query | node | router |
|---|---|---|
| `* \| blocks_count` | `{"blocks_count":"2"}` | `{"blocks_count":"12"}` |
| `level:error \| blocks_count` | `{"blocks_count":"1"}` | `{"blocks_count":"3"}` |
| `* \| block_stats` | two block-stat rows | all twelve log rows |
| `* \| field_names` | 7 names | 8, including `_stream_id` |

`BlocksCountPipe.apply` is `len(rows)` — a ROW count wearing a block count's
name, and a number a reader has no way to question. `BlockStatsPipe.apply` is
`return rows`: a silent no-op that answers the query as if the pipe were not
there, which is the sentence four lines above the refusal it was missing from.
`field_names` counts `_stream_id`, which the SHARD synthesized on the wire and
no store holds.

All five refused now, naming the endpoint that does federate — the
`/select/logsql/` forms of `field_names`, `field_values` and `facets` fan out
and merge correctly, and have done throughout.

**Why five rounds of review did not find them.** Every round concentrated on
the functions the previous round's findings named. These sit one call away from
all of it, behind a type switch that already had the right shape and the wrong
membership, and the first reviewer to enumerate cluster reads BY PIPE rather
than by endpoint found all five in one pass. Attention follows the last defect,
which is exactly where the next one is not.

## 52. Every JSON boolean was stored as "false", and the switch that did it was dead code

`internal/ingest/jsonline.go`, the `simdjson.Bool` case:

```go
case simdjson.Bool:
    if val.Int() != 0 { fields[key] = "true" } else { fields[key] = "false" }
```

`simdjson`'s `Value.Int()` returns 0 for every kind that is not a `Number`
(`value.go:220-222`), so on a `Bool` the condition was always false and the
branch was unreachable. **Every JSON boolean ever ingested — `true` and
`false` alike — was stored as the string `"false"`**, at HTTP 200 with
`{"ingested":1,"skipped":0}`. `v:=true` matched no row in the store and
`v:=false` matched all of them.

`Value.Bool()` is declared four lines below `Value.Int()` in the same
dependency file and was called nowhere in this repository.

**The same line lost integer digits.** Every number went through `Float()`, so
`9007199254740993` — one past the last integer a float64 holds exactly — came
back `9007199254740992`. A snowflake id, a trace id and an epoch-nanosecond
timestamp are all in that range, and the row is off by one with nothing to say
so. An integer literal now keeps the wire's own digits.

**And the refusal I shipped one commit earlier answered 429.** The five
store-aware pipes were refused with `ErrRejected`, which `HTTPStatus` maps to
`429 Too Many Requests` — the code for "try again later" on a query no amount
of waiting will make answerable. The `join`/`union`/`stream_context` refusals
had the same status for the same reason and had had it all along.
`ErrNotDistributable` answers 400.

**What found all three.** A reviewer told to enumerate cluster reads BY PIPE
rather than by endpoint, and to sweep the ingest protocols, which five previous
rounds had not touched at all. The boolean bug is not subtle and is not new; it
is in the first `switch` of the most-used ingest path. Nothing had looked
there, because every round looked where the last round's findings were.

## 53. The facets fix removed a bound instead of narrowing it, and could OOM a shard

Entry 48 changed `max_values_per_field` from deleted to `0` on the shard
request, and made `timeFacet` read `<= 0` as unlimited. Both halves were wrong
in the same way: `limit` is a RESULT SHAPE and unlimited is cheap;
`max_values_per_field` is a CARDINALITY BOUND, and `_time` has roughly one
distinct value per row in a log store.

Measured on one shard, the default cluster dashboard path, no special
parameter:

| rows in window | shard body | wall | allocated |
|---|---|---|---|
| 40,000 | 3,389,259 B | 27.9 ms | +30 MiB |
| 160,000 | 13,649,261 B | 114.7 ms | +127 MiB |
| 640,000 | 54,929,263 B | 482 ms | +496 MiB |

85.8 bytes of response per row, dead linear. `peerMaxBodyBytes` is 256 MiB and
an over-cap body is discarded as `PeerMalformed`, so **above ~3.1M rows in the
queried window every cluster facets request fails** — after every shard has
allocated ~2.4 GiB building a body the router throws away. The same request
measured 3,389,259 B through the cluster path against 113 B direct: 29,993×
body amplification, for an answer that kept one field.

The shards are sent the CALLER's value now. That fixes the defect entry 48 was
written for — a caller asking 5000 was capped at each shard's 1000 and got
`{"facets":[]}` — without removing the bound: ask 5000 and every shard uses
5000, ask nothing and every shard uses its default, and the coordinator applies
the same number again over the union. An explicit `0` is bounded at 100,000
rows in `timeFacet` rather than being infinite, and the field is dropped past
it, which is what it always did past 1000.

**And the test entry 48 shipped for this could not fail.** It stubbed the
shard's facets body with `facetShard`, so `timeFacet` — the function it names —
never executed. A reviewer reverted the fix completely and the test stayed
green. Replaced with one that runs against a real storage node; it goes red
with the fix reverted, and it caught a second defect while being written:
`timeFacet` inherited `q.LastN` from the endpoint's `limit`, so one response
carried a `_time` distribution summing to 25 beside an `svc` distribution
summing to 30.

**The shape, again.** Entry 48's own last line says the closed test "is the
least bad of three, not an answer". This entry is the same lesson one file
over: a bound was in the way of a fix, and removing it was easier than
narrowing it. Both the removal and its test shipped in one commit, and neither
the removal nor the test had been run against a real shard.

## 54. The mechanism that closes the idempotency window has never been called

`Store.AppendGroupIdempotent` commits the write id in the SAME manifest record
as the group, and its own doc comment says exactly why:

> The id is committed in the SAME manifest record as the group, because one
> record is one transaction. Written separately there would be a window in which
> the rows are visible and the receipt is not -- and a retry landing in that
> window duplicates every row, which is the exact failure this exists to
> prevent, made rarer and therefore harder to find.

It has no production caller. It never has. The path production takes is
`Writer.FlushWithReceipt`, which is:

```go
if err := w.Flush(); err != nil { return err }
return w.store.CommitReceipt(id)
```

Two separate operations — precisely the shape the doc warns about, written four
files away from the warning.

**Measured, not argued.** Ingest ten rows under a write id, flush, and stop
before the receipt, which is what a crash there leaves behind. Reopen the store:

    10 rows durable, receipt remembered = false

The rows are on disk and the id is not, so a client retry of the same id is
treated as a first attempt and duplicates all ten. That is the failure
`AppendGroupIdempotent` exists to prevent, live on the only path that runs.

**Why it is not a small fix.** `AppendGroupIdempotent` cannot simply replace
`FlushWithReceipt`, because the writer batches: `Flush`'s own doc says "the
buffer is shared by every request and every syslog connection on the tenant, so
a row added here is routinely carried away by another goroutine's Flush". One
write id therefore does not correspond to one group, and the
one-record-one-transaction property is unavailable through that path by
construction.

Nor do the obvious variants work:

- Commit the receipt BEFORE the flush: a crash between them then loses the rows
  and reports success. Strictly worse — a duplicate is recoverable, a silent
  loss is not.
- Stamp the id onto every group the flush writes: a PARTIAL flush then leaves
  some groups carrying the receipt, so on restart the id is remembered, the
  client's retry is refused as a duplicate, and the rows in the groups that were
  never written are lost. Also worse, for the same reason.
- Align the flush boundary with the request when a write id is present: correct,
  and it gives up the cross-request batching that the ingest throughput numbers
  are built on. That is a real design decision with a measurable cost, not an
  edit.

So the entry is the deliverable. The window is narrow — a crash inside one
process's microsecond gap per request — and it is real, and it is now measured
by a test that FAILS if it ever closes, rather than asserting the current
behaviour forever. Task #433 holds the design decision.

**Closed for the common case, at no cost. See entry 68.** The fourth variant
above — "align the flush boundary with the request" — was priced as giving up
cross-request batching, and it does not have to: the id can ride the group the
flush ALREADY enqueues, without changing when the flush happens or what it
contains.

**The shape, again.** Round six's closing note was "the mechanism was built and
never wired to a reader", and named four. The gate written for it found sixteen
more. This is the most expensive one so far: not an unused helper, but the
correct implementation of a guarantee, sitting beside the incorrect one that
production calls, with a doc comment on the correct one describing the defect in
the other.

## 55. A one-node cluster reported `version_mismatch`, and POST had stopped being bigger than a URL

Two defects on one request, found by asking what a 1.2 MB query does.

`withFormInURL` folds a POST form into the peer's URL, which is right for the
ordinary case and, past a point, takes away the reason POST exists. A request
line and its headers are bounded together by net/http's `MaxHeaderBytes`, 1 MiB
by default, and a peer over it answers **431 from the server** — before the
handler, so with no protocol-version header and no error class.

The client checks the version FIRST, deliberately: a peer on an unknown version
may have produced a body that parses and means something else. That reasoning is
about a body a *handler* produced. Against a 431 it read the silence as a version
statement. Measured, one router, one shard, one build, one binary:

```
POST /select/logsql/hits   query=_stream:{app="x"} AND level:in(v,v,…)   1,200,031 bytes

503 simdlogs: 1 of 1 shards could not answer completely (0(version_mismatch)).
```

There is no version mismatch in a one-node cluster. The remedy that message
asks for — check node versions, upgrade the odd one out — has nothing to act on.
The same query against a **non-router** node is answered, so clustered mode had
quietly lost a capability single-node has, and said something false about why.

At 120 KB it answers 200. The transition is at net/http's limit, which is why
this was never seen: every query anyone had tried fit in a URL.

**Both halves are load-bearing, and each was probed alone.** Reverting the
overflow path alone: 503 again, now `0(unavailable)` — correctly classified and
still refused. Reverting the classification alone: `version_mismatch` returns.
Neither fix covers for the other.

**The precedence test guards less than it looked like it guarded.** The overflow
travels as a form body, and Go's `ParseForm` copies `PostForm` *first* — so on
the peer a body value beats a query value, the opposite of the ordering the
small path relies on (the `stats count()` defect, which answered 10 by GET and 2
by POST -- recorded in the commit that introduced withFormInURL, not as a
numbered entry here; an earlier draft of this paragraph cited "entry 108", and
this file's entries run 1-55). What actually holds is that the two sets are **disjoint**: a key already
in the query is never put in the body. Sending the body unconditionally at every
size leaves `TestTheRouterPlanStillWinsOverALargeForm` **green**; only removing
the skip turns it red. The test is a guard on the disjointness and on nothing
else, and its comment says so now rather than claiming the threshold.

The threshold is 60 KiB rather than something near the 1 MiB ceiling, so the
ordinary GET path is untouched — no dashboard emits 60 KB of parameters — and
only the case that cannot work in a URL at all changes shape.


## 56. The fix reached eleven endpoints and missed the twelfth — the one it was written for

Entry 55 moved a large POST form into a request body so a query bigger than a
URL could reach the shards. It moved the FORM's contribution and left the query
string alone. `federatedSelect` writes the planned query into the shard **URL**
before that runs (`shardQueryURL` -> `vals.Set("query", ...)`), so on
`/select/logsql/query` the key was already a URL key, was skipped, and could not
move; with nothing else in the form there was no body at all.

Measured, one router and one shard, `level:in(v,v,…)`:

| raw query | encoded shard URL | result |
|---|---|---|
| 520,001 | 1,039,993 | 200 |
| 560,001 | — | **503 `0(unavailable)`**, the shard never reached |
| 1,200,001 | — | **503 `0(unavailable)`** |
| 1,200,001, **single node** | n/a | **200** |

The sweep found eleven delivering the whole query as a form body and one
failing. It was described as "all twelve fan-out endpoints"; `surfaceRoutes()`
classifies **fourteen** federated reads, and the sweep -- like the test written
from it -- covered nine of them. The committed tests used
`/select/logsql/hits` and `/select/logsql/facets`. The repository already
recorded why that endpoint is different — `cluster_postform_test.go:19-20`:
"that one survives because planQuery rebuilds the shard URL from the parsed form
itself." **The property that saved it from the previous defect is the property
that broke it under this one.**

The fix resolves the merged set first and sends all of it one way, with
`RawQuery` cleared. That also removes the precedence argument the first version
leaned on: `ParseForm` copies `PostForm` before the URL query, so a set split
across both has the body winning, and "the two are disjoint by construction" is
a property one refactor away from being false.

## 57. A limit the router strips over GET reached the shards over POST

`withoutLimits` deletes `limit` and `max_values_per_field` from the shard
request so each shard answers unbounded and the coordinator bounds the merged
set once. It deleted them from `r.URL.RawQuery`. On a POST they are in the
**body**, so the deletion removed nothing.

Measured, three shards, 30 rows, `query=*&field=user&limit=2`:

```
GET  200 {"values":[{"value":"u0","hits":5},{"value":"u1","hits":5}]}
POST 200 {"values":[{"value":"u0","hits":4},{"value":"u1","hits":4},
                    {"value":"u2","hits":2},{"value":"u3","hits":2}]}
```

`u0` has five hits cluster-wide and the POST answer says four: each shard
truncated to its own top 2 and the coordinator summed the truncated lists. Six
endpoints affected, on both the small and the large path. Pre-existing, and
found because the new test was *named* for this property and had picked the one
endpoint (`facets`) where the plan **sets** `limit=0` rather than deleting it.

**Two attempts at the fix, and a test that could not see the first one fail.**
Deleting from `out.Form`/`out.PostForm` after the clone does nothing: on a POST
that nothing has parsed yet, both are nil, and `withFormInURL` later parses the
body fresh and puts the caller's limit back. `ParseForm` has to run first.

The test that missed it compared the GET and POST **answers**. With ties in the
fixture, two differently-truncated shard-local lists sum to the same visible
number, so it stayed green with the fix reverted. Asserting on what the **shard
receives** failed immediately, and fails on 8 of 9 cases for either half of the
fix.

## 58. Three more gates that could not fail, in the commit that was about gates

- **`TestTheClusterBackupCarriesItsOwnSpread` populated the field it checked.**
  It did `man.SpreadNanos = man.Spread()` itself and then marshalled, so
  commenting out the production assignment left the whole `internal/api` package
  green — while the test's own doc said it "has to fail if the field stops being
  marshalled". Both probes that were run (broken arithmetic, dropped json tag)
  tested the test's own arithmetic. It is driven through
  `/admin/cluster/backup` now and reads `spreadNanos` out of the tar; the
  reviewer's mutation gives `spreadNanos=0` against a 172800000000000 span.

- **`buildExcluded` dropped three files that ARE compiled on linux.**
  `strings.Contains(line, plat) && !strings.Contains(line, "linux")` excludes
  every `//go:build !windows` file. Three in `internal/storage`, and two
  documented declarations became invisible to the gate. 254 -> 256 documented
  declarations, which reproduces exactly.

  **The other numbers in the first version of this entry do not.** "18
  production names lost their readers, `dirLock` 15 down to 1, `errLockHeld` 3
  to 0" was recomputed by running `countReads` over the tree under each rule:
  **59** names gain readers, **15** cross the gate's threshold, `dirLock` goes
  **1 -> 8** (the direction inverted) and `errLockHeld` **0 -> 2**. And the new
  rule also EXCLUDES three files the old one wrongly included --
  `diskfree_other.go`, `quota_other.go`, `mmap_other.go` -- costing fourteen
  names their readers, `errNoStatfs` 2 -> 0. Harmless today and the opposite of
  the one-directional story the entry told.

  **The replacement was wrong too, in the other direction.** A small recursive
  evaluator treated an unmodelled tag as SATISFIED, which is safe until the tag
  appears under a NEGATION: `!purego` evaluated false and excluded a file the
  compiler includes -- the exact failure its own comment said it avoided. Its
  test asserted `{"!purego", false}` and `{"(linux || darwin) && !cgo", false}`,
  both contradicted by `go/build/constraint`, the second under `CGO_ENABLED=0`,
  which is this repository's stated posture. It also hardcoded linux/amd64, so a
  test named "agrees with the compiler" was false on every cross-architecture
  lane.

  It calls `build.Default.MatchFile` now -- the compiler's own answer -- and
  reads the LEADING comment block only. Scanning every comment made a
  `//go:build windows` line quoted inside a doc comment the file's constraint;
  `unwired_test.go` contains two such lines and they survived only by not
  starting their line.

- **The founding shape is no longer detected, and three places said otherwise.**
  Treating a composite-literal key as a read — necessary, because treating it as
  a write failed on correct code — means a field written only inside a composite
  literal is missed. `PeerResponse.HighWatermark` and `ReplicasConsulted` are
  both of that kind, and `ReplicasConsulted` left the exempt baseline because
  the detector was weakened, not because anything was wired. The commit
  disclosed the weakening; the file header and `countReads`' own doc still
  asserted the opposite in two places. Both corrected, with the disclosure where
  the code is rather than only in a commit message.

Also corrected: `maxPeerQueryBytes` is 60 KiB of **encoded** parameters, and
percent-encoding a LogsQL query roughly doubles it, so the raw budget is about
half what the comment claimed.

**Three drafts of the switch point were wrong, and the third argued the second
was wrong for a reason that applied to itself.** "30,001 against 31,001" is a
1,000-wide bracket, not a switch point. "30,719 / 61,439 in the URL against
30,720 / 61,442 in the body" was called one raw byte and two encoded bytes off.
Its replacement — "30,720 raw / 61,440 encoded still in the URL against 30,721 /
61,443 in the body … 61,442 is not an attainable length in this shape's sequence
at all (…61,439, 61,440, 61,443…)" — names four lengths this generator cannot
emit, including both of its own published boundaries, and asserts a sequence
containing a Δ1 and a Δ3 where the real step is a constant 4. `bigQuery` appends
`,v` per iteration: +2 raw, +4 encoded, because `,` becomes `%2C`.

Measured, by feeding each n to `withFormInURL` and recording which side it came
back on — not by arithmetic, which is what produced all three wrong versions:

| shape | last in URL | first in body |
|---|---|---|
| `query` alone | n=30722, raw 30,723, enc **61,437** | n=30723, raw 30,725, enc **61,441** |
| `query` + `step=1h` | n=30718, raw 30,719, enc **61,437** | n=30719, raw 30,721, enc **61,441** |

Attainable encoded lengths across the bound: …61,433, 61,437, 61,441, 61,445….
`maxPeerQueryBytes` is 61,440 and the comparison is `len(enc) > maxPeerQueryBytes`,
strict — but 61,438, 61,439 and 61,440 are not attainable in this shape, so the
bound itself cannot be observed exactly with this generator and the switch is
only ever seen as the gap between 61,437 and 61,441. A boundary quoted to the
byte from a shape whose step is 4 is a boundary nobody measured.

`PeerUnavailable`'s "remedy —
another replica — is the one that can actually help" is false for the 431 that
motivated the change: the refusal is deterministic and every replica gives the
same answer; what the class buys is an accurate name. `docs/wrong.md` cited an
"entry 108" in a file whose entries run 1-55.

## 59. A bare `pack_json`'s packed value is short, and the row beside it is right

Entry 58's `MatCols` split fixed the leak on a projected pack. Measured against
the staged `victoria-logs` binary, one row with `svc` as a stream field:

```
VL  * | pack_json as p
    row {"_msg":"hello","_stream":"{svc=\"api\"}","_stream_id":"0000…aa07…",
         "_time":"2026-08-16T03:00:00Z","lvl":"info","p":"…","svc":"api"}
    p   {"_time":…,"_stream_id":…,"_stream":…,"_msg":…,"lvl":…,"svc":…}
```

So `MatAll = true` for a bare pack is **correct** — VictoriaLogs does return the
full record — and the two projected shapes match this server exactly:

```
VL  * | fields lvl | pack_json as p             {"lvl":"info","p":"{\"lvl\":\"info\"}"}
VL  * | stats by (svc) count() n | pack_json as p  {"svc":"api","n":"1","p":"{\"svc\":…,\"n\":…}"}
```

What differs is the packed VALUE on the bare shape. Measured field by field
rather than as a group -- the first version of this entry said VL's `p` carries
"`_time`, `_stream_id` and `_stream`" and this server's carries none of them,
and that is wrong about `_stream`:

```
VL  p keys  [_msg _stream _stream_id _time lvl svc]
SL  p       {"_msg":"hello","_stream":"{svc=\"api\"}","lvl":"info","svc":"api"}
```

`_stream` is present on both. The gap is `_time` and `_stream_id`. `pack_json` runs in the
query layer and those three are synthesized at serialization
(`appendRowJSON`), so the pack cannot see them. The row is right and the packed
value is short — the mirror image of the defect entry 58 fixed, and the reason
that test's three rows all happened to carry a projecting pipe.

Recorded rather than fixed: making them agree means putting `_time` and
`_stream_id` onto the record before the pipes run, which changes what every pipe
sees. Task #437. Not "the pair" -- naming `_stream`/`_stream_id` there would
have added a field that is already present and still left `_time` missing.

Two latent ones closed alongside it. `applyCoordinatorPipes` set `MatAll: true`
into a `Query` that `ApplyPipes` never reads either flag from — inert, and
inert on a flag whose meaning had just changed to "synthesize the stream pair",
so the day the coordinator writes with `withStream=true` it would put the pair
back on merged stats rows. Removed. And `FacetList`'s sub-query cleared `MatAll`
and not `MatCols`, so an inherited `MatCols` would make a timestamps-only scan
read every column of every matching row, against a 256 MiB budget it shares with
the parent request. Unreachable today, which is why it would have been found
late.


## 60. The fix reached eleven of fourteen, and the test written to prove it covered nine

Entry 56 fixed the large-POST path and said "eleven fan-out endpoints", "all
twelve". `surfaceRoutes()` classifies **fourteen** federated reads.
`TestEveryFanOutEndpointCarriesALargeQuery` listed ten, one of which
(`/select/logsql/hits_count`) is not a route at all and skipped forever on its
404 — so it covered **nine of fourteen** and missed `/select/logsql/stats_query`
and `stats_query_range`, the Grafana-dashboard shape the whole change is about.

This repository had already learned that and written it down:
`TestEveryFederatedReadIsInTheCompletenessSuite` exists because *"federatedEndpoints
was a hand-kept list that had drifted to nine of the fourteen federated reads"*.
A hand-kept list drifted to nine of fourteen again, in a test written to prove
coverage.

The set is derived from `surfaceRoutes()` now, with each route's own parameters
and its own query language — `/select/sql` gets a large `SELECT ... OR ...`
chain rather than LogsQL, and the two Elasticsearch routes are **skipped with
the reason** (a JSON body never enters the form path) rather than fed LogsQL and
counted. Twelve exercised, two skipped, and the count itself is asserted.

## 61. A limit the plan DELETED came back over a POST form, on the endpoint the change was named after

`shardQueryURL` removes `limit` from the shard request when the plan has a
coordinator half: the shards must return everything and the bound is applied
once over the merged rows. `withFormInURL` merges form keys "not already in the
shard URL" — and a key the plan **deleted** is not in the URL. Measured, three
shards of ten rows, `&limit=5`, HTTP 200 throughout:

| | single node | cluster GET | cluster POST form |
|---|---|---|---|
| `* \| stats count() c` | 30 | 30 | **15** |
| `* \| stats by (level) count() c` | 10/10/10 | 10/10/10 | **5/5/5** |

It reproduces before entry 56 as well, so that commit did not introduce it — it
rewrote the function around it and added a comment asserting the opposite:
*"with RawQuery cleared there is one source and the plan wins because the plan
is what was merged in"*. A union merge does not preserve a deletion.

Fixed by recording **which parameters the plan owns**, set or deleted, rather
than by deleting this one from `r.Form` as well — that would fix `limit` and
leave the next deleted parameter to be found the same way. Two of the three
pieces are load-bearing; `withoutLimits`'s marking is redundant with its own
form deletion and says so.

Found alongside it and left open: a cluster and a single node disagree on which
rows `sort ... | limit` keeps. Both cluster methods agree with each other, so it
is not this defect. Task #438, and the case stays in the test with the
comparison switched off and a pointer, rather than dropped.

## 62. Two subtests could not fail, and a 400 named the wrong half

`TestALimitInAPostFormDoesNotReachTheShards`'s two `facets` rows pass with
either half of the fix deleted: facets passes `limit` through `unlimited`
(`vals.Set("limit","0")`), so the key is always already in the URL and never
enters `extra`. They were immune before the fix. Kept for the
`max_values_per_field` half, which is real, and labelled `limitImmune` so the
pass is not read as coverage. The other seven rows fail on both mutations.

And `withoutLimits`' new `ParseForm` fails on a malformed **Content-Type** too —
`parsePostForm` runs `mime.ParseMediaType` first — while the refusal said "this
request's query string could not be parsed". Measured: `Content-Type: text/plain;
charset` on a request whose query string is perfectly correct, answered 200
before and 400-blaming-the-query-string after. It names the half that is
actually unreadable now.

Also closed: `fanOutChecked` dropped `formBody` when the caller had built a body
of its own. Impossible today — the two endpoints that build a body send JSON, so
`withFormInURL` returns before it looks at anything — but that is a statement
about what clients send, not a property of the code, and `RawQuery` is now
cleared so a dropped `formBody` would hand the shard **neither**. It is refused
with an explanation instead of reasoned away.

## 63. The fix that suppressed a shard limit the plan deliberately keeps

Entry 61 recorded which parameters the plan owns so a POST form could not
re-add a deleted one. It marked `limit` **unconditionally**, and
`shardQueryURL` deletes it only when there is a coordinator half — its own
comment says *"with no coordinator half the shard limit is exactly right and
stays"*. Over a POST it stopped staying. Measured, one shard,
`POST query=*&limit=5`:

```
before   shard received  limit=5&query=%2A
after    shard received  query=%2A
GET      shard received  limit=5&query=%2A     (unchanged)
```

The answer stayed correct — `mergeRows` applies the bound from the original
request — so this is blast radius, not a wrong number.

**The first published triple — 3,808,890 / 955 / 190.4 B/row — had no recorded
fixture, and the number is a property of the rows.** Re-measured twice it gave
3,493,317 / 877 / 174.7 and 3,762,650 / 944 / 188.1: same order, same
conclusion, different digits every time, because nothing said what a row was.
The fixture, written down so the number means something:

```
20,000 rows of NDJSON, 2,184,890 bytes in (109.2 B/row), one storage node:
  {"_time":"2026-08-14T00:00:SS.mmmZ","_msg":"request N completed",
   "level":"info","svc":"api","user":"uK"}     SS=i%60 mmm=i%1000 K=i%50

  query=*          3,762,650 bytes   188.1 B/row out
  query=*&limit=5        944 bytes
  ratio            3,985.9x  (the other two runs: 3,988.4 and 3,983.3)
  peerMaxBodyBytes 268,435,456 -> ~1.43M rows per shard before the read fails
```

Every shard streamed its whole matching set to the router, on the exact path
POST exists for. What is stable across all three measurements is the ratio —
about four thousand — and the order of the per-row cost; the digits are not,
and quoting them to seven figures without the fixture claimed a precision the
measurement never had.

Marked only when the plan deletes it now. `query` is not marked at all:
`shardQueryURL` always `Set`s it, so it is always a URL key and the existing
rule covers it — that marking was inert, and an inert marking reads as a
load-bearing one.

**And the both-bodies refusal fired on a request with no body.** The guard read
`body != nil`, and both Elasticsearch handlers pass `io.ReadAll(r.Body)`, which
never returns nil — an empty body is an empty non-nil slice. Measured,
`POST /_count?q=<70 KiB>` with a form content type and no body: **200 before,
400 after**, with a message saying the request carried two bodies when it
carried none.

## 64. Three labels that labelled nothing, and a 400 on half the routes

- **`limitImmune` was a struct field with no reader.** Entry 62 said the immune
  rows were "labelled rather than left to read as coverage"; the label was a
  field nothing consulted, and the repo's unwired gate skips `_test.go` so
  nothing could catch it. It gates the `limit` assertion now.
- **The coverage count could not fire.** `n++` ran *before* the JSON-body skip,
  so `n` was always 14 and `if n < 12` was unreachable — two more routes could
  start skipping in silence. Worse, the log line then said *"14 federated reads
  carried a 1200001-byte query"* immediately after two SKIP lines. Twelve
  carried it.
- **The same malformed request was 400 on six routes and 200 on six.**
  `withoutLimits` called `ParseForm` for any POST/PUT regardless of content
  type, and `ParseForm` runs `mime.ParseMediaType` first — so a malformed
  `Content-Type` failed on a body the router was never going to read, and only
  on the six routes that reach it. `facets` escaped by argument-evaluation
  order alone: `maxValuesParam` primes `ParseForm` and swallows the error first.
  It parses only a form content type now and all twelve answer 200.

  Which retires entry 62's own fix: naming the Content-Type in the refusal was
  the right answer to the wrong defect — the refusal should not have happened.

Two stale claims corrected. `pack_test.go` still carried *"VL's `p` CONTAINS
`_time`, `_stream_id` and `_stream`, and this server's does not"* — the sentence
entry 59 was corrected to remove; `_stream` is on both and the gap is `_time`
and `_stream_id`. The switch point published here was wrong for the third time
and is corrected in entry 58 with the measurement that produced it.

## 65. The fix for "an empty body is not two bodies" changed one arm of a switch, and the other arm made it a silent wrong number

Entry 62 changed `body != nil` to `len(body) > 0` on the refusing arm of a
two-arm switch, on the premise — correct — that `io.ReadAll` never returns nil.
The premise falsifies the OTHER arm too, and that one was left as
`body == nil`. For an empty body `len(body) == 0` **and** `body != nil`, so
**neither arm ran**: the switch was non-exhaustive at exactly the value the
commit was about. `withFormInURL` has already executed `out.URL.RawQuery = ""`
by then, so the shard was handed an empty query string and an empty body.

Measured, spy shard answering `count:3` when it sees `q` and `count:1000` when
it does not:

```
POST /_count?q=<1 KiB filter>   form content type, no body
  -> 200 {"count":3}      shard saw q of 1025 bytes    correct
POST /_count?q=<70 KiB filter>  same headers, no body
  -> 200 {"count":1000}   shard saw q of 0 bytes       WRONG, silent
```

A/B with that one line reverted to `body != nil`: the 70 KiB case answers 400.
**The fix turned a loud refusal into a silent wrong number at exactly the size
the large-form path exists for.** Also reproduces with
`application/x-www-form-urlencoded; charset=UTF-8` and `Content-Length: 0`.

The comment three lines above it states the consequence verbatim — *"dropping
it would hand the shard neither, and the answer would be a smaller one at HTTP
200"* — and so does entry 62. `TestAnEmptyBodyIsNotTwoBodies` passed at that
commit while the shard was asked nothing: it asserted `!= 400` and the absence
of one phrase, and neither is affected by what the shard receives. It now
asserts the shard was asked the caller's query, and the non-exhaustive switch
reddens it.

The switch is gone. One `if formBody != nil` with the length test inside it is
total in `len(body)` by construction, which is the property that was missing.

## 66. Gating on the content type did not remove the six-vs-six split; it moved it one character along

Entry 64 closed with *"it parses only a form content type now and all twelve
answer 200"*. Measured on the neighbouring content type,
`application/x-www-form-urlencoded; charset` — jQuery's default header with the
value lost — it was **7 refused / 5 answered**:

```
query 200 | hits 400 | facets 200 | field_names 400 | field_values 400
stream_field_names 400 | stream_field_values 400 | streams 400 | stream_ids 400
stats_query 200 | stats_query_range 200 | sql 200
```

`mime.ParseMediaType` returns the media type **and** `ErrInvalidMediaParameter`,
so `parsePostForm` reads and parses the body perfectly and `ParseForm` returns
the correct values with a non-nil error. `isFormPost` accepts; `withoutLimits`
then refuses a form it has just parsed correctly. Whether it refuses at all
depends on whether an earlier `FormValue` primed `r.PostForm` — the same
argument-evaluation-order accident entry 64 said it had removed, now covering
five routes.

The obvious repair is the wrong one. `errors.Is(err, mime.ErrInvalidMediaParameter)`
and carry on looks right and is not, because `parsePostForm` keeps the header's
error and **discards** `url.ParseQuery`'s:

```
ct "...urlencoded; charset", body "query=%zz&limit=2"
  err      = mime: invalid media parameter     the body error is gone
  PostForm = map[limit:[2]]                    `query` is gone with it
```

A dropped `query` forwarded at HTTP 200 — the defect class, reintroduced by the
fix for it. So the header is CORRECTED before parsing instead
(`normalizeFormContentType`), which restores the precedence: `ParseForm` then
fails on `%zz` and the request is refused, naming the body.

`withFormInURL` carried a second copy of `isFormPost`'s rule and disagreed with
it on what a `ParseForm` error means, which is why `hits` was the last route
still answering 400. It calls the predicate now.

## 67. A header fault reported as a shard fault, and a counter that counted its own failures

Three smaller findings from the same round.

**The diagnosis regression.** `POST /select/logsql/field_values`,
`Content-Type: text/plain; charset`, parameters in the body. Before the content
-type gate: `400 this request's Content-Type ("text/plain; charset") could not
be parsed`. After it, against real storage nodes:
`503 2 of 2 shards could not answer completely (0(rejected),1(rejected))`.
Consistent across the twelve routes, and it sends an operator to inspect the
storage nodes for a fault in their own request's header. A body under a content
type no form parser will read is now refused at the router on all twelve,
naming the Content-Type — `/select/sql` included, which reached its own "SQL
must start with SELECT" on the empty string and blamed a statement the caller
did send.

**`carried` counted failures as successes.** `carried.Add(1)` sat after a
`t.Errorf`, and `t.Errorf` does not stop a subtest. Under a mutation appending
one byte to every shard query, all twelve subtests failed AND the summary line
printed *"12 of 46 federated reads carried a 1200001-byte query"* — asserting
the thing that had just failed. It reads `12 of 14` now (the federated count,
not every route the mux registers) and the guard fires at 1.

**`limitImmune` was a reader that gated nothing.** Entry 64 turned a struct
field with no reader into a field with a reader — and deleting the three lines
changed no result, because facets' shard `limit` is `0` and the assertion looked
for `limit=2`. A field with no reader had become a reader with no effect. It is
`limitSetTo string` now and asserts what the shard WAS told (`limit=0`), which
reddens when the plan stops setting the unlimited value.

**The shape, twice more.** Two of the three defects in entries 65 and 66 are the
previous round's fixes, and both were shipped with a test that passed through
them. The round before that said the same. What distinguishes the fixes that
held from the ones that did not is not care taken: it is whether the assertion
names what the SHARD RECEIVED. Every test that asserted a status code passed
through its own defect; every test that asserted the shard's parameter set
caught it.

## 68. The idempotency window closed by moving the id, not the flush

Entry 54 measured a window between a flush and its receipt, listed four
variants, rejected three as strictly worse, and priced the fourth — align the
flush boundary with the request — as *"correct, and it gives up the
cross-request batching that the ingest throughput numbers are built on. That is
a real design decision with a measurable cost, not an edit."*

That priced the wrong thing. `FlushWithReceipt` was ALREADY `Flush()` per
replicated write, so the batching a request-aligned boundary would give up is
only the batching among *concurrent* replicated writes — and none of it has to
be given up, because the question is not WHEN the flush happens or WHAT it
contains. It is which manifest record the id goes in.

`flushLocked` enqueues exactly one job, and one job is one group. So when

- there are rows buffered (there is a group for the id to ride), and
- `len(w.live) == 0` (nothing else in flight, so every group this writer has
  already handed out is committed, and manifest records are appended in order —
  "this record is durable" implies "every earlier one is"),

the id goes to that job and the worker commits it through
`AppendGroupIdempotent`: **one record, one transaction, no window**. The group
is still whatever was buffered, from however many requests. Neither condition
can be relaxed: dropping the second is entry 54's rejected "stamp the id on
every group", which commits a receipt for rows a partial flush never wrote.

Measured as the manifest sequence delta across one `FlushWithReceipt` of ten
rows under a write id:

```
before   seq +2   group record, then receipt record   -- two fsyncs, one window
after    seq +1   one record carrying both            -- one fsync, no window
```

The delta is the measurement, not a reopen: both designs have the same end
state when no crash happens, and the record count is what "one transaction"
means. It is also one `Sync()` less per replicated write, because `man.commit`
syncs once per record.

Under concurrency the conditions fail and `CommitReceipt` runs as before, so
the window narrows rather than moving. `AppendGroupIdempotent` returning
`ErrDuplicateWrite` — two retries of one id racing past the middleware's check
— falls back to a plain `AppendGroup`: the group holds every row buffered on
the tenant, and dropping it would lose other callers' rows to a duplicate that
is not theirs.

**The gate found its own stale exemption.** `AppendGroupIdempotent` was on
`TestDocumentedMechanismsHaveCallers`' exempt list with a note explaining that
production deliberately took the other path. Wiring it reddened the gate, which
is what an exemption list is for when the reason behind it stops being true —
*"an exemption nobody removes is how the next unwired mechanism gets in under
it"*, and this one removed itself.

Each of the four tests replacing the old window test was probed by mutation:
forcing the two-step path reddens the record-count test, deleting the
`ErrDuplicateWrite` fallback reddens the racing-duplicate test, and forcing
`rode` true reddens the nothing-left-to-flush test.

## 69. The refusal written for a header fault refused what a single node answers

Entry 67 added `unreadableBody`: a POST with a non-empty body under a non-form
content type was refused with 400 naming the Content-Type, because the
parameters in that body are unreadable and the shards would be asked the URL
query alone.

It refuses requests that have no problem. Every parameter can already be in the
URL, and then the body is irrelevant — a single node ignores it:

```
POST /select/logsql/query?query=*   body {}   ct application/json
  single node  200 (the rows)
  router       400 this request's Content-Type ("application/json") is not a form…
```

Same for `null`, `" "`, `text/plain`, an empty `Content-Type` with a body,
chunked framing and `Expect: 100-continue`. It also stayed **order-dependent**,
which is the accident it was written to remove: the check asks whether bytes
remain in the body, and `ParseMultipartForm` — reached through an earlier
`FormValue` on four of the twelve routes — drains a multipart body first. So
`multipart/form-data` split 4 answered / 8 refused, and three of the four
answering routes produced entry 67's own `503 … (0(rejected))` against real
storage nodes.

**The refusal was papering over the real defect one level down.** `planQuery`
defaulted a blank `query` to `*`, where `parseRequest`'s own comment says the
reference requires one and *"defaulting to match-all answered a client's bug
with the entire store"*. Measured, a three-row store and a filter matching one
row:

```
GET /select/logsql/query            single 400   router 200, 3 of 3 rows
GET /select/logsql/query?query=     single 400   router 200, 3 of 3 rows
GET /select/logsql/query?query=%20  single 400   router 200, 3 of 3 rows
POST form, body junk=1              single 400   router 200, shard asked `*`
```

That is the amplifier under every "the shards were asked the empty query"
finding in entries 55-68: on this route a dropped filter was never a smaller
answer, it was the **entire store at HTTP 200**.

So the refusal is deleted and the default is gone. A body the router cannot
parse as a form is ignored, exactly as a single node ignores it, and the request
is then refused for the reason that is true — there is no `query`. Every case
above now answers identically on a router and a single node, and the test
asserts that equality rather than a status code someone chose.

Also corrected: entry 63's derived ratio, published as `~3,990x`. 3,762,650 /
944 = **3,985.9**.

## 70. `q` in a form body, on `/_count`: the shard was asked nothing and counted everything

`federatedESCount` read the body unconditionally. `withFormInURL`'s `ParseForm`
then found it drained, saw no form, and returned `formBody == nil` — and
`fanOutChecked` forwarded the caller's raw `q=level%3Aerror` bytes with no
content type, which `clusterClient.do` relabels `application/json`. A shard's
`ParseForm` will not read a JSON body, so it was asked no filter at all.

Measured, spy shard answering 3 when it sees `q` and 1000 when it does not:

```
POST /_count?q=level:error   form CT, empty body  -> 200 {"count":3}     shard q="level:error"
POST /_count                 form CT, q in body   -> 200 {"count":1000}  shard q=""
POST /_count?pretty=1        form CT, q in body   -> 200 {"count":1000}  shard q=""
```

Wire-level the shard saw `ct="application/json" rawquery="" body="q=level%3Aerror"`.

The body is left unread when the content type says it is a form, so
`withFormInURL` parses it and folds `q` into the shard URL — the path the
large-form case already takes. `/_search` under the same shape was loud
(`400 invalid character 'q'`) and is unchanged.

## 71. The fix for a 503 traded 16 cells for 31, and the test could not see it

Entry 69 deleted `unreadableBody` and stopped `planQuery` defaulting a blank
query to `*`. Measured across twelve federated reads × thirteen framings,
router against a real storage node, that commit **fixed 16 cells and regressed
31**: nine to eleven routes moved from a 400 naming the caller's own header to
`503 … 2 of 2 shards could not answer completely (0(rejected),1(rejected))` —
verbatim the answer entry 67 exists to remove, and one a client retries forever
because a retry re-fans-out and can never succeed.

```
curl -F 'query=*'             node 200:12    router 200:1  502:1  503:10
text/plain, params in body    node 400:11    router 400:2  502:1  503:9
```

**Three causes, all "the router does not read what the node reads".**

`isFormPost` matched only `application/x-www-form-urlencoded`. A single node
parses multipart — `FormValue` calls `ParseMultipartForm` — so the router never
folded a multipart form into the shard URL and the shards were asked nothing. It
matches both encodings now, through a `parseFormBody` that calls the right
parser. A multipart body that will not PARSE is ignored rather than fatal, which
is also what a node does: `FormValue` discards that error and reads the URL
query.

The router fanned out an empty selector where a node refuses. `parseRequest`
refuses a blank `query` on every select endpoint; `fanOutChecked` now applies
the same rule before any shard is asked, so the caller gets the node's own 400
instead of a 5xx pointing at the storage nodes.

And `/select/logsql/stats_query_range` on a NODE answered 200 to a request with
no parameters at all, fabricating a matrix:

```
GET /select/logsql/stats_query_range
  200 {"resultType":"matrix","result":[{"metric":{},
       "values":[[1690951540,""],[1690951540,""],[1690951540,""]]}]}
```

A constant garbage epoch and empty-string values. `docs/lld/api.md` says `query`
is required on every select endpoint; this was the route where that was false,
and it was the only pair of cells where the router's refusal was RIGHT and the
node's answer was wrong.

**The test could not see any of it.** It used a recording shard — a spy that
answers 200 to anything — and one route. Against that spy, 11 of 12 federated
reads answered 200 with the shard asked `query=""` for the multipart,
`text/plain` and no-Content-Type framings: the defect class entries 55-70 exist
to remove, hidden by the fixture. The matrix now runs every federated read
against a REAL storage node and compares to that same node. Removing multipart
support reddens 11 cells, removing the missing-query guard reddens 20, and
accepting a blank query on `stats_query_range` reddens 2.

## 72. A parameter no storage node reads, bought with a working capability

Entry 70 stopped `federatedESCount` reading the body when the content type is a
form, so `withFormInURL` could fold `q` into the shard URL.

`curl -d` sends `Content-Type: application/x-www-form-urlencoded` **by
default**, so a real Elasticsearch document took that path: `ParseForm` turned
the JSON into one URL key and the shard got an empty body.

```
curl -d '{"query":{"term":{"level":"error"}}}' <router>/_count
  before  200 {"count":1}          after  503
  shard   url ".../_count?%7B%22query%22...%7D=" ct="" body=""
  and with ?allow_partial_response=1: 206 {"count":0}
```

A silently wrong number on the partial path, and `/_search` — which still read
its body unconditionally — answered 200 to the identical request, so the two ES
endpoints disagreed with each other.

**And `q` is not a parameter any storage node reads.** `esCount` decodes the
JSON body; there is no `q` handling in `es.go` at all, so
`/_count?q=level:error` with a valid body answers about the whole store on a
single node too. The test that passed was passing against a recording shard
that called `FormValue("q")` itself — a spy asserting a contract the real thing
does not have.

Reverted. The replacement test compares the router to a single node across three
content types, which is the only claim there was to make.

**Two of the three `normalizeFormContentType` calls were inert, and that was
measured rather than assumed.** `ParseForm` caches: the first call populates
`r.Form` and returns the error, every later call sees `r.Form` non-nil and
returns nil. So correcting the header only matters where an error from THAT
first parse is treated as fatal — `withoutLimits`, and nowhere else.

```
delete it from withoutLimits    2 tests red
delete it from withFormInURL    nothing red
delete it from fanOutChecked    nothing red
```

The matrix that establishes the second and third rows covers
`application/x-www-form-urlencoded; charset` across all twelve federated reads,
so "nothing red" is a statement about coverage that exists rather than coverage
that does not. Both copies deleted: an inert guard reads as a load-bearing one,
which is how three rounds' worth of dead code got in.

## 73. The bare pack, fixed — and the first fix put the fields on rows that never had them

Entry 59 measured a bare `pack_json`'s value as short: `_time`, `_stream` and
`_stream_id` are synthesized at serialization (`appendRowJSON`) and `pack_json`
runs in the query layer, so it could not see them. The row beside the packed
value was right, which is what makes it hard to see — a client reading `p` and a
client reading the row got different records out of one response.

```
VL  p keys  [_msg _stream _stream_id _time lvl svc]
SL  p       {"_msg":"hello","_stream":"{svc=\"api\"}","lvl":"info","svc":"api"}
```

The rule is one sentence: **a bare pack packs the row as it will be
serialized.** So `_time` comes from `r.Time` when the row has one, and the
stream pair from the row's own values.

**The first version of it was wrong in the mirror direction.** `rowStreamPair`
synthesized the EMPTY stream when a row carried none — which is what
serialization does, because a full record is always in some stream — and that
put two fields onto every projected and stats row:

```
VL    * | fields lvl | pack_json as p   ->  p {"lvl":"info"}
first fix                                   p {"lvl":"info","_stream":"{}",
                                               "_stream_id":"0000…55b5"}
```

Those are the two shapes entry 59 had already measured as CORRECT, broken by
the fix for the third. A projection is not a record: the pair is emitted only
when the row carries a non-empty `_stream`.

**And the second version dropped a field the row did carry.** A row whose
`_stream` column is present but EMPTY — which happens because the store
materializes the column for a whole flush — had `_stream` skipped from the loop
(to be emitted once from the pair) and then no pair emitted, so the field
vanished. The skip is now conditional on the pair being emitted: the pack's job
is to pack the row, not to improve it.

Measured on the built binary, all four shapes:

```
* | pack_json as p                        {_time,_msg,lvl,svc,_stream,_stream_id}
* | fields lvl | pack_json as p           {"lvl":"info"}
* | stats by (svc) count() n | pack_json   {"svc":"api","n":"1"}
* | fields _time, _msg | pack_json as p    {"_time":…,"_msg":"hello"}
```

The three projected shapes are byte-identical to the reference. The bare one
matches its key SET; the ORDER differs — VL emits
`_time, _stream_id, _stream, _msg, …` and this server emits
`_time, <row fields>, _stream, _stream_id`, because matching VL's order means
matching its internal field ordering, which this server does not have. The test
asserts the set for that reason and says so.

## 74. "Inert, measured" — measured with the two routes it mattered for filtered out

Entry 72 deleted `normalizeFormContentType` from `withFormInURL` on the ground
that nothing reddened. Nothing reddened because the parity matrix filters
`rt.body != ""`, and that excludes exactly the two Elasticsearch routes — the
ones that pass a NON-NIL body to `fanOutChecked`, so its missing-query guard
(whose `FormValue` is what primes `r.Form` for every other route) is skipped and
`withFormInURL`'s parse becomes the FIRST parse of the request.

With `; charset` that parse returns `ErrInvalidMediaParameter`, `withFormInURL`
returns nil, and the router refuses what a node answers:

```
POST /_count   ct "…urlencoded; charset"  body {"query":{"match_all":{}}}
  single node 200 {"count":30}
  router      400 the request body is not a readable form
POST /_search  same content type          200 / 400 the same way
```

jQuery's spelling, on the two routes whose body IS a document. Restored, and the
matrix now runs the ES routes with their own framings — a JSON body under four
content types — so deleting the call again reddens two cells instead of none.

**Two more claims in that entry were wrong.** There were TWO call sites, not
three, and one was deleted — so "two of the three are deleted" and "both copies
deleted" describe a change that did not happen, and the row
`delete it from fanOutChecked → nothing red` measures deleting a call
`fanOutChecked` never had. And the surviving row is off: deleting it from
`withoutLimits` reddens **four** top-level tests, not two.

```
TestAMalformedContentTypeAnswersTheSameOnEveryFederatedRead
TestAMalformedMediaParameterDoesNotChangeWhatTheShardIsAsked
TestAMalformedBodyIsStillRefusedUnderAMalformedMediaParameter
TestEveryFederatedReadAnswersWhatASingleNodeAnswers
```

An "inert, measured" claim is worth exactly what its coverage is worth, and this
one's coverage had a filter in it.

## 75. Multipart temp files were never removed

`net/http` removes them itself — `finishRequest` calls
`w.req.MultipartForm.RemoveAll()` — but it checks the request the SERVER holds,
and every `r.WithContext(...)` in this server's chain (the query deadline in
`guard`, the tenant middleware, the write-id middleware) hands the handler a
COPY. `ParseMultipartForm` then sets `MultipartForm` on a copy the server never
sees, so nothing removes anything.

```
40 MiB multipart to /select/logsql/query, node and router:
  /tmp/multipart-* grows by one 41,943,040-byte file per request, and they
  persist. 32 files = 1.25 GiB left behind.
```

Bounded per request by `MaxBodyBytes`, unbounded in total, on a server whose job
is to run for months. `guard` parses the multipart form itself now, before any
copy is made, and defers `RemoveAll` — so every copy shares the pointer and the
cleanup reaches the form the handler used. A second `ParseMultipartForm`
downstream returns nil immediately, so no handler changes.

**Two versions of the test could not fail.** The first posted to a real route on
a test server that happens to make no copy — `MaxQueryDuration` unset, no
tenancy — so `net/http` cleaned up correctly and the test passed with the fix
removed: it measured a configuration in which the defect does not exist. The
handler now makes a copy deliberately, which is the condition.

The second still created no files, because the padding part had no `filename`.
`multipart.ReadForm` keeps VALUE parts in memory and returns
`ErrMessageTooLarge` past the budget; only FILE parts spill. With
`filename="pad.bin"` the fix removed leaves three files behind for three
requests, and the fix leaves none.

## 76. A parameter sent twice asked the cluster a different question than it asked a node

`withFormInURL` merged the caller's form UNDER the query string, so a key
present in both took the URL's value. A node does the opposite for a urlencoded
form: `ParseForm` puts `PostForm` before the URL query and `FormValue` reads the
first, so the BODY wins. Both answered 200:

```
POST /select/logsql/stats_query?query=level:error | stats count() c
     ct urlencoded, body query=* | stats count() c
  node   "30"      router  "10"
same shape, limit=1 in the URL and limit=5 in the body:
  node   5 rows    router  3 rows
```

**Correction (entry 79).** This entry originally read "limit=1 in the URL and
limit=3 in the body: node 3 rows, router 1 row". That is not what a three-shard
router does, and the test shipped with the same numbers. Re-measured on this
entry's own base commit: `url=1 body=3` gives node 3 rows and router 3 rows,
**equal**, because the router applies the URL's limit=1 per shard and there are
three shards. "router 1 row" is the ONE-shard result -- it came from the
one-shard fixture this entry elsewhere says cannot see the defect, and was
carried into both the prose and the test unchanged. `url=1 body=5` is the
version that discriminates: node 5, router 3.

Under MULTIPART the two already agreed — Go appends multipart values AFTER the
URL query — so the router was implementing multipart precedence for both
encodings.

`r.Form` already encodes the right precedence for each encoding, so the merge
forwards its values verbatim and the shard gets the node's answer either way,
multi-valued parameters included.

**What the URL must still win is the plan's own rewrite**, and that is now
MARKED rather than inferred from "it is in the URL". `federatedSelect` marks
`query` — a mark entry 63 removed as inert, which it was under the old rule and
is not under this one: removing it now reddens
`TestAPostFormDoesNotOverwriteTheRoutersOwnQuery` and
`TestALimitThePlanDeletedDoesNotComeBackOverAPostForm`.

**And the obvious companion marking is inert.** Marking `withoutLimits`' bounds
the same way reddens nothing, because that function deletes them from the
clone's `Form` and `PostForm` too — and a key absent from `r.Form` cannot be
re-added by the merge. Not kept: third time this session that "mark it too, for
safety" turned out to be inert.

Two envelopes were maps where a node writes a struct, so `encoding/json` sorted
their keys and the router answered `{"data":…,"status":…}` against a node's
`{"status":…,"data":…}`. Structs now, which is what makes a byte comparison
possible at all.

**The fixture had to be repaired twice before it could measure anything.** The
first `loadedPair` built the router with `wmRouter`, which calls
`SetReplicas(len(backends))` — three REPLICAS of one shard, not three shards —
so the router answered from one node holding a third of the data: 10 against the
node's 30, which looks exactly like the defect and is a fixture that cannot see
it. And the assertion compared line counts and a substring until both sides
agreed on the number, at which point it still passed on two different
envelopes; it compares bodies byte for byte now.

## 77. A field faceted twice on a node became a doubled number on a router

`FacetList` iterates the stored column names and then appends `_stream` and
`_stream_id` at the tail, because they are synthesized onto every record. Once
`_stream_fields` is configured they are ALSO stored columns — so both were
faceted twice, once from the column and once from the tail.

```
30 rows, 3 streams of 10
  node   "_stream" appears TWICE, 10/10/10 in each
  router "_stream" once, 20/20/20
```

Both HTTP 200, and the truth is 10. The duplicate on the node is merely odd; the
router's merge sums the pair by (field, value), so it becomes a **number twice
the size of the data**, on the endpoint a dashboard uses to draw a distribution.

The main loop skips them now: they are emitted once, from `Streams`/`StreamIDs`,
which is the authoritative source for a field that exists whether or not a
column was materialized for it.

Found by giving the router/node differential REAL DATA. The parity matrix it
grew out of compares status codes over an empty store, which is "returns the
same three digits over no data" — both sides answered 200 here, and the
difference is entirely in the body.


## 78. Three fixes, three new defects: a lost field, a consumed body, and a buffered one

Reviewer round on `9b5bcf4..3bd502f` — the range that fixed entries 73–77.

**`pack_json` and `pack_logfmt` dropped a `_time` the row carried.** Entry 73
records getting exactly this right for `_stream`: "the skip is conditional on
the pair being emitted now". The same conditional was not applied to `_time`,
so a `NoTime` row carrying a `_time` FIELD lost it from both halves — nothing
emitted it from `r.Time` and the loop skipped the field. Measured, four rows:

| query | before 39e5716 | after | victoria-logs |
|---|---|---|---|
| `… \| stats by (_time) count() c \| pack_json as p` | `{"_time":"…00Z","c":"1"}` | `{"c":"1"}` | `{"_time":"2026-08-16T03:00:00Z","c":"1"}` |
| `… \| rename level as _time \| pack_json as p` | `{"_time":"error",…}` | `{"c":"2"}` for both rows | — |

Reachable through `stats by (_time)`, `rename … as _time`, `copy … as _time`
and the router's `jsonLineToRow`. Uncovered: the existing table's
"projection that KEPT `_time`" case has `NoTime` false, so `_time` comes from
`r.Time` and its key set is right either way. That fixture also disagreed with
itself — `r.Time` said 2026-08-06 and its `_time` field said 2026-08-16 — and
nothing checked the value, so it did not matter which won. The table now
asserts values as well as keys, and carries the two rows that reach the defect.

**`guard`'s multipart parse consumed the body of the two routes that read it
themselves.** Entry 75 claimed "a second `ParseMultipartForm` downstream returns
nil immediately, so no handler changes" — true of handlers that parse a form,
false of handlers that read `r.Body`:

| | before entry 75 | after |
|---|---|---|
| `/_count`, multipart, body `{"query":{"match_all":{}}}` | node 200 `{"count":12}` | node **400** `simdlogs: EOF`, router **503** |
| `/_search`, same | 200 | **400** |

And it buffered the body of routes that read nothing at all. 40 MiB multipart
POST, server-side `TotalAlloc` delta: `/metrics` 0 → **128 MiB**, `/` 0 → 128
MiB, `/alerts` 0 → 128 MiB, plus a temp file written and removed per request.
`/metrics` is deliberately exempt from the query semaphore, so this is 3.2× the
body in heap churn on the one route that must answer under load.

The parse is now per route (`routeSpec.form`). An opt-in list gets one entry
wrong silently, so it is not trusted: `TestNoRouteLeavesAMultipartTempFileBehind`
posts a spilling multipart body to **every** path `Handler()` registered and
fails on any file left behind.

Two things had to be got right for that gate to mean anything:

- **A positive control.** The check is an absence, and an absence has two
  causes. The first version lowered the spill threshold to 1 KiB to make the
  sweep cheap — but a handler's own `r.FormValue` calls `ParseMultipartForm`
  with net/http's `defaultMaxMemory`, also 32 MiB and not ours, so nothing
  spilled and the gate would have passed on a leaking server. A deliberately
  leaking handler now runs first and the test fails outright if it leaves no
  file.
- **Two phases.** A file that exists *while* a request is in flight is the
  mechanism working. `/select/logsql/tail` is meant to stay open, and checking
  right after the request reported its in-flight file as a leak. Files are
  attributed per route in phase one and judged after `ts.Close()`, which waits
  for every handler.

**It found one.** `/internal/replica/group` answered 400 and left a temp file:
`serveReplicaGroup` reads `digest` with `r.FormValue` *before* the method
switch, and its POST branch then `io.ReadAll`s the same body to adopt the group.
`protocols.go` already states the rule — "Read from the URL, never
`r.FormValue`: FormValue parses the BODY" — recorded after a line-protocol write
stored nothing while answering 204. Same defect on the anti-entropy path, where
the consequence is a shard that can never converge. `cluster_client` sends
`?digest=`, so the URL is where it already was.

**`_stream_id` was still doubled in `FieldNameCounts`.** Entry 77 fixed
`FacetList` and not the endpoint beside it: `_stream` guarded against the stored
column, `_stream_id` appended unconditionally. Six rows each carrying a
client-supplied `_stream_id`, one shard:

```
node   [… {"value":"_stream_id","hits":6},{"value":"_stream_id","hits":6} …]
router [{"value":"_stream_id","hits":12}, …]
```

Word for word entry 77's shape, both at HTTP 200. The new test supplies the
field, which `loadedPair`'s rows do not — without it the stored column never
exists and the test passes on the broken code.

## 79. Four tests that could not fail, one of which put a wrong number in the record

Same review round. None of these is a code defect; each is a check that was
green for a reason other than the one claimed.

**The limit subtest was a coincidence.** `limit=1` in the URL and `limit=3` in
the body, over the three-shard fixture: a router applying the URL's limit=1 per
shard returns 3 rows, which is what the body's limit=3 asks for. Measured on
entry 76's own base commit:

```
url=1 body=3   node 3 rows, router 3 rows, EQUAL
url=1 body=4   node 4 rows, router 3 rows, DIFFER
url=1 body=5   node 5 rows, router 3 rows, DIFFER
```

**And entry 76 published the wrong number.** It read "node 3 rows, router 1
row". That is the ONE-shard result — it came from the one-shard fixture the same
entry says cannot see the defect, and was carried into the prose and the test
unchanged. Corrected in place, in entry 76.

**The matrix envelope was never compared.** `federatedVector` and
`federatedMatrix` were both changed from a map to a struct so the router's key
order matches a node's; only the vector one was exercised. Reverting the matrix
envelope to a map left the whole suite green while the router answered
`{"data":{"result":…,"resultType":…},"status":…}` against a node's
`{"status":…,"data":{"resultType":…,"result":…}}`. A `stats_query_range` case
now goes through the same byte comparison.

**The facets test could not tell the synthesis from the column.** "Appears
once" is satisfied both by emitting `_stream`/`_stream_id` from `Streams`/
`StreamIDs` and skipping the stored columns — what `FacetList` does — and by
keeping the columns and deleting the tail. Deleting the tail left every package
green, while `_stream_id` vanished from facets entirely and `_stream` reported
the raw column value `""` where the row serializes `{svc="s0"}`. The contract is
click-through: the new test pastes each faceted value back into a filter and
requires it to select rows.

**A contradictory comment shipped.** Two adjacent paragraphs on the same line of
code, one saying `query` is not marked because marking it was inert, the next
saying it is marked always and load-bearing. The first was true before the rule
it depended on was removed in the same range. Deleted.

## 80. The `_time` fix reached two of the three copies, and missed the one on the wire

`packJSON` and `packLogfmt` were fixed to keep a `_time` FIELD that a `NoTime`
row carries. `appendRowJSON` — the serializer the packs exist to mirror, and
the one that writes the response — still dropped it. So one response answered
the question both ways:

```
* | stats by (_time) count() c | pack_json as p
  row  {"c":"1"}
  p    {"_time":"2026-08-16T03:00:00Z","c":"1"}
```

against victoria-logs, which puts `_time` in both. That is verbatim the failure
entry 73 says the pack fix existed to remove — "a client reading `p` got a
different record from a client reading the row, out of one response" — inverted
and still live. The whole suite was green with the fix and without it.

The test asserts the two halves **agree**, not that either matches a literal:
whatever the row says, the pack of the same row must say. Three shapes reach
it: `stats by (_time)`, `rename x as _time`, `copy x as _time`.

## 81. Three more routes on the wrong side of the form/document line

Entry 78 split routes into "parses a form" and "reads its own body" and got the
set wrong three ways.

**`/select/vector` is a third document route.** It decodes a JSON body, and the
pre-parse consumed it: 200 at the commit before the pre-parse, 400 `EOF` after,
while `/_count` was fixed in the same commit. It is also the only route that
reads parameters *and* a document, so `form` cannot be set correctly for it
either way — `true` eats the document, `false` drops a multipart `start`/`end`.
Its time window now comes from the URL, which is where it can only ever have
been: the body is the document.

**`/health`, `/-/healthy` and `/-/ready` leaked a temp file per request**, and
the gate could not see it. They are registered bare — outside `guard`, so no
pre-parse, no `RemoveAll` and **no `MaxBytesReader`** — and `health.go` called
`r.FormValue("format")`. The leak needs a middleware that replaced the request
first, and with authentication OFF `withTenant` makes no copy for these paths,
so net/http cleans up and the gate passes. `withPrincipal` does copy. On an
authenticated server, a 33 MiB multipart POST to each:

```
/health     200  multipart-105144472
/-/healthy  200  multipart-842413133
/-/ready    200  multipart-920247530     all three survive the close
```

Unbounded, on routes that answer unauthenticated callers by design. `format`
reads the URL now, and the gate runs a second time against an authenticated
server — the configuration in which the defect exists was the one it never
built.

**`adminSpec().form = true` was justified by a reader that did not exist.** The
comment said "serveReplicaState reads `digest` through `r.FormValue`", which
the same change had just made untrue by moving that read to the URL. No admin
handler reads a form. It cost six routes exactly what entry 78 had removed from
three — 128.2 MiB against 0.2 MiB of `TotalAlloc` for a 40 MiB multipart POST —
including `/admin/acknowledge-degraded`, which is `nosem` and therefore chosen
to stay answerable under load.

## 82. A doubled count replaced by a wrong one, on the endpoint next to the one that was right

Entry 78 stopped `FieldNameCounts` listing `_stream_id` twice by guarding the
synthesized entry against the stored column. That kept the wrong number: the
column counts rows that **supplied** the field, while every returned row
**serializes** one. Six rows, three carrying `_stream_id`:

| | `_stream_id` |
|---|---|
| `field_names` | 3 |
| `facets` | 3 + 3 = **6** |
| the rows themselves | all six carry one |

So it went from disagreeing with itself to disagreeing with `facets` over the
same store — which is the shape entry 77 was about, one step along.

`FacetList` had already resolved the identical collision the other way: skip
the stored column, emit from the authoritative source. `FieldNameCounts` does
that now, so the two endpoints agree by construction rather than by both
happening to be right.

The fixture had to change too. Entry 78's gave the field to **all six** rows,
which makes the column count and the row count coincide — the one ratio at
which a fix that keeps the column's number passes. Three of six tells them
apart, and both mutations (count the column; drop the synthesized entry) are
red.

## 83. The `_time` fix emitted it TWICE, and the test's own comparison hid it

Entry 80 made the `_time` field survive on a `NoTime` row. A `NoTime` row can
carry **two** `_time` fields — neither `rename x as _time` nor `copy x as _time`
overwrites an existing key — and the new guard kept both:

```
* | stats by (_time) count() c | copy _time as t2 | rename t2 as _time

before entry 80  {"c":"1"}
after            {"_time":"…00Z","c":"1","_time":"…00Z"}
victoria-logs    {"_time":"2026-08-16T03:00:00Z","c":"1"}
```

One JSON object with a duplicate key, which every decoder resolves differently.
Reachable on the plain wire row, not only inside a pack.

**And the test could not see it.** `TestARowAndItsPackAgree` unmarshalled both
halves into `map[string]string`, and a map keeps the last value for a repeated
key — so its comparison method erased precisely the failure the change enabled.
It reads key SEQUENCES now, through `json.Decoder`, because neither a map nor a
struct can report a duplicate. Both directions are probed: keeping duplicates
reddens it (`the row repeats a key: [_time c _time p]`), and dropping `_time`
again reddens it the other way.

The rule in all three copies is now "emit that key at most once, whichever
source it comes from".

## 84. Making the node right made the node and the router disagree

Entry 80 fixed `appendRowJSON`, and the router's `jsonLineToRow` lifts `_time`
out of the fields into `Row.Time` and **dropped the field**. For
`stats by (_time)` that field is the group key, so the merge grouped every row
together:

```
* | stats by (_time) count() c      4 rows, 2 distinct timestamps
  node    {"_time":"…03:00:00Z","c":"2"} {"_time":"…03:00:01Z","c":"2"}
  router  {"c":"4"}
```

Both at HTTP 200. The router was wrong before the fix too, but so was the node —
so this converted "both wrong" into "a cluster and a node disagree about a
number", which is worse.

The field is kept as well as lifted now. It costs nothing on the way out,
because the serializer emits `_time` at most once, so an ordinary log row
serializes byte for byte as before; and it costs no allocation, because the
field slice is already sized by `countFields(line)`, which counts `_time`.

Four tests encoded the old contract — including the reference decoders that
`TestRowScannerMatchesTheDecoder` and `FuzzJSONLineToRow` compare against — and
were updated with it. The new parity test compares a router against a node over
the same rows rather than against a literal, and checks the group key is present
at all, since two identical empty answers would satisfy a comparison.

## 85. Three more from the same round

**`timeWindowURL` had no test, and reproducing the defect needed a preamble.**
The first attempt put the multipart envelope directly after the JSON document
and passed against the defect. The reason is that `json.Decoder` reads AHEAD
into its own buffer and those bytes never return to `r.Body`, so a boundary
placed right after the document is swallowed and the later parse finds nothing.
Everything before the first boundary is a legal multipart preamble, so 64 KiB of
it puts the boundary past the read-ahead — and then reverting `/select/vector`
to `r.FormValue` leaves one temp file per request, which is what the fix
prevents.

Entry 81 also had the mechanism backwards: it said `form: false` "drops the
multipart `start`/`end`". Measured, `form: false` with an `r.FormValue` in the
handler *reads* that tail and leaks, because there is no pre-parse and therefore
no deferred `RemoveAll`. Reading the URL is what stops it.

**`adminSpec().form = false` was unpinned.** No admin handler parses a form, so
the leak gate cannot see the flag either way; what is visible is the cost.
A 4 MiB multipart POST, server-side `TotalAlloc` delta:

| route | `form: true` | `form: false` |
|---|---|---|
| `/flags` | 16.96 MB | 0.16 MB |
| `/admin/backup` | **8.56 MB** | 0.14 MB |
| `/admin/acknowledge-degraded` | 16.95 MB | 0.15 MB |

Asserted with a wide margin rather than an exact number — half the body size
separates them by an order of magnitude, so it is a check and not a benchmark.

**Correction (entry 87).** This table first published `/admin/backup` at 16.97
MB, which is the other two rows' number copied across; re-measured over five
runs it is 8.56 MB. The test's own comment gave a third figure, "~13 MiB", which
matches none of them and is now the measured range. All three answer 200 in both
configurations, so the difference is the buffering and nothing else.

**Prose that stayed behind the code.** Four places still said "the two
Elasticsearch routes" after the set became three; and the LLD said the leak gate
"runs twice … with a positive control", where they are two separate functions
and only the plain one has a control. What establishes the authenticated one is
the mutation — reverting `format` to `r.FormValue` reddens it naming all three
health routes — and the LLD says that now instead.

## 86. The empty stream was reported only when it was the ONLY stream

`Streams` fell back to "every row is in the empty stream" when nothing was in a
named one. A store ingested partly with `_stream_fields` and partly without has
both, and the empty half was dropped. Measured against the staged
victoria-logs binary, six rows, three named:

| | simdlogs | victoria-logs |
|---|---|---|
| `/select/logsql/streams` | `[{svc="s0"}:3]` | `[{svc="s0"}:3, {}:3]` |
| `/select/logsql/stream_ids` | one value | two values |
| `/select/logsql/field_names` | `_stream`:6 | `_stream`:6 |

so one store gave two answers about how many rows exist: `field_names` and
`/query` counted six, `/streams` and the `_stream` facet counted three.

**The empty stream cannot be read off the `_stream` column.** A row ingested
without `_stream_fields` has no such column at all — it is absent from
`StatsByField`, not present with `""` — so the first attempt, mapping `""` to
`{}`, changed nothing and the measurement said so. Every row is in exactly one
stream, so the empty stream is the **remainder**: `Count(q)` minus the rows in
a named stream. That one expression covers the all-empty store and the mixed
store, replacing the special case that only handled the first.

Mixed ingestion is not exotic — it is what a store looks like while
`_stream_fields` is being rolled out, and what any second shipper that does not
set it produces.

## 87. The empty-stream fix replaced an under-count with an over-count

Entry 86 made the empty stream the REMAINDER: `Count(q)` minus the rows in a
named stream. It added that to the count already read off the `_stream` column,
so every row whose column is `""` was counted twice.

Both ways a row reaches the empty stream have to be handled, and entry 86 only
noticed one of them:

- **no `_stream` column at all** — ingested without `_stream_fields`, so absent
  from `StatsByField`. This is what the remainder is for.
- **a column materialized to `""`** — the store materializes the column for a
  whole flush group, so a row that never carried the field comes back with `""`
  once any row in its group did. `streamValues` already returns these.

Measured on ONE ingest request of six rows, three carrying `svc`:

| | simdlogs after entry 86 | victoria-logs |
|---|---|---|
| `/streams` | `{}`:6 and `{svc="s0"}`:3 — **nine hits over six rows** | `{svc="s0"}`:3 and `{}`:3 |
| `/stream_ids` | 6 and 3 | 3 and 3 |
| `field_names` `_stream` | 6 | 6 |

so `field_names` and `/query` said six while `/streams` said nine — the same
one-store-two-answers shape entry 86 set out to remove, with the sign flipped.
A missing number is not improved by a wrong one.

The empty stream holds exactly the rows the named ones do not, so it is an
ASSIGNMENT — `Count(q) - named` — and both paths fall out of it.

**The fixture is the finding.** Entry 86's test posted its six rows as six
separate requests, which land in six flush groups, so the streamless rows had no
column at all and the `""` branch never ran. One request puts them in one group.
The test now runs both shapes against separate stores, and its own
"hits total 6" assertion — right all along — reddens on either.

Two more from the same round:

- **`/stream_ids`'s assertion could not fail.** `StreamIDs` builds one entry per
  `Streams` entry, so comparing the two lengths is structural. Reverting the
  fix reddened three assertions and left that one green. It compares the HITS
  now.
- **`packLogfmt`'s guard was pinned by nothing.** All four subtests of the
  row-and-pack agreement test packed JSON, so `hasDuplicate` never saw a logfmt
  pack and reverting that copy left the whole suite green — while
  `pack_logfmt` emitted `_time=… c=1 _time=…`. Two logfmt cases now run through
  the same comparison, with a logfmt parser that keeps the key ORDER, since a
  map would collapse the duplicate exactly as the JSON half once did.

## 88. `limit` bounds the scan on a node and the output on a router — two different answers

Task #438, left open by entry 61 as "a cluster and a single node disagree on
which rows `sort ... | limit` keeps". It is not about `sort`. It is the
endpoint's `limit`, and it breaks four query shapes.

`limit` is `LastN`, and a node applies it to the **scan**: `Run` returns the
newest n rows newest-first and every pipe then runs on those n
(`internal/query/engine.go:425`). The router applied it after the coordinator
pipes — taking the last n of an ascending result and reversing it. Measured,
three shards of ten rows with distinct timestamps, `&limit=5`, where the newest
five all have `level=error`:

| query | node | router |
|---|---|---|
| `* \| limit 5` | 02:09..02:05 | **00:04..00:00** |
| `* \| sort by (_time)` | 02:05..02:09 | **02:09..02:05** |
| `* \| sort by (_time) \| limit 5` | 02:05..02:09 | **00:04..00:00** |
| `* \| offset 2 \| limit 3` | 02:07..02:05 | **00:04..00:02** |
| `* \| filter level:info \| sort by (_time)` | nothing | **five `info` rows** |
| `*` | 02:09..02:05 | 02:09..02:05 |
| `* \| stats count() c` | 30 | 30 |

Three of these are the **opposite rows**, not the opposite order. All at 200.

**Two defects, two fixes.** The bound moves to where a node applies it: the
merged rows are sorted newest-first and truncated to n *before* the coordinator
pipes, and the post-pipeline truncation is gone. That fixes the first four rows.

The fifth needs the plan to change. A shard that ran a filtering row-local pipe
first has already discarded rows the bound would have kept, and the router
cannot put them back — so under a bounding `limit` the push-down now stops at
the first pipe that can change the row count. `query.ChangesRowCount` names the
one-to-one pipes explicitly and **defaults to true**: a pipe added to the
language is treated as unsafe to push down, which costs a shard's full match set
and never an answer. The opposite default would make it silently eligible and
the failure would be a short answer at 200.

`* | fields _time, _msg` still pushes down with its shard `limit`, and that is
not an exception: one row in, one row out means each shard's newest n contains
the cluster's newest n. A first version forced the whole chain to the
coordinator and reddened
`TestAShardLimitThePlanKeepsIsNotSuppressedByAPostForm`, which is exactly the
row that says so.

**Why `stats` is unaffected**: `runStats` runs its own scan and never sees the
bound, which is why `* | stats count() c` answers 30 on a node with `&limit=5`
and not 5. `limitBoundsOutput` already named that set and is unchanged — only
the place it is consulted moved.

**Still open, and measured rather than assumed gone.** The node emits a field
the pipeline named in second position; the coordinator rebuilds rows from each
shard's JSON in ingest order:

```
node    {"_time":"…02:05Z","level":"error","_msg":"m","user":"u5",…}
cluster {"_time":"…02:05Z","_msg":"m","level":"error","user":"u5",…}
```

Same rows, same values, same order, different key order — for
`| filter level:error` and for `sort by (level, _time)`. It matters to a
byte-comparing consumer and not to a JSON one. The parity test therefore
compares each line as an **object**, in row order, so the row question is not
hidden behind the key question; row count, row order, and every key and value
are exact.

Thirteen query shapes are compared against a single node holding the same rows.
Four mutations redden it: removing the pre-pipeline bound (6 subtests), keeping
the merge ascending (9), pushing filtering pipes down anyway (1), and making
`ChangesRowCount` always false (1).

## 89. Two of the three reasons a watermark moved for nothing

Task #434. `checkWatermark` reports a lagging replica and does not refuse,
because the first version's 503 turned out to fire on a healthy cluster. Three
causes were named in the comment; two are now closed and one is not, and the
difference between "closed" and "named" is what decides whether the refusal can
come back.

**Closed: the watermark was the max over OPEN tenants and over data currently
HELD.** `highWatermark()` scanned `forEachTenantDetached`, so evicting a tenant
dropped it — a one-node cluster reading tenant 2 and then tenant 1 reported
itself going backwards — and retention deleting the newest rows dropped it
legitimately. It is a running maximum now, kept on the server rather than
derived per call, so what it reports is *the newest timestamp this node has
accepted*. That moves for exactly one reason.

**Closed: the history was keyed by shard INDEX.** `SetBackends` may repoint an
index at a different machine, and the new machine inherited the old one's floor
— so a freshly added, legitimately empty replica read as lagging. The entry
records **which peer** set the high, and a high set by a peer the topology no
longer lists is replaced rather than enforced. Keeping the shard key matters:
the signal wanted is cross-replica (two replicas of one shard holding 12 and 8
rows), and a peer compared only against its own history cannot show that.

**Open: a restart re-derives it from the stores that load.** A replica whose
newest data retention already deleted comes back below its sibling with nothing
wrong. The maximum is monotonic *within a process* and not across one, and
closing that needs it to be durable. `TestARestartStillLowersTheWatermark`
states the shape and says in its own failure message that when it stops holding,
the refusal can be turned back on.

Three mutations redden the tests: making the watermark non-monotonic, letting a
departed peer's floor stand, and not recording which peer set the high. The
second of those first appeared to survive — the mutation left `present`
assigned and never read, which does not compile, and a grep for `--- FAIL`
counts a build failure as zero.

## 90. The repair duplicate was a check-then-act in the store, not in the router

Task #428. The router admits one repair at a time, and its own comment said the
latch is per-process and cannot help when two routers point at the same
cluster — closing that needs the decision at the destination, the only
participant that can see it already holds the group.

The destination already had the check. `AdoptGroup` asks `hasDigest` and then
appends, and **the two steps took the store lock separately**. Measured, one
digest of four rows, callers released together:

| concurrent adopts | said adopted | groups in the store | rows |
|---|---|---|---|
| 2 | 1 | 1 | 4 |
| 4 | 2 | **2** | **8** |
| 8 | 3 | **3** | **12** |

Every loser returned `adopted=false`. A caller counting successes saw exactly
one while the store held three — which is how a repair pass reports
`complete:true, blocked:0` over a shard it has just doubled, and why the
duplication could not be seen from the router at all.

One lock across both steps. End to end, two separate routers repairing one
shard: the replica that was missing a group holds it once, and reverting the
lock reddens that test on every run of ten.

**Both passes may still report `copied>0`, and that is not the defect.** Each
decided from a state that was true when it read it, and a report is about the
decision. The rows are the assertion.

The router's latch stays. It stops one router doing the whole pass twice, which
wastes a round of fetches even when nothing duplicates — the cheap half, and no
longer the only half.

## 91. A cluster archive presented as an old node archive, and named the one flag that cannot help

Task #431, first entry off the unwired baseline. `ValidateClusterBackup`'s doc
says it is *"called BEFORE anything is unpacked"* because *"a restore that
discovers the mismatch halfway has already written some of it"* — and nothing
called it. It is the example the unwired-mechanism gate's own header cites.

It had no caller because **there is no cluster restore**. `simdlogs restore`
restores one node's archive; a cluster backup is `cluster.json` plus one tar per
shard, and nothing reads it back. So the question is not "where should the
validation run" but "what happens to the operator who has a cluster backup and
the restore command they were given".

Measured, a real two-shard cluster archive (14848 bytes: `cluster.json` 619,
`shard-0.tar` 6656, `shard-1.tar` 4608):

```
simdlogs restore -src cluster.tar -dst DIR                storage: the backup archive carries no manifest
simdlogs restore -src cluster.tar -dry-run                storage: the backup archive carries no manifest
simdlogs restore -src cluster.tar -dst DIR -allow-unverified   the same, and one empty LOCK file written
```

Nothing corrupt landed — the tar's entries are not group names, so they are
refused one at a time — and that is the whole of the good news. The message
names `-allow-unverified`, whose help text is *"restore a pre-format-1 archive,
which carries no manifest"*, and that flag fails identically. An operator was
told their archive was unverifiable and pointed at the one option that cannot
help.

`ErrClusterArchive` and `ClusterArchiveError` are raised at the tar entry, not
after the scan: the entries that follow are per-shard tars, and rejected one by
one they produce a limit or a name error whose text says nothing about what the
archive is. The error carries the manifest bytes out, and the restore command
decodes them, **validates them with `ValidateClusterBackup`**, and prints the
shard count, each shard's archive and rows, and the procedure — including that
the shard count must match, because rows are placed by a function of it.

Validated before quoted, which is the point of running it here: two archives
claiming shard 0 would otherwise be printed as a shard list an operator could
act on. Four malformed manifests are refused by name.

The gate's baseline is a ratchet in both directions, and this is the first entry
it has given back: an exemption that gains a production reader fails the gate,
so it cannot outlive its reason.

## 92. The count could be alerted on and nothing could say what it was

Task #431, second entry off the unwired baseline. `QuarantinedGroups` returns
every quarantine record — which group, why, its checksum, its size, when, and
where the file went. Its entry said *"the COUNT reaches production
(`countQuarantined` → the `simdlogs_storage_quarantined_groups` gauge); only the
LISTING has no reader"*, and that is exactly the gap: an operator could put an
alert on "one group is quarantined" and had no way to ask which one.

`/admin/storage/quarantine` serves it through `Store.Quarantined`. Admin-only,
like every other storage endpoint — the records name file paths and checksums,
which describe the shape of the data.

**Four gates fired on the new route before it worked**, which is the route
surface doing its job:

```
/admin/storage/quarantine is registered but not classified
/admin/storage/quarantine has no contract and no exemption
docs say "46 routes" and the mux registers 47   (twice, two documents)
surfaceRoutes() classifies 46 and the mux registers 47
```

and then a fifth caught a real defect: `tenantPaths` is an explicit allow-list,
and without an entry `s.tn(r)` had nothing to return, so the handler panicked
into a 500. `TestNoRouteAnswersWithAServerErrorOnAStorageNode` named it.

Two behaviours are worth stating because they were chosen rather than fallen
into:

- **`[]`, never `null`.** A client that distinguishes them reads `null` as "this
  node cannot say" and an empty list as "nothing is quarantined". Those are
  different answers, and the encoder's default for a nil slice is the wrong one.
- **A router refuses.** Its own store quarantines nothing, so an empty list
  there reads as "nothing is wrong" about shards it has not asked.

Three mutations redden it: answering `null`, letting a router answer, and making
the store report nothing.

## 93. Three shapes of "unwired", and only one of them was a defect

Working down task #431's baseline turned up that "no production reader" is three
different findings wearing one label, and the right answer differs for each.

**A mechanism with no reader — wire it.** `ValidateClusterBackup` (entry 91) and
`QuarantinedGroups` (entry 92). Both had an operator-visible gap behind them.

**A name that lies about its category — rename it.** `SetMaxRows` and
`SetDirRereadInterval` were listed as "a dead exported setter". They are not
dead: fourteen tests call them, and production sets both through config
(`-search.maxRows`, `-readiness-reread-interval`). They are test hooks whose
names did not say so, which is why they read as unwired at all — the baseline
already has a category for exactly this, holding `SetFaultHookForTest` and
`FailAt`. `…ForTest` on both, and they move from "genuinely unwired" to
"deliberate", which is what they always were.

**A constant nobody writes — delete it.** `FieldRequestID`, `FieldStatus`,
`FieldDurationMS` and `FieldRows` are log-field names, and no log line uses any
of them. The block's own comment says field names are constants so that `tenant`
in one file and `tenant_id` in another cannot become two fields — and a name
nobody writes cannot cause that drift, while its presence tells a reader those
fields are in the logs. Their only other references in the module were their own
exemption entries. The day a request line needs `duration_ms`, the constant is
one line and will then be true.

The baseline is 16 entries down to 11. It is a ratchet in both directions, so
none of these can come back quietly: an exemption that gains a reader fails the
gate just as a new name without one does.

## 94. Two superseded paths, kept alive by the tests that tested them

Task #431, continued. Both entries said "superseded" and both were true, and
neither was reachable from production — the question each poses is what happens
to the tests that keep it compiling.

**`SnapshotAll` was a two-line wrapper that threw away the number.** It calls
`SnapshotAllWithSeq` and drops the manifest sequence — the value that makes a
snapshot verifiable, and the reason the `WithSeq` form exists at all: reading
the sequence in a second lock acquisition gives a different number, and the
archive then declares a watermark covering a group it does not contain. Its four
callers are tests, and every one of them wanted the pair anyway. Deleted, and
they take it.

**`RestoreTar` is the superseded UNSTAGED restore**, replaced by Task 5.2's
staged one, and its own doc says so — *"The files are already written in that
case -- this is the unstaged restore"*. Twelve callers, all tests, and eight of
them are not testing restoring at all: they use it as the harness for
`readBackup`'s entry-by-entry validation (size, checksum, `ReadGroup` parse,
ordering, the terminator). That harness is worth keeping and is not worth
reaching through a staged restore, which would test the staging too.

So it moved into a `_test.go` file, where production cannot call it, and the two
callers in `internal/api` moved to `Restore` — which is what a real restore
does, and both passed unchanged.

That is a fourth shape for entry 93's list: **a superseded API that survives as
a test harness — scope it to tests rather than delete or keep**. The baseline is
16 entries down to 9.

## 95. The unwired baseline is empty, and none of it was one kind of thing

Task #431 is closed. The gate's exemption list is 16 entries down to **8**, and
all eight are deliberate test hooks that say so in their names. The "genuinely
unwired" section is empty.

What the sixteen actually were, which is the finding worth keeping:

| shape | what to do | which |
|---|---|---|
| a mechanism with no reader | **wire it** | `ValidateClusterBackup`, `QuarantinedGroups` |
| a name that lies about its category | **rename it** | `SetMaxRows`, `SetDirRereadInterval`, `routeCount`, `ExecuteCount` |
| a constant nobody writes | **delete it** | `FieldRequestID`, `FieldStatus`, `FieldDurationMS`, `FieldRows` |
| a superseded API kept alive by its own tests | **scope it to tests** | `RestoreTar` |
| a wrapper that drops what makes the call safe | **delete it** | `SnapshotAll` |
| dead code that reads as a live endpoint | **delete it** | `readiness` — 51 lines, no caller, while `/-/ready` goes elsewhere |
| a deliberate test hook | **keep, and say so** | the eight that remain |

Only two of the sixteen were the defect the gate was written for. Six were names
that made a test-only helper look like production API — which is not nothing:
that is exactly what made them read as unwired, and a reader scanning for what
production uses was misled by every one.

Two of the deletions closed a real hazard rather than tidying. `SnapshotAll`
let a caller take a snapshot without the manifest sequence, and the sequence is
what stops an archive declaring a watermark covering a group it does not
contain. `readiness` was a whole superseded handler that an editor's jump-to
would land on before the live one.

The list ratchets both ways, so none of this can come back quietly: an
exemption that gains a production reader fails the gate, and a new name without
one fails it too.

## 96. The watermark needed a generation, not durability

Task #434 asked for a durable watermark so lag could be refused again. It did
not need one. What the refusal was missing was never persistence — it was the
ability to tell a **restart** from **lag**, and one header settles that.

The watermark comparison had been demoted to a log line because three benign
causes each looked exactly like a lagging replica:

| cause | why it lowered the watermark | closed by |
|---|---|---|
| a per-query maximum | a narrow query saw fewer groups than a wide one | node-level running maximum (`s.hwOwn`, CAS) |
| a different replica answering | replica B's floor compared against replica A's | recording `peer` with the floor |
| the node restarted | the in-memory maximum started over at 0 | **this entry** |

The first two are properties the node can fix about itself. The third cannot
be: after a restart the node's watermark is genuinely lower, and no amount of
care on the reader's side distinguishes that from a replica that has fallen
behind. Durable state was the obvious answer and the wrong one — it makes every
node's correctness depend on a file surviving, for a fact that expires in
milliseconds.

A **per-process generation** is enough. `newNodeGeneration()` (crypto/rand hex,
time+pid fallback) is stamped into the `Server` at construction and travels on
every peer envelope as `X-Simdlogs-Generation`. `observe` then has evidence:

```go
if peer == h.peer && gen != "" && h.gen != "" && gen != h.gen {
    prev = h.hw
    h.hw, h.peer, h.gen = hw, peer, gen
    return false, prev, true // a restart, not lag
}
```

Same peer, different generation, lower watermark: that is a restart, and the
floor is re-based rather than refused. Same peer, same generation, lower
watermark: nothing benign explains it, and the read is refused —
`p.Complete = false`, which surfaces as 503, with `allow_partial_response=1`
as the recoverable 206.

**The fourth guard is the one that makes it deployable.** `observe` returns
`certain`, and `checkWatermark` refuses only when `certain`. Either side
predating the header leaves `certain` false, so during the first half of a
rolling upgrade — when every peer is an older build sending no generation — a
fall is logged and never refused. A false 503 across a whole cluster is worse
than the short answer it would be preventing.

Each guard was probed by mutation, and the fourth is why this entry has a
number. Three of them redden the suite when removed:

| mutation | result |
|---|---|
| never refuse (`p.Complete` untouched) | 3 tests red |
| a restart counts as lag (`if false`) | 3 tests red |
| no generation on the wire | 3 tests red |
| **refuse without evidence (drop `certain`)** | **green** |

Green, because every fixture carried a generation — `certain` was true
everywhere and the guard could not be seen. The guard that only matters during
an upgrade is invisible to a suite where nothing is mid-upgrade.
`TestAPeerWithNoGenerationIsReportedNotRefused` builds the older node's
envelope by hand, omitting exactly one header, and the fourth mutation reddens.

What the fixtures mean now, which is the other half of the change:

| fixture | before | now |
|---|---|---|
| a replica whose watermark falls within one generation | logged | **503**, 206 with `allow_partial_response=1` |
| a peer that restarts (new generation) | logged | tolerated, floor re-based |
| a restarted peer's **sibling** falling | logged | still refused — a restart excuses one peer |
| a peer sending no generation | logged | logged |
| an unchanged watermark | logged | not lag |
| a replaced machine | logged | does not inherit the old floor |

The superseded claim, corrected here rather than left standing: "#434 needs a
durable watermark before lag can be refused (a restart still lowers it)". It
does not. It needs to know which process answered.
