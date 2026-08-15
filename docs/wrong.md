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
- **`TestBackupIsCompleteUnderConcurrentRetention` was a measured no-op.** Over
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
