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
