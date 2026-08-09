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
