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
