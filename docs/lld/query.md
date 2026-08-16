# LLD: query

Source: `internal/query/` (logsql, engine, pipes, fastpipes, parallel, bitset,
filter, subquery, stream, stats_range, sql, vector, time_filter, hll, math).

## The Query

`engine.go`. One `Query` is a time window `[From, To)`, a conjunction of
`Pred`s or a boolean `Filter` tree, an optional `Pipes` chain, and output
controls:

- `Limit` — the `| limit N` pipe: the FIRST n matches, forces the serial path.
- `LastN` — the endpoint `limit` parameter: the n most RECENT matches, newest
  first (what a log viewer shows; the reference draws the same distinction).
- `MaxRows` — a cap that errors rather than truncates; it only has to DETECT
  overflow, so it keeps the parallel scan.
- `MatCols` — materialize every COLUMN (a stats or by-pipe row needs every
  field the pipeline touches). `MatAll` — the full-RECORD output the API reads
  as `withStream`, which adds `_stream` and `_stream_id`. The two were one flag,
  and setting it for a stats query put `_stream`/`_stream_id` onto stats rows
  that have no stream.
- `Now` — request time for relative `_time:` filters.

## Parsing

`logsql.go`. The grammar: `AND` (juxtaposition or keyword), `OR`, `NOT`,
parentheses; `field:value` (equality), `field:="exact"`, `field:~"re"`
(regexp or substring), numeric `>N >=N <N <=N`, `in(a,b,c)`, `val*` prefix,
`*sub*`/`*sub` substring, `_stream:{k="v",k=~re}` selectors, `!term`, and a
bare word as substring on `_msg`. A flat conjunction lowers to `Query.Preds`
(the lean path); any OR/NOT/parentheses becomes a `Filter` tree
(`Expr`: `OpLeaf`/`OpAnd`/`OpOr`/`OpNot`).

Predicate kinds (`PredKind`, engine.go): equality, contains, regexp, numeric
comparisons, set membership, prefix, numeric range, length range, string
range, case-insensitive contains, ordered sequence, IPv4 range, field-vs-field
comparisons, and the time predicates (range, day-range, week-range) plus
`_stream_id` equality. LogsQL math-interval notation is honoured:
`range(a, b)` excludes both ends, `range[a, b]` includes them (measured
against VictoriaLogs).

Pipes (`pipes.go`, `pipes_more.go`, `pipes_introspect.go`,
`pipes_vlparity.go`): `stats` (count, sum, avg, min, max, uniq, count_uniq,
count_uniq_hash, quantile, values, uniq_values, sum_len, count_empty,
row_any, histogram, row_min, row_max, rate, rate_sum; `by (...)` grouping;
`if (<filter>)` conditional sampling), `sort`, `limit`/`head`, `fields`,
`uniq`, `top`, `tail`, `offset`, `rename`, `delete`/`drop`, `filter`/`where`,
`unpack_json`, `unpack_logfmt`, `unpack_syslog`, `unpack_words`, `extract`,
`extract_regexp`, `format`, `rank`, `replace`, `replace_regexp`, `copy`,
`len`, `drop_empty_fields`, `collapse_nums`/`pattern`, `math`/`eval`,
`decolorize`, `pack_json`, `pack_logfmt`, `sample`, `first`, `last`,
`unroll`, `json_array_len`, and the introspection pipes `field_names`,
`field_values`, `facets`, `blocks_count`, `block_stats`. The alias layer
(`keep`, `order`, `mv`, `cp`, `del`/`rm`, `skip`) and the full inventory are
in `docs/vl-parity.md` (status: complete, tiers 0–5).

`stats` is special: it aggregates during the group scan and never
materializes the matched rows. `runCountFast`, `runTopFast`, `runUniqFast`
(fastpipes.go) short-circuit whole classes straight to the bitset path.

## Execution

`Run` (engine.go):

1. Resolve relative time predicates against `q.Now`.
2. `store.Groups(from, to)` — group skip from the in-memory index.
3. `groupCanMatch` — footer bloom + dict binary search rejects groups that
   cannot match, before any column decode. For a filter tree it prunes on the
   AND-of-equality leaves only (an OR branch or non-equality leaf could still
   match).
4. `LastN > 0` → `runNewest`: walk groups newest backwards, keep each group's
   last n, stop when no older group can beat the oldest kept row.
5. ≥ `parallelMinGroups = 4` surviving groups and no `Limit` → `runParallel`
   (parallel.go): one goroutine per worker over a channel of group indices,
   results merged in group order — identical output to serial, asserted by a
   differential test. Below 4, the goroutine overhead outweighs the win (a
   needle touches one or two groups). The merge sizes its result from the
   finished parts before copying: every worker has returned by then, so the
   total is known, and growing by `append` instead reallocated the whole
   result log2(groups) times. It still answers nil, not an empty slice, when
   nothing matched — the serial path does, and the two are compared.
6. `appendMatches` per survivor group:
   - bitset `SetAll`, then the time mask: when the group straddles the window,
     `TimeRangeMaskInto` skips whole 512-row blocks from the checkpoint
     header and decodes only the boundary blocks;
   - predicate bitsets: `Eq` picks by selectivity — count ≤ n/8 reads the
     posting list directly (`EqualityRows`), otherwise the vectorized
     residual scan (`simd.EqualScalarInto` + `MaskBits` pack, `eqMaskInto`);
     every other kind marks which dict values match (the test runs once per
     distinct value, not per row) and maps rows through the indices;
   - `cnt == 0` → never decode the timestamps; bounded queries trim the
     bitset (`KeepFirst`/`KeepLast`) so decodes stay within the returned
     set, and the timestamp decode span is narrowed to the first and last
     surviving row — a bounded query decodes only the rows it keeps (the
     facets path used to decode a whole 131072-row group to materialize a
     thousand; `1a85d8a`, wrong.md entry 35);
   - timestamps: point-read via checkpoint blocks when matches are sparse
     (`cnt*512 < span`), else span-decode into a buffer from the
     `tsscratch.go` pool, returned before `appendMatches` does — every time
     read is copied into a `Row.Time` first, and `histoGroup` borrows the
     same way for its buckets;
   - materialization decodes each referenced column once (only the dict
     values matched rows reference — `DictDecodeSome`) and indexes into it;
     ≤ 64 matches take the direct per-row path (arena setup does not
     amortize); larger sets use one `Field` arena for the whole group.

`Count`, `Histogram`, `Hits` (engine.go) skip materialization entirely —
group-skip, predicate bitsets, popcount. `Hits` buckets into
step-aligned buckets, empty buckets present with zero counts (a graph needs
the gap drawn), capped at `maxHitsBuckets = 100K`.

## Cancellation and budgets (`executor.go`)

Go does not abort a running handler when its request context is cancelled. A
client that hangs up, a proxy that times out, a `-max-query-duration` that
elapses — none of them stop a scan already walking groups. Before this the
`Deadline` and `MaxBytes` fields on `Query` were the only thing that could end
one, which is why they exist and why their own comment named this as the
missing half.

**Cancellation is threaded into the engine, not wrapped around it.** The scan
already had a per-group checkpoint — `q.exceeded(bytes)`, called after every
group in every scan loop and from every parallel worker — and that is where the
context is read. Every call site that already stopped for a byte budget now
also stops for a cancelled context, a deadline, a memory ceiling or a group
ceiling, with no second check to remember at any of them. `RunPipeline` adds
one more between pipes: a sort or a join over a large result is a phase with no
group boundary in it, so a query cancelled during one used to run to completion
and then discover nobody was waiting.

**The reason is recorded, not returned.** `exceeded` returns a bool and the
scan functions return rows; threading an error through all of them would touch
the package for a value only the top of the stack reads. The FIRST stop records
why, on the Query, and the caller reads it after. First, not last: a cancelled
context and an exhausted budget can both become true while the scan unwinds,
and reporting the second sends the caller to fix the wrong thing. The reason
lives behind a *pointer* to an atomic, like `Stopped`, because a Query is
copied by value in four places — subqueries, introspection, the executor — and
sharing it across the copy is also the behaviour wanted: a cancelled parent
stops its subqueries.

`Executor{Store, Limits}` with `Execute(ctx, q, sink) error` is the API for new
callers. `Query.Bind(ctx, Limits)` is how the existing ones get cancellation:
the HTTP layer has eighteen sites that build a Query and call the engine, and
converting them all in one change would rewrite every read endpoint for a
benefit `Bind` delivers on its own.

**Typed causes, stable statuses.** Each is a different thing for a client to do
about it:

| error | status | why |
|---|---|---|
| `ErrCanceled` | 499 | the client went away; nothing reads the body |
| `ErrDeadlineExceeded` | 504 | transient, retryable |
| `ErrRowLimit`, `ErrByteLimit`, `ErrTooManyGroups` | 413 | the request asks for more than the server gives; retrying it unchanged cannot succeed |
| `ErrMemoryLimit` | 503 | about this server now, so retryable |
| `ErrRejected` | 429 | admission control (task 6.2) |

Every budget used to answer **504** whatever caused it, so a client that
disconnected, one that asked for too many bytes and one that ran out of time
were indistinguishable in an access log — and a client retrying a 504 against a
byte budget retried forever. `TestQueryBudgetsBoundTheScanNotOnlyThePrefilter`
moved from 504 to 413 with that change; it is a visible contract change.

`MaxGroups` fires after the time window and the footer prune and before a
single column is decoded — the only limit that protects the machine rather than
the answer. `MaxMemory` shares the byte accounting, which is gated on
`countsBytes()`: with only the memory ceiling set, the running total stayed at
zero and the ceiling could never be reached, which is a limit that is
configuration nothing reads.

## Limits are semantics-preserving

A budget may **refuse** a query. It may not answer a different one.

That was broken in exactly the way it is easy to miss. `selectQuery` set
`MaxRows` for every non-projecting pipe chain — `PipesProject` is false for
`sort`, `offset`, `limit`, and every row rewriter (`delete`/`rename`/`copy`/
`format`/`math`/`extract`/`unpack_*`/`filter`/`replace`) — and reported the
overflow for exactly one shape, a bare select. So `* | sort by (x)` over an
oversized result had its input cut by the scan, sorted the prefix, and returned
**200 OK**. A sort of the first N rows is not the first N of the sort; a
`| offset` skipped into a truncated set; a `| join` joined against one. Nothing
in the response said so.

The cap is now recorded where it trips, not left to the caller to notice:
`Run` and `runParallel` call `q.stop(ErrRowLimit)`, `stopErr()` carries it, and
the HTTP layer reports 413 for **every** shape. An explicit LogsQL `| limit N`
as the first pipe is pushed into the scan, which then stops at N by
construction — bounded semantics, so it is answered rather than refused.

`-search.maxRows` overflow answers **413**, not the 400 it used to. The request
is well-formed and asks for more than the server will give, which is what 413
says; it is also the code every other row, byte and group ceiling already used,
so a client could not previously tell this refusal from a malformed query.

### The ceilings nothing else measured

| limit | bounds | why the others do not cover it |
| --- | --- | --- |
| `-search.maxGroupKeys` | distinct `by` keys in `stats`/`uniq`/`top` | `maxRows` counts scanned rows, of which a high-cardinality aggregate may read few; `maxQueryBytes` counts materialized row bytes, which an aggregate does not accumulate. The map is proportional to the key space alone. |
| `-search.maxPipeRows` | rows one pipe may produce | a left join on a key that is not unique on the right multiplies: two results each inside `maxRows` become an output no budget covered. Union appends for the same reason. |

Both are checked as the state is BUILT, not on the finished result — noticing
after the map exists is noticing after the cost. The three aggregate fast paths
(`runCountFast`, `runTopFast`, `runUniqFast`, and the leading
`stats by (f) count()` shortcut) read the footer's posting counts and never
build the accumulator map, so they carry the same ceiling: a bound written only
on the map would have covered the path that is *not* taken for the common
single-field shape.

`stream_context` capped its context scan with a `Limit` and computed context
over the first two million rows of the window. A neighbour that exists was
dropped because rows elsewhere in the window spent the budget — a wrong answer
the caller could not see. It is a `MaxRows` now, and the pipe errors. Its
overflow is translated to `ErrPipeRowLimit` rather than reported as
`ErrRowLimit`: the caller's row budget is not the knob, the window is.

`applyBudget` now shares the parent's context and stop-reason pointer with
subqueries, so the first stop anywhere in the query tree is the one reported.
Before, a subquery recorded its reason on a `Query` the caller threw away and
every cause surfaced as the generic "time or byte budget" — including a
cancelled client, which is not a budget at all.

`runSub` also materializes whole records unless the subquery's own chain
projects, by the same rule the top-level select uses. It did not, so sub rows
carried `_time` and nothing else and `join by (f) (<sub>)` computed its key
from an absent field: every key was the empty one, nothing matched, and the
join returned the outer rows unchanged. A left join that never joins looks
exactly like a left join with no matches, which is how it survived.

## Streaming a bare select (`iterator.go`)

`Run` returns `[]Row`: every matching row is in memory before the first byte
reaches the client, so peak memory is the size of the ANSWER rather than of the
working set — and an answer that does not fit is not slow, it is impossible.
`-search.maxRows` was the response to that: refuse a bare select whose result
exceeds a cap. The right refusal to have and the wrong thing to need, because
the query it refuses is one the machine could answer a group at a time.

`ScanEach(store, q, fn)` walks matching rows in group (time) order and hands
each group's rows to `fn`. Peak rows held is one group's matches, or a bounded
window of them when the scan fans out — regardless of how many rows match. The
slice `fn` receives is only valid for the call: the serial walk reuses one
backing array, which is what makes a million-row select allocate like a
one-group one. `wg.Wait` is **deferred**, not a statement — as a statement it
was skipped on a panic or `runtime.Goexit` out of the sink while `defer
sn.Close()` still ran, so the mapping went away with producers inside it. A
sink that panics over 64 groups segfaulted 5/5 runs before that was a `defer`.

On the streamed path `MaxBytes` and `MaxMemory` bound the **working set** — the
current batch — not a running total. They coincide on the materialized path,
where every row produced is still in the answer; on a stream the rows are
handed to the sink and released. Accumulating meant a walk holding 8 KiB at a
time was refused by a 32 KiB ceiling, and over HTTP (where the same counter is
`-search.maxQueryBytes`) a large answer got 200, a truncated body and
`unexpected EOF` — where the materialized path had returned a clean 413 with
nothing on the wire. Streaming made the failure worse than the thing it
replaced. A stream is therefore unbounded in *total* bytes, which is the point;
the wall-clock deadline still bounds it, so with `-search.maxRows=-1` an answer
a client cannot drain within `-search.maxDuration` is cut off mid-body.

`Streamable(q)` is the whole decision, and it refuses three things:

| refused | why |
| --- | --- |
| any pipe | `stats`/`sort`/`uniq` are defined over the whole result — a sort cannot emit its first row until it has seen its last. The row-wise pipes are applied by `RunPipeline` to a materialized slice, so streaming past them means re-implementing each as an operator. |
| `LastN` (`limit=`) | `runNewest` walks groups BACKWARDS and keeps each one's last n; its order is not group order and its early stop depends on rows already kept. |
| `MaxRows` in force | the cap must be decided before the first byte; a stream that discovered the overflow at row n+1 has already sent n rows. Those stay materialized — the cap is what makes materializing them safe. |

So over HTTP the streamed path is exactly the uncapped bare select
(`-search.maxRows=-1`), which was the configuration with no bound at all.
`simdlogs_query_streamed_total` counts them; the two bodies are byte-identical
by construction, so the counter is the only way to tell which path answered.

Above `parallelMinGroups` the walk stays parallel. One channel per group,
handed to the consumer in group order through a window of `workers` slots: a
producer cannot start group n+workers until the consumer has taken group n, so
the window is the memory bound and the order needs no sort at the end. A serial
walk would have made big uncapped selects slower than they were, which is
memory bought with throughput.

**Failure after the first byte.** An NDJSON body that stops early parses line
for line and carries no length and no terminator, so a short answer is
indistinguishable from a complete one. `streamSelect` tracks whether any byte
reached the `ResponseWriter`: before that a failure is a normal status with the
normal remedy text; after it, the handler panics with `http.ErrAbortHandler`
and the client sees a broken stream. `wg.Wait` before returning is not
tidiness — the snapshot is unmapped when `ScanEach` returns, and a producer
still reading a group's blob after that is a use-after-unmap.

## Admission and the worker budget (`budget.go`)

Two separate questions with two separate mechanisms.

**How many workers a running scan may use.** Three fan-out sites in
`parallel.go` sized themselves at `runtime.GOMAXPROCS(0)` — the right number
for one query on an idle machine and the wrong one for every other case: ten
concurrent queries on a 32-core box spawned 320 workers for 32 cores, all doing
memory-bound column decode and evicting each other's cache lines. They now draw
from a process-wide `WorkerBudget` installed by the server
(`-max-scan-workers`, default `GOMAXPROCS`). `Acquire` never blocks — a query
that waited for its full share would stall behind another scan for no gain, it
can make progress with fewer — and never grants zero, because a scan with no
workers cannot finish.

That floor is **per caller**, not one in total: with the budget empty every
concurrent acquirer takes its own floor, so 500 callers against a budget of 2
report 500 in use. What bounds it is the class semaphore — at most
`MaxConcurrentQuery + MaxConcurrentWrite` scans exist at once — not this
budget. `simdlogs_scan_workers_in_use` can therefore legitimately exceed
`simdlogs_scan_workers_total`, and the excess is the count of scans that found
the budget empty.

**Whether a query starts at all.** `Admission` bounds reads **per tenant**
(`-max-queries-per-tenant`, default 16). A process-wide limit alone lets one
tenant fill it: the tenant with the most aggressive dashboard takes every slot
and everyone else sees 429 for work the server had room to do.

Per tenant and *only* per tenant. The first version also had a global gate,
and nothing ever configured it — `NewAdmission` was called with `MaxPerTenant`
and `Wait`, never `MaxConcurrent`, so the channel was always nil. Everything
hanging off it was dead: `Stats` reported `len(nil chan)` = 0 in flight however
many queries were admitted, `-query-queue-wait` was read only on the branch
that returned at `global == nil` (a refusal came back in 7 µs with the wait set
to an hour), and `ErrQueueTimeout` could not be produced at all. The
process-wide gate is the class semaphore in `middleware.go` and always was; a
second one in front of it is two numbers an operator has to keep consistent for
no gain. The queue wait moved to where the waiting happens, and a queued query
holds **no** slot — it used to take the tenant slot and then wait, so
`MaxPerTenant` bounded "in flight or queued" while its own doc said "in
flight".

Applied in `guard` (`middleware.go`) after the class semaphore, for every read.
The key is **classed**: a live tail draws from `tail\0<tenant>` and an ordinary
read from `<tenant>`, so each class has its own pool. Two contracts meet here
and one key cannot serve both — a tail is open for hours by design and must not
consume query slots, and a tail must not be exempt either, being the
longest-lived read the server has. It *was* exempt, on a `!spec.stream` clause
justified by the argument for exempting writes.

Writes are never refused by it: an ingest request does not hold memory for the
length of a scan, and dropping data an agent cannot re-send is a worse failure
than a slow query.

| Outcome | Error | Status |
| --- | --- | --- |
| row cap exceeded | `ErrRowLimit` | 413 |
| aggregate cardinality | `ErrTooManyGroupKeys` | 413 |
| a pipe produced too many rows | `ErrPipeRowLimit` | 413 |
| tenant at its limit | `ErrRejected` | 429 |
| queue wait elapsed | `ErrQueueTimeout` (wraps `ErrRejected`) | 429 |
| client hung up while queued | `ErrCanceled` | 499 — **not** counted as a rejection; the server did not decline it |
| execution deadline | `ErrDeadlineExceeded` | 504 |

A queue timeout is 429 and not 504: the server never started the query, so
nothing timed out except the wait for permission, and 504 would send a client
to shorten a time range that was never scanned. `-query-queue-wait` defaults to
0 — refuse immediately, which is right for an interactive endpoint, and a queue
deeper than the client's patience does work nobody collects.

`/metrics` exposes `simdlogs_scan_workers_total`, `_in_use`,
`simdlogs_query_admission_in_flight`, `_queued` and `_rejected_total`.

## Streams

`stream.go`. A stream is a distinct `_stream` label set `{k="v",...}` (keys
sorted). `StreamID` is a 128-bit hash (fnv128a) in the reference's
48-hex-character form with the 16-zero tenant prefix; ids are computed inside
one tenant's store, the scope they are compared in. `EmptyStream = "{}"` is
the stream of rows with no configured stream fields. The stream endpoints
back onto `StatsByField("_stream")`.

## Stats endpoints

`stats_range.go`. `StatsQueryInstant` (the Prometheus vector envelope; the
`time` param names the window end — an instant query, not a range one) and
`StatsQueryRange` (matrix, step from the `step` param or 1/30 of the range).
Both require a stats pipe in the query (`errNotStats` otherwise); the server
falls back to the `by=` group-by extension (a key this server owns — the
reference has no equivalent, so it does not pretend to be a Prometheus
vector).

## Subqueries and joins

`subquery.go`. `join`, `union`, `in (subquery)` (resolved at run time,
`Pred.Sub`), `stream_context` (bounded by `streamContextCap = 2,000,000` rows,
or `-search.maxPipeRows` when that is smaller).

### stream_context is scoped to the stream

`before N after N` returns the N rows around a match **in the match's own
`_stream`**. Scoped to the query window instead — which is what it did — the
neighbours of an error on one host are whatever other hosts happened to write
at the same moment: on a busy server `before 5 after 5` returned ten lines from
ten unrelated processes and none of the ten from the process that failed. Not a
smaller answer, a different one, and indistinguishable from a correct one.

A row with no stream fields configured is in `EmptyStream`, which is one
stream, so an unconfigured deployment gets the window-scoped behaviour it had —
because there genuinely is only one stream to scope to.

Matches are re-identified in the context scan by their **content** — timestamp
and every field — not by timestamp. Matching by timestamp meant any row written
in the same millisecond as a match got its own N lines of context, and a row in
a *different* stream at the same instant pulled in a second stream's worth of
rows nobody asked for.

The context scan goes through `ScanPage` (see `order.go`), so equal timestamps
have the defined `(time, group id, row index)` neighbour order rather than
whichever the scan happened to produce. A match at its stream's edge is clamped
to that stream, never padded from the window. Overlapping ranges collapse: two
matches three rows apart with `before 5` share most of their neighbours, and
each shared row is emitted once, in time order.

## SQL and vector surfaces

`sql.go`: a `SELECT` subset (`*`/columns/aggregates, `WHERE` with
`=`/`!=`/`<>`/`<`/`>`/`<=`/`>=`/`LIKE`, `GROUP BY`, `ORDER BY`, `LIMIT`)
translates to LogsQL and runs through the same engine; anything outside the
subset is a clear error, never a silent wrong answer.

`vector.go`: brute-force exact cosine k-NN over a bring-your-own embedding
column, `k` default 10, results carry `_score`. An ANN index is a future
optimization, not a claim.

The candidates go through a **bounded min-heap of exactly k**, not a
collect-everything-and-sort. The sort was never the problem; the slice was. A
window holding ten million embeddings built a ten-million-entry candidate list
to return ten rows, so memory was proportional to the corpus and not to the
answer, and `k` — the one thing a caller controls — had no effect on it. The
heap's root is the WEAKEST kept candidate, so a new score is decided in one
comparison and nearly every row on a large corpus touches nothing. Rows are
materialized for the k survivors only.

Four ceilings, because they bound four quantities and a limit only limits the
quantity it is expressed in:

| flag | bounds |
| --- | --- |
| `-search.maxVectorK` | the answer's size |
| `-search.maxVectorDim` | the cost of one comparison (the query vector is client-supplied) |
| `-search.maxVectorCandidates` | how many stored vectors are scored — the top 10 of a billion is a small answer and a billion comparisons |
| `-search.maxQueryBytes` | the materialized result: ten rows carrying megabyte payloads is small by every other measure |

A refusal is recorded on the query's stop reason as `ErrVectorSearch`, which
wraps `ErrRowLimit` so the existing 413 mapping covers it. The handler reports
it **before** setting `Content-Type` and taking a writer — it used to commit
the response to NDJSON before the search ran, so a budget stop wrote its status
into a response already on the wire.

## Parallelism summary

- Ingest: sharded parse (`NumCPU/3` writers) + flush pool (`min(4, NumCPU)`).
- Query: per-group worker pool at ≥ 4 groups, drawn from the process-wide
  `WorkerBudget` rather than sized at `GOMAXPROCS` per query; Count/Histogram
  have their own parallel paths (partial maps/counts merged at the end).
  `ScanEach` keeps the same fan-out behind a bounded in-order window.
- Cluster: fan-out per shard with one live replica each (see
  `lld/cluster.md`).
