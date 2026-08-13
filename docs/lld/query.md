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
- `MatAll` — materialize every column (bare selects, live tail).
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
   needle touches one or two groups).
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
     bitset (`KeepFirst`/`KeepLast`) so decodes stay within the returned set;
   - timestamps: point-read via checkpoint blocks when matches are sparse
     (`cnt*512 < span`), else span-decode;
   - materialization decodes each referenced column once (only the dict
     values matched rows reference — `DictDecodeSome`) and indexes into it;
     ≤ 64 matches take the direct per-row path (arena setup does not
     amortize); larger sets use one `Field` arena for the whole group.

`Count`, `Histogram`, `Hits` (engine.go) skip materialization entirely —
group-skip, predicate bitsets, popcount. `Hits` buckets into
step-aligned buckets, empty buckets present with zero counts (a graph needs
the gap drawn), capped at `maxHitsBuckets = 100K`.

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

`subquery.go`. `join` (stream-scoped), `union`, `in (subquery)` (resolved at
run time, `Pred.Sub`), `stream_context` (bounded by `streamContextCap =
2,000,000` rows). `stream_context` runs the parent's filter over the window
expanded by the before/after rows.

## SQL and vector surfaces

`sql.go`: a `SELECT` subset (`*`/columns/aggregates, `WHERE` with
`=`/`!=`/`<>`/`<`/`>`/`<=`/`>=`/`LIKE`, `GROUP BY`, `ORDER BY`, `LIMIT`)
translates to LogsQL and runs through the same engine; anything outside the
subset is a clear error, never a silent wrong answer.

`vector.go`: brute-force exact cosine k-NN over a bring-your-own embedding
column, `k` default 10, results carry `_score`. An ANN index is a future
optimization, not a claim.

## Parallelism summary

- Ingest: sharded parse (`NumCPU/3` writers) + flush pool (`min(4, NumCPU)`).
- Query: per-group worker pool at ≥ 4 groups; Count/Histogram have their own
  parallel paths (partial maps/counts merged at the end).
- Cluster: fan-out per shard with one live replica each (see
  `lld/cluster.md`).
