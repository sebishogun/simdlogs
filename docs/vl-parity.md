# VictoriaLogs LogsQL parity — full inventory and plan

Source of truth for closing the LogsQL gap. Every VL LogsQL surface element is
listed as **[done]** or **[todo]** with its design. Authoritative "done" set is
the parser: `parsePipes` and `aggKind` in `internal/query/logsql.go`, `PredKind`
in `internal/query/engine.go`, the `mux.HandleFunc` table in
`internal/api/server.go`. Update this file as items land.

## Implemented

- **Pipes**: stats, sort, limit/head, fields, uniq, top, tail, offset, rename,
  delete/drop, filter/where, unpack_json, unpack_logfmt, unpack_syslog,
  unpack_words, extract, extract_regexp, format, rank, replace, replace_regexp,
  copy, len, drop_empty_fields, collapse_nums/pattern, math/eval, decolorize,
  pack_json, pack_logfmt, sample, first, last, unroll, json_array_len.
- **Stats**: count, sum, avg, min, max, uniq, count_uniq, count_uniq_hash,
  quantile/median, values, uniq_values, sum_len, count_empty, row_any.
- **Filters**: `=` exact, phrase, `*` prefix/substring, `~` regex, `<,<=,>,>=`
  numeric, `in(a,b,c)`, `range()`, `len_range()`, `string_range()`, `i()`.
- **Beyond VL** (VL has none of these): SQL over logs, ranked full-text index,
  HLL/t-digest/top-k sketches, tiered object storage, pattern mining, alerting,
  Grafana datasource, semantic/vector search.

## Remaining, tiered by cost

Cost tier = how much it touches beyond a local dict/row transform. Implement in
tier order; within a tier, order is free.

### Tier 0 — trivial aliases (1 task)

VL accepts short aliases for pipes we already have. Add each as an extra `case`
label in `parsePipes` next to the existing pipe.

- `keep` → fields · `order` → sort · `mv` → rename · `cp` → copy ·
  `del`/`rm` → delete · `skip` → offset.

Risk: none. Just alternate spellings.

### Tier 1 — dict/row-local, no engine-model change (3 tasks)

These evaluate per dict value or per row, exactly like the filters/stats already
added; no time window, no cross-row state.

**1a. Filters** (`predBitsetCol` hit-switch in engine.go + `parseMatcher`):
- `seq("a","b",...)` — the phrases occur in order within the field. Hit = each
  phrase found and their start offsets strictly increasing.
- `ipv4_range("lo","hi")` / `ipv4_range(cidr)` — parse the dict value as IPv4 to
  uint32, test lo ≤ v ≤ hi. New `Pred` fields: two uint32 bounds (reuse Num/Num2).
- `eq_field(f)` / `ne_field` / `lt_field` / `le_field` / `gt_field` / `ge_field`
  — compare this field against **another field** per row. Defeats dict-only
  prefilter, so evaluate at the row level in `appendMatches` (needs both columns
  decoded), not in `predBitsetCol`. Add a `Pred.Field2` and a row-level pass.

**1b. Stats** (`AggKind` + `accSample`/`formatAgg` in pipes.go):
- `histogram(field)` — VL: bucket numeric values into a fixed set of ranges,
  emit as a JSON array of `{"vmrange":"a...b","hits":n}`. Reuse the quantile
  sample buffer, bucket at render. Bounded by the same sample cap.
- `row_min(field [, out...])` / `row_max(field [, out...])` — track the row whose
  `field` is min/max in the group, emit the requested `out` fields (all fields
  if none). Needs the scan-path stats to carry the winning row's out-field
  values: extend `statSlot` with a `best float64` + `bestVals []string`, and
  `accSample` to receive the out-field values (extend its `valOf` closure to a
  multi-field getter). Medium within the tier.

**1c. Stats modifier — conditional aggregates** (parser + eval):
- `count() if (level:error) as errors`, `sum(x) if (status:>=500)` — VL attaches
  an optional `if (<filter>)` to any aggregate. Parse an `Expr` after `if`, store
  it on `Agg`, and in `accSample` skip the sample for that agg when the row's
  fields do not satisfy the expr. Needs the row's fields available to the filter
  inside `accSample` — thread a field getter. High value (dashboards lean on it).

**1d. Pipe — hash** (pipes_more.go + parser):
- `hash(field) as result` — set `result` to a stable hash (our `simd` hash or
  FNV) of the field value. Row transform.

### Tier 2 — time-window aware (2 tasks)

These need the query time window (`q.From`, `q.To`) available where it is not
today: in the stats renderer and in the LogsQL lexer for in-query `_time`.

**2a. In-query `_time` filter** (lexer + parseTerm + engine `_time` mask):
- `_time:5m` (last 5 minutes, relative to now), `_time:2024-01-01`,
  `_time:[start, end]`, `_time:(start, end]`, `_time:>=ts`. Today the window
  comes only from HTTP `start`/`end`; add a `_time:` matcher that sets/intersects
  `q.From`/`q.To` (the mask machinery in `appendMatches` already exists). Needs a
  duration/timestamp value parser and "now" injected (do NOT call time.Now in a
  pure fn — pass it in via the Query, like the HTTP layer already stamps).
- `_time:day_range[hh:mm, hh:mm]`, `_time:week_range[Mon, Fri]` — match by
  time-of-day / day-of-week. Per-row test on the decoded timestamp; a row-level
  predicate over `_time`, evaluated in `appendMatches`.

**2b. Rate stats + range endpoint**:
- `rate()` = group count ÷ (q.To−q.From in seconds); `rate_sum(field)` =
  group sum ÷ seconds. Plumb the window seconds into `statEntryRow`/`formatAgg`
  (add a param, or stamp it on `StatsPipe` at plan time).
- `/select/logsql/stats_query_range` — run a stats query bucketed over N time
  steps across the range (time series for graphing). Reuse `Histogram` +
  per-bucket stats; register in server.go, fan-out+merge in cluster.go like
  `federatedStatsQuery`.

### Tier 3 — cross-row / subquery (2 tasks)

**3a. Introspection pipe forms** (pipes.go, logic exists in introspect.go):
- `field_names` — collapse the stream to distinct field names + hit counts.
- `field_values field [limit N]` — value→count for one field.
- `facets [N] [max_values M]` — top values per field.
  Each is a terminal aggregate like `stats`; implement as a source pipe handled
  in `RunPipeline` (like the leading-stats special case) reusing
  `FieldValues`/`Facets`.

**3b. Subquery + windowing pipes** (engine + a subquery executor):
- `join by (fields) (<subquery>)` — run the subquery, index its rows by the join
  key, attach matched fields to the outer rows.
- `union (<subquery>)` — append the subquery's full result set.
- `in(<subquery>)` filter — `field:in(<subquery>)`; run the subquery, collect its
  values, become an `In` set.
- `stream_context before N after N` — for each match, also return the N rows
  before/after it in time order within the same stream. Needs the surrounding
  rows, so it runs against the store, not just the matched row set.
  These share one new capability: **execute a nested LogsQL query and reuse its
  rows/values**. Build that executor once, then the four are thin.

### Tier 4 — stream-id data model (1 task)

VL has a first-class stream = the set of `_stream` label values, with a stable
id. We treat `_stream` labels as ordinary fields, so these need a stream
registry (id = hash of the sorted stream labels, materialized per group or in a
side index).

- Filter `_stream_id:<id>` and `_stream:{label="v"}` id resolution.
- Endpoints: `/select/logsql/streams`, `/stream_ids`, `/stream_field_names`,
  `/stream_field_values`.
  Design: derive stream id at ingest from the `_stream`/stream-label fields,
  store a per-group distinct-streams table (small), answer the endpoints from it
  without a row scan. Largest single item; do last.

### Tier 5 — storage introspection (1 task)

- `blocks_count` pipe — number of storage blocks a query scanned (one number).
- `block_stats` pipe — per-block stats (rows, bytes, columns). Both read the
  storage layer's block metadata; expose a reader method and a terminal pipe.

## Verification per feature

- A unit test in `internal/query` (or `internal/api` for endpoints) asserting the
  parse + result against a hand-built store, in the style of the existing
  `*_test.go`.
- Where VL semantics are subtle (histogram buckets, syslog, `_time` relative,
  stream ids), confirm against the staged `internal/bench/victoria-logs` binary
  before locking the expected value.
- `go test ./...` green, `gofmt` clean, before each commit.

## Not gaps (documented so they are not re-audited)

- Ingestion protocols: all present, mounted at shorter paths than VL
  (`/_bulk`, `/loki/api/v1/push`, `/v1/logs`, `/api/v2/logs`). Path prefixes
  differ only.
- `keep`/`order`/etc. — aliases, Tier 0.
- Exact-prefix `field:="v"*`, empty `field:""`, any `field:*` — subsumed by the
  implemented `Prefix`/`Eq`/`Contains` kinds.
