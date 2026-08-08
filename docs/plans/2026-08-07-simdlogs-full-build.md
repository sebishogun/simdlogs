# simdlogs — full production build plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** A production-ready log management database in Go that matches VictoriaLogs' API surface 1:1, adds the Elasticsearch search surface, and beats them on every metric (10–100× selective scans, 2–4× ingest, fraction of memory/allocations).

**Architecture:** Time-ordered immutable row groups (64–128K rows) with per-group skip footers (min/max, dict, bloom, cardinality); columnar dict+bitpacked encoding with SIMD decode; vectorized query execution over column batches via the simd kernels; bitset algebra for filter composition; simdjson-mask-pass ingest with zero per-line allocations; mmap + prefetch; LogsQL subset + ES search surface on top.

**Tech Stack:** Go 1.26.2 (1.27 on release), `github.com/sebishogun/simd` (compare/compress/reduce/sort/nary/scan + 4 new kernels), `github.com/sebishogun/simdjson` (ingest parsing, structural utilities). Reference: `../victorialogs-reference` (read-only, cited by file:line).

**Design doc:** `docs/design.md` (committed, `bcdb0a6`). **Discipline:** disassemble first, profile when needed, measurements into `docs/wrong.md`, gates bare or with `pipefail`.

**Phases** — P0 foundations, P1 storage core, P2 ingest, P3 query engine, P4 API surface, P5 kernels in the simd repo, P6 benchmark harness + gates, P7 production hardening. Phases 1–4 are the vertical that makes the benchmark real; 5 can start in parallel (independent repo); 6 is the proof; 7 is the product.

---

## Phase 0: Foundations

### Task 0.1: Module, dependencies, layout

**Files:**
- Modify: `go.mod`
- Create: `cmd/simdlogs/main.go`, `internal/storage/`, `internal/query/`, `internal/ingest/`, `internal/api/`, `internal/bench/`, `docs/wrong.md`

**Step 1:** Add the two dependencies and the skeleton main:

```bash
go get github.com/sebishogun/simd@latest github.com/sebishogun/simdjson@latest
```

`main.go` is a placeholder that prints the tier: `fmt.Println(simd.Tier())`.

**Step 2:** Verify: `go build ./... && go vet ./...` clean; `go run ./cmd/simdlogs` prints `avx512` (or the host tier).

**Step 3:** Commit: `chore: skeleton, deps and layout`.

### Task 0.2: The benchmark contract (published before any measurement)

**Files:**
- Create: `docs/benchmark-contract.md`

**Step 1:** Write the contract: same corpus (committed, reproducible), same machine, warm and cold cache; query classes — (a) selective time-window, (b) field equality with high selectivity, (c) full scan-and-count, (d) date_histogram + terms aggregations; metrics — query latency (p50/p99), ingest throughput and latency percentiles, RSS, allocations; both engines as servers on the same wire API, VictoriaLogs from the reference clone as a subprocess; methodology — one process per benchmark, shuffled order, minima of eight, idle machine (load < 1), tier named.

**Step 2:** Commit: `docs: the benchmark contract, published before the implementation`.

### Task 0.3: Corpus

**Files:**
- Create: `internal/bench/corpus/` (generated fixture + a committed slice)

**Step 1:** A generator that produces realistic logs (timestamps, levels, service names, messages with repeated and unique parts) at a fixed seed; commit the generator and a 100 MB slice (about 2M records, ~50 bytes avg).

**Step 2:** Verify: `go run ./internal/bench/corpus -seed 42 -bytes 100MB` is deterministic (two runs, same hash).

**Step 3:** Commit: `bench: the corpus generator and a committed slice`.

---

## Phase 1: Storage core

The heart. Immutable row groups; everything else reads them. No query, no ingest, no API until the group is read/written fast and correctly.

### Task 1.1: The row group format

**Files:**
- Create: `internal/storage/group.go`, `internal/storage/group_test.go`
- Create: `internal/storage/column.go`, `internal/storage/column_test.go`

**Step 1: Write the failing tests first** — the format spec, test by test:

The group is a byte-append-only blob:

```
[header][columns...][footer]
header : magic u32, version u32, rows u32, columns u32
column : name len u32, name bytes, value type u8, enc bytes...
footer : time-min i64, time-max i64, per-column meta... , footer len u32
per-column meta: name len u32, name, type u8, dict-flag u8,
                 min/max (typed), cardinality u32, bloom bits,
                 data offset u32, data len u32
```

Column encodings (type → enc):
- dict: a value table (sorted, deduped) + per-row u32 indices, bitpacked to `ceil(log2(cardinality))` bits when cardinality < 65536 else varint
- u64/i64/f64 timestamps-like: delta + SIMD varint (u64/f64 stored as u64 bits with XOR-prefix for floats — see Task 1.5 for the SIMD path; scalar reference first)
- string non-dict: bitshuffle + zstd (Task 1.5; scalar reference first)

Tests: round-trip a group with every column type and mixed dict/non-dict columns; byte-exact encoding (golden bytes committed); empty group; single row; 128K rows; max-name lengths; UTF-8 names.

**Step 2:** Run to verify fail: `go test ./internal/storage/` — FAIL (nothing exists).

**Step 3:** Implement `column.go` (dict encode/decode, bitpack via `math/bits` scalar reference, varint scalar reference) and `group.go` (header/footer marshal/unmarshal, append, read).

**Step 4:** `go test ./internal/storage/` PASS; `gofmt -l .` empty; `go vet ./...` clean.

**Step 5:** Commit: `storage: row group format with dict and bitpacked columns`.

### Task 1.2: Skip footer queries

**Files:**
- Create: `internal/storage/footer.go`, `internal/storage/footer_test.go`

**Step 1:** Tests first: given a group footer, `timeRangeMatches(from, to)`, `columnExists(name)`, `dictContains(name, value)` (bloom or exact dict scan), `rangeOverlaps(name, min, max)` — each with false and true cases, including the boundaries (inclusive min, exclusive max).

**Step 2:** Implement: footer parse into typed metadata; bloom filter (two 64-bit hashes → bitset) with the SIMD hash from the simd library when it lands (Task P5.3), FNV-1a fallback until then; exact dict lookup as the strong check behind the bloom.

**Step 3:** PASS + commit: `storage: skip footer with per-column stats and blooms`.

### Task 1.3: Group store, mmap, immutable append

**Files:**
- Create: `internal/storage/store.go`, `internal/storage/store_test.go`
- Create: `internal/storage/mmap.go` (build tag unix), `internal/storage/mmap_other.go`

**Step 1:** Tests first: `OpenStore(dir)` creates; `AppendGroup(rows)` returns a group id; `ReadGroup(id)` round-trips; reopening the store reads all groups; concurrent read of one group while appending another is safe (mmap + append to a new file: groups are one-file-each OR one-file-with-append — decide: **one file per group**, `group-<id>.bin`, because mmap append is racy; the group index lives in a tiny `index.json`-style file rewritten on commit).

**Step 2:** Implement with `syscall.Mmap` on the group file (read-only), `os.File` for append; the index file lists groups in order with offsets. The group reader sits behind an interface (`GroupReader`) from day one: mmap is the first implementation, async pread the second if the benchmark shows page-fault latency or thrash under concurrency — ClickHouse and DuckDB disagree on this and both are fast; the gate decides.

**Step 3:** Crash-safety test: kill the process mid-append (simulate by truncating the file), reopen, assert the partial group is not in the index and no panic.

**Step 4:** PASS + commit: `storage: immutable group store on mmap`.

### Task 1.4: Timestamp column: delta + varint, and the allocation profile

**Files:**
- Modify: `internal/storage/column.go`

**Step 1:** The ingest path appends timestamps; the query path scans them. Implement scalar delta+varint encode/decode; a test asserting: 1M monotonic timestamps round-trip; decode produces zero allocations (`testing.AllocsPerRun` == 0).

**Step 2:** Disassemble the decode loop (`go test -c -o /tmp/x.test . && go tool objdump -s 'storage\.decodeTimestamps' /tmp/x.test`); the SIMD replacement (P5.1) must beat the disassembled loop, and the wrong.md entry records the scalar baseline instructions/cycle counts.

**Step 3:** Commit: `storage: delta+varint timestamps, zero-alloc decode`.

### Task 1.5: String columns: bitshuffle+LZ4 (default), zstd optional; numeric: XOR-prefix

**Files:**
- Modify: `internal/storage/column.go`
- Create: `internal/storage/codec_test.go`

**Step 1:** Tests first (golden bytes committed): bitshuffle transpose (8-bit planes) round-trips; **LZ4 (klauspost/compress) on shuffled bytes decodes to the original — LZ4 is the default codec because its decode is SIMD-friendly and 3–10× faster than zstd, and decode speed is the scan bottleneck**; zstd remains selectable per column (cold/archival groups); float XOR-prefix (Prometheus-style, first-difference of bits) round-trips.

**Step 2:** Implement with scalar references. These are the SIMD targets for P5.2/P5.4; the scalar versions are the conformance baseline forever (the differential tests exercise both).

**Step 3:** PASS + commit: `storage: bitshuffle+LZ4 strings, XOR-prefix floats (scalar baseline)`.

### Task 1.5a: Token bloom per group (the text skip)

**Files:**
- Create: `internal/storage/tokenbloom.go`, `internal/storage/tokenbloom_test.go`

**Step 1:** Tests first: tokenizing a string column at group write time produces a per-column token bloom (word-level tokens, the VL shape); `contains("word")` and `~"word"` queries consult the bloom and skip groups that cannot contain the token; false-positive behavior matches bloom semantics (never false-negative); the bloom size is bounded (per-group budget, saturating like the reference's).

**Step 2:** Implement with the SIMD hash (P5.3) once it lands; FNV-1a fallback until then.

**Step 3:** PASS + commit: `storage: per-group token bloom — the text skip structure`.

**Step 4 (decision rule, recorded in wrong.md):** the benchmark contract's text-selective query class decides whether a full token index (postings, Quickwit-style) is Phase 8. If the contract shows scan + token bloom covers the query mix, no index; if the mix is text-selective, the index becomes the lever. The gate data, not taste, makes this call.

### Task 1.6: Group merges (compaction)

**Files:**
- Create: `internal/storage/merge.go`, `internal/storage/merge_test.go`

**Step 1:** Tests first: merge two adjacent groups → one group with rows in time order; merge with overlap (same-second timestamps) is stable; merged group re-encodes dicts (cardinality may drop across groups); the old files are removed after the new group is fsynced.

**Step 2:** Implement: read both groups' columns, stream-merge by timestamp, re-encode. This is I/O-bound; the SIMD win is in the re-encode (bitpack) — deferred to P5.

**Step 3:** PASS + commit: `storage: group merges`.

---

## Phase 2: Ingest

### Task 2.1: The jsonline parse pipeline

**Files:**
- Create: `internal/ingest/jsonline.go`, `internal/ingest/jsonline_test.go`

**Step 1:** Tests first: parse a line into field names + values without per-line allocations (`AllocsPerRun` == 0 for a 50-byte line); a batch of 10K lines parses and appends into one group; malformed lines are counted and skipped (never fatal).

**Step 2:** Implement on simdjson: `simdjson.ForEachLine`-style splitting (or the decoder with the parallel index when a batch is ≥ 8 MB), then per-line `Parse` into a `Value`-walk that appends to the group's column buffers. Reuse everything (buffers, arenas).

**Step 3:** Disassemble the hot walk (`objdump -s 'ingest\.appendValue'`); the per-field switch must be a jump table, not a compare chain; fix if not.

**Step 4:** PASS + commit: `ingest: jsonline with zero per-line allocations`.

### Task 2.2: Dict interning into the current group

**Files:**
- Create: `internal/ingest/interning.go`, `internal/ingest/interning_test.go`

**Step 1:** Tests first: repeated field values intern to the same dict id within a group; a 100M-value run keeps interning memory bounded (per-group dict, discarded on group flush); interning across groups produces different ids (correct — dict is per group).

**Step 2:** Implement with a per-group map[string]uint32; the SIMD-hash path (P5.3) replaces the map key hashing later; the map itself is the interning table.

**Step 3:** PASS + commit: `ingest: per-group dict interning`.

### Task 2.3: Group flush and the write path

**Files:**
- Create: `internal/ingest/writer.go`, `internal/ingest/writer_test.go`

**Step 1:** Tests first: buffered batches flush at 64K rows OR 64 MB OR 2s, whichever first; a flush is crash-safe (write group file, fsync, then index update); concurrent ingest from N goroutines produces groups in timestamp order per writer shard (shard by hash of the log stream id — VL-style stream partitioning).

**Step 2:** Implement: per-shard writer with a batch arena; flush to the store.

**Step 3:** PASS + commit: `ingest: sharded writers with crash-safe flush`.

### Task 2.4: Ingest endpoints (the VL surface)

**Files:**
- Create: `internal/api/insert.go`, `internal/api/insert_test.go`

**Step 1:** Tests first (table-driven against recorded request bodies): `/insert/jsonline` (NDJSON), `/insert/elasticsearch` (bulk NDJSON with action lines — action lines consumed, docs ingested, `errors:false` in the response), `/insert/loki/` (push JSON form and protobuf form — vendored minimal proto), `/insert/opentelemetry/` (OTLP protobuf logs), `/insert/datadog/` (JSON), `/insert/splunk/` (HEC JSON), `/insert/journald/` (JSON lines), `/insert/ready` (200).

**Step 2:** Implement over the writer (2.3). The Loki and OTLP protobufs are the only external-schema dependency; vendor the minimal `.proto` and generate, or hand-decode the wire format (fields are stable and few — hand-decode is the honest choice, with a conformance test against recorded bytes from the reference clone's own fixtures if present).

**Step 3:** PASS + commit: `api: the VictoriaLogs ingest surface`.

---

## Phase 3: Query engine

### Task 3.1: The bitset and mask algebra

**Files:**
- Create: `internal/query/bitset.go`, `internal/query/bitset_test.go`

**Step 1:** Tests first: set/clear/test; AND/OR/NOT/ANDNOT between bitsets; iteration over set bits (tzcnt loop, not the 64-step loop — the reference's `bitmap.go:128` is the anti-pattern, cite it); popcount.

**Step 2:** Implement with `math/bits` (TrailingZeros64) and the simd `nary` u64 kernels where the operation is elementwise AND/OR/XOR over aligned words.

**Step 3:** Disassemble the iteration loop; the tzcnt path must be visible; wrong.md records the measurement against the reference's bit loop.

**Step 4:** PASS + commit: `query: bitset algebra`.

### Task 3.2: The planner

**Files:**
- Create: `internal/query/plan.go`, `internal/query/plan_test.go`

**Step 1:** Tests first: a LogsQL-ish query `_time:[now-5m, now] AND level:=error` produces: time range → candidate groups → per-column dict/bloom check → intersection order by cheapest-first (estimated cost from cardinality); the plan is a DAG of (group, filter-bitset) pairs, not a list of row scans.

**Step 2:** Implement: query AST (subset: time range, eq, contains, range, exists, AND/OR/NOT — the P4 parser feeds this), group selection via footer, filter → bitset producers.

**Step 3:** Test the skip: a query whose time range excludes 95% of groups must never open those group files (count opens in a test hook).

**Step 4:** PASS + commit: `query: planner with group skipping`.

### Task 3.3: Vectorized residual scan

**Files:**
- Create: `internal/query/scan.go`, `internal/query/scan_test.go`

**Step 1:** Tests first: scan a dict column with `simd.MaskBits`-style equality (equality on encoded indices — one vector compare per 64 rows); range on numeric columns via `simd.Compare` masks; `contains` on strings: SIMD `IndexAny`/`IndexAll` for the probe bytes then scalar verify on candidates (the simdjson escape-scan pattern); correctness vs a scalar reference on fuzz input.

**Step 2:** Implement over the column buffers: decode lanes lazily — only the rows the bitset selects get materialized.

**Step 3:** Disassemble the equality scan; the `vpcmpeqb`/`kmov` (or NEON equivalents) must be in the kernel, not the Go loop; wrong.md records the scalar-vs-kernel delta.

**Step 4:** PASS + commit: `query: vectorized residual scan`.

### Task 3.4: Aggregation primitives

**Files:**
- Create: `internal/query/agg.go`, `internal/query/agg_test.go`

**Step 1:** Tests first: sum/count/min/max over a selected bitset; histogram over time buckets (the `hits` shape); terms (group by dict id — the dict IS the group); percentiles (needs sorted values — `sort` kernel); cardinality (the SIMD hash + bitset).

**Step 2:** Implement with `simd.Reduce`/`simd.Sort` over the decoded lanes; buckets allocate once.

**Step 3:** PASS + commit: `query: aggregation primitives`.

### Task 3.5: Parallel query execution

**Files:**
- Create: `internal/query/parallel.go`, `internal/query/parallel_test.go`

**Step 1:** Tests first: a query over N groups executes on min(GOMAXPROCS, N) workers with identical results to the serial path (differential); results merge in group order.

**Step 2:** Implement: worker pool over the plan DAG; aggregation results are per-worker partials merged deterministically.

**Step 3:** PASS + commit: `query: parallel group execution`.

---

## Phase 4: API surface

### Task 4.1: LogsQL subset

**Files:**
- Create: `internal/query/logsql.go`, `internal/query/logsql_test.go`

**Step 1:** Tests first: parse the subset — `field:value`, `field:="exact"`, `field:~"regex"`, `_time:[a,b]`, `AND`/`OR`/`NOT`, `*` wildcard field, `stats by (field) count()` — into the AST; errors with positions for malformed input; the subset is documented as such in the README.

**Step 2:** Implement the parser (hand-rolled, no regex-based lexing — the lexer is a byte scanner; disassemble it and keep it branch-light).

**Step 3:** PASS + commit: `query: LogsQL subset parser`.

### Task 4.2: /select/logsql/* endpoints

**Files:**
- Create: `internal/api/select.go`, `internal/api/select_test.go`

**Step 1:** Tests first (recorded request/response fixtures): `query` (rows with the matched fields, `_time`, `_stream`, `_msg` defaults), `query` with `limit`/`offset`/`sort`, `hits` (time buckets), `stats_query`/`stats_query_range`, `facets` (top values per field), `field_names`/`field_values`, `stream_field_names`/`stream_field_values`/`stream_ids`/`streams`, `tail` (long-poll on the ingest stream), `query_time_range`.

**Step 2:** Implement over the query engine. Responses match the reference's JSON shape (field names, ordering) — verify against the reference clone's own handlers (`app/vlselect/logsql/logsql.go` and the qtpl templates) and record any divergence in the README.

**Step 3:** PASS + commit: `api: the VictoriaLogs select surface`.

### Task 4.3: The ES search surface (the extra)

**Files:**
- Create: `internal/api/es.go`, `internal/api/es_test.go`

**Step 1:** Tests first: `_search` with bool/term/terms/range (time ranges map to the time partition!)/match_phrase/exists/wildcard; `size`/`sort`/`_source`; `_msearch`; `_count`; `_bulk` (already ingest); aggregations: terms, date_histogram (fixed_interval), stats, percentiles, cardinality — response shape byte-compatible with ES where a client would notice (hits.total, buckets.key_as_string), documented divergences in the README.

**Step 2:** Implement over the query engine (4.1–3.5); the time-range-to-partition mapping is the headline feature — the query planner already does it.

**Step 3:** Verify with a real client: a Grafana ES datasource query and a Filebeat bulk against the running server (recorded in `internal/bench/`).

**Step 4:** PASS + commit: `api: the Elasticsearch search surface`.

---

## Phase 5: Kernel additions (in the simd repo, `../GO_SIMD`)

Each kernel follows the existing pattern: C source in `csrc/`, generated `.s` for the tiers, register files, conformance vs the portable reference, wrong.md measurements. All four land in the simd library first, then simdlogs consumes them.

### Task 5.1: SIMD varint decode

**Files:**
- Create: `../GO_SIMD/csrc/varint.c` (decode only; encode stays scalar)
- Modify: `../GO_SIMD/internal/kernel/kernel.go`, text.go export

Spec: decode a stream of LEB128 varints into u64s; the vector path processes 8-byte words, extracting continuation bits with mask+shift (the classic SIMD-varint structure); the portable reference stays the conformance oracle. Tests: 1M random and monotonic varints vs reference; alignment cases; truncated input.

### Task 5.2: Bitpacked column decode

**Files:**
- Create: `../GO_SIMD/csrc/bitpack.c`

Spec: unpack N-bit values (N ∈ 1..64) from a packed word stream to u64s. The reference's dict indices are the first consumer. Tests: all N, random values, misaligned starts.

### Task 5.3: SIMD hash (xxhash-class)

**Files:**
- Create: `../GO_SIMD/csrc/hash.c`

Spec: a fast non-cryptographic hash over bytes, 8 bytes/cycle with the vector width; used for dict interning keys and bloom filters. Must not change output for the same input across tiers (deterministic). Tests: golden vectors, distribution over a corpus.

### Task 5.4: Bitshuffle transpose

**Files:**
- Create: `../GO_SIMD/csrc/bitshuffle.c`

Spec: byte↔bitplane transpose for 8-bit planes; the string-column codec's SIMD half. Tests: round-trip for all sizes 1..4096 bytes; parity with the scalar reference.

Each lands with: `make` regeneration, conformance suite green, gate numbers recorded, wrong.md entry. Then `go get` bumps in simdlogs and the storage tasks 1.4/1.5/2.2 swap their scalar call sites for the kernels, re-measuring the delta.

---

## Phase 6: Benchmark harness and gates

### Task 6.1: The head-to-head harness

**Files:**
- Create: `internal/bench/harness.go`, `internal/bench/harness_test.go`
- Create: `internal/bench/vlclient.go` (VictoriaLogs as a subprocess from `../victorialogs-reference`)

**Step 1:** Tests first: the harness starts both servers on the same corpus, runs the contract's query set, records per-query p50/p99 and ingest rates; a smoke run with 1/100th of the corpus completes in under a minute.

**Step 2:** Implement: subprocess management (build the reference from the clone once, cached), wire clients for both (VL speaks its own JSON; we speak ours; the contract fixes the query set so both are exercised), result recording in the snapshot format (machine, tier, go version, date, queries, latencies).

**Step 3:** Run the smoke; record the first numbers even if embarrassing — they go in wrong.md, not the README.

**Step 4:** Commit: `bench: the head-to-head harness`.

### Task 6.2: The gate

**Files:**
- Create: `Makefile` (bench-run, bench-check, bench-update, bench-agree with `-shuffle=on`)
- Create: `tools/benchcheck/` (port the simdjson benchcheck: minima vs baseline, 8% threshold, name-based, order-independent)

**Step 1:** Gate over our own performance (not the head-to-head — that's a report, not a gate): parse, scan, agg, ingest benchmarks in `internal/bench/`, baseline in `testdata/bench/amd64.txt`, `make bench-check` green on an idle machine.

**Step 2:** Commit: `bench: the gate, shuffled and minima-based`.

### Task 6.3: First head-to-head numbers

**Step 1:** Quiet machine, full contract run. Publish the table in the README **with every metric, including losses** — the transparency rule from the sibling repos.

**Step 2:** Commit: `docs: first head-to-head numbers against the reference`.

---

## Phase 7: Production hardening

### Task 7.1: Retention and delete

**Files:**
- Create: `internal/storage/retention.go`, tests

Time-based retention (drop groups fully outside the window), `_time:` delete queries (VL semantics: delete by query), and per-stream retention. Tests: retention never removes a group a query could see; delete is durable.

### Task 7.2: The tail and streams API completeness

The `/select/logsql/tail` long-poll over the ingest stream with correct resume semantics; `stream_ids`/`streams` responses matching the reference shape.

### Task 7.3: Concurrency and resource limits

Per-query memory caps (VL has `search.maxQueryLen`, agg memory limits — mirror them), ingest backpressure, graceful shutdown (flush + fsync), `-reload`-style flag handling.

### Task 7.4: The full LogsQL, phase two

Beyond the subset: `stats` with all the reference operators we documented, `_stream` filtering, `first`/`last`/`sort` by fields, `with` clauses — each landed with a differential test against the reference clone's behavior on the same query.

### Task 7.5: The release pass

README with the published contract numbers, docs for the ES surface, versioning, the toolchain bump to Go 1.27 on release, and the wrong.md sweep (every measurement that argued against a change is recorded).

---

## The plan's honesty clause

Nothing in phases 1–4 may depend on a phase-5 kernel to be correct — the scalar references are the conformance baseline forever, and the kernels replace them by measurement. When a measurement contradicts a design decision here, the decision changes and the entry lands in `docs/wrong.md` — that is the plan working, not failing.
