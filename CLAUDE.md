# CLAUDE.md — working on simdlogs

This file is the working contract for Claude Code on this repository. The
rules of AGENTS.md are reproduced in full below (this file is self-contained
and carries every mandatory rule: boundary/status, read order, package
ownership, compatibility, concurrency, performance, gates, the wrong-record,
and the roadmap rule). AGENTS.md is the source of truth: where this header
and AGENTS.md ever disagree, AGENTS.md wins.

## Core tenets: performance-aware programming

**These are the core tenets of this codebase. Read them before writing a line.**

The stance is Casey Muratori's: *performance-aware programming*. Not
"optimization" as a phase that happens after the code works — knowing roughly
what the machine will do with what you write, while you write it. The
alternative is not "clean code that gets optimized later"; it is code whose
shape forecloses the fast version, and the rewrite costs more than thinking for
five minutes did.

Two ideas underneath everything below:

- **Know the order of magnitude before you type.** How many times does this run
  — once, per request, per row, per element? What does one iteration touch?
  Nobody needs a cycle count; everybody needs to know whether they just wrote
  something that runs 200,000 times and allocates.
- **The machine is not an abstract machine.** It has caches, a prefetcher, wide
  registers, and many cores. Code that pretends otherwise leaves 10-100x on the
  floor, and no amount of later profiling recovers a layout decision.

**How the tenets relate.** They are not a list of independent good ideas. The
data-layout ones exist to make the bulk operation POSSIBLE:

    struct-of-arrays + grouped lifetimes + zero per-element allocation
        -> contiguous, uniformly-typed arrays
            -> the kernel can run at all
                -> SIMD, and the parallel shard boundaries come free

You cannot vectorize an array-of-structs: the lanes are not adjacent. You
cannot vectorize a slice that is really a graph of separately-allocated
objects. You cannot keep a kernel fed if every element costs an allocation.
So struct-of-arrays, arenas and lifetime grouping are not housekeeping to do
after the fast path works — they are the precondition for the fast path
existing, and the reason a layout decision made carelessly cannot be recovered
by profiling later.

Read the sections in that order, and design in that order.

### 1. Zero allocations wherever it is possible at all

Not "few" — zero, on any path that runs per element, per record, per row or per
request.

The checklist, in the order it usually pays:

- **Nothing per-element or per-record that can be per-batch.** A `map` built
  per record, a `fmt.Sprintf` per line, a `[]byte`->`string` per field: at 200k
  records those are 200k allocations and 200k pieces of GC work. Reach for a
  byte scan over the fixed shape instead of a reflective decode into a map.
- **Size every slice and map you can size.** `make([]T, 0, n)` when n is known
  or estimable. Growing from nil reallocates and copies the whole thing at
  every doubling.
- **Reuse the caller's buffer.** Append into a supplied `[]byte`, compact in
  place when the write cursor provably trails the read cursor, take a `dst`
  parameter rather than returning a fresh allocation.
- **Do not scan twice.** If a later stage already parses the data, do not
  validate it fully first — do the O(1) structural check and let the one place
  that parses report the rest.
- **Escape analysis is part of the design.** A pointer stored in an interface,
  a closure capturing a local, a returned slice of a local array: each forces a
  heap allocation. `go build -gcflags=-m` says which.
- **Prefer a wider type to a pointer chase.** An index into a slab beats a
  pointer when the slab is contiguous — it is smaller, it does not escape, and
  it keeps the array vectorizable.

Verify with `-benchmem`. `0 allocs/op` is a target you can actually hit on a
hot path, and worth stating in the doc comment when you hit it.

### 2. Think about the data, then the code

Muratori's central point, and the one most often skipped. The layout of the
data decides the speed; the instructions are usually a detail.

**Struct-of-arrays over array-of-structs** for anything scanned columnwise. A
filter that reads one field should stream that field's array, not stride
through whole records pulling in fifteen fields it does not want. This is the
single highest-leverage decision in a columnar store, and it is made when the
type is declared, not later.

**Group lifetimes; allocate them together.** Objects born together and dying
together belong in one allocation. A per-request arena — one buffer that
everything for that request is carved out of, released in one move when the
request ends — replaces thousands of individual allocations and frees with a
single pointer reset. It also gives locality for free: everything the request
touches is contiguous. Where the lifetime is per-batch, per-group or
per-connection instead, the same applies at that scope. The rule is that the
allocation boundary should match the lifetime boundary; when it does not, you
get either leaks or a per-object free.

**Use the whole cache line.** Touch it once and consume all of it. Block a pass
to fit L1/L2 rather than striding across a large array repeatedly. Keep hot
fields adjacent and cold fields elsewhere so a line carries only what the loop
reads. Watch for false sharing when threads write adjacent words.

Locality is a hypothesis to check with perf counters, not a rule to apply
blindly: windowing won in simdcsv and did nothing in simdjson.

### 3. Do the work in bulk — use SIMD

This family exists for it. Whole-slice work goes through the kernels, not a
hand-written scalar loop. Where no kernel exists for the shape, say so
explicitly rather than quietly writing the scalar loop and leaving it.

Check the dispatch actually reaches the kernel at runtime: every complex kernel
in `simd` was dead code from v1.14.0 to v1.20.0 because nothing walked the
tables the runtime indexes.

A per-element function call defeats vectorization outright — measured at 11
extra instructions per element, a 2.56x ratio. If the API shape forces one, the
API shape is the bug.

### 4. Don't do the work at all

The fastest code is the code that does not run. Prune before you decode: a
bloom filter that rejects a group, a time window that skips a block, a column
never materialized because nothing asked for it. simdlogs' rare-needle path
beats a full scan by rejecting groups without decoding them, not by decoding
faster.

Hoist invariants out of loops. Compute once what does not change. Do not scan
twice — if a later stage already parses the data, do the O(1) structural check
and let the one place that parses report the rest.

### 5. Multi-threaded where it is beneficial

And only there. Parallelism pays when the work per shard clears the
coordination cost; below that it is slower and less predictable.

Shard on a boundary the data already has (groups, blocks, row ranges), give
each worker its own output buffer, merge once. Never share a mutable buffer
between goroutines without saying so in the doc comment. `-race` is a gate.

### 6. `sync.Pool` is the last resort — and it has to be correct

Reach for it last. Most allocation wins are a size hint, an arena, or a
caller-supplied buffer: free at runtime, no correctness hazard. A pool costs
Get/Put, a miss allocates anyway, and it introduces a class of bug the others
cannot have.

When a pool IS the right answer, these are not optional:

- **The buffer must be fully overwritten before anything reads it.** A pooled
  buffer arrives holding a PREVIOUS request's data. If any path reads an
  element it did not write, that request's data is silently served to this one
  — a correctness bug, cross-request data leakage, not a performance one. Know
  the property holds and say WHY in the doc comment; do not assume it.
- **Prove it with a poisoning test.** Fill pooled buffers with a value that
  cannot occur, then assert the pooled result equals the unpooled result
  exactly. Write that test FIRST. This is the only thing that catches the bug,
  because the unpooled path zeroes and therefore hides it.
- **Ownership must be unambiguous.** A pooled buffer must not escape into a
  returned value, be captured by a goroutine that outlives the Put, or be
  aliased by a slice the caller keeps. Returning a slice of a pooled array is a
  use-after-free in all but name.
- **Put back exactly what you took**, reset to a known state, once. A double
  Put hands the same array to two callers at the same time.
- **Pool a pointer, not a slice.** A `[]T` placed in an `any` allocates on
  every Put, which is the cost the pool exists to remove.
- **Sizing is part of the contract.** A pool of mixed sizes either wastes the
  large buffers or reallocates on the small ones; decide which and say so.
- **Testing note:** `sync.Pool.Put` drops the value at random one time in four
  under `-race`, so any test asserting reuse across a single round trip is red
  a quarter of the time. Assert reuse within a few attempts, not on a
  particular one.

### Then measure

These tenets are where to start, not a substitute for the benchmark.
Fast-looking code that was never measured is a guess. The noise floor, the
interleaved A/B discipline, and "disassemble before you theorise" apply to code
written this way exactly as they apply to a tuning change — and a claim with no
number behind it does not go in a doc.

## Where everything is

- **Boundary and status:** docs/architecture.md — what ships (and what does
  not: no metrics ingestion, no consensus, no tagged release).
- **Current low-level design:** docs/lld/storage.md, ingest.md, query.md,
  api.md, cluster.md — one per layer, exact current source. cluster.md
  carries the per-endpoint status table, which a test checks against the mux,
  and the outstanding items are in docs/release-readiness.md.
- **Planned direction (not shipped):** docs/roadmap.md and
  docs/plans/2026-08-13-simdlogs-production-design.md (status: approved) /
  2026-08-13-simdlogs-production.md.
- **Verification discipline:** docs/verification.md — gates, report tests,
  benchmark contract (load < 1 policy; quiet-bench.sh's 1.5 default is a
  looser helper default), crash recovery, cross-arch.
- **Design:** docs/design.md — the historical architecture and the
  falsifiable claim; the completed build plan is
  docs/plans/2026-08-07-simdlogs-full-build.md.
- **Reference (read-only, never imported):** ../victorialogs-reference — the
  spec to match and the evidence to beat; cite it file:line. The staged
  binary at internal/bench/victoria-logs powers the head-to-head reports.
- **The record:** docs/wrong.md — every measurement that argued against a
  change, whether or not code changed.

## The kernels this design calls "new" are already shipped

The design's Phase 5 lists four kernels to build in the simd repo: SIMD
varint decode, bitpacked decode, SIMD hash, and bitshuffle. All four now
exist in published simd (v1.18.0-v1.20.0): simd.VarintDecode,
simd.BitUnpackInto, simd.HashUint64, simd.Bitshuffle/Unbitshuffle, plus
RunLengthDecodeInt32, MergeSortedUint32, the columnar family
(CompressBitsInto, SumValid, CountValid) and FormatFloat64. So the storage
codecs consume the kernels directly -- the plan's "scalar reference first,
swap to the kernel in P5" collapses to "scalar reference as the conformance
oracle, kernel as the shipped path" from the start. The evidence rule still
holds: the scalar reference is the forever conformance baseline, and every
kernel use is differential-tested against it.

## Consuming simd

`go get github.com/sebishogun/simd@latest` -- currently v1.20.0. Pure Go, no
cgo; the kernels are committed assembly. simdjson likewise for ingest parsing.

---

# Working on simdlogs

## The goals, in one breath

Beat VictoriaLogs on every metric they publish — query latency (orders of
magnitude on selective scan-heavy queries), ingest throughput, memory, and
allocations — while matching their API surface 1:1 and adding the Elasticsearch
search surface they do not have. Heavy use of the simd library everywhere;
simdjson where the shape fits.

## The boundary and status

simdlogs is a disk-backed columnar log database, single binary, no tagged
release: the storage format and HTTP surface are not under a stable-version
promise, so pin a commit when deploying. It ingests logs only — there is no
metrics ingestion; `/metrics` and `/alerts` are export surfaces. The cluster
layer is application-level sharding/replication with no consensus, no leader
election, no transactions, no automatic membership. Every route is classified
federated / router-local / refused and the classification is checked against
the mux, so the per-endpoint status in `docs/lld/cluster.md` is derived rather
than remembered — read it there rather than inferring from any prose list.
What is still outstanding is in `docs/release-readiness.md`. Planned direction lives in
`docs/roadmap.md` and the `docs/plans/2026-08-13-simdlogs-production*.md`
files; never present planned work as shipped behavior. `README.md` and the
`docs/` tree describe the current source; the authoritative "done" sets are
the code itself (the `mux.HandleFunc` table in `internal/api/server.go`, the
parser in `internal/query/logsql.go`, the flags in `cmd/simdlogs/main.go`).

## Read order

Before touching anything: `README.md`, then `docs/architecture.md`, then the
layer LLD you are about to change (`docs/lld/storage.md`, `ingest.md`,
`query.md`, `api.md`, `cluster.md`), then `docs/verification.md` for how a
claim gets trusted and `docs/roadmap.md` so planned work is never presented
as shipped. `docs/wrong.md` is read before proposing any change that repeats
a measured idea. Work on the production-hardening track reads the approved
design and its task plan first — `docs/plans/2026-08-13-simdlogs-production-design.md`
and `docs/plans/2026-08-13-simdlogs-production.md`. The historical records —
`docs/design.md` and `docs/plans/2026-08-07-simdlogs-full-build.md` — are read
as relevant when a claim or assumption traces back to them.

## Package ownership

- `internal/storage` — the immutable group format, store, mmap, and the ops
  (retention, tiering, backup, cold). Format changes are versioned; old
  groups must keep reading (the v7 + per-block-codec-flag design is the
  compatibility surface).
- `internal/ingest` — line parsing, field mapping, writer + flush pool.
- `internal/query` — parsers, pipes, the vectorized scan, bitsets,
  parallelism, streams, stats, SQL/vector surfaces.
- `internal/api` — the HTTP surface, tenancy, routing, cluster fan-out.
- `internal/bench` — corpora and the head-to-head harness; reports, not
  gates. `cmd/simdlogs` owns flags and lifecycle.

A change to one layer's contract (a query parameter, a route, a wire shape, a
column codec) must update the corresponding LLD in the same commit.

## Disassemble first, always

Before proposing a cause for anything slow, before writing a variant, before
reading a profile delta — **build it and read the instructions**.

```
go test -c -o /tmp/x.test .
go tool objdump -s 'pkg\.functionName' /tmp/x.test | less
```

Go compiles in seconds; every guess that skips the disassembly costs a
build-measure-revert cycle and risks a wrong conclusion landing in
`docs/wrong.md` as fact.

What the disassembly says that nothing else does:

- **Register pressure.** A large stack frame with the loop counter or a flag
  spilled and reloaded per iteration. No performance counter reports this.
- **Whether a bounds check was eliminated**, and whether an index multiply is
  a shift or a multiply.
- **Whether a call was inlined**, and whether `append(b, s...)` became inline
  stores or a `memmove` call.
- **Which branch the compiler laid out as fallthrough.**

Profile when a profile is the right tool — for allocation counts, GC pressure,
or cache misses — but the disassembly comes first.

## The reference codebase

`../victorialogs-reference` is a working clone of VictoriaLogs, read as the
spec for what we must match and the evidence of what we must beat. Cite it by
file:line when a design decision comes from it (e.g.
`lib/logstorage/consts.go:29` for the 8M-row blocks). It is a reference, not a
dependency: we never import it, we read it.

When a claim about their behavior is load-bearing, verify it against the
clone before writing it down. Compatibility is verified three ways, never by
status code alone: the shape of the response body, whether a query argument
changes the answer, and — when the staged binary is present — the VL binary's
own answer. The VL binary is staged at `internal/bench/victoria-logs`
(gitignored) by `go build ./app/victoria-logs` in `../victorialogs-reference`;
tests skip loudly when it is absent. A status-code probe alone reported 0 gaps
while seven endpoints answered 200 with an unreadable body — see
`docs/wrong.md`.

## Concurrency

Concurrent access is a correctness contract, not an afterthought: parallel
ingest shards share one store (`AppendGroup` is concurrency-safe), the flush
pool runs behind the parser, query scans fan out per group, and the router
fans out per shard. Any new shared state must be `-race` clean and the
serial-versus-parallel agreement test must cover it.

## Benchmarks

The code-layout noise floor is **8.3%**. Anything smaller cannot be told from
nothing by wall-clock. When a change is expected to be worth less than that:

- compare **instructions retired** and **cycles** with
  `perf stat -e instructions:u,cycles:u`, which are layout-independent;
- and read the disassembly, which is the only thing that explains *why*.

A/B builds must be **interleaved** in one session and compared on the minimum,
never across sessions. Run the machine quiet: wait for load average under 1.

**Never pipe a gate through `tail`** (or anything else) without `pipefail`:
the pipe reports the last command's status and the failure vanishes. Run
gates bare, or `set -o pipefail` first.

The head-to-head harness runs both engines as servers on the same wire API,
same corpus, same machine — VictoriaLogs from the reference clone as a
subprocess. The benchmark contract is published before the implementation is
measured.

## Gates

Every change runs the three core gates bare (or under `set -o pipefail`)
before commit:

    go test ./...
    go test -race ./...
    go vet ./...

Release gates add a clean `gofmt -l` and the quiet-machine (load < 1)
discipline for anything published as a measurement. `gofmt -l .` is clean
across the repository; this paragraph described it as red on
`internal/storage/group.go` for long enough that the description outlived the
condition, which is the shape `docs/wrong.md` records under "a deleted
behaviour leaves its justification behind".

## The record

`docs/wrong.md` holds measurements that argued against changes, including
changes that were then reverted. A finding that cost a measurement belongs
there whether or not any code changed — the entry is the deliverable.

## Roadmap

`docs/roadmap.md` is the only place planned work lives, with measurable
exits and no promises; `docs/plans/2026-08-13-simdlogs-production.md` is the
task-by-task plan that executes it. Do not add a feature "for the roadmap"
and ship it in the same commit — roadmap work lands on its own branch,
tests-first, each task as a commit. A stage is done only when its exits pass.

## Toolchain

Go 1.26.5 now; switch to 1.27 the day it is released. Nothing in this
codebase may depend on a toolchain quirk that makes that switch painful.

## Sweep for unmeasured shapes before tuning measured ones

The largest wins in the sibling repos came from finding shapes no benchmark
measured, not from optimizing known rows. When the goal is competitive
standing, the first move is a sweep for the unmeasured pair, not a profile of
a measured one.
