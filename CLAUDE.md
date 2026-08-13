# CLAUDE.md — working on simdlogs

This file is the working contract for Claude Code on this repository. The
rules of AGENTS.md are reproduced in full below (this file is self-contained
and carries every mandatory rule: boundary/status, read order, package
ownership, compatibility, concurrency, performance, gates, the wrong-record,
and the roadmap rule). AGENTS.md is the source of truth: where this header
and AGENTS.md ever disagree, AGENTS.md wins.

## Where everything is

- **Boundary and status:** docs/architecture.md — what ships (and what does
  not: no metrics ingestion, no consensus, no tagged release).
- **Current low-level design:** docs/lld/storage.md, ingest.md, query.md,
  api.md, cluster.md — one per layer, exact current source. cluster.md also
  records the known router-merge defects (streams/stream_ids/plain
  stats_query/hits are stale and answer empty/bogus; facets/tail/alerts and
  other endpoints are router-local — representative, not complete): the
  cluster surface is experimental, not production-safe.
- **Planned direction (not shipped):** docs/roadmap.md and
  docs/plans/2026-08-13-simdlogs-production-design.md (status: approved) /
  2026-08-13-simdlogs-production.md.
- **Verification discipline:** docs/verification.md — gates, report tests,
  benchmark contract (load < 1 policy; quiet-bench.sh's 1.5 default is a
  looser helper default), crash recovery, cross-arch, the gofmt blocker on
  internal/storage/group.go.
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
oracle, kernel as the shipped path" from the start. The honesty clause still
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
election, no transactions, no automatic membership — and it is experimental,
not production-safe: the `streams`, `stream_ids`, plain `stats_query`, and
`hits` router merges are stale and answer empty/bogus results, and `facets`,
`tail`, `/alerts` and other endpoints are router-local, not federated. The
enumeration is representative, not complete — the per-endpoint status lives
in `docs/lld/cluster.md`; do not read an endpoint's absence from this list
as federation. Planned direction lives in
`docs/roadmap.md` and the `docs/plans/2026-08-13-simdlogs-production*.md`
files; never present planned work as shipped behavior. `README.md` and the
`docs/` tree describe the current source; the authoritative "done" sets are
the code itself (the `mux.HandleFunc` table in `internal/api/server.go`, the
parser in `internal/query/logsql.go`, the flags in `cmd/simdlogs/main.go`).

## Read order

Before touching anything: `README.md`, then `docs/architecture.md`, then the
layer LLD you are about to change (`docs/lld/storage.md`, `ingest.md`,
`query.md`, `api.md`, `cluster.md`), then `docs/verification.md` for how a
claim gets trusted. `docs/wrong.md` is read before proposing any change that
repeats a measured idea.

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
