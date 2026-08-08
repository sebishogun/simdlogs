# CLAUDE.md — working on simdlogs

This file is the working contract for Claude Code on this repository. It is
derived from AGENTS.md (below, verbatim) plus the design and the build plan;
where this header and AGENTS.md ever disagree, AGENTS.md wins.

## Where everything is

- **Design:** docs/design.md — the architecture and the falsifiable claim.
- **Build plan:** docs/plans/2026-08-07-simdlogs-full-build.md — phases P0-P7,
  task by task, tests-first. Implement in order; each task ends in a commit.
- **Reference (read-only, never imported):** ../victorialogs-reference — the
  spec to match and the evidence to beat; cite it file:line.
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
clone before writing it down.

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

## Toolchain

Go 1.26.2 now; switch to 1.27 the day it is released. Nothing in this
codebase may depend on a toolchain quirk that makes that switch painful.

## Sweep for unmeasured shapes before tuning measured ones

The largest wins in the sibling repos came from finding shapes no benchmark
measured, not from optimizing known rows. When the goal is competitive
standing, the first move is a sweep for the unmeasured pair, not a profile of
a measured one.
