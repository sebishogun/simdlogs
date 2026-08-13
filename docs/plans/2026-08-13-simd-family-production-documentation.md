# SIMD Family Production Documentation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Produce source-backed architecture, low-level design, roadmap, verification, and agent-context documentation for all ten SIMD-family repositories without changing implementation files.

**Architecture:** Work in one isolated documentation worktree per repository. Preserve existing benchmark and decision records, make every README distinguish shipped behavior from planned behavior, and give each repository a self-contained document map. This plan changes Markdown only; production Go implementation belongs to later sessions that execute each repository's `*-production.md` plan.

**Tech Stack:** Markdown, Go module documentation, Git worktrees, repository Go/Make verification commands, GitHub CLI for release-status checks.

---

## Ground rules

- Repositories: `simd`, `simdblas`, `simdjson`, `simdcsv`, `simdvec`,
  `simdlogs`, `simdhttp`, `simdcbor`, `simdparquet`, and `simdimage`.
- Allowed changes: files ending in `.md` only.
- Forbidden changes: Go source, tests, generated files, `go.mod`, `go.sum`,
  Makefiles, workflows, benchmark data, SVGs, native libraries, and release
  artifacts.
- Retain historical plans and `docs/wrong.md`. Add links and status notes rather
  than rewriting historical conclusions as current architecture.
- `README.md` describes what ships today. Future APIs appear only in production
  designs, LLDs, and roadmaps, clearly labeled as planned.
- Every repository must contain both `AGENTS.md` and `CLAUDE.md` with explicit
  repository context. Neither may rely solely on machine-global instructions.
- Before final integration, fetch and rebase each documentation branch onto its
  current `main`, rerun verification, inspect the Markdown-only diff, and push
  without force.

### Task 1: Record branch bases and prepare the root worktree

**Files:**
- No repository files change in this task.
- Worktrees: `/tmp/opencode/simd-docs`, `/tmp/opencode/simdblas-docs`,
  `/tmp/opencode/simdjson-docs`, `/tmp/opencode/simdcsv-docs`,
  `/tmp/opencode/simdvec-docs`, `/tmp/opencode/simdlogs-docs`,
  `/tmp/opencode/simdhttp-docs`, `/tmp/opencode/simdcbor-docs`,
  `/tmp/opencode/simdparquet-docs`, `/tmp/opencode/simdimage-docs`.

**Step 1: Create the missing root documentation worktree**

Use the `using-git-worktrees` skill. Create `/tmp/opencode/simd-docs` from the
current `simd/main` on a documentation branch. Do not reuse or modify the main
working tree.

**Step 2: Record every branch base**

Run `git status --short --branch`, `git rev-parse HEAD`, and
`git merge-base HEAD main` in every documentation worktree. Save the SHAs in the
session notes, not in repository files.

Expected: each worktree is clean except for already committed documentation
work; no source modifications are present.

**Step 3: Confirm the allowed file policy**

Run `git diff --name-only main...HEAD` in every worktree and inspect every path.

Expected: all existing branch-only paths end in `.md`.

### Task 2: Complete the root `simd` documentation contract

**Worktree:** `/tmp/opencode/simd-docs`

**Files:**
- Create: `AGENTS.md`
- Modify: `CLAUDE.md`
- Create: `docs/architecture.md`
- Create: `docs/lld/api-and-memory.md`
- Create: `docs/lld/kernels-and-dispatch.md`
- Create: `docs/lld/generation-and-platforms.md`
- Create: `docs/verification.md`
- Create: `docs/plans/2026-08-13-simd-production-design.md`
- Create: `docs/plans/2026-08-13-simd-production.md`
- Modify: `docs/README.md`
- Modify: `README.md`
- Retain: `ROADMAP.md`, `docs/api.md`, `docs/platforms.md`, `docs/kernels.md`,
  `docs/wrong.md`, and `docs/research/*.md`

**Step 1: Write the production design**

Document the shipped v1 package boundary, fallback-first dispatch, generated
assembly model, platform tiers, API stability, allocation/sizing contracts,
and the rule that new kernels require source, disassembly, correctness, and
cross-tier evidence. Link existing detailed references instead of copying them.

**Step 2: Write the LLDs**

Specify public operation families and alias/length rules in
`api-and-memory.md`; feature detection, per-operation dispatch, and fallback in
`kernels-and-dispatch.md`; generator inputs, emitted artifacts, ABI rules, and
platform gates in `generation-and-platforms.md`.

**Step 3: Write the future production plan**

Turn remaining `ROADMAP.md` and `docs/plans/kernels-backlog.md` work into staged
TDD tasks. Do not claim unimplemented kernels or platform support.

**Step 4: Write verification and agent context**

Document `make verify`, `make check-emission`, cross-tier tests, pure-Go tests,
hardware reports, fuzzing, race tests, disassembly, benchmark noise, and release
gates. Make `AGENTS.md` and `CLAUDE.md` agree on the required reading order.

**Step 5: Link the document set**

Add concise links to `README.md` and `docs/README.md`. Keep the existing API,
platform, tutorial, guide, hardware, and research documents canonical.

**Step 6: Verify**

Run:

```bash
go test ./...
make verify
make check-emission
git diff --check
git diff --name-only main...HEAD
```

Expected: all gates pass; all changed paths end in `.md`.

**Step 7: Commit**

```bash
git add README.md AGENTS.md CLAUDE.md docs
git commit -m "docs: define simd production architecture"
```

### Task 3: Complete `simdblas` documentation

**Worktree:** `/tmp/opencode/simdblas-docs`

**Files:**
- Modify: `README.md`
- Create: `AGENTS.md`
- Create: `CLAUDE.md`
- Create: `docs/architecture.md`
- Create: `docs/lld/delegation-and-guards.md`
- Create: `docs/lld/level-2-and-level-3.md`
- Create: `docs/roadmap.md`
- Create: `docs/verification.md`
- Create: `docs/wrong.md`
- Create: `docs/plans/2026-08-13-simdblas-production-design.md`
- Create: `docs/plans/2026-08-13-simdblas-production.md`
- Retain: `docs/guide.md`, `CHANGELOG.md`, `CONTRIBUTING.md`

**Step 1: Write architecture and LLDs**

Describe the embedded `gonum/blas` implementation, caller-owned registration,
delegation as the compatibility oracle, measured thresholds, packing and
blocking decisions, numerical fallback conditions, and BLAS32/BLAS64 process
state. Keep exact current thresholds sourced from code.

**Step 2: Write roadmap and production plan**

Stage compatibility hardening, unaccelerated routine evaluation, complex and
packed/banded coverage, workload benchmarks, and release criteria. A routine is
not promised until it beats delegation outside the noise floor and passes
differential tests.

**Step 3: Write verification and the adverse record**

Document differential tests, fast-path engagement tests, race/vet gates, BLAS
conformance, architecture coverage, and benchmark procedure. Move no historical
facts out of the README; summarize measured rejected variants in
`docs/wrong.md` with links back to their source.

**Step 4: Write agent context and navigation**

Create `AGENTS.md` and `CLAUDE.md`; add the document map to `README.md` without
changing measured claims.

**Step 5: Verify and commit**

Run:

```bash
go test ./...
go test -race ./...
go vet ./...
git diff --check
git diff --name-only main...HEAD
```

Expected: all gates pass and only Markdown paths changed.

```bash
git add README.md AGENTS.md CLAUDE.md docs
git commit -m "docs: define simdblas production architecture"
```

### Task 4: Complete `simdjson` documentation

**Worktree:** `/tmp/opencode/simdjson-docs`

**Files:**
- Modify: `README.md`
- Create: `AGENTS.md`
- Modify: `CLAUDE.md`
- Create: `docs/architecture.md`
- Create: `docs/lld/index-and-validation.md`
- Create: `docs/lld/value-and-ownership.md`
- Create: `docs/lld/streaming-and-parallelism.md`
- Create: `docs/lld/marshal-and-codegen.md`
- Create: `docs/roadmap.md`
- Create: `docs/verification.md`
- Create: `docs/plans/2026-08-13-simdjson-production-design.md`
- Create: `docs/plans/2026-08-13-simdjson-production.md`
- Retain: `docs/wrong.md`, `docs/competition.md`, `docs/cpp-baseline.md`,
  `docs/parallelism.md`, `docs/lazy-paths.md`, committed benchmark snapshots,
  figures, and completed plans

**Step 1: Write architecture and LLDs**

Describe the two-stage index, validating and non-validating entry points,
partial/path scans, value lifetimes, mmap ownership, streaming serial semantics,
bounded parallel APIs, stdlib-compatible marshal/unmarshal, and generated
encoder boundary. Source every limit and API name from current declarations and
tests.

**Step 2: Write roadmap and production plan**

Consolidate only still-open, measured opportunities from the competition,
parallelism, lazy-path, and wrong records. Preserve pre-1.0 status and do not
revive rejected experiments without new evidence.

**Step 3: Write verification and agent context**

Document stdlib differential suites, conformance corpora, fuzz targets, tier and
pure-Go tests, cross builds, benchmark gates, the 8.3 percent layout floor, and
the nested comparison-module limitation. Synchronize `AGENTS.md` and
`CLAUDE.md`.

**Step 4: Link, verify, and commit**

Run:

```bash
go test ./...
go test -race ./...
go vet ./...
go test -tags purego ./...
make fmt-check
make test-tiers
git diff --check
git diff --name-only main...HEAD
```

Expected: repository gates pass. Record the existing `bench/go.mod` vet blocker
if full `make verify` still requests dependency edits; do not modify module
files.

```bash
git add README.md AGENTS.md CLAUDE.md docs
git commit -m "docs: define simdjson production architecture"
```

### Task 5: Complete `simdcsv` documentation

**Worktree:** `/tmp/opencode/simdcsv-docs`

**Files:**
- Modify: `README.md`
- Create: `AGENTS.md`
- Create: `CLAUDE.md`
- Create: `docs/architecture.md`
- Create: `docs/lld/reader.md`
- Create: `docs/roadmap.md`
- Create: `docs/verification.md`
- Create: `docs/wrong.md`
- Create: `docs/plans/2026-08-13-simdcsv-production-design.md`
- Create: `docs/plans/2026-08-13-simdcsv-production.md`

**Step 1: Write architecture and the reader LLD**

Document whole-input buffering, delimiter scans, quote handling, scratch reuse,
record lifetimes, field-count behavior, malformed-input differences from
`encoding/csv`, and the exact current exported surface. Record the stale source
comment about nonexistent parse helpers as a known implementation-doc defect;
do not edit the Go file in this session.

**Step 2: Write roadmap, future plan, verification, and wrong record**

Stage safety/compatibility decisions, streaming options, ownership tests,
malformed-input fuzzing, and performance gates. Record the measured quoted-path
loss and rejected delegation/short-span variants.

**Step 3: Write agent context and navigation**

Create `AGENTS.md` and `CLAUDE.md`; add links to `README.md` while preserving its
current historical benchmark language.

**Step 4: Verify and commit**

Run:

```bash
go test ./...
go test -race ./...
go vet ./...
git diff --check
git diff --name-only main...HEAD
```

```bash
git add README.md AGENTS.md CLAUDE.md docs
git commit -m "docs: define simdcsv production architecture"
```

### Task 6: Complete `simdvec` documentation

**Worktree:** `/tmp/opencode/simdvec-docs`

**Files:**
- Modify: `README.md`
- Create: `AGENTS.md`
- Create: `CLAUDE.md`
- Create: `docs/architecture.md`
- Create: `docs/lld/index-and-search.md`
- Create: `docs/roadmap.md`
- Create: `docs/verification.md`
- Create: `docs/wrong.md`
- Create: `docs/plans/2026-08-13-simdvec-production-design.md`
- Create: `docs/plans/2026-08-13-simdvec-production.md`

**Step 1: Write architecture and LLD**

Document contiguous row-major storage, copied vectors, cosine normalization,
metric scoring, `GemvParallelInto`, score-buffer reuse, top-k selection, ID
semantics, dimension errors, and the current external-serialization requirement.

**Step 2: Write roadmap, plan, verification, and wrong record**

Stage concurrency, mutation, persistence, filtering, batching, and scale work
behind explicit compatibility and performance gates. Preserve the measured and
rejected int8 experiment in `docs/wrong.md`.

**Step 3: Write agent context and navigation**

Create both agent files and add links to the README without implying an
approximate index or unshipped lifecycle methods.

**Step 4: Verify and commit**

Run:

```bash
go test ./...
go test -race ./...
go vet ./...
git diff --check
git diff --name-only main...HEAD
```

```bash
git add README.md AGENTS.md CLAUDE.md docs
git commit -m "docs: define simdvec production architecture"
```

### Task 7: Complete `simdlogs` product documentation

**Worktree:** `/tmp/opencode/simdlogs-docs`

**Files:**
- Modify: `README.md`
- Modify: `AGENTS.md`
- Modify: `CLAUDE.md`
- Create: `docs/architecture.md`
- Create: `docs/lld/ingest.md`
- Create: `docs/lld/storage.md`
- Create: `docs/lld/query.md`
- Create: `docs/lld/api.md`
- Create: `docs/lld/cluster.md`
- Create: `docs/roadmap.md`
- Create: `docs/verification.md`
- Create: `docs/plans/2026-08-13-simdlogs-production-design.md`
- Create: `docs/plans/2026-08-13-simdlogs-production.md`
- Retain: `docs/design.md`, `docs/vl-parity.md`,
  `docs/benchmark-contract.md`, `docs/scale-curve.md`, `docs/wrong.md`, and the
  completed full-build plan

**Step 1: Rebase before source-backed writing**

Fetch the current `main` and rebase the documentation branch before describing
the shipped API. Main has advanced beyond the branch's original audit. Resolve
documentation conflicts only and preserve main's newer `docs/wrong.md` entries.

**Step 2: Write product design and architecture**

Describe the disk-backed columnar log database, ingest and query surfaces,
storage lifecycle, LogsQL boundary, non-VictoriaLogs APIs, app-level cluster
model, compact mode, and explicit non-goals. Link the completed parity and
historical build records.

**Step 3: Write subsystem LLDs**

Cover protocol normalization and batching; group files, dictionaries, indexes,
retention, backup, and compaction; parser and execution pipelines; HTTP route
contracts; and shard/replica fan-out and merge behavior. Source route and flag
lists from current code after rebase.

**Step 4: Write roadmap, future plan, and verification**

Separate compatibility, durability, operations, cluster, scale, and release
gates. Link the benchmark contract and scale curve instead of duplicating their
tables.

**Step 5: Update agent context and navigation**

Add the complete reading order and current shipped status to both agent files.
Run the deferred prose review against the rebased README and scale document.

**Step 6: Verify and commit**

Run:

```bash
go test ./...
go test -race ./...
go vet ./...
git diff --check
git diff --name-only main...HEAD
```

```bash
git add README.md AGENTS.md CLAUDE.md docs
git commit -m "docs: define simdlogs production architecture"
```

### Task 8: Design the production `simdhttp` stack

**Worktree:** `/tmp/opencode/simdhttp-docs`

**Files:**
- Modify: `README.md`
- Create: `AGENTS.md`
- Create: `CLAUDE.md`
- Create: `docs/architecture.md`
- Create: `docs/lld/router.md`
- Create: `docs/lld/http1-head-parser.md`
- Create: `docs/lld/http1-body-framing.md`
- Create: `docs/lld/net-http-integration.md`
- Create: `docs/roadmap.md`
- Create: `docs/verification.md`
- Create: `docs/wrong.md`
- Create: `docs/plans/2026-08-13-simdhttp-production-design.md`
- Create: `docs/plans/2026-08-13-simdhttp-production.md`

**Step 1: Correct the README boundary**

State that the shipped API is only the borrowed-buffer request-head parser.
Document its current Host, target, header-value, body-framing, and resource-limit
gaps. Move router, helpers, middleware, and complete framing claims to planned
documents.

**Step 2: Write architecture and router LLD**

Design a concrete low-allocation router that remains native to `net/http`,
`http.Handler`, `Request.PathValue`, middleware, `httptest`, HTTP/2, TLS,
proxies, observability, and WebSocket upgrades. Define route precedence,
parameters, method/host matching, conflict detection, 404/405 behavior, and
allocation ownership without interfaces inside the match loop.

**Step 3: Write parser and framing LLDs**

Define compatible-default and strict-security profiles; request-target, Host,
header, `Content-Length`, `Transfer-Encoding`, `Connection`, `Expect`, and
trailer validation; duplicate and CL/TE ambiguity rejection; fixed-length and
chunked streaming bodies; limits; pipelining boundaries; and bounded drain.

**Step 4: Write integration LLD, roadmap, and production plan**

Specify optional endpoint errors, `simdjson` helpers, query/form helpers,
standard middleware, raw/canonical header views, observability seams, and the
boundary before any custom server/connection engine. Stage parser safety before
router and helper expansion.

**Step 5: Write verification, wrong record, and agent context**

Require differential tests against `net/http`, request-smuggling corpora,
fuzzing, no-panic/no-hang limits, route differential tests, race tests,
disassembly, and realistic router/parser benchmarks. Record the changed typical
speed result and unsafe long-header finding.

**Step 6: Verify and commit**

Run:

```bash
go test ./...
go test -race ./...
go vet ./...
go test -run '^$' -bench BenchmarkSweep -benchmem -count=6 -shuffle=on .
go test -fuzz FuzzParseAgainstNetHTTP -fuzztime 30s .
git diff --check
git diff --name-only main...HEAD
```

Expected: tests and fuzz smoke pass; only Markdown paths changed.

```bash
git add README.md AGENTS.md CLAUDE.md docs
git commit -m "docs: design production simdhttp"
```

### Task 9: Design the full RFC 8949 `simdcbor` codec

**Worktree:** `/tmp/opencode/simdcbor-docs`

**Files:**
- Modify: `README.md`
- Create: `AGENTS.md`
- Create: `CLAUDE.md`
- Create: `docs/architecture.md`
- Create: `docs/lld/data-model.md`
- Create: `docs/lld/decoder.md`
- Create: `docs/lld/encoder.md`
- Create: `docs/lld/streaming-lazy-and-diagnostic.md`
- Create: `docs/roadmap.md`
- Create: `docs/verification.md`
- Create: `docs/wrong.md`
- Create: `docs/plans/2026-08-13-simdcbor-production-design.md`
- Create: `docs/plans/2026-08-13-simdcbor-production.md`

**Step 1: Correct the README boundary**

Document the current JSON-shaped `Marshal`, `Unmarshal`, and `Skip` subset,
including float64 integer loss, text-only map keys, rejected indefinite forms,
dropped tags, duplicate behavior, depth cap, and restricted marshal types. State
that full RFC 8949 support is planned.

**Step 2: Write architecture and data-model LLD**

Define root `simdcbor`, `simdcbor/value`, `simdcbor/diag`, and
`internal/codec` seams; all major/simple/float values; arbitrary map keys;
tags; duplicate policies; deterministic/canonical profiles; `RawMessage`; lazy
values; and the explicit JSON-shaped adapter.

**Step 3: Write decoder, encoder, and streaming LLDs**

Specify definite and indefinite state machines, break handling, limits,
shortest-form validation, integer and float fidelity, tag hooks, map-key
comparison, deterministic ordering, sequence streaming, zero-copy lifetimes,
diagnostic notation, and allocation/scratch rules.

**Step 4: Write roadmap, production plan, verification, and wrong record**

Stage malformed-input safety and data model first, then streaming, canonical
encoding, tags, lazy values, and diagnostics. Require RFC vectors,
`fxamacker/cbor` interoperability, duplicate/canonical profiles, fuzzing,
cross-architecture tests, race tests, and realistic selective-decode benchmarks.

**Step 5: Write agent context and verify**

Run:

```bash
go test ./...
go test -race ./...
go vet ./...
go test -run '^$' -bench BenchmarkSweep -benchmem -count=6 -shuffle=on .
git diff --check
git diff --name-only main...HEAD
```

Expected: all current gates pass; only Markdown paths changed.

```bash
git add README.md AGENTS.md CLAUDE.md docs
git commit -m "docs: design production simdcbor"
```

### Task 10: Design the complete `simdparquet` file library

**Worktree:** `/tmp/opencode/simdparquet-docs`

**Files:**
- Modify: `README.md`
- Create: `AGENTS.md`
- Create: `CLAUDE.md`
- Create: `docs/architecture.md`
- Create: `docs/lld/encodings-and-pages.md`
- Create: `docs/lld/schema-and-levels.md`
- Create: `docs/lld/file-reader.md`
- Create: `docs/lld/file-writer.md`
- Create: `docs/lld/compression-indexes-and-filtering.md`
- Create: `docs/roadmap.md`
- Create: `docs/verification.md`
- Create: `docs/wrong.md`
- Create: `docs/plans/2026-08-13-simdparquet-production-design.md`
- Create: `docs/plans/2026-08-13-simdparquet-production.md`

**Step 1: Correct the README boundary**

State that only `DecodeRLEBitpackedHybrid` ships. Document the crafted-input
hang/panic risks, error taxonomy, destination sizing, current thresholds, and
absence of a file reader/writer. Move plain, delta, page, schema, and file claims
to planned documents.

**Step 2: Write architecture and encoding LLD**

Define root, `simdparquet/encoding`, `simdparquet/compress`,
`simdparquet/format`, and optional Arrow seams. Specify overflow-safe page
decoding, all Parquet encodings, dictionaries, levels, checksums, page limits,
and no-panic/no-hang behavior for untrusted bytes.

**Step 3: Write schema, reader, writer, and filtering LLDs**

Cover physical/logical types, field IDs, nesting, repetition/definition levels,
metadata/footer validation, row groups, typed column APIs, projection, indexes,
predicate pushdown, compression, streaming reads/writes, ownership, and
transactional finalization.

**Step 4: Write roadmap, production plan, verification, and agent context**

Stage safe page codecs before schemas and files, then interoperability,
projection/filtering, writers, and Arrow. Require Apache Parquet golden files,
cross-language round trips, malformed corpus fuzzing, resource limits, race and
cross-architecture tests, and TPC-style benchmarks.

**Step 5: Verify and commit**

Run the current tests with a timeout because crafted inputs can hang:

```bash
go test -timeout 2m ./...
go test -race -timeout 5m ./...
go vet ./...
git diff --check
git diff --name-only main...HEAD
```

Expected: existing gates pass or any pre-existing hang is reported without
editing source; only Markdown paths changed.

```bash
git add README.md AGENTS.md CLAUDE.md docs
git commit -m "docs: design production simdparquet"
```

### Task 11: Complete the approved `simdimage` and FFmpeg LLD set

**Worktree:** `/tmp/opencode/simdimage-docs`

**Files:**
- Modify: `README.md`
- Create: `AGENTS.md`
- Create: `CLAUDE.md`
- Create: `docs/architecture.md`
- Create: `docs/lld/raster-and-frames.md`
- Create: `docs/lld/audio.md`
- Create: `docs/lld/media-types-and-ownership.md`
- Create: `docs/lld/ffmpeg-abi-and-loader.md`
- Create: `docs/lld/ffmpeg-bundles.md`
- Create: `docs/lld/pipelines-and-providers.md`
- Create: `docs/roadmap.md`
- Create: `docs/verification.md`
- Create: `docs/wrong.md`
- Retain: `docs/plans/2026-08-13-simdimage-production-design.md`
- Create: `docs/plans/2026-08-13-simdimage-production.md`

**Step 1: Correct the README boundary**

Describe only the current three planar operations as shipped. Document missing
dimension/radius validation, overlap safety, vertical scratch allocation, and
the stale resize package comment as known gaps. Link the approved future design
without suggesting FFmpeg is already a dependency.

**Step 2: Write architecture, raster, and audio LLDs**

Define Go-owned frame, plane, pixel-format, color, HDR, scratch, overlap, and
PCM contracts. Specify where SIMD kernels fit and require disassembly and
measured wins before replacing FFmpeg operations.

**Step 3: Write media ownership and ABI LLDs**

Specify exact rational timestamps; packets, frames, streams, retain/close and
borrowed-view lifetimes; no retained Go pointers; custom I/O callback registry;
explicit runtimes; one coherent library family; fixed-signature upstream
`purego`; no cgo, C shim, subprocess, or per-pixel FFI; and separate FFmpeg 8/9
ABI tables.

**Step 4: Write bundle and pipeline LLDs**

Specify system and official pinned LGPL sources, optional target modules,
embedded compressed payload extraction, signatures, manifests, digest caches,
atomic publication, SBOM/source/license records, bounded synchronous cores,
optional staged pipelines, hardware frames, and separate non-FFmpeg providers.

**Step 5: Write roadmap, future production plan, verification, and wrong record**

Use the eight approved stages from the production design. Require ABI probes,
two-major and desktop-trio CI, callbacks/race/leak tests, malformed-media fuzzing,
conformance corpora, child-process crash tests, bundle reproducibility, hardware
fallback, and real decode/filter/encode measurements.

**Step 6: Write agent context, verify, and commit**

Run:

```bash
go test ./...
go test -race ./...
go vet ./...
git diff --check
git diff --name-only main...HEAD
```

Expected: current raster gates pass; all branch-only paths end in `.md`.

```bash
git add README.md AGENTS.md CLAUDE.md docs
git commit -m "docs: complete simdimage production LLDs"
```

### Task 12: Run cross-family consistency review

**Files:**
- Modify only inaccurate or inconsistent Markdown found during review.

**Step 1: Check every required role**

For every repository, confirm the presence of `README.md`,
`docs/architecture.md`, at least one `docs/lld/*.md`, `docs/roadmap.md`,
`docs/verification.md`, `docs/wrong.md`, the dated production design and plan,
`AGENTS.md`, and `CLAUDE.md`.

**Step 2: Check shipped-versus-planned language**

Search READMEs for every future package, API, codec, route, format, platform,
and release claim. Confirm each is either currently source-backed or clearly
labeled planned and linked to a roadmap.

**Step 3: Check agent files**

Confirm both agent files in every repository include product boundary, current
status, reading order, package rules, disassembly-first policy, 8.3 percent
benchmark floor, bare-gate/`pipefail` rule, verification commands, release
gates, and `docs/wrong.md` policy.

**Step 4: Check links and names**

Resolve every local Markdown link and verify every README code identifier
against current exported declarations. Do not fix stale source comments in this
documentation-only session; record them in the repository roadmap or
verification document.

**Step 5: Request independent documentation review**

Use the `requesting-code-review` skill. Review all ten branches for factual
errors, unsafe ownership claims, unsupported benchmark claims, broken links,
and contradictions between README, architecture, LLD, roadmap, `AGENTS.md`, and
`CLAUDE.md`.

**Step 6: Commit review corrections separately per repository**

Use a concise `docs:` commit in each affected worktree. Never combine
repositories in one commit.

### Task 13: Rebase and run final verification

**Files:**
- No new planned files; modify Markdown only to resolve rebase conflicts.

**Step 1: Fetch and rebase one repository at a time**

Fetch `origin/main`, rebase the documentation branch, and preserve all
concurrent implementation/dependency changes. Never force-push and never modify
non-Markdown conflicts without user approval.

**Step 2: Run repository gates**

Run each repository's commands from `docs/verification.md`. Run gates bare, not
through output-truncating pipes. For `simdjson`, report the nested module blocker
if it still exists rather than changing dependencies.

**Step 3: Confirm Markdown-only diffs**

Run `git diff --name-only main...HEAD` and inspect every path in all ten
worktrees.

Expected: every changed path ends in `.md`.

**Step 4: Confirm clean branch state**

Run `git status --short --branch` and `git diff --check` in every worktree.

Expected: clean worktrees and no whitespace errors.

### Task 14: Integrate and publish documentation branches

**Step 1: Inspect each integration**

Before changing any main branch, inspect `git status`, the full branch diff,
recent commits, and the base-to-tip commit list.

**Step 2: Fast-forward local main branches**

Fast-forward only. Do not merge with a merge commit and do not reset concurrent
work.

**Step 3: Push without force**

Push each main branch to its existing origin. Do not create tags or GitHub
releases for documentation-only changes.

**Step 4: Verify remote SHAs**

Compare local `main`, `origin/main`, and GitHub's default branch SHA for each
repository.

**Step 5: Report results**

Report the ten pushed SHAs, verification commands and outcomes, any pre-existing
blockers, and the fact that no production implementation or release was made.
