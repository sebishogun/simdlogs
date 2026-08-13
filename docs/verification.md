# Verification

How anything gets measured or trusted in this repository. The discipline is
the same one the sibling SIMD repos hold to; the numbers and commands here
are current for the rebased main.

## Gates (run always, bare or with `set -o pipefail`)

```sh
go test ./...
go test -race ./...
go vet ./...
```

The suite covers storage round trips and backward-compatible posting formats,
crash-safe append, retention, backup/restore, tenant isolation, ingest
protocols, LogsQL parsing and execution, SQL/vector surfaces, Elasticsearch
search, live tail, replication/federation, parser and regex panic-safety, and
serial-versus-parallel query agreement.

**Never pipe a gate through `tail`** (or anything else) without `pipefail`:
the pipe reports the last command's status and the failure vanishes. This has
laundered a red fuzz run, a red README gate, and two red bench-check runs
into green exits in this family of repositories. Run gates bare, or
`set -o pipefail` first.

## Report tests (env-gated, not unit gates)

These are reports: they print numbers and land in commit messages and the
README, and they are never CI gates.

```sh
# realistic 1M-row query mix (default N; SIMDLOGS_REAL_N for the curve points)
SIMDLOGS_REAL=1 go test -run TestRealistic -v -timeout 90m ./internal/bench/

# single-node scale curve (sizes via SIMDLOGS_SCALE_SIZES, default 3M/30M/100M)
SIMDLOGS_SCALE=1 go test -run TestScale -v -timeout 30m ./internal/bench/

# disk-backed scale point against VictoriaLogs; stage internal/bench/victoria-logs first
SIMDLOGS_SCALEVL=1 SIMDLOGS_SCALEVL_N=100000000 \
  go test -run TestScaleVsVL -v -timeout 60m ./internal/bench/

# compact-mode tradeoff
SIMDLOGS_COMPACT=1 SIMDLOGS_REAL=1 go test -run TestRealistic -v -timeout 90m ./internal/bench/
```

### Staging the VictoriaLogs binary

The head-to-head runs VictoriaLogs as a subprocess from a prebuilt binary at
`internal/bench/victoria-logs` (gitignored), produced by building the
reference clone:

```sh
cd ../victorialogs-reference
go build -o /path/to/simdlogs/internal/bench/victoria-logs ./app/victoria-logs
```

Absent the binary, the VL half skips cleanly and the simdlogs half still
records a number — the harness says what it did not run. The binary is a
reference artifact only: the repo never imports VictoriaLogs code.

## The benchmark contract

`docs/benchmark-contract.md` is published before the implementation is
measured, so the numbers cannot be chosen to fit the code. It fixes the
corpus, the query classes, the metrics, and the method. The head-to-head
harness (`internal/bench/harness_test.go`) runs both engines as servers on
the same wire API, same machine, same corpus, interleaved in one process.

Method, the discipline:

- Deterministic corpus (fixed seed), reproducible by hashing two runs; both
  engines ingest byte-identical input.
- Both engines interleaved in one session — never an A/B across sessions.
- Each query class is the **minimum** of many samples after warmup (never a
  mean; the minimum is the least-perturbed run).
- Run the machine quiet: wait for load average under 1
  (`scripts/quiet-bench.sh` waits bounded — 180 minutes by default — and
  exits 3, never hangs).
- The instruction-set tier is named in every snapshot (the scale numbers were
  measured on amd64/AVX-512; **no wall-clock claim is made for another
  architecture**).
- Losses are published alongside wins (`docs/wrong.md`, the README, the scale
  curve). The 1M selective-window loss (0.8x) sits in the committed table.

## The noise floor

The code-layout noise floor here is **8.3%**. Anything smaller cannot be told
from nothing by wall-clock, and more samples do not help — layout noise is
per-build, not per-run. When a change is expected to be worth less than that:

- compare **instructions retired** and **cycles** with
  `perf stat -e instructions:u,cycles:u`, which are layout-independent;
- and read the disassembly, which is the only thing that explains *why*.

A/B builds must be interleaved in one session and compared on the minimum.
The wall clock has lied here before: it called an 11% instruction win a 24%
regression on a loaded box (`docs/wrong.md`); instructions retired settled
it.

## Disassemble first, always

Before proposing a cause for anything slow, before writing a variant, before
reading a profile delta — build it and read the instructions:

```
go test -c -o /tmp/x.test .
go tool objdump -s 'pkg\.functionName' /tmp/x.test | less
```

Go compiles in seconds; every guess that skips the disassembly costs a
build-measure-revert cycle and risks a wrong conclusion landing in
`docs/wrong.md` as fact. What the disassembly says that nothing else does:
register pressure (a spilled loop counter), eliminated bounds checks, index
multiply vs shift, inlined calls vs `memmove`, and which branch the compiler
laid out as fallthrough. Use gdb when a live register or breakpoint is
needed. Profile when a profile is the right tool (allocations, GC pressure,
cache misses) — but the disassembly comes first.

## Cross-architecture

Every simd kernel has a portable fallback through the `simd` package, so
missing architecture kernels affect speed, not availability. The query and
storage tests run the scalar paths as the conformance oracle and the SIMD
paths as the shipped path, differential-tested. Cross-arch verification is
`go test ./...` on the target arch with the tier name recorded; wall-clock
claims are amd64/AVX-512 only.

## Malformed input

- Parser panic-safety across malformed queries (`internal/query/hardening_test.go`):
  fuzz-style malformed LogsQL never panics the server (recoverPanic is the
  last line of defense; the parser is the first).
- Regex panic-safety: an invalid user pattern compiles to "matches nothing",
  never a panic; the parser validates up front for a clean 400.
- Ingest leniency: malformed lines are counted and skipped, never fatal; the
  batch boundary still flushes.
- `stream_context` is memory-bounded (`streamContextCap = 2,000,000`).

## Crash recovery

- Append is write-temp-fsync-rename: a crash between temp and rename leaves a
  `.tmp` file that `OpenStore` ignores; a truncated partial flush is skipped
  by the index.
- Graceful shutdown (SIGINT/SIGTERM): stop accepting, drain in-flight (15 s),
  flush writers, unmap stores — no buffered rows lost, no mmap leaks.
- Restore: `storage.RestoreTar` unpacks a `/admin/backup` tar into a fresh
  directory with entry names flattened so an archive cannot escape.

## Compatibility corpus

The VictoriaLogs comparison is a report against the real binary, not a
unit-test gate. The suites that drive it: `compat_test.go` (40/40 LogsQL on
the committed corpus), `shapes_test.go` (wire-shape comparisons),
`apisurface_test.go` + `perops_test.go` (every query argument changes the
answer, on both engines), `realistic_test.go` (15-field Zipfian footprint and
query mix). The four findings of the API-surface work are in
`docs/wrong.md` — a status-code probe reported 0 gaps while seven endpoints
answered 200 with a body no client can read and five ingest paths 404'd.
That is why the surface is verified by shape and by answer, not by status.

## What is a gate vs a report

- Gates: `go test ./...`, `go test -race ./...`, `go vet ./...`, and any
  contract test with a hard assertion (crash recovery, backup/restore,
  tenant isolation, format round trips, parser safety).
- Reports: every head-to-head number, the scale curve, footprint, and the
  README tables. Reports are published with their method and machine; they
  are not gates. A report that argues against a change is the deliverable of
  the work that produced it (`docs/wrong.md`).
