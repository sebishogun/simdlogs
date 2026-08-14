# simdlogs Production-Readiness Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Turn simdlogs into a secure, durable, bounded, observable, release-quality log database whose single-node and static-cluster behavior is tested under crashes, corruption, overload, and partial failure.

**Architecture:** Keep immutable columnar groups, mmap reads, the existing LogsQL engine, and static application-level sharding. Add authenticated tenancy, bounded request/query execution, durable atomic storage primitives, checksummed format parsing, reference-counted group snapshots, manifest-backed lifecycle operations, explicit distributed consistency, and versioned operational contracts.

**Tech Stack:** Go 1.26.5, `github.com/sebishogun/simd v1.20.0`, `github.com/sebishogun/simdjson v0.6.0`, Go standard-library HTTP/TLS/crypto packages, and narrowly selected protocol dependencies only where handwritten implementations would be riskier.

---

## Status And Scope

Status: execution-ready draft, revised 2026-08-14 from the current source audit. Nothing in this plan is shipped until its code, tests, LLD, changelog, and gates land.

This file is the one canonical implementation plan linked by `docs/roadmap.md`. It supersedes the shorter plan previously stored at this path. Do not create another production roadmap.

The target is production readiness inside the repository's product boundary:

- A logs-only database. Metrics ingestion remains out of scope.
- The documented Elasticsearch subset, not the complete Elasticsearch product.
- A static cluster with explicit consistency and failure behavior, not automatic membership, transactions, or general consensus.
- A single binary with local durable storage, authenticated multi-tenancy, backup/restore, retention, tiering, alert/rule configuration, and usable query surfaces.

There are two release exits:

1. **Single-node production exit:** Phases 0 through 7 complete. Cluster flags remain experimental and disabled by default.
2. **Full production exit:** Phases 0 through 10 complete, including the static-cluster contract and release artifacts.

## Non-Negotiable Contracts

1. An acknowledged write survives process death and power-loss assumptions documented by the fsync policy.
2. Malformed or unsupported input never receives an unqualified success response.
3. No query, ingest request, tenant, rule, or cluster peer can consume unbounded resources by default.
4. A missing shard never produces an unmarked successful partial result.
5. Every mmap remains valid while a reader owns it and is eventually unmapped after the last owner releases it.
6. Every persisted structure is bounds-checked and integrity-checked before use.
7. Unsupported API behavior is rejected explicitly; it is never silently ignored.
8. Current public routes retain their names. Behavior changes that correct silent loss or silent wrong answers are allowed and documented.
9. v7 groups remain readable. Any new writer format gets a new version and golden backward-compatibility fixtures.
10. Benchmarks are reports, never correctness gates. Published measurements use the corrected harness and the repository benchmark contract.

## Execution Rules For Subagents

- Start each task in an isolated worktree from the latest `main`.
- Read `README.md`, `docs/architecture.md`, the affected LLD, `docs/verification.md`, `docs/roadmap.md`, `docs/wrong.md`, and this plan before editing.
- Use test-driven development: failing test, observed failure, minimal implementation, passing targeted test, full gates, commit.
- One task equals one commit unless the task explicitly names multiple fixture-first commits.
- Update the affected LLD in the same commit as behavior.
- Never edit generated benchmark numbers without a valid rerun.
- Never run two subagents concurrently when their task file lists overlap.
- Run gates bare: `go test ./...`, `go test -race ./...`, `go vet ./...`, `gofmt -l .`, and `git diff --check`.
- Disassemble before making performance claims. Deltas below 8.3% use instructions and cycles.
- Record rejected measured changes in `docs/wrong.md`.

## Dependency Order

| Wave | Tasks | May run in parallel |
|---|---|---|
| 1 | 0.1-0.4 | 0.2 and 0.3 after 0.1 |
| 2 | 1.1-1.6, 2.1 | Security and storage foundations may run in separate worktrees |
| 3 | 2.2-2.6, 3.1-3.6 | Ingest tasks may parallelize by protocol after 2.1 |
| 4 | 4.1-4.4 | Sequential; all touch store lifecycle |
| 5 | 5.1-5.4, 6.1-6.2 | Backup and initial query executor may run separately |
| 6 | 6.3-6.8, 7.1-7.4 | Query tasks are sequential; ops tasks may run separately |
| 7 | 8.1-8.7 | Sequential cluster contract |
| 8 | 9.1-9.4, 10.1-10.3 | Verification before release |

---

## Phase 0: Truthful Baseline And Enforced Gates

### Task 0.1: Align The Design And Roadmap With The Current Audit

**Files:**
- Modify: `docs/plans/2026-08-13-simdlogs-production-design.md`
- Modify: `docs/roadmap.md`
- Modify: `docs/architecture.md`
- Modify: `docs/verification.md`

**Steps:**
1. Add the production blockers from this plan to the design: auth, limits, ingest acknowledgement, directory fsync, checksums, mmap leases, query cancellation, explicit cluster consistency, and benchmark-harness repair.
2. Remove the design assumption that a fixed five-minute mmap grace is safe.
3. Change roadmap exits from report-only compatibility to asserted body equivalence.
4. Mark the two release exits and preserve every not-shipped warning.
5. Run `go test ./internal/tests/docs` if present; otherwise run the repository link checks and `git diff --check`.
6. Commit: `docs: define complete production hardening scope`.

### Task 0.2: Make Formatting And CI Baselines Green

**Files:**
- Modify: `internal/storage/group.go`
- Create: `.github/workflows/ci.yml`
- Modify: `docs/verification.md`

**Steps:**
1. Run `gofmt -l .` and record the current failing file.
2. Reformat only `internal/storage/group.go`; assert `gofmt -l .` is empty.
3. Add CI jobs for `go test ./...`, `go test -race ./...`, `go vet ./...`, `go test -tags purego ./...`, `gofmt -l .`, and `git diff --check`.
4. Add Linux amd64 as the required job and compile jobs for supported non-amd64 targets.
5. Run all local gates.
6. Commit: `ci: enforce production baseline gates`.

### Task 0.3: Convert Compatibility Reports Into Assertions

**Files:**
- Modify: `internal/bench/compat_test.go`
- Modify: `internal/bench/apisurface_test.go`
- Modify: `internal/bench/shapes_test.go`
- Create: `internal/api/contracts_test.go`
- Modify: `docs/vl-parity.md`

**Steps:**
1. Add a failing test proving normalized body differences fail rather than log.
2. Make route probes assert status, content type, schema, and answer changes.
3. Keep VL-dependent assertions skipped loudly only when the staged binary is absent.
4. Separate contract gates from report benchmarks so normal CI never requires VL.
5. Run `go test ./internal/api ./internal/bench`.
6. Commit: `test: enforce API and LogsQL compatibility contracts`.

### Task 0.4: Repair The Benchmark Harness Before New Claims

**Files:**
- Modify: `internal/bench/perops_test.go`
- Modify: `internal/bench/realistic_test.go`
- Modify: `internal/bench/harness_test.go`
- Modify: `internal/bench/scalevl_test.go`
- Modify: `docs/benchmark-contract.md`
- Modify: `docs/verification.md`
- Modify: `docs/wrong.md`

**Steps:**
1. Add harness tests proving timed intervals exclude readiness sleeps.
2. Build a fresh store/process for each ingest sample so warmup and seven samples do not multiply the query corpus.
3. Build one fixed corpus for query timing and verify its row count before each engine run.
4. Interleave engines and query classes in one session with identical order.
5. Report accept latency and query-ready latency separately for asynchronous engines.
6. Preserve existing README tables as historical snapshots; do not replace numbers in this task.
7. Run harness unit tests without env-gated scale runs.
8. Commit: `test: correct benchmark sampling and corpus isolation`.

---

## Phase 1: Security, Admission Control, And Server Lifecycle

### Task 1.1: Introduce Typed Server Configuration And Safe Defaults

**Files:**
- Create: `internal/config/config.go`
- Create: `internal/config/config_test.go`
- Modify: `internal/api/server.go`
- Modify: `cmd/simdlogs/main.go`
- Modify: `docs/lld/api.md`

**Implementation contract:**

```go
type Limits struct {
	MaxBodyBytes       int64
	MaxDecompressed    int64
	MaxLineBytes       int
	MaxFieldsPerRecord int
	MaxFieldNameBytes  int
	MaxFieldValueBytes int
	MaxQueryRows       int
	MaxQueryBytes      int64
	MaxQueryDuration   time.Duration
	MaxConcurrentQuery int
	MaxConcurrentWrite int
	MaxOpenTenants     int
}
```

**Steps:**
1. Write table tests for defaults, invalid zero/negative values, and CLI overrides.
2. Add finite production defaults; zero must not mean unlimited outside explicit test configuration.
3. Pass configuration into `api.NewServer` through an options struct rather than global flags.
4. Keep test helpers able to request small deterministic limits.
5. Run `go test ./internal/config ./internal/api ./cmd/simdlogs`.
6. Commit: `feat: add bounded server configuration`.

### Task 1.2: Enforce Methods, Media Types, Body Limits, And Compression Limits

**Files:**
- Create: `internal/api/middleware.go`
- Create: `internal/api/middleware_test.go`
- Modify: `internal/api/protocols.go`
- Modify: `internal/api/server.go`
- Modify: `internal/api/es.go`
- Modify: `docs/lld/api.md`

**Steps:**
1. Add failing tests for wrong method (`405` plus `Allow`), unsupported media type (`415`), oversized body (`413`), malformed gzip (`400`), and decompression limit (`413`).
2. Replace unbounded `io.ReadAll` with `http.MaxBytesReader` and a decompressed byte-counting reader.
3. Support `Content-Encoding: gzip` only where declared; reject unknown encodings.
4. Apply endpoint-specific content-type allowlists.
5. Ensure error responses are JSON for JSON protocols and OTLP-compliant for OTLP.
6. Run `go test ./internal/api`.
7. Commit: `feat: enforce HTTP ingestion contracts`.

### Task 1.3: Add TLS And Secure Listener Configuration

**Files:**
- Create: `internal/config/tls.go`
- Create: `internal/config/tls_test.go`
- Modify: `cmd/simdlogs/main.go`
- Modify: `docs/lld/api.md`
- Modify: `README.md`

**Steps:**
1. Add tests for missing key/cert pairs, invalid certificates, minimum TLS version, and optional client CA.
2. Add `-tls.certFile`, `-tls.keyFile`, and `-tls.clientCAFile`.
3. Use TLS 1.2 minimum and prefer TLS 1.3.
4. Refuse public non-loopback startup without TLS unless `-insecure-http` is explicit.
5. Document reverse-proxy deployment as an alternative trust boundary.
6. Run command and config tests.
7. Commit: `feat: add TLS and mTLS listener support`.

### Task 1.4: Add Authentication, Authorization, And Trusted Tenancy

**Files:**
- Create: `internal/api/auth.go`
- Create: `internal/api/auth_test.go`
- Create: `internal/config/auth.go`
- Create: `internal/config/auth_test.go`
- Modify: `internal/api/tenant.go`
- Modify: `internal/api/server.go`
- Modify: `docs/lld/api.md`

**Implementation contract:**

```go
type Role string
const (
	RoleIngest Role = "ingest"
	RoleQuery  Role = "query"
	RoleAdmin  Role = "admin"
	RoleMetric Role = "metrics"
)

type Principal struct {
	Subject string
	Roles   map[Role]bool
	Tenants map[TenantKey]bool
}
```

**Steps:**
1. Add a route-permission matrix test covering every registered endpoint.
2. Add constant-time bearer-token verification from hashed token configuration and optional mTLS subject mapping.
3. Reject arbitrary tenant headers unless the authenticated principal is authorized for that tenant.
4. Reject malformed tenant IDs instead of mapping them to tenant 0.
5. Restrict `/admin/backup`, `/flags`, rule management, and diagnostic endpoints to administrators.
6. Make liveness optionally unauthenticated and keep readiness details protected.
7. Run `go test -race ./internal/api ./internal/config`.
8. Commit: `feat: enforce authenticated tenant RBAC`.

### Task 1.5: Bound Tenant Lifecycle And Per-Tenant Resources

**Files:**
- Modify: `internal/api/tenant.go`
- Create: `internal/api/tenant_limits_test.go`
- Modify: `internal/ingest/writer.go`
- Modify: `docs/lld/api.md`

**Steps:**
1. Write a failing test that creates more tenants than allowed and verifies no unbounded goroutine/directory growth.
2. Add a tenant registry with maximum open tenants, idle eviction, active-request references, and orderly writer close.
3. Add per-tenant ingest/query semaphores and byte/row counters.
4. Ensure health, UI, and unrelated routes do not create a tenant.
5. Expose rejected/evicted tenant metrics without tenant-ID labels by default.
6. Run race tests with concurrent tenant creation and eviction.
7. Commit: `feat: bound tenant resource lifecycle`.

### Task 1.6: Correct HTTP And Process Shutdown

**Files:**
- Modify: `cmd/simdlogs/main.go`
- Modify: `internal/api/server.go`
- Modify: `internal/api/syslog_listen.go`
- Create: `internal/api/shutdown_test.go`
- Modify: `docs/verification.md`

**Steps:**
1. Add subprocess tests for SIGTERM with active query, active ingest, live tail, TCP syslog, and background ops.
2. Add `ReadTimeout`, `IdleTimeout`, `MaxHeaderBytes`, per-route write deadlines, and query contexts; do not break live tail.
3. Retain and close UDP/TCP syslog listeners before closing writers.
4. Stop retention, recompaction, rules, and alert goroutines before closing stores.
5. Check `http.Server.Shutdown` errors; do not unmap stores while requests remain.
6. Add a forced-shutdown path that cancels request contexts and waits for reader leases.
7. Run the subprocess test under `-race`.
8. Commit: `fix: make shutdown drain every server resource`.

---

## Phase 2: Ingest Acknowledgement And Protocol Correctness

### Task 2.1: Introduce A Structured Ingest Result

**Files:**
- Create: `internal/ingest/result.go`
- Modify: `internal/ingest/jsonline.go`
- Modify: `internal/ingest/logfmt.go`
- Modify: `internal/ingest/loki.go`
- Modify: `internal/ingest/datadog.go`
- Modify: `internal/ingest/otel.go`
- Modify: `internal/ingest/otelproto.go`
- Modify: `internal/ingest/journald.go`
- Modify: `internal/ingest/syslog.go`
- Modify: `internal/api/protocols.go`

**Implementation contract:**

```go
type Result struct {
	Accepted int
	Rejected int
	Warnings []Warning
}

type Error struct {
	Kind   ErrorKind
	Offset int64
	Err    error
}
```

**Steps:**
1. Add table tests distinguishing fatal envelope errors from per-record rejections.
2. Make every parser return `(Result, error)`.
3. Return fatal errors for malformed top-level payloads and unsupported protocol structures.
4. Preserve NDJSON's per-line rejection semantics and surface rejected counts.
5. Map errors to protocol-appropriate HTTP responses.
6. Run all ingest and API tests.
7. Commit: `refactor: make ingest acceptance explicit`.

### Task 2.2: Fix Parallel Ingest Durability And Stream Fields

**Files:**
- Modify: `internal/ingest/jsonline.go`
- Modify: `internal/ingest/writer.go`
- Modify: `internal/ingest/options.go`
- Create: `internal/ingest/parallel_contract_test.go`
- Modify: `internal/api/server.go`
- Modify: `docs/lld/ingest.md`

**Steps:**
1. Add fault-injection tests proving any shard-writer close failure makes the request fail.
2. Add small-versus-large body tests proving identical `_stream` values and schemas.
3. Pass deployment stream fields into every temporary writer.
4. Make request `_stream_fields` override deployment defaults without appending two values for one row.
5. Return all parallel writer errors, preserving the first and counting failed shards.
6. Run `go test -race ./internal/ingest ./internal/api`.
7. Commit: `fix: preserve durability and stream identity in parallel ingest`.

### Task 2.3: Complete OTLP/HTTP Logs

**Files:**
- Modify: `internal/ingest/otel.go`
- Modify: `internal/ingest/otelproto.go`
- Modify: `internal/api/protocols.go`
- Create: `internal/ingest/otel_conformance_test.go`
- Modify: `docs/lld/ingest.md`

**Steps:**
1. Add JSON and protobuf golden fixtures for scalar, bytes, array, and kvlist `AnyValue` values.
2. Add fixtures for resource attributes, scope attributes, trace ID, span ID, severity number/text, flags, observed time, event name, and dropped-attribute counts.
3. Enforce exact OTLP media types and gzip behavior.
4. Return OTLP `partial_success` with rejected-record count and message.
5. Reject malformed protobuf rather than accepting a parsed prefix as full success.
6. Verify JSON and protobuf requests normalize to identical stored rows.
7. Run OTLP conformance tests and API tests.
8. Commit: `feat: complete OTLP HTTP log ingestion`.

### Task 2.4: Complete Loki And Datadog Wire Support

**Files:**
- Modify: `internal/ingest/loki.go`
- Create: `internal/ingest/lokipb.go`
- Modify: `internal/ingest/datadog.go`
- Modify: `internal/api/protocols.go`
- Create: `internal/ingest/loki_conformance_test.go`
- Create: `internal/ingest/datadog_conformance_test.go`
- Modify: `go.mod`
- Modify: `go.sum`
- Modify: `docs/lld/ingest.md`

**Steps:**
1. Add current Loki JSON and snappy-protobuf PushRequest fixtures, including structured metadata.
2. Add only the minimal snappy dependency; do not import the Loki server module.
3. Decode both Loki encodings into the same record model.
4. Add Datadog array/single-record, gzip, nested-value, timestamp, and `ddtags` fixtures.
5. Make malformed payloads return errors rather than empty successes.
6. Run protocol tests and `go mod tidy`.
7. Commit: `feat: complete Loki and Datadog ingestion contracts`.

### Task 2.5: Make Elasticsearch Bulk Semantics Explicit

**Files:**
- Modify: `internal/api/protocols.go`
- Modify: `internal/api/es.go`
- Create: `internal/api/es_bulk_contract_test.go`
- Modify: `docs/lld/api.md`

**Steps:**
1. Add fixtures for `index`, `create`, `update`, `delete`, malformed action lines, missing document lines, and per-item parse failures.
2. Implement `index` and `create` as the supported append operations with correct item status and `_id` handling.
3. Reject `update` and `delete` explicitly unless real update/delete storage semantics are implemented.
4. Preserve one response item per input action and set top-level `errors` correctly.
5. Never ingest an update wrapper as an empty log record.
6. Run ES contract tests.
7. Commit: `fix: enforce Elasticsearch bulk action semantics`.

### Task 2.6: Harden Native Syslog And Journald

**Files:**
- Modify: `internal/api/syslog_listen.go`
- Modify: `internal/ingest/syslog.go`
- Modify: `internal/ingest/journald.go`
- Create: `internal/api/syslog_contract_test.go`
- Create: `internal/ingest/journald_conformance_test.go`
- Modify: `docs/lld/ingest.md`

**Steps:**
1. Add RFC5424, RFC3164, RFC6587 newline, and RFC6587 octet-counted fixtures.
2. Add TCP read deadlines, connection limits, frame-size limits, TLS option, and scanner error reporting.
3. Propagate flush failures to TCP connection handling and metrics; count UDP loss where detectable.
4. Add journald malformed binary-length tests and rejected-entry reporting.
5. Include native syslog ingestion in row and error metrics.
6. Run race tests with many concurrent syslog connections.
7. Commit: `feat: harden native log transports`.

---

## Phase 3: Durable Storage Format And Recovery Foundation

### Task 3.1: Centralize Durable Atomic File Replacement

**Files:**
- Create: `internal/storage/atomicfile.go`
- Create: `internal/storage/atomicfile_unix.go`
- Create: `internal/storage/atomicfile_other.go`
- Create: `internal/storage/atomicfile_test.go`
- Modify: `internal/storage/store.go`
- Modify: `internal/storage/recompact.go`
- Modify: `internal/storage/cold.go`
- Modify: `internal/storage/backup.go`
- Modify: `docs/lld/storage.md`

**Steps:**
1. Create fault hooks for create, write, file sync, close, rename, directory open, and directory sync.
2. Add a table test for failure at every phase and verify final/temp-file state.
3. Implement one helper that writes fully, fsyncs the file, checks close, renames, and fsyncs the parent directory.
4. Use mode `0600` for log data unless configuration explicitly changes it.
5. Replace every ad hoc temp-write/rename implementation.
6. Run storage tests under `-race`.
7. Commit: `fix: make every storage replacement durably atomic`.

### Task 3.2: Add Bounds-Checked Parsing And Checksummed v8 Groups

**Files:**
- Modify: `internal/storage/group.go`
- Create: `internal/storage/group_v7.go`
- Create: `internal/storage/group_v8.go`
- Create: `internal/storage/group_corrupt_test.go`
- Add: `internal/storage/testdata/v7/*.bin`
- Modify: `docs/lld/storage.md`

**Format contract:** v8 retains the current header and sections, writes a v8-specific footer length, and appends a CRC32C over every preceding byte. The reader branches by group version. v7 remains read-only compatible.

**Steps:**
1. Add tests for every truncation point, oversized column count, overflowing offset/length, overlapping section, invalid column type, inconsistent row count, and checksum mismatch.
2. Add golden v7 fixtures before changing the writer.
3. Split parsing into `readGroupV7` and `readGroupV8`; neither may index a slice before validating it.
4. Add maximum row/column/section limits before allocation.
5. Write v8 by default and read v7/v8 identically through query tests.
6. Add `FuzzReadGroup` with a no-panic invariant.
7. Run storage, query, purego, and race gates.
8. Commit: `feat: add checksummed bounds-safe group format`.

### Task 3.3: Add Exclusive Store Locking And A Durable Manifest

**Files:**
- Create: `internal/storage/lock_unix.go`
- Create: `internal/storage/lock_windows.go`
- Create: `internal/storage/lock_test.go`
- Create: `internal/storage/manifest.go`
- Create: `internal/storage/manifest_test.go`
- Modify: `internal/storage/store.go`
- Modify: `docs/lld/storage.md`

**Manifest contract:** length-prefixed records with CRC32C; records contain sequence, add-group IDs, remove-group IDs, and optional write receipt. Replay stops at the last complete valid record. Manifest compaction uses the atomic-file helper.

**Steps:**
1. Add a subprocess test proving a second process cannot open the same directory for writing.
2. Add manifest replay tests for truncation at every byte and CRC corruption.
3. On a legacy directory without a manifest, validate current group files and atomically bootstrap one manifest snapshot.
4. Make group visibility follow committed manifest state rather than filename glob alone.
5. Ensure append sequence IDs never collide across restart or restore.
6. Run crash and race tests.
7. Commit: `feat: add exclusive storage lock and durable manifest`.

### Task 3.4: Add Process-Kill Crash Recovery Matrix

**Files:**
- Create: `internal/storage/crash_test.go`
- Create: `internal/storage/testhelper_test.go`
- Modify: `docs/verification.md`

**Steps:**
1. Build a helper subprocess controlled by phase markers.
2. SIGKILL at buffering, temp create, partial write, file sync, rename, directory sync, manifest append, manifest sync, and post-ack phases.
3. Reopen and assert: no partial group; every acknowledged batch present once; no uncommitted batch visible; no duplicate rows.
4. Repeat the matrix for recompaction and manifest compaction.
5. Run the test repeatedly with `-count=20` on Linux.
6. Commit: `test: prove crash recovery at every persistence phase`.

### Task 3.5: Add Corruption Policy And Storage Health

**Files:**
- Create: `internal/storage/health.go`
- Create: `internal/storage/corruption_test.go`
- Modify: `internal/storage/store.go`
- Modify: `internal/api/metrics.go`
- Modify: `internal/api/server.go`
- Modify: `docs/lld/storage.md`

**Steps:**
1. Add tests with one corrupt group among healthy groups.
2. Add configurable `fail` and `quarantine` policies; default to fail-ready while keeping healthy groups inspectable through an administrative recovery mode.
3. Move quarantined files atomically and record original path, reason, and checksum.
4. Expose storage health, corrupt group count, and last error to readiness and metrics.
5. Require operator acknowledgement before a degraded store becomes ready.
6. Run API and storage tests.
7. Commit: `feat: isolate and report corrupt groups`.

### Task 3.6: Make Write Failure And Retry Semantics Explicit

**Files:**
- Modify: `internal/ingest/writer.go`
- Create: `internal/ingest/writer_failure_test.go`
- Modify: `internal/api/protocols.go`
- Modify: `docs/lld/ingest.md`

**Steps:**
1. Add fault tests for disk full, short write, sync failure, mmap failure after rename, and manifest failure.
2. Ensure failed buffers are not silently discarded before their request receives an error.
3. Replace the permanently sticky undifferentiated error with a typed degraded state and recovery policy.
4. Document whether each error is retryable and whether a retry can duplicate data.
5. Return `503` plus retry metadata for transient storage failures.
6. Run writer and API race tests.
7. Commit: `fix: define write failure and retry behavior`.

---

## Phase 4: Correct mmap Ownership, Retention, And Tiering

### Task 4.1: Replace Raw Reader Slices With Leased Store Snapshots

**Files:**
- Create: `internal/storage/snapshot.go`
- Create: `internal/storage/snapshot_test.go`
- Modify: `internal/storage/store.go`
- Modify: `internal/query/engine.go`
- Modify: `internal/query/introspect.go`
- Modify: `internal/query/stats_range.go`
- Modify: `docs/lld/storage.md`
- Modify: `docs/lld/query.md`

**Implementation contract:**

```go
type Snapshot struct {
	Groups []*Reader
}

func (s *Store) Snapshot(from, to int64) (*Snapshot, error)
func (s *Snapshot) Close() error
```

Internally each group version owns an atomic reference count, retired flag, and one-shot unmap. A snapshot increments references while holding the store lock. The final release unmaps a retired version.

**Steps:**
1. Add tests holding a snapshot across replacement, unlink, and store close.
2. Implement group-version ownership and snapshot acquisition/release.
3. Change every query/introspection path to `defer snapshot.Close()`.
4. Make store close stop new snapshots and wait for current snapshots through context cancellation.
5. Delete the fixed-time ownership assumption.
6. Run `go test -race ./internal/storage ./internal/query ./internal/api`.
7. Commit: `refactor: make mmap lifetime reader-owned`.

### Task 4.2: Make Retention Atomic And Leak-Free

**Files:**
- Modify: `internal/storage/retention.go`
- Create: `internal/storage/retention_fault_test.go`
- Modify: `internal/api/retention.go`
- Modify: `docs/lld/storage.md`

**Steps:**
1. Add tests for unlink failure, concurrent snapshot, restart after failure, and eventual unmap/disk reclamation.
2. Do not remove a group from committed manifest state when deletion policy cannot be persisted.
3. Commit manifest removal first, unlink second, and retain retryable tombstones for failed unlink.
4. Retire the mmap through snapshot ownership and verify it unmaps after the final reader.
5. Expose retention failure and pending tombstone metrics.
6. Run fault tests under `-race`.
7. Commit: `fix: make retention durable and leak-free`.

### Task 4.3: Serialize Recompaction And Cold Demotion Safely

**Files:**
- Modify: `internal/storage/recompact.go`
- Modify: `internal/storage/cold.go`
- Create: `internal/storage/tiering_fault_test.go`
- Modify: `docs/lld/storage.md`

**Steps:**
1. Add tests racing retention, recompaction, demotion, promotion, query snapshots, and shutdown.
2. Add a structural-operation mutex or per-group generation check so a stale candidate cannot recreate a removed group.
3. Swap versions only if the expected generation is still current.
4. Retire old mappings through leases, not elapsed time.
5. Check every remove error and prevent restart resurrection.
6. Make promotion idempotent, collision-safe, fsynced, checksummed, and sequence-aware.
7. Run race and crash tests.
8. Commit: `fix: serialize safe tiering transitions`.

### Task 4.4: Stop And Drain Every Background Operation

**Files:**
- Modify: `internal/api/tiering.go`
- Modify: `internal/api/retention.go`
- Modify: `internal/api/logrules.go`
- Modify: `internal/api/alerts.go`
- Modify: `internal/api/server.go`
- Create: `internal/api/background_test.go`

**Steps:**
1. Add a goroutine-leak test around repeated server start/stop.
2. Give every loop a context and wait group.
3. Stop loops before store close and wait for active operations.
4. Reject new operations once shutdown starts.
5. Run race tests with rapid start/stop cycles.
6. Commit: `fix: drain background services on shutdown`.

---

## Phase 5: Backup, Restore, Compaction, And Disk Governance

### Task 5.1: Make Backups Snapshot-Consistent And Self-Validating

**Files:**
- Modify: `internal/storage/backup.go`
- Create: `internal/storage/backup_manifest.go`
- Create: `internal/storage/backup_contract_test.go`
- Modify: `internal/api/server.go`
- Modify: `docs/lld/storage.md`

**Backup manifest fields:** format version, creation time, tenant key, manifest sequence/high watermark, group name, group version, row count, byte size, and checksum.

**Steps:**
1. Add concurrent retention/append/recompaction backup tests.
2. Flush the selected tenant before capturing the snapshot.
3. Back up leased reader bytes rather than reopening mutable paths.
4. Include and validate the manifest; fail the stream if any captured group cannot be emitted.
5. Add response trailers or an initial metadata envelope so late stream errors are detectable.
6. Restrict backup to admin authorization and one concurrent backup per tenant.
7. Run backup tests at hundreds of groups.
8. Commit: `feat: add consistent checksummed backups`.

### Task 5.2: Add Atomic Restore And A Supported Restore Command

**Files:**
- Modify: `internal/storage/backup.go`
- Create: `internal/storage/restore.go`
- Create: `internal/storage/restore_test.go`
- Create: `cmd/simdlogs/restore.go`
- Modify: `cmd/simdlogs/main.go`
- Modify: `README.md`
- Modify: `docs/lld/storage.md`

**Steps:**
1. Add tests for traversal, duplicate names, oversized entries, bad checksum, mixed tenant, unsupported format, existing destination, and interrupted restore.
2. Restore into a sibling staging directory with file/count/total-byte limits.
3. Validate every group through `ReadGroup`, fsync files and staging directory, then atomically rename into an empty destination.
4. Never overwrite a live or locked store.
5. Add `simdlogs restore -src FILE -dst DIR` with dry-run validation.
6. Open and query the restored store in the integration test.
7. Commit: `feat: add atomic validated restore command`.

### Task 5.3: Add Manifest-Backed Small-Group Compaction

**Files:**
- Create: `internal/storage/compact_groups.go`
- Create: `internal/storage/compact_groups_test.go`
- Modify: `internal/storage/manifest.go`
- Modify: `internal/api/tiering.go`
- Modify: `cmd/simdlogs/main.go`
- Modify: `docs/lld/storage.md`

**Steps:**
1. Add a test creating thousands of one-row request groups and asserting compaction preserves byte-equivalent query answers.
2. Add a single manifest transaction that adds compacted outputs and removes all inputs atomically.
3. Preserve stable timestamp/row ordering and all column values.
4. Crash at output write, output sync, manifest commit, and input unlink; assert no duplicate/lost rows after reopen.
5. Add thresholds for minimum group count, maximum input bytes, age, and IO rate.
6. Run scale-neutral correctness tests; measure performance separately before choosing defaults.
7. Commit: `feat: compact small immutable groups safely`.

### Task 5.4: Add Disk Pressure And Storage Quotas

**Files:**
- Create: `internal/storage/quota.go`
- Create: `internal/storage/quota_test.go`
- Modify: `internal/api/metrics.go`
- Modify: `internal/api/server.go`
- Modify: `cmd/simdlogs/main.go`
- Modify: `docs/lld/api.md`

**Steps:**
1. Add fake-free-space tests for warning, reject, and recovery thresholds.
2. Reject new writes before the filesystem reaches critical exhaustion while preserving queries and administration.
3. Add per-tenant byte quotas and global reserve bytes.
4. Make readiness degraded before writes begin failing.
5. Expose capacity, rejected writes, and quota state metrics.
6. Commit: `feat: enforce disk and tenant storage budgets`.

---

## Phase 6: Bounded, Cancellable, Streaming Query Execution

### Task 6.1: Introduce A Context-Aware Query Executor

**Files:**
- Create: `internal/query/executor.go`
- Create: `internal/query/executor_test.go`
- Modify: `internal/query/engine.go`
- Modify: `internal/query/parallel.go`
- Modify: `internal/query/pipes.go`
- Modify: `internal/api/server.go`
- Modify: `docs/lld/query.md`

**Implementation contract:**

```go
type Limits struct {
	MaxRows   int
	MaxBytes  int64
	MaxGroups int
	Deadline  time.Duration
	Memory    int64
}

type Executor struct {
	Store  Store
	Limits Limits
}

func (e *Executor) Execute(ctx context.Context, q *Query, sink Sink) error
```

**Steps:**
1. Add cancellation tests at group pruning, scan, parallel worker, materialization, aggregation, sort, and subquery phases.
2. Add typed errors for cancellation, timeout, row limit, byte limit, memory limit, and concurrency rejection.
3. Thread context through all query functions and worker loops.
4. Stop workers promptly and release snapshots on every error.
5. Map typed errors to stable HTTP statuses and JSON error envelopes.
6. Run query and API race tests.
7. Commit: `refactor: add cancellable query executor`.

### Task 6.2: Add Global And Per-Tenant Query Admission

**Files:**
- Create: `internal/query/budget.go`
- Create: `internal/query/budget_test.go`
- Modify: `internal/api/server.go`
- Modify: `internal/api/tenant.go`
- Modify: `internal/api/metrics.go`

**Steps:**
1. Add tests for global concurrency, per-tenant concurrency, queue timeout, and fair release.
2. Add weighted semaphores for query and ingest work.
3. Prevent every concurrent request from creating its own `GOMAXPROCS` worker pool; allocate workers from a shared budget.
4. Track decoded bytes, materialized rows, result bytes, and aggregate cardinality.
5. Return `429` for admission rejection and `504` for execution deadline.
6. Commit: `feat: govern query concurrency and memory`.

### Task 6.3: Stream Bare Row Queries

**Files:**
- Create: `internal/query/iterator.go`
- Create: `internal/query/iterator_test.go`
- Modify: `internal/query/engine.go`
- Modify: `internal/api/server.go`
- Modify: `docs/lld/query.md`

**Steps:**
1. Add a test querying more rows than fit the configured materialization budget while consuming them through a sink.
2. Implement a row iterator for non-materializing query plans.
3. Serialize NDJSON in bounded buffers and stop on client cancellation or result-byte limit.
4. Keep aggregation and globally reordering pipes on the bounded materialized path.
5. Verify streamed and old materialized answers are byte-equivalent on a deterministic corpus.
6. Add an allocation regression benchmark without making it a CI gate.
7. Commit: `feat: stream unpiped query results`.

### Task 6.4: Fix Pipeline Limits And Eliminate Silent Truncation

**Files:**
- Modify: `internal/api/server.go`
- Modify: `internal/query/pipes.go`
- Modify: `internal/query/subquery.go`
- Create: `internal/query/limit_contract_test.go`
- Modify: `docs/lld/query.md`

**Steps:**
1. Add failing cases for sort, offset, rename, delete, format, join, union, and stream context above `MaxRows`.
2. Ensure an execution budget never changes a successful query's answer silently.
3. Return a typed limit error unless an explicit LogsQL `limit` proves bounded semantics.
4. Replace stream-context's silent two-million-row degradation with an explicit error.
5. Bound join fanout, union rows, group-by cardinality, `uniq`, and sort memory.
6. Run full query tests.
7. Commit: `fix: make query limits semantics-preserving`.

### Task 6.5: Define Stable Ordering And Cursor Pagination

**Files:**
- Create: `internal/query/order.go`
- Create: `internal/query/order_test.go`
- Create: `internal/api/cursor.go`
- Create: `internal/api/cursor_test.go`
- Modify: `internal/api/server.go`
- Modify: `docs/lld/api.md`

**Steps:**
1. Pin single-node ordering against current reference behavior for equal timestamps, overlapping groups, and out-of-order ingest.
2. Define a stable tuple `(timestamp, group sequence, row index)`.
3. Implement newest/oldest selection correctly across overlapping groups.
4. Add HMAC-protected opaque cursors containing query hash, tenant, direction, and last tuple.
5. Reject cursor reuse with a different query or tenant.
6. Verify pagination returns every row exactly once across concurrent appends using a snapshot watermark.
7. Commit: `feat: add stable cursor pagination`.

### Task 6.6: Implement Real Stream-Scoped Context

**Files:**
- Modify: `internal/query/subquery.go`
- Create: `internal/query/stream_context_test.go`
- Modify: `docs/lld/query.md`

**Steps:**
1. Add fixtures with interleaved streams and duplicate timestamps.
2. Scope context to `_stream_id` rather than the entire time window.
3. Fetch bounded neighbors before/after each match using timestamp and row tie-breakers.
4. Deduplicate overlapping context ranges.
5. Return a limit error when requested context exceeds budget.
6. Commit: `fix: make stream_context stream-correct`.

### Task 6.7: Make Vector Search Reachable And Bounded

**Files:**
- Create: `internal/ingest/value.go`
- Modify: `internal/ingest/jsonline.go`
- Modify: `internal/ingest/otel.go`
- Modify: `internal/ingest/writer.go`
- Modify: `internal/storage/group.go`
- Modify: `internal/query/vector.go`
- Create: `internal/api/vector_contract_test.go`
- Modify: `docs/lld/ingest.md`
- Modify: `docs/lld/query.md`

**Steps:**
1. Define configured vector fields and accepted JSON/OTLP array representation.
2. Add tests for dimension consistency, NaN/Inf rejection, maximum dimension, and mixed records.
3. Extend the writer to emit `ColVector` through public ingest.
4. Replace collect-all-and-sort with a bounded top-K heap.
5. Enforce maximum `k`, dimensions, candidates, time, and result bytes.
6. Add reopen and cross-group vector tests.
7. Commit: `feat: connect bounded vector ingest and search`.

### Task 6.8: Correct Elasticsearch And SQL Query Contracts

**Files:**
- Modify: `internal/api/es.go`
- Modify: `internal/query/sql.go`
- Create: `internal/api/es_contract_test.go`
- Create: `internal/api/sql_contract_test.go`
- Modify: `docs/lld/api.md`

**Steps:**
1. Add tests proving `hits.total` is pre-size total and returned hits obey global size/order.
2. Implement `exists` or reject it; reject unknown/unsupported clauses with `400`.
3. Reject unsupported non-time ranges and `terms` until implemented deliberately.
4. Add SQL result limits, cancellation, and unsupported syntax errors.
5. Use the shared query executor for ES and SQL.
6. Commit: `fix: enforce ES and SQL query contracts`.

---

## Phase 7: Health, Observability, Rules, And UI

### Task 7.1: Implement Meaningful Liveness And Readiness

**Files:**
- Modify: `internal/api/server.go`
- Create: `internal/api/health.go`
- Create: `internal/api/health_test.go`
- Modify: `docs/lld/api.md`

**Steps:**
1. Add a health-state matrix for starting, ready, storage degraded, disk low, shutting down, and cluster incomplete.
2. Keep liveness process-only.
3. Make readiness require writable storage, no unacknowledged fatal writer error, acceptable disk reserve, and required cluster peers.
4. Return machine-readable JSON details to authorized callers.
5. Preserve simple compatibility routes while making their status truthful.
6. Commit: `feat: add truthful health and readiness`.

### Task 7.2: Complete Metrics, Structured Logs, And Audit Events

**Files:**
- Modify: `internal/api/metrics.go`
- Create: `internal/api/metrics_contract_test.go`
- Create: `internal/observability/log.go`
- Modify: `cmd/simdlogs/main.go`
- Modify: `docs/lld/api.md`

**Steps:**
1. Create a metric-name/meaning contract test.
2. Add route/status latency, active/queued query, ingest queue, rejected records, flush errors, corrupt groups, snapshot leases, retention/tiering, backup, quota, and cluster metrics.
3. Avoid unbounded tenant/field labels.
4. Replace standard log prints with `log/slog` fields for request ID, tenant, route, shard, and error class.
5. Add audit events for auth failures, admin backup/restore, rule changes, corruption acknowledgement, and topology reload.
6. Commit: `feat: add production observability contracts`.

### Task 7.3: Make Alert And Metric Rules Configurable And Stoppable

**Files:**
- Modify: `internal/api/logrules.go`
- Modify: `internal/api/alerts.go`
- Create: `internal/config/rules.go`
- Create: `internal/api/rules_contract_test.go`
- Modify: `cmd/simdlogs/main.go`
- Modify: `docs/lld/api.md`

**Steps:**
1. Add configuration fixtures for tenant, query, window, interval, operation, threshold, and labels.
2. Validate metric/label names and alert operators.
3. Evaluate bounded windows through the query executor, not all history.
4. Start and stop rules under server context; expose last evaluation/error/time.
5. Add an authenticated management API only if file-based reload is insufficient; otherwise document restart/reload semantics.
6. Commit: `feat: make log rules operationally usable`.

### Task 7.4: Fix And Secure The Embedded UI

**Files:**
- Modify: `internal/api/ui.html`
- Modify: `internal/api/ui.go`
- Create: `internal/api/ui_test.go`
- Modify: `README.md`

**Steps:**
1. Add browser-shape tests for the current dense hits response.
2. Fix histogram rendering and query errors.
3. Remove arbitrary tenant selection for non-admin users; use authenticated tenant context.
4. Add CSP, frame, content-type, and referrer security headers.
5. Add cursor pagination and cancellation to the UI.
6. Commit: `fix: make the embedded UI match secured APIs`.

### Single-Node Production Exit

Before enabling a single-node release candidate, all of these must pass:

- Every Phase 0-7 task complete.
- 24-hour ingest/query/retention/recompaction soak with no race, mmap growth, goroutine growth, or acknowledged data loss.
- Repeated crash matrix and restore drill.
- Security review of auth, tenant isolation, TLS, backup, and admin routes.
- Finite default resource limits documented in README and LLDs.
- Cluster remains explicitly experimental and disabled by default.

---

## Phase 8: Production-Safe Static Cluster

### Task 8.1: Add A Versioned Internal Cluster Protocol And Client

**Files:**
- Create: `internal/api/cluster_protocol.go`
- Create: `internal/api/cluster_client.go`
- Create: `internal/api/cluster_protocol_test.go`
- Modify: `internal/api/cluster.go`
- Modify: `docs/lld/cluster.md`

**Wire contract:** every internal response carries protocol version, shard ID, replica ID, completeness, high watermark, result or typed error, and trace ID. Public response envelopes are not reused as internal merge state.

**Steps:**
1. Add version mismatch, timeout, malformed response, TLS failure, and authorization tests.
2. Replace `http.DefaultClient` with configured clients using deadlines, connection pools, mTLS, and bounded response bodies.
3. Copy required content type and tracing headers explicitly.
4. Reject unknown protocol versions.
5. Commit: `feat: version the internal cluster wire protocol`.

### Task 8.2: Add Idempotent Writes And Explicit Consistency

**Files:**
- Create: `internal/storage/receipts.go`
- Create: `internal/storage/receipts_test.go`
- Modify: `internal/storage/manifest.go`
- Modify: `internal/api/cluster.go`
- Create: `internal/api/cluster_write_test.go`
- Modify: `docs/lld/cluster.md`

**Steps:**
1. Generate or accept a cryptographically random write ID and persist it with the manifest commit.
2. Add bounded receipt retention and manifest compaction.
3. Add `one`, `quorum`, and `all` consistency; default production configuration to `all` until repair is proven.
4. Return success only when the selected consistency level acknowledges the same write ID.
5. Retry safely without duplicate rows on replicas that already committed the write.
6. Return replica-level failure metadata to authorized callers and metrics.
7. Commit: `feat: add idempotent consistent cluster writes`.

### Task 8.3: Eliminate Silent Partial Reads

**Files:**
- Modify: `internal/api/cluster_client.go`
- Modify: `internal/api/cluster.go`
- Create: `internal/api/cluster_failure_test.go`
- Modify: `docs/lld/cluster.md`

**Steps:**
1. Add one/all replica failure tests for every federated endpoint.
2. Try replicas only on retryable transport/server errors, not arbitrary `4xx` responses.
3. Default to failure if any required shard has no complete replica.
4. Support explicit `allow_partial_response=1` with status `206`, missing-shard headers, and a response shape that marks incompleteness.
5. Cancel peer requests when the client cancels or the global deadline expires.
6. Commit: `fix: make cluster read completeness explicit`.

### Task 8.4: Fix Existing Endpoint Merge Contracts Fixture-First

**Files:**
- Create: `internal/api/cluster_envelope_test.go`
- Modify: `internal/api/cluster.go`
- Modify: `docs/lld/cluster.md`

**Steps:**
1. Add current-envelope fixtures for streams, stream IDs, plain stats query, hits, stats range, field values, and ES search.
2. Fix shared `values` envelopes for streams and stream IDs.
3. Merge dense hit series by labels and timestamps.
4. Merge stats-range series with identical labels instead of concatenating duplicates.
5. Apply field-value and ES limits globally after merge; preserve exact total counts.
6. Propagate backend errors and response content types.
7. Commit separately per merge family.

### Task 8.5: Implement Distributed LogsQL Planning

**Files:**
- Create: `internal/query/distributed.go`
- Create: `internal/query/distributed_test.go`
- Create: `internal/api/cluster_plan.go`
- Modify: `internal/api/cluster.go`
- Modify: `docs/lld/cluster.md`

**Steps:**
1. Classify pipes as row-local, mergeable aggregate, globally ordering, or coordinator-only.
2. Add a correctness-first fallback that fetches paged filtered rows and applies coordinator-only pipes once under budgets.
3. Add mergeable states for count, sum, min, max, average (sum+count), HLL count-uniq, histogram, top, and grouped aggregates.
4. Define quantile behavior: a tested mergeable sketch with documented tolerance, or explicit router rejection until one exists.
5. Add single-node-versus-N-shard differential tests for every committed pipe.
6. Never execute a global pipeline independently on shards and concatenate its final rows.
7. Commit: `feat: add correct distributed LogsQL execution`.

### Task 8.6: Complete Or Reject Every Router Surface Explicitly

**Files:**
- Modify: `internal/api/server.go`
- Modify: `internal/api/cluster.go`
- Create: `internal/api/cluster_surface_test.go`
- Modify: `docs/lld/cluster.md`

**Steps:**
1. Build a route table that labels each endpoint federated, router-local-by-design, or rejected.
2. Federate facets, tail, SQL, vector, metrics, alerts, flags, and health where semantically valid.
3. For admin backup/restore, implement coordinated cluster operations in Task 8.7 rather than returning local data.
4. Return a stable explicit error for any remaining unsupported router endpoint.
5. Assert no router-mode request silently reads the router's empty local store.
6. Commit: `feat: make the cluster route surface complete`.

### Task 8.7: Add Repair, Coordinated Backup, And Chaos Tests

**Files:**
- Create: `internal/api/cluster_repair.go`
- Create: `internal/api/cluster_repair_test.go`
- Create: `internal/api/cluster_backup.go`
- Create: `internal/api/cluster_backup_test.go`
- Create: `internal/api/cluster_chaos_test.go`
- Modify: `docs/lld/cluster.md`

**Steps:**
1. Compare replica manifest watermarks and write receipts.
2. Add bounded read repair/anti-entropy that copies validated immutable groups and never deletes the last good replica.
3. Add a coordinated backup barrier: flush all shards, capture per-shard high watermark, back up one complete replica per shard, and write a cluster manifest.
4. Add restore validation for topology, shard identity, and duplicate write receipts.
5. Run chaos scenarios: replica kill, router kill, network timeout, stale replica, corrupt replica, rolling restart, and disk full.
6. Require exact query answers or explicit failure throughout.
7. Commit: `feat: add static-cluster repair and backup`.

---

## Phase 9: Verification, Scale, And Soak

### Task 9.1: Add Fuzz, Fault, And Cross-Architecture Gates

**Files:**
- Create: `internal/ingest/fuzz_test.go`
- Create: `internal/query/fuzz_test.go`
- Create: `internal/storage/fuzz_test.go`
- Create: `.github/workflows/fuzz.yml`
- Create: `.github/workflows/cross.yml`
- Modify: `docs/verification.md`

**Steps:**
1. Fuzz every ingest envelope, LogsQL parser, ES parser, group parser, manifest, backup manifest, restore tar, and cluster envelope.
2. Assert no panic, bounded allocation, and deterministic typed error.
3. Add nightly fault/crash repetitions.
4. Test purego and cross-build/run where infrastructure supports amd64, arm64, ppc64le, s390x, and riscv64.
5. Commit: `test: add production fuzz and cross-platform gates`.

### Task 9.2: Run Long Soak And Resource-Leak Tests

**Files:**
- Create: `internal/tests/soak/soak_test.go`
- Create: `scripts/soak.sh`
- Modify: `docs/verification.md`

**Steps:**
1. Run concurrent ingest, query, tenant churn, retention, recompaction, backup, rules, and graceful restarts.
2. Record goroutines, mmap count, virtual memory, RSS, disk use, manifest size, file count, and query latency.
3. Assert bounded growth after steady state.
4. Provide one-hour developer and 24-hour release modes.
5. Commit: `test: add storage and lifecycle soak gate`.

### Task 9.3: Reproduce Scale And Head-To-Head Measurements

**Files:**
- Modify: `internal/bench/realistic_test.go`
- Modify: `internal/bench/scalevl_test.go`
- Modify: `internal/bench/cluster_test.go`
- Modify: `docs/scale-curve.md`
- Modify: `docs/wrong.md`
- Modify: `README.md`

**Steps:**
1. Run corrected realistic and scale harnesses on a quiet machine.
2. Record machine, CPU tier, Go version, commit, load, corpus hash, sample count, minima, peak RSS, and disk bytes.
3. Reproduce single-node points before any cluster claim.
4. Run a three-node point above one-node capacity.
5. Publish losses alongside wins and preserve superseded tables as historical records.
6. Commit: `docs: publish reproducible production measurements`.

### Task 9.4: Perform Security And Recovery Drills

**Files:**
- Create: `docs/security.md`
- Create: `docs/runbooks/recovery.md`
- Create: `docs/runbooks/backup-restore.md`
- Create: `docs/runbooks/cluster-degraded.md`
- Modify: `docs/verification.md`

**Steps:**
1. Test tenant escape, role bypass, header spoofing, token timing, oversized requests, decompression bombs, and cluster impersonation.
2. Restore a backup onto a clean machine and verify row/checksum/query equality.
3. Recover from one corrupt group and one lost replica using documented commands.
4. Record expected RTO/RPO and unresolved limitations.
5. Commit: `docs: add production security and recovery runbooks`.

---

## Phase 10: Stable Contract And Release

### Task 10.1: Freeze API, Storage, And Upgrade Contracts

**Files:**
- Modify: `docs/lld/storage.md`
- Modify: `docs/lld/api.md`
- Modify: `docs/lld/cluster.md`
- Create: `docs/compatibility.md`
- Modify: `README.md`

**Steps:**
1. List stable routes, request parameters, response schemas, error schemas, auth behavior, and cluster protocol version.
2. State v7 read compatibility and v8 write/read policy.
3. Define rolling-upgrade order and mixed-version cluster behavior.
4. Add golden API and storage fixtures to release CI.
5. Commit: `docs: freeze production compatibility contracts`.

### Task 10.2: Add Reproducible Release Artifacts

**Files:**
- Create: `.github/workflows/release.yml`
- Create: `scripts/release-check.sh`
- Create: `CHANGELOG.md`
- Create: `Dockerfile`
- Create: `docs/deployment.md`

**Steps:**
1. Build static binaries for supported targets from a clean tag.
2. Produce checksums, SBOM, provenance, and signed artifacts.
3. Build a non-root minimal container with read-only root filesystem guidance and mounted data/config paths.
4. Run restore, startup, health, ingest, and query smoke tests against release artifacts.
5. Generate changelog entries from merged task commits without inventing shipped work.
6. Commit: `release: add reproducible signed artifacts`.

### Task 10.3: Cut The Production Release

**Files:**
- Modify: `README.md`
- Modify: `docs/roadmap.md`
- Modify: `CHANGELOG.md`

**Steps:**
1. Run all core, race, vet, formatting, purego, fuzz, crash, restore, cluster, soak, compatibility, security, and artifact gates.
2. Confirm no unresolved critical/high production blocker remains.
3. Confirm current benchmark claims come only from the corrected harness.
4. Tag the release only after artifacts build from the exact tag.
5. Change README status from experimental/pin-a-commit only when the matching release exists.
6. Commit: `release: prepare simdlogs production release`.

---

## Final Acceptance Matrix

| Area | Required evidence |
|---|---|
| Security | Route-permission matrix, tenant spoof tests, TLS/mTLS tests, security drill |
| Ingest | Protocol conformance, malformed-input failures, parallel write fault tests |
| Durability | Directory-fsync atomic helper, v8 checksum, manifest, kill matrix |
| Recovery | Corruption isolation, atomic restore, backup drill, runbook |
| mmap lifecycle | Lease tests, retention/recompact races, bounded mmap count in soak |
| Query | Cancellation at every phase, finite defaults, streaming, pagination, no silent truncation |
| Vector | Public ingest, dimension checks, bounded top-K, reopen test |
| Operations | Truthful readiness, complete metrics, stoppable rules, structured logs |
| Cluster | Idempotent consistency, no silent partials, exact distributed plan, repair and chaos |
| Compatibility | Hard assertions for bodies/arguments, explicit ES subset, versioned internal wire |
| Performance | Corrected interleaved harness, quiet-machine metadata, current scale points |
| Release | CI green, signed reproducible artifacts, SBOM, changelog, stable contracts |

## Per-Task Completion Checklist

- Failing test observed before implementation.
- Minimal implementation passes targeted test.
- Affected LLD updated in the same commit.
- `go test ./...` passes.
- `go test -race ./...` passes.
- `go vet ./...` passes.
- `go test -tags purego ./...` passes when storage/query/SIMD behavior changed.
- `gofmt -l .` is empty.
- `git diff --check` passes.
- No unrelated user or agent changes modified.
- Any rejected measured idea recorded in `docs/wrong.md`.
- Commit message matches the task's named boundary.
