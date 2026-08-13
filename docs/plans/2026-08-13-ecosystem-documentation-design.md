# SIMD ecosystem documentation design

## Goal

Bring the complete ten-repository SIMD family up to one source-backed
documentation standard. Each repository must explain both its shipped surface
and its approved production direction without presenting roadmap work as
implemented behavior.

## Scope

The repositories are `simd`, `simdlogs`, `simdjson`, `simdblas`, `simdcsv`,
`simdvec`, `simdhttp`, `simdcbor`, `simdparquet`, and `simdimage`.

The dependency audit treats published `simd v1.20.0` as the shared baseline.
Dependency-file changes are owned by a separate concurrent task; this work does
not edit `go.mod` or `go.sum`.

This is a documentation-only effort. It may add or modify Markdown files, but
it does not change Go source, tests, generated files, build scripts, native
libraries, repository dependencies, or release artifacts.

## Documentation shape

Every repository is self-contained and uses the following document roles:

| File | Role |
| --- | --- |
| `README.md` | Shipped API, measured behavior, current limitations, release status, and navigation. |
| `docs/architecture.md` | Product boundary, components, dependencies, and data flow. |
| `docs/lld/*.md` | Concrete APIs, ownership, algorithms, errors, concurrency, limits, and invariants. |
| `docs/roadmap.md` | Staged future work and measurable exit criteria. |
| `docs/verification.md` | Correctness, fuzzing, race, compatibility, architecture, disassembly, benchmark, and release gates. |
| `docs/wrong.md` | Retained rejected ideas, regressions, and adverse measurements. |
| `docs/plans/2026-08-13-*-production-design.md` | Approved product and architecture decisions. |
| `docs/plans/2026-08-13-*-production.md` | Executable TDD roadmap for a later implementation session. |
| `AGENTS.md` | Repository operating contract for coding agents. |
| `CLAUDE.md` | Repository-specific constraints and required reading order. |

An existing equivalent file may be retained instead of duplicated when it
already fulfills the role and all navigation points to it.

Every README remains a technical front page. It should answer, in order:

- what the package does and what it does not do;
- how to install and call it;
- the observable API and data-ownership contract;
- where the measured advantage comes from;
- the benchmark method, including losses and unmeasured architectures;
- limitations and fallback behavior;
- how correctness is checked;
- release or development status;
- where the rest of the ecosystem lives.

The size follows the package. `simd`, `simdlogs`, and `simdjson` need navigation
into focused references. Small workload packages need compact READMEs with
complete contracts, not copied sections from the root `simd` manual.

Released repositories retain source-backed API, ownership, compatibility, and
benchmark facts. Unreleased repositories distinguish their current small
surfaces from approved production designs. Historical plans remain historical;
they are not silently rewritten as current architecture.

## Agent context

Every repository must contain both `AGENTS.md` and `CLAUDE.md`. Each file must
state:

- the product boundary and explicit non-goals;
- the shipped status at the time the document was written;
- the required reading order for architecture, LLD, roadmap, verification, and
  decision records;
- package-specific ownership, malformed-input, concurrency, and compatibility
  rules;
- the requirement to build and inspect disassembly before attributing a
  performance change or adding a hot-loop variant;
- the benchmark noise policy and required cycle/instruction measurements;
- the repository verification and release gates;
- the rule that roadmap intent must never be described as shipped behavior.

`CLAUDE.md` is explicit repository context, not a pointer that depends on a
machine-global configuration. The two files may share facts, but neither may
contradict the other.

## Sources of truth

- API names and behavior: exported declarations and tests.
- Requirements and dependencies: the post-upgrade module files.
- Performance: committed benchmark tables, snapshots, and charts only.
- Feature status: parsers, endpoint registration, implementation tests, and
  completed plans.
- Release status: local tags and GitHub releases.
- SIMD platform scope: the published `simd v1.20.0` release and platform
  reference.

Conflicting benchmark snapshots are not blended. The README names one current
table and links the longer measurement record. Historical plans and
`docs/wrong.md` remain records rather than being rewritten as current prose.

## Verification

Each repository must pass its normal Go tests after documentation changes.
Where a repository already has a stronger Make target, that target runs too.
Local Markdown links and named exported functions are checked directly during
the audit. The diff from the branch base must contain Markdown files only.
Final integration rebases each documentation branch onto the current local main
branch so concurrent dependency upgrades are preserved.

All resulting main branches are pushed to their existing remotes after tests
pass. No tag or GitHub release is created merely because documentation changed;
release publication requires a clean release-specific review.
