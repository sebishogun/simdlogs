# SIMD Ecosystem Documentation Implementation Plan

> **Status:** Superseded by
> [`2026-08-13-simd-family-production-documentation.md`](2026-08-13-simd-family-production-documentation.md),
> which covers the complete ten-repository architecture, LLD, roadmap,
> verification, and agent-context documentation scope. This file remains the
> record of the earlier README-refresh phase.

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refresh all nine SIMD-dependent repositories as source-backed technical documentation and push the verified main branches.

**Architecture:** Audit source, tests, benchmark records, tags, and releases before editing prose. Give `simdlogs` a full current-state front page, then apply the same contract-first structure at the natural scale of each smaller repository. Keep dependency changes separate and rebase documentation branches over them before integration.

**Tech Stack:** Go 1.25/1.26 modules, Markdown, Git worktrees, GitHub CLI, repository Make targets.

---

### Task 1: Refresh simdlogs current-state documentation

**Files:**
- Modify: `README.md`
- Modify only if navigation needs it: `docs/*.md`

**Steps:**
1. Inventory exported commands, registered HTTP endpoints, LogsQL pipes, storage contracts, and current benchmark records.
2. Replace the stale construction-phase status and conflicting benchmark summaries with one source-backed current state.
3. Document install/run, persistence, ownership, compatibility surface, scale tradeoffs, verification, and limitations.
4. Keep `docs/wrong.md` and completed plans historical.
5. Run `go test ./...` and any repository verification target.
6. Commit the documentation changes.

### Task 2: Refresh the released libraries

**Files:**
- Modify: `../simdblas/README.md`
- Modify: `../simdjson/README.md`
- Modify: `../simdcsv/README.md`
- Modify: `../simdvec/README.md`

**Steps:**
1. Check every current API, benchmark, requirement, limitation, tag, and GitHub release claim.
2. Remove stale operation counts and four-project ecosystem tables.
3. Link the canonical ecosystem inventory in `simd` instead of duplicating it.
4. State the `simd v1.20.0` dependency after the concurrent upgrade lands.
5. Run each repository's tests and stronger verification target where present.
6. Commit separately in each repository.

### Task 3: Refresh the active unreleased projects

**Files:**
- Modify: `../simdhttp/README.md`
- Modify: `../simdcbor/README.md`
- Modify: `../simdparquet/README.md`
- Modify: `../simdimage/README.md`

**Steps:**
1. Check exported APIs and tests for sizing, aliasing, malformed-input, and allocation contracts.
2. Keep benchmark charts and measured losses; add install, API, limits, verification, and status sections.
3. State that the projects use `simd v1.20.0` and that wall-clock numbers are amd64-only.
4. Run tests and Make verification targets.
5. Commit separately in each repository.

### Task 4: Integrate concurrent dependency work

**Steps:**
1. Fetch each local main branch state after the dependency agent finishes.
2. Rebase each documentation branch onto its current main branch.
3. Resolve only documentation conflicts; never overwrite module changes.
4. Rerun each repository's verification after rebase.
5. Fast-forward each local main branch.

### Task 5: Final review and push

**Steps:**
1. Confirm all nine worktrees are clean and all intended commits are on main.
2. Confirm `go.mod` names `simd v1.20.0` in every dependent repository.
3. Review `git diff` ranges for accidental source changes and broken links.
4. Push each main branch to its existing origin without force.
5. Verify local and remote main SHAs agree.
6. Report pushed SHAs, verification commands, documentation changes, and any release work intentionally left pending.
