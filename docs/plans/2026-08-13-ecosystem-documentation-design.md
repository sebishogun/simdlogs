# SIMD ecosystem documentation design

## Goal

Bring the nine repositories built on `simd` up to the same source-backed
documentation standard as the `simd` v1.20 documentation, without making the
small libraries carry a manual larger than their API.

## Scope

The repositories are `simdlogs`, `simdjson`, `simdblas`, `simdcsv`, `simdvec`,
`simdhttp`, `simdcbor`, `simdparquet`, and `simdimage`.

The dependency audit treats published `simd v1.20.0` as the shared baseline.
Dependency-file changes are owned by a separate concurrent task; this work does
not edit `go.mod` or `go.sum`.

## Documentation shape

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

The size follows the package. `simdlogs` and `simdjson` need navigation into
focused references. The four small workload packages need compact READMEs with
complete contracts, not copied sections from the root `simd` manual.

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
the audit. Final integration rebases each documentation branch onto the current
local main branch so concurrent dependency upgrades are preserved.

All resulting main branches are pushed to their existing remotes after tests
pass. No tag or GitHub release is created merely because documentation changed;
release publication requires a clean release-specific review.
