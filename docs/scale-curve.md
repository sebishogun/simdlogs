# Scale curve: simdlogs vs VictoriaLogs

Head-to-head, both engines ingested and queried from disk on the same machine,
interleaved, so the **ratios are load-robust** even when absolute numbers are
not (both run under identical conditions). Corpus is the synthetic
**unique-hex** shape — the deliberate worst case for footprint (every value
distinct, nothing to dedupe). The realistic 15-field Zipfian corpus is far
kinder on footprint (3.47× of VL, see `realistic_test.go`).

Ratios are simdlogs advantage (>1 = simdlogs wins); footprint is × of VL
(lower is better, <1 would beat VL).

| N | needle | selective | aggregation | ingest | footprint (× VL) |
|-----|--------|-----------|-------------|--------|------------------|
| 1M | 12.3× | 0.8× | 5.1× | 8.2× | — (VL <0.005GB, rounds to 0) |
| 10M | 19.7× | 1.1× | 8.4× | 2.56× | 19.6× |
| 100M | 25.0× | 2.0× | 19.2× | 1.11× | 20.9× |
| 1B | 12.9× | 1.9× | 3.4× | 1.56× | 19.4× |

At 1B: simdlogs ingest 8m42s (1.92M rec/s) vs VL 13m34s (1.23M rec/s); needle
1.68ms vs 21.7ms; selective 350ms vs 664ms; agg 5.6ms vs 19.3ms; 50.1GB vs
2.58GB.

## What the curve shows

- **simdlogs wins every query dimension + ingest at every scale, 1M→1B.**
- **Query wins peak mid-curve then ease at 1B.** needle 12→20→25→13×,
  aggregation 5→8→19→3.4×. The needle is O(groups) (a bloom check per group);
  at a billion rows that per-group cost shows, so the lead narrows but stays a
  clear win (1.68ms vs VL's 21.7ms). A global value→group index would make it
  O(1) — the endgame noted in `docs/wrong.md`.
- **Ingest advantage narrows then holds** (8.2→2.6→1.1→1.56×): VL parallelizes
  ingest well at scale; simdlogs stays ahead throughout.
- **Footprint is the tradeoff, worst-case ~20×** on the unique-hex corpus (3.47×
  realistic). This is by construction: our 42%-of-file inverted index is exactly
  what wins the query columns above, and VL has no such index. Measured, not a
  bug — see `docs/wrong.md`. Opt-in compact mode narrows it at a query-speed
  cost; the default keeps the speed.

## Reproduce

    # one decade
    SIMDLOGS_SCALEVL=1 SIMDLOGS_SCALEVL_N=10000000 \
      go test -run TestScaleVsVL -v -timeout 30m ./internal/bench/

    # 1B needs ~50GB scratch -- point it off tmpfs
    SIMDLOGS_SCALEVL=1 SIMDLOGS_SCALEVL_N=1000000000 SIMDLOGS_SCALE_DIR=$HOME/scale-tmp \
      go test -run TestScaleVsVL -v -timeout 60m ./internal/bench/

For the tightest absolute numbers run on a quiet machine (load avg < 1); the
ratios here hold under load because the two engines are measured back-to-back.
