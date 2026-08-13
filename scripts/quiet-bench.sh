#!/usr/bin/env bash
# Wait for the machine to go quiet, then run the benchmark gate twice.
#
# Wall-clock benchmarks are worthless under load, and this box runs the user's
# own work. So the gate waits rather than measuring noise -- but it waits a
# BOUNDED number of times and always exits, because a poll loop with no ceiling
# is how a watcher once span for forty minutes.
#
#   scripts/quiet-bench.sh [max-wait-minutes] [load-threshold]
set -o pipefail

MAX_WAIT_MIN=${1:-180}
THRESHOLD=${2:-1.5}
OUT=${QUIET_BENCH_OUT:-/tmp/simdlogs-bench}
mkdir -p "$OUT"

quiet() {
	awk -v t="$THRESHOLD" '{ exit !($1 < t && $2 < t * 1.5) }' /proc/loadavg
}

waited=0
while ! quiet; do
	if [ "$waited" -ge "$MAX_WAIT_MIN" ]; then
		echo "GAVE UP after ${waited}m: load average never dropped below $THRESHOLD (now: $(cat /proc/loadavg))"
		exit 3
	fi
	sleep 60
	waited=$((waited + 1))
done

echo "machine quiet after ${waited}m (loadavg: $(cat /proc/loadavg)) -- measuring"

# Measure a FIXED commit in its own worktree. The working tree is being edited
# while this waits, and a benchmark that races an edit measures neither state.
REPO=$(cd "$(dirname "$0")/.." && pwd)
REV=$(git -C "$REPO" rev-parse HEAD)
WT=${QUIET_BENCH_WT:-/tmp/simdlogs-bench-wt}
rm -rf "$WT"
git -C "$REPO" worktree prune
git -C "$REPO" worktree add --detach "$WT" "$REV" >/dev/null 2>&1 || {
	echo "could not create the worktree; refusing to measure a moving tree"
	exit 4
}
# The staged reference binary is untracked, so the worktree has no copy.
cp "$REPO/internal/bench/victoria-logs" "$WT/internal/bench/" 2>/dev/null
echo "measuring $REV in $WT"
cd "$WT" || exit 1
trap 'cd /; git -C "$REPO" worktree remove --force "$WT" >/dev/null 2>&1' EXIT

# Two runs. Only what both agree on is real: one run on this box has produced
# a 94.9% "regression" in code nothing touched.
for run in 1 2; do
	echo "=== run $run: microbenchmarks ==="
	go test -run XXX -bench . -benchtime 1x -timeout 40m ./internal/... 2>&1 |
		grep -E "^(Benchmark|ok|FAIL|panic)" | tee "$OUT/micro-$run.txt"

	echo "=== run $run: per-operation head-to-head vs VictoriaLogs ==="
	SIMDLOGS_OPS=1 go test -count=1 -run TestPerOperation -v -timeout 40m ./internal/bench/ 2>&1 |
		grep -E "per-operation|simdlogs |SKIP|UNFAIR|FAIL|^ok" | tee "$OUT/perops-$run.txt"

	echo "=== run $run: disk footprint ==="
	go test -count=1 -run TestDiskFootprint -v ./internal/bench/ 2>&1 |
		grep -E "bytes on disk" | tee "$OUT/disk-$run.txt"

	if ! quiet; then
		echo "WARNING: load rose during run $run ($(cat /proc/loadavg)) -- treat it as suspect"
	fi
done

echo "=== done; results in $OUT ==="
