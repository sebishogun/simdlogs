#!/usr/bin/env bash
# Interleaved A/B of two prebuilt test binaries, compared on the minimum.
#
#   ab-bench.sh <old.test> <new.test> [rounds] [benchtime]
#
# Interleaved because the two builds must meet the same machine: running all of
# A then all of B compares two different minutes. Minimum rather than mean
# because the noise here is one-sided -- something else stealing the CPU can
# only make a run slower.
set -o pipefail

OLD=$1
NEW=$2
ROUNDS=${3:-2}
BT=${4:-5x}
[ -x "$OLD" ] && [ -x "$NEW" ] || { echo "usage: ab-bench.sh <old.test> <new.test> [rounds] [benchtime]"; exit 2; }

run() { # binary -> "name ns"
	"$1" -test.run XXX -test.bench . -test.benchtime "$BT" 2>/dev/null |
		awk '/^Benchmark/ { print $1, $3 }'
}

declare -A best_old best_new
for ((r = 1; r <= ROUNDS; r++)); do
	while read -r name ns; do
		[ -z "$ns" ] && continue
		cur=${best_old[$name]}
		if [ -z "$cur" ] || [ "$ns" -lt "$cur" ]; then best_old[$name]=$ns; fi
	done < <(run "$OLD")
	while read -r name ns; do
		[ -z "$ns" ] && continue
		cur=${best_new[$name]}
		if [ -z "$cur" ] || [ "$ns" -lt "$cur" ]; then best_new[$name]=$ns; fi
	done < <(run "$NEW")
done

printf '%-52s %12s %12s %8s\n' BENCHMARK BEFORE AFTER CHANGE
regressions=0
for name in $(printf '%s\n' "${!best_new[@]}" | sort); do
	o=${best_old[$name]}
	n=${best_new[$name]}
	[ -z "$o" ] && { printf '%-52s %12s %12s %8s\n' "$name" - "$n" new; continue; }
	pct=$(awk -v o="$o" -v n="$n" 'BEGIN { printf "%.1f", (n - o) * 100.0 / o }')
	# 8.3% is this codebase's measured code-layout noise floor: a change smaller
	# than that cannot be told from nothing by wall clock.
	flag=""
	over=$(awk -v p="$pct" 'BEGIN { print (p > 8.3) ? 1 : 0 }')
	if [ "$over" = "1" ]; then
		flag=" <-- CHECK with instructions:u"
		regressions=$((regressions + 1))
	fi
	printf '%-52s %12s %12s %7s%%%s\n' "$name" "$o" "$n" "$pct" "$flag"
done
echo
echo "$regressions benchmark(s) outside the 8.3% noise floor"
