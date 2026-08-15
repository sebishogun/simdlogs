#!/usr/bin/env bash
# Run the soak.
#
#   scripts/soak.sh              1 hour   (developer mode)
#   scripts/soak.sh release      24 hours (release gate)
#   scripts/soak.sh 10m          any duration up to the 24h ceiling
#
# The soak is opt-in through SIMDLOGS_SOAK and this script is the supported way
# to set it. It is opt-in because a test that deliberately does not finish
# quickly must never be reachable from an ordinary `go test ./...`: in CI, in a
# pre-commit hook, in an editor's save action. The failure mode of that is not a
# slow build, it is a machine carrying hundreds of live server processes.

set -euo pipefail

case "${1:-1h}" in
  release) DURATION=24h ;;
  dev|"")  DURATION=1h  ;;
  *)       DURATION="$1" ;;
esac

# go test's own timeout, with headroom over the soak itself. Without one, a
# soak that hangs rather than finishing holds the machine indefinitely -- and
# `go test` defaults to 10 minutes, which would kill a legitimate 1-hour run
# and report it as a failure.
GO_TIMEOUT="$(python3 - "$DURATION" <<'PY'
import re, sys
spec = sys.argv[1]
units = {'s': 1, 'm': 60, 'h': 3600}
total = 0
for n, u in re.findall(r'(\d+)([smh])', spec):
    total += int(n) * units[u]
if total == 0:
    sys.exit("unparseable duration: " + spec)
print(f"{total + 600}s")   # ten minutes of headroom for setup and teardown
PY
)"

echo "soak: duration=$DURATION go-timeout=$GO_TIMEOUT"
echo "soak: this runs continuous load; ^C stops it and the test reports what it had"

# `timeout` on the outside as well as -timeout on the inside. Two bounds
# because they fail differently: -timeout fires inside the process and prints a
# goroutine dump, which is what you want for a hang; `timeout` fires from
# outside and works even if the process is wedged in a way the runtime cannot
# report. The outer one is deliberately slacker so the inner one wins when both
# could.
OUTER="$(python3 -c "import sys; print(int(sys.argv[1].rstrip('s')) + 300)" "$GO_TIMEOUT")"

# Bare, not piped. A pipe reports the LAST command's status, so `... | tee log`
# exits 0 on a failed soak -- this repository has had three red gates laundered
# into green exits exactly that way. Redirect if a log is wanted:
#   scripts/soak.sh release > soak.log 2>&1
set -o pipefail
SIMDLOGS_SOAK=1 SIMDLOGS_SOAK_DURATION="$DURATION" \
  timeout "${OUTER}" \
  go test -count=1 -timeout "$GO_TIMEOUT" -v -run 'TestSoak$' ./internal/tests/soak/
