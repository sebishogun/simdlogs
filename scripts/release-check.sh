#!/usr/bin/env bash
# Smoke-test a release binary.
#
#   scripts/release-check.sh                 build one and test it
#   scripts/release-check.sh ./simdlogs      test an existing binary
#
# This runs the ARTIFACT, not the source tree. Everything else in this
# repository tests packages in-process; a release can still fail in ways no
# package test can see -- a missing embedded asset, a flag that does not parse,
# a binary that needs a libc the target does not have -- and those only show up
# by starting the thing that ships.

set -euo pipefail

BIN="${1:-}"
WORK="$(mktemp -d)"
PORT="${SIMDLOGS_CHECK_PORT:-19428}"
SRV=""

cleanup() {
  # The server first, then the directory. Reversed, the store's files vanish
  # under a running process and the shutdown path reports failures that are
  # this script's fault.
  if [ -n "$SRV" ] && kill -0 "$SRV" 2>/dev/null; then
    kill "$SRV" 2>/dev/null || true
    wait "$SRV" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

step() { printf '\n=== %s\n' "$1"; }
fail() { printf 'FAIL: %s\n' "$1" >&2; exit 1; }

if [ -z "$BIN" ]; then
  step "build"
  BIN="$WORK/simdlogs"
  VERSION="$(git describe --tags --always --dirty 2>/dev/null || echo dev)"
  COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  # The flags a release build uses, so this checks what ships rather than a
  # near-miss: static, trimmed, stamped.
  CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
    -o "$BIN" ./cmd/simdlogs
fi

step "version"
"$BIN" -version || fail "-version does not run"

step "static linking"
if command -v file >/dev/null 2>&1; then
  if file "$BIN" | grep -q "dynamically linked"; then
    fail "the binary is dynamically linked; it will not run on a scratch image"
  fi
fi

step "start"
"$BIN" -storage "$WORK/data" -addr "127.0.0.1:$PORT" >"$WORK/server.log" 2>&1 &
SRV=$!

# Wait for readiness rather than sleeping. A fixed sleep is either too short
# (flaky) or too long (slow), and neither reports what went wrong.
for _ in $(seq 1 100); do
  if curl -fsS "http://127.0.0.1:$PORT/-/ready" >/dev/null 2>&1; then break; fi
  if ! kill -0 "$SRV" 2>/dev/null; then
    cat "$WORK/server.log" >&2
    fail "the server exited during startup"
  fi
  sleep 0.1
done
curl -fsS "http://127.0.0.1:$PORT/-/ready" >/dev/null || {
  cat "$WORK/server.log" >&2
  fail "never became ready"
}

step "health"
curl -fsS "http://127.0.0.1:$PORT/health" >/dev/null || fail "/health"
curl -fsS "http://127.0.0.1:$PORT/-/healthy" >/dev/null || fail "/-/healthy"
curl -fsS "http://127.0.0.1:$PORT/metrics" | grep -q '^vl_' || fail "/metrics has no vl_ metrics"

step "ingest"
printf '{"_time":"2026-06-01T12:00:00Z","_msg":"release check","level":"info"}\n' \
  | curl -fsS -X POST --data-binary @- \
      -H 'Content-Type: application/x-ndjson' \
      "http://127.0.0.1:$PORT/insert/jsonline" >/dev/null || fail "ingest"

step "query"
# The row has to come BACK. An ingest that answered 200 and stored nothing is
# the failure this whole repository keeps finding, and it is invisible unless
# something reads.
ROWS="$(curl -fsS "http://127.0.0.1:$PORT/select/logsql/query?query=%2A" | grep -c 'release check' || true)"
[ "$ROWS" -ge 1 ] || fail "the ingested row did not come back (found $ROWS)"

step "backup"
curl -fsS "http://127.0.0.1:$PORT/admin/backup" > "$WORK/backup.tar" || fail "backup"
[ -s "$WORK/backup.tar" ] || fail "the backup is empty"

step "restore"
"$BIN" restore -src "$WORK/backup.tar" -dry-run || fail "the backup does not validate"
"$BIN" restore -src "$WORK/backup.tar" -dst "$WORK/restored/tenant-0-0" || fail "restore"
# The GROUP files, not a manifest. A restored store carries its groups and
# bootstraps its manifest when a server first opens it -- checking for a
# manifest here fails on a restore that worked.
ls "$WORK"/restored/tenant-0-0/group-*.bin >/dev/null 2>&1 \
  || fail "the restored store has no group files"

step "restored data answers"
kill "$SRV"; wait "$SRV" 2>/dev/null || true; SRV=""
"$BIN" -storage "$WORK/restored" -addr "127.0.0.1:$PORT" >"$WORK/restored.log" 2>&1 &
SRV=$!
for _ in $(seq 1 100); do
  curl -fsS "http://127.0.0.1:$PORT/-/ready" >/dev/null 2>&1 && break
  sleep 0.1
done
ROWS="$(curl -fsS "http://127.0.0.1:$PORT/select/logsql/query?query=%2A" | grep -c 'release check' || true)"
[ "$ROWS" -ge 1 ] || {
  cat "$WORK/restored.log" >&2
  fail "the restored store does not answer with the row (found $ROWS)"
}

step "shutdown"
kill -TERM "$SRV"
for _ in $(seq 1 100); do
  kill -0 "$SRV" 2>/dev/null || break
  sleep 0.1
done
if kill -0 "$SRV" 2>/dev/null; then fail "did not exit on SIGTERM"; fi
wait "$SRV" 2>/dev/null || true
SRV=""

printf '\nrelease check passed: %s\n' "$("$BIN" -version)"
