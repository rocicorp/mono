#!/usr/bin/env bash
# Model-checks ChangeLogCoverage.tla against every configuration and asserts
# the expected outcome of each. Downloads tla2tools.jar on first run.
set -uo pipefail
cd "$(dirname "$0")"

JAR="${TLA_TOOLS:-.tla2tools.jar}"
if [[ ! -f "$JAR" ]]; then
  echo "fetching tla2tools.jar ..."
  curl -fsSL -o "$JAR" \
    https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar
fi

# config            expected  what it establishes
CASES=(
  "Fixed            pass      safety holds once a reseed takes back reservations"
  "Today            fail      a reseed under an open reservation breaks its promise"
  "RevalidateOnly   fail      re-reading bounds at confirm time is not enough"
  "SeedConfirm      fail      confirming on seedWatermark promises a span the log has no boundary for"
  "NoPause          pass      the purge pause has no safety role at this abstraction"
  "Liveness-Lease   pass      with the cap armed, no reservation pins the log forever"
  "Liveness-NoLease fail      without it, a wedged restore pins the log forever"
  "Liveness-Restore pass      a cap that outlasts a restore still lets followers return"
  "Liveness-ShortLease fail   a cap shorter than a restore turns it into a loop"
)

status=0
for case in "${CASES[@]}"; do
  read -r cfg expected desc <<<"$case"
  out=$(java -cp "$JAR" tlc2.TLC -workers auto -config "$cfg.cfg" ChangeLogCoverage 2>&1)
  if grep -q "^Model checking completed. No error has been found." <<<"$out"; then
    actual=pass
  elif grep -qE "^Error: (Invariant|Temporal properties)" <<<"$out"; then
    actual=fail
  else
    actual=error
    printf '%s\n' "$out" | tail -20
  fi
  if [[ "$actual" == "$expected" ]]; then
    printf '  ok    %-20s %s (%s)\n' "$cfg" "$expected" "$desc"
  else
    printf '  FAIL  %-20s expected %s, got %s\n' "$cfg" "$expected" "$actual"
    status=1
  fi
done
exit $status
