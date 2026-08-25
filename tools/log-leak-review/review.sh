#!/usr/bin/env bash
# Review a diff for customer app data reaching the logs.
#
# Exit 0 = pass/warn, 1 = block, 2 = review unavailable.
set -uo pipefail

here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
rules="$here/rules.md"
model=${LOG_LEAK_REVIEW_MODEL:-claude-sonnet-5}
effort=${LOG_LEAK_REVIEW_EFFORT:-medium}
max_bytes=${LOG_LEAK_REVIEW_MAX_BYTES:-200000}
deadline=${LOG_LEAK_REVIEW_TIMEOUT:-180}

case "${1:-}" in
  --staged) range=--cached ;;
  '') echo "usage: review.sh <range>|--staged" >&2; exit 2 ;;
  *) range=$1 ;;
esac

diff_text=$(git diff -U10 --diff-filter=ACMR "$range" -- \
  '*.ts' '*.tsx' '*.js' '*.mjs' 2>/dev/null)
if [ -z "$diff_text" ]; then
  echo "log-leak-review: no JavaScript or TypeScript changes" >&2
  exit 0
fi

has_unignored_sink() {
  awk '
    /^@@/ {previous = ""; next}
    /^-/ {next}
    /^[ +]/ {
      added = /^\+/
      line = substr($0, 2)
      sink = line ~ /(\.(debug|info|warn|error|log)(\?\.)?\(|console\.|new Error\(|throw )/
      ignored = previous ~ /^[[:space:]]*\/\/[[:space:]]*log-leak-ignore([[:space:]]*--.*)?[[:space:]]*$/
      if (added && sink && !ignored) found = 1
      previous = line
    }
    END {exit !found}
  '
}

if ! has_unignored_sink <<<"$diff_text"; then
  echo "log-leak-review: no reviewable log sinks" >&2
  exit 0
fi

if [ "${#diff_text}" -gt "$max_bytes" ]; then
  diff_text="${diff_text:0:$max_bytes}
[diff truncated at ${max_bytes} bytes]"
fi

command -v claude >/dev/null || {
  echo "WARNING: Claude CLI not found; install it to enable log leak review." >&2
  exit 2
}

claude auth status >/dev/null 2>&1 || {
  echo "WARNING: Claude is not logged in; run 'claude auth login'." >&2
  exit 2
}

run_claude() {
  if command -v perl >/dev/null; then
    perl -e 'alarm shift; exec @ARGV' "$deadline" "$@"
  else
    "$@"
  fi
}

wait_with_spinner() {
  local pid=$1 i=0
  local frames=('|' '/' '-' '\')
  if [ -t 2 ]; then
    while kill -0 "$pid" 2>/dev/null; do
      printf '\rlog-leak-review: reviewing %s' "${frames[$((i % 4))]}" >&2
      i=$((i + 1))
      sleep 0.1
    done
    printf '\r\033[K' >&2
  fi
  wait "$pid"
}

out_file=$(mktemp "${TMPDIR:-/tmp}/log-leak-review.XXXXXX") || exit 2
trap 'rm -f "$out_file"' EXIT

printf '%s\n' "$diff_text" | run_claude claude -p \
  --model "$model" \
  --effort "$effort" \
  --tools "Read,Grep,Glob" \
  --permission-mode dontAsk \
  --append-system-prompt "$(cat "$rules")" \
  "The unified diff on stdin is about to be pushed. Apply the rules and emit the verdict." \
  >"$out_file" 2>/dev/null &
pid=$!
wait_with_spinner "$pid"
rc=$?
out=$(<"$out_file")

if [ $rc -ne 0 ]; then
  echo "WARNING: Claude log leak review failed (rc=$rc)." >&2
  exit 2
fi
if [ -z "$out" ]; then
  echo "WARNING: Claude log leak review returned no output." >&2
  exit 2
fi

IFS= read -r verdict <<<"$out"
case "$verdict" in
  'VERDICT: PASS'|'VERDICT: WARN') rc=0 ;;
  'VERDICT: BLOCK') rc=1 ;;
  *) echo "log-leak-review: invalid verdict: $verdict" >&2; exit 2 ;;
esac
printf '%s\n' "$out"
exit "$rc"
