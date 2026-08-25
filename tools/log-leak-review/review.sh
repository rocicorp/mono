#!/usr/bin/env bash
# Review a diff for customer app data reaching the logs.
#
# Exit 0 = pass/warn, 1 = block, 2 = review unavailable.
set -uo pipefail

here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
rules="$here/rules.md"
model=${LOG_LEAK_REVIEW_MODEL:-claude-sonnet-5}
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
  echo "log-leak-review: claude CLI not found" >&2
  exit 2
}

run_claude() {
  if command -v perl >/dev/null; then
    perl -e 'alarm shift; exec @ARGV' "$deadline" "$@"
  else
    "$@"
  fi
}

out=$(printf '%s\n' "$diff_text" | run_claude claude -p \
  --model "$model" \
  --tools "Read,Grep,Glob" \
  --permission-mode dontAsk \
  --append-system-prompt "$(cat "$rules")" \
  "The unified diff on stdin is about to be pushed. Apply the rules and emit the verdict." \
  2>/dev/null)
rc=$?

if [ $rc -ne 0 ] || [ -z "$out" ]; then
  echo "log-leak-review: review did not complete (rc=$rc)" >&2
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
