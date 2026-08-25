#!/usr/bin/env bash
# Review a diff for customer app data reaching the logs.
#
#   tools/log-leak-review/review.sh origin/main..HEAD
#   tools/log-leak-review/review.sh --staged
#
# Exit 0 = clean or warnings only, 1 = blocking finding, 2 = could not run
# (fails open: the caller lets the push through and says so).
#
# Written for bash 3.2, which is what macOS ships.
set -uo pipefail

here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
rules="$here/rules.md"
model=${LOG_LEAK_REVIEW_MODEL:-claude-sonnet-5}
max_bytes=${LOG_LEAK_REVIEW_MAX_BYTES:-200000}
deadline=${LOG_LEAK_REVIEW_TIMEOUT:-180}

case "${1:-}" in
  --staged) range=--cached ;;
  "")       echo "usage: review.sh <range>|--staged" >&2; exit 2 ;;
  *)        range=$1 ;;
esac

# Ten lines of context so the model can see what a logged variable is near.
diff_text=$(git diff -U10 --diff-filter=ACMR "$range" -- \
  '*.ts' '*.tsx' '*.js' '*.mjs' 2>/dev/null)
if [ -z "$diff_text" ]; then
  echo "log-leak-review: no TS/JS changes in $range, nothing to review" >&2
  exit 0
fi

# Cheap pre-filter: does any *added* line look like a log sink or a thrown
# Error? Most pushes touch no logging at all and should cost nothing.
if ! printf '%s\n' "$diff_text" \
  | grep -qE '^\+.*(\.(debug|info|warn|error|log)(\?\.)?\(|console\.|new Error\(|throw )'
then
  echo "log-leak-review: no new log sinks in $range, skipping the model" >&2
  exit 0
fi

if [ "${#diff_text}" -gt "$max_bytes" ]; then
  diff_text="${diff_text:0:$max_bytes}
[diff truncated at ${max_bytes} bytes]"
fi

command -v claude >/dev/null || { echo "log-leak-review: claude CLI not found" >&2; exit 2; }

# macOS has no timeout(1); perl's alarm is the portable stand-in.
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

# The model sometimes prefaces the verdict with reasoning; keep the contract.
verdict=$(printf '%s\n' "$out" | sed -n '/^VERDICT:/,$p')
if [ -z "$verdict" ]; then
  echo "log-leak-review: no VERDICT line in output" >&2
  exit 2
fi

printf '%s\n' "$verdict"
printf '%s\n' "$verdict" | grep -q '^VERDICT: BLOCK' && exit 1
exit 0
