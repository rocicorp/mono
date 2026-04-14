#!/bin/bash

set -e

start_time=$(($(date +%s%N) / 1000000))

# Run syncpack lint, oxlint, and oxfmt check in parallel
pids=()
results=()

# Get files changed vs origin
changed_files=$(git diff --name-only origin/main 2>/dev/null | grep -E '\.(ts|tsx|js|jsx|mjs|cjs|mts|cts|json)$' || true)

printf "Pre-push check"

npx syncpack lint > /dev/null 2>&1 &
pids+=($!)
results+=("syncpack lint")

if [ -n "$changed_files" ]; then
  npx oxlint --quiet --no-error-on-unmatched-pattern $changed_files > /dev/null 2>&1 &
  pids+=($!)
  results+=("lint")

  npx oxfmt --check $changed_files > /dev/null 2>&1 &
  pids+=($!)
  results+=("fmt check")
fi

failed=0
for i in "${!pids[@]}"; do
  if ! wait "${pids[$i]}"; then
    echo ""
    echo "FAILED: ${results[$i]}"
    failed=1
  else
    printf "."
  fi
done

if [ $failed -ne 0 ]; then
  exit 1
fi

end_time=$(($(date +%s%N) / 1000000))
elapsed=$(( end_time - start_time ))
echo " Done in ${elapsed}ms"
