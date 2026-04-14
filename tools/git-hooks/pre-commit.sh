#!/bin/bash

set -e

# Get staged files that oxfmt can format
files=$(git diff --cached --name-only --diff-filter=ACM)


if [ -n "$files" ]; then
  npx oxfmt --no-error-on-unmatched-pattern --write $files
  echo "$files" | xargs git add
fi
