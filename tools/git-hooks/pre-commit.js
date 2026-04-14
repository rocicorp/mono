#!/usr/bin/env zx

const files = (
  await $`git diff --cached --name-only --diff-filter=ACM`.quiet()
).stdout
  .trim()
  .split('\n')
  .filter(Boolean);

if (files.length > 0) {
  await $`oxfmt --no-error-on-unmatched-pattern --write ${files}`.quiet();
  await $`git add ${files}`;
}
