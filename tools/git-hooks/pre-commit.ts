#!/usr/bin/env node

// To use this add a file called .git/hooks/pre-commit with the following content:
//
// #!/bin/sh
// ./tools/git-hooks/pre-commit.ts

import {$} from 'zx';

const files = (
  await $`git diff --cached --name-only --diff-filter=ACM`.quiet()
).stdout
  .trim()
  .split('\n')
  .filter(Boolean);

if (files.length > 0) {
  await $`npx oxfmt --no-error-on-unmatched-pattern --write ${files}`.quiet();
  await $`git add ${files}`;
}
