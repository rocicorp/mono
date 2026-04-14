#!/usr/bin/env zx
// oxlint-disable no-console

const changedFiles = (await $`git diff --name-only origin/main`.quiet()).stdout
  .trim()
  .split('\n')
  .filter(Boolean);

process.stdout.write('Pre-push check');

const checks = [
  {name: 'syncpack lint', result: $`syncpack lint`.quiet().nothrow()},
];

if (changedFiles.length > 0) {
  checks.push(
    {
      name: 'lint',
      result: $`oxlint --quiet --no-error-on-unmatched-pattern ${changedFiles}`
        .quiet()
        .nothrow(),
    },
    {
      name: 'fmt check',
      result: $`oxfmt --check ${changedFiles}`.quiet().nothrow(),
    },
  );
}

const results = await Promise.all(checks.map(c => c.result));

let failed = false;
for (let i = 0; i < results.length; i++) {
  if (results[i].exitCode !== 0) {
    console.log(`\nFAILED: ${checks[i].name}`);
    failed = true;
  } else {
    process.stdout.write('.');
  }
}

if (failed) process.exit(1);

console.log(` Done in ${Math.round(performance.now())}ms`);
