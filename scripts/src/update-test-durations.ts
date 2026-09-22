/* oxlint-disable no-console */
/**
 * Merges vitest JSON reporter output into test-durations.json, which the
 * duration-aware shard sequencer (packages/shared/src/tool/duration-sequencer.ts)
 * uses to balance CI shards.
 *
 * Usage:
 *
 *   TEST_PG_MODE=nopg pnpm run test --reporter=default --reporter=json --outputFile=/tmp/nopg.json
 *   TEST_PG_MODE=pg-17 pnpm run test --reporter=default --reporter=json --outputFile=/tmp/pg17.json
 *   pnpm run update-test-durations /tmp/nopg.json /tmp/pg17.json
 *
 * Each input adds or replaces the entries for the files it ran (taking the
 * max when the same file ran in several projects, e.g. per browser). Entries
 * whose file no longer exists are dropped. Durations are wall-clock
 * milliseconds per file, including hooks.
 */

import {existsSync, readFileSync, writeFileSync} from 'node:fs';
import {relative, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

const root = fileURLToPath(new URL('../../', import.meta.url));
const durationsPath = resolve(root, 'test-durations.json');

type JsonTestResults = {
  testResults: {name: string; startTime: number; endTime: number}[];
};

function main(inputs: string[]): void {
  if (inputs.length === 0) {
    console.error(
      'usage: update-test-durations <vitest-json-report>... (see file header)',
    );
    process.exit(2);
  }

  const durations = JSON.parse(readFileSync(durationsPath, 'utf8')) as Record<
    string,
    number
  >;

  const seen = new Map<string, number>();
  for (const input of inputs) {
    const report = JSON.parse(readFileSync(input, 'utf8')) as JsonTestResults;
    for (const result of report.testResults) {
      const key = relative(root, result.name).replaceAll('\\', '/');
      const ms = Math.round(result.endTime - result.startTime);
      seen.set(key, Math.max(seen.get(key) ?? 0, ms));
    }
  }
  for (const [key, ms] of seen) {
    durations[key] = ms;
  }

  let pruned = 0;
  for (const key of Object.keys(durations)) {
    if (!existsSync(resolve(root, key))) {
      delete durations[key];
      pruned++;
    }
  }

  const sorted = Object.fromEntries(
    Object.keys(durations)
      .sort()
      .map(key => [key, durations[key]]),
  );
  writeFileSync(durationsPath, JSON.stringify(sorted, null, 2) + '\n');
  console.log(
    `test-durations.json: updated ${seen.size}, pruned ${pruned}, ${Object.keys(sorted).length} total`,
  );
}

main(process.argv.slice(2));
