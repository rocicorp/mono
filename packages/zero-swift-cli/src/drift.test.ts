import {readFileSync, writeFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {expect, test} from 'vitest';
import {recordDriftFile} from './drift.ts';

const repo = fileURLToPath(new URL('../../../', import.meta.url));
const update = process.env['UPDATE_GENERATED'] === '1';

/**
 * The Swift drift tests check the Swift ports of these apps against the
 * vectors, so they are checked in; this keeps them current with the TS
 * queries and mutators.
 */
const apps = [
  {
    samples: 'apps/zbugs-swift/drift-samples.ts',
    output: 'apps/zbugs-swift/Tests/ZBugsTests/drift-vectors.json',
  },
];

for (const {samples, output} of apps) {
  test(`drift vectors for ${samples} are current`, async () => {
    const vectors = await recordDriftFile(repo + samples);
    const path = repo + output;
    if (update) {
      writeFileSync(path, vectors);
    }
    expect(vectors).toBe(readFileSync(path, 'utf8'));
  }, 60_000);
}
