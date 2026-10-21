import {mkdirSync, readFileSync, writeFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {runInNewContext} from 'node:vm';
import {expect} from 'vitest';

/**
 * Bumped when the shape of every vector file changes. Each file also carries
 * its own `kind`, so readers can reject vectors they don't understand.
 */
export const VECTOR_FORMAT_VERSION = 1;

const vectorsDir = fileURLToPath(new URL('../vectors/', import.meta.url));
const generatedDir = fileURLToPath(new URL('../generated/', import.meta.url));

export const updateVectors = process.env['UPDATE_VECTORS'] === '1';

/**
 * Throws when this runtime's `JSON.parse` misreads escaped keys. Once Node
 * 24.22's V8 has parsed an object keyed by a backslash, it reads the JSON text
 * `{"\n":2}` as keyed by a backslash too. Vectors written on such a runtime
 * would hold its wrong answers, so generate them with the Node in `.nvmrc`.
 * The probe runs in a fresh context because the bug depends on what the
 * process parsed before.
 */
function assertSoundJSONParse(): void {
  const key: unknown = runInNewContext(
    String.raw`JSON.parse('{"\\\\":1}'); Object.keys(JSON.parse('{"\\n":2}'))[0]`,
  );
  if (key !== '\n') {
    throw new Error(
      `JSON.parse in Node ${process.version} misreads escaped keys; ` +
        'generate vectors with the Node version in .nvmrc',
    );
  }
}

assertSoundJSONParse();

export type VectorFile<T> = {
  formatVersion: number;
  kind: string;
  description: string;
  /** Fixtures the cases share, such as schemas and datasets. */
  shared?: Record<string, unknown> | undefined;
  cases: T[];
};

/**
 * Writes `vectors/<name>.json` when `UPDATE_VECTORS=1`, otherwise checks that
 * the checked-in file matches what the JS implementation produces now. A
 * mismatch means the JS behavior changed and the Swift client must follow
 * (regenerate with `pnpm run generate`).
 */
export function emitVectors<T>(
  name: string,
  kind: string,
  description: string,
  cases: T[],
  shared?: Record<string, unknown>,
  {compact = false}: {compact?: boolean} = {},
): void {
  const file: VectorFile<T> = {
    formatVersion: VECTOR_FORMAT_VERSION,
    kind,
    description,
    ...(shared ? {shared} : {}),
    cases,
  };
  // Large files go one case per line, to keep diffs readable.
  const text = compact
    ? compactText(file)
    : JSON.stringify(file, null, 2) + '\n';
  const path = vectorsDir + name + '.json';
  if (updateVectors) {
    writeFileSync(path, text);
    return;
  }
  let existing: string;
  try {
    existing = readFileSync(path, 'utf8');
  } catch {
    throw new Error(
      `Missing vectors/${name}.json. Run \`pnpm run generate\` in packages/zero-conformance.`,
    );
  }
  expect(
    existing,
    `vectors/${name}.json is stale. Run \`pnpm run generate\` in packages/zero-conformance.`,
  ).toBe(text);
}

/**
 * Writes `generated/<name>.json`, which is not checked in. For large seeded
 * fuzz corpora: CI regenerates them before running the Swift suites, so
 * they are always current and never need a staleness check.
 */
export function emitGeneratedVectors<T>(
  name: string,
  kind: string,
  description: string,
  cases: T[],
  shared?: Record<string, unknown>,
): void {
  const file: VectorFile<T> = {
    formatVersion: VECTOR_FORMAT_VERSION,
    kind,
    description,
    ...(shared ? {shared} : {}),
    cases,
  };
  mkdirSync(generatedDir, {recursive: true});
  writeFileSync(generatedDir + name + '.json', JSON.stringify(file) + '\n');
}

function compactText<T>(file: VectorFile<T>): string {
  const {cases, ...rest} = file;
  const head = JSON.stringify(rest);
  const lines = cases.map(c => JSON.stringify(c));
  return `${head.slice(0, -1)},"cases":[\n${lines.join(',\n')}\n]}\n`;
}
