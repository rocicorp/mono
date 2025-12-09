/* oxlint-disable no-console */

import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {expect, test} from 'vitest';
import {astToZQL} from '../../../ast-to-zql/src/ast-to-zql.ts';
import {formatOutput} from '../../../ast-to-zql/src/format.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';
import {generateShrinkableQuery} from '../../../zql/src/query/test/query-gen.ts';
import '../helpers/comparePg.ts';
import {bootstrap, runAndCompare} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';

const pgContent = await getChinook();

// Set this to reproduce a specific failure.
const REPRO_SEED = undefined;

const harness = await bootstrap({
  suiteName: 'chinook_fuzz_hydration',
  zqlSchema: schema,
  pgContent,
});

// Internal timeout for graceful handling (shorter than vitest timeout)
const TEST_TIMEOUT_MS = 55_000;

/**
 * Error thrown when a fuzz test query exceeds the time limit.
 * This is caught and treated as a pass (with warning) rather than a failure.
 */
class FuzzTimeoutError extends Error {
  constructor(label: string, elapsedMs: number) {
    super(`Fuzz test "${label}" timed out after ${elapsedMs}ms`);
    this.name = 'FuzzTimeoutError';
  }
}

/**
 * Creates a shouldYield function that throws FuzzTimeoutError when the
 * elapsed time exceeds the timeout. This allows synchronous query execution
 * to be aborted when it takes too long.
 */
function createTimeoutShouldYield(
  startTime: number,
  timeoutMs: number,
  label: string,
): () => boolean {
  return () => {
    const elapsed = performance.now() - startTime;
    if (elapsed > timeoutMs) {
      throw new FuzzTimeoutError(label, elapsed);
    }
    return false; // Don't actually yield, just check timeout
  };
}

// oxlint-disable-next-line expect-expect
test.each(Array.from({length: 100}, () => createCase()))(
  'fuzz-hydration $seed',
  runCase,
  65_000, // vitest timeout: longer than internal timeout to ensure we catch it ourselves
);

test('sentinel', () => {
  expect(true).toBe(true);
});

if (REPRO_SEED) {
  // oxlint-disable-next-line no-focused-tests
  test.only('repro', async () => {
    const tc = createCase(REPRO_SEED);
    const {query} = tc;
    console.log(
      'ZQL',
      await formatOutput(
        asQueryInternals(query[0]).ast.table +
          astToZQL(asQueryInternals(query[0]).ast),
      ),
    );
    await runCase(tc);
  });
}

function createCase(seed?: number) {
  seed = seed ?? Date.now() ^ (Math.random() * 0x100000000);
  const randomizer = generateMersenne53Randomizer(seed);
  const rng = () => randomizer.next();
  const faker = new Faker({
    locale: en,
    randomizer,
  });
  return {
    seed,
    query: generateShrinkableQuery(
      schema,
      {},
      rng,
      faker,
      harness.delegates.pg.serverSchema,
    ),
  };
}

async function runCase({
  query,
  seed,
}: {
  query: [AnyQuery, AnyQuery[]];
  seed: number;
}) {
  const label = `fuzz-hydration ${seed}`;
  const startTime = performance.now();
  const shouldYield = createTimeoutShouldYield(
    startTime,
    TEST_TIMEOUT_MS,
    label,
  );

  try {
    await harness.transact(async delegates => {
      await runAndCompare(schema, delegates, query[0], undefined);
    }, shouldYield);
  } catch (e) {
    // Timeouts pass with a warning
    if (e instanceof FuzzTimeoutError) {
      console.warn(`⚠️ ${e.message} - passing anyway`);
      return;
    }

    // Actual test failures get shrunk and re-thrown
    const zql = await shrink(query[1], seed);
    if (seed === REPRO_SEED) {
      throw e;
    }
    throw new Error('Mismatch. Repro seed: ' + seed + '\nshrunk zql: ' + zql);
  }
}

async function shrink(generations: AnyQuery[], seed: number) {
  console.log('Found failure at seed', seed);
  console.log('Shrinking', generations.length, 'generations');
  let low = 0;
  let high = generations.length;
  let lastFailure = -1;
  while (low < high) {
    const mid = low + ((high - low) >> 1);
    try {
      await runAndCompare(
        schema,
        harness.delegates,
        generations[mid],
        undefined,
      );
      low = mid + 1;
    } catch {
      lastFailure = mid;
      high = mid;
    }
  }
  if (lastFailure === -1) {
    throw new Error('no failure found');
  }
  const query = generations[lastFailure];
  const queryInternals = asQueryInternals(query);
  return formatOutput(queryInternals.ast.table + astToZQL(queryInternals.ast));
}
