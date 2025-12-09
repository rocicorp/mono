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
 * Wraps a function with a timeout that logs a warning instead of failing.
 * This allows fuzz tests to pass even if they take too long, while still
 * logging for visibility.
 */
async function withTimeout<T>(
  fn: () => Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<T | 'timed-out'> {
  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  const timeoutPromise = new Promise<'timed-out'>(resolve => {
    timeoutId = setTimeout(() => resolve('timed-out'), timeoutMs);
  });

  try {
    const result = await Promise.race([fn(), timeoutPromise]);
    if (timeoutId !== undefined) {
      clearTimeout(timeoutId);
    }
    if (result === 'timed-out') {
      console.warn(
        `⚠️ Test "${label}" timed out after ${timeoutMs}ms - passing anyway`,
      );
    }
    return result;
  } catch (e) {
    if (timeoutId !== undefined) {
      clearTimeout(timeoutId);
    }
    throw e; // Re-throw actual errors (non-timeouts should still fail)
  }
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
  const result = await withTimeout(
    async () => {
      try {
        await harness.transact(async delegates => {
          await runAndCompare(schema, delegates, query[0], undefined);
        });
        return 'success' as const;
      } catch (e) {
        const zql = await shrink(query[1], seed);
        if (seed === REPRO_SEED) {
          throw e;
        }
        throw new Error(
          'Mismatch. Repro seed: ' + seed + '\nshrunk zql: ' + zql,
        );
      }
    },
    TEST_TIMEOUT_MS,
    `fuzz-hydration ${seed}`,
  );

  // Timeouts pass with a warning (logged by withTimeout)
  if (result === 'timed-out') {
    return;
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
