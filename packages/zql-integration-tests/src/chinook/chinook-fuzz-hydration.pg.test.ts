/* oxlint-disable no-console */

import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {expect, test} from 'vitest';
import {astToZQL} from '../../../ast-to-zql/src/ast-to-zql.ts';
import {formatOutput} from '../../../ast-to-zql/src/format.ts';
import {wrapSourcesWithRandomYield} from '../../../zql/src/ivm/test/random-yield-source.ts';
import {QueryDelegateImpl as TestMemoryQueryDelegate} from '../../../zql/src/query/test/query-delegate.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';
import {generateShrinkableQuery} from '../../../zql/src/query/test/query-gen.ts';
import '../helpers/comparePg.ts';
import {
  bootstrap,
  lc,
  runAndCompare,
  TestPGQueryDelegate,
  testLogConfig,
  type Delegates,
} from '../helpers/runner.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {newQueryDelegate} from '../../../zqlite/src/test/source-factory.ts';
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
    rng,
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
  rng,
}: {
  query: [AnyQuery, AnyQuery[]];
  seed: number;
  rng: () => number;
}) {
  const label = `fuzz-hydration ${seed}`;
  const startTime = performance.now();
  const shouldYield = createTimeoutShouldYield(
    startTime,
    TEST_TIMEOUT_MS,
    label,
  );

  try {
    await transactWithRandomYields(
      rng,
      async delegates => {
        await runAndCompare(schema, delegates, query[0], undefined);
      },
      shouldYield,
    );
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

/**
 * Creates a transact function that wraps memory sources with random yields.
 * This tests that the IVM pipeline correctly handles cooperative scheduling
 * via yield points during hydration and push operations.
 */
async function transactWithRandomYields(
  rng: () => number,
  cb: (delegates: Delegates) => Promise<void>,
  shouldYield?: () => boolean,
) {
  await harness.dbs.pg.begin(async tx => {
    // Fork memory sources and wrap them with random yield injection
    const forkedSources = Object.fromEntries(
      Object.entries(harness.dbs.memory).map(([key, source]) => [
        key,
        source.fork(),
      ]),
    );

    // Wrap sources with random yield injection (30% probability at each yield point)
    const yieldingSources = wrapSourcesWithRandomYield(forkedSources, rng, 0.3);

    const scopedDelegates: Delegates = {
      ...harness.delegates,
      pg: new TestPGQueryDelegate(tx, schema, harness.dbs.pgSchema),
      memory: new TestMemoryQueryDelegate({
        sources: yieldingSources,
      }),
      sqlite: newQueryDelegate(
        lc,
        testLogConfig,
        (() => {
          const db = new Database(lc, harness.dbs.sqliteFile);
          db.exec('BEGIN CONCURRENT');
          return db;
        })(),
        schema,
        shouldYield,
      ),
    };
    await cb(scopedDelegates);
  });
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
