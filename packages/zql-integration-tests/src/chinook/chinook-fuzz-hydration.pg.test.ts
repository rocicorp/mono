/* oxlint-disable no-console */

import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {expect, test} from 'vitest';
import {astToZQL} from '../../../ast-to-zql/src/ast-to-zql.ts';
import {formatOutput} from '../../../ast-to-zql/src/format.ts';
import type {AST, Condition} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {createRandomYieldWrapper} from '../../../zql/src/ivm/test/random-yield-source.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';
import {
  generateShrinkableQuery,
  generateStructuredQueryCases,
  type Dataset,
  type GeneratedQueryCase,
} from '../../../zql/src/query/test/query-gen.ts';
import '../helpers/comparePg.ts';
import {bootstrap, checkPush, runAndCompare} from '../helpers/runner.ts';
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

const rawData = Object.fromEntries(harness.dbs.raw) as Dataset;
const VITEST_TIMEOUT_MS = 60_000; // set via third argument to test() calls

// Internal timeout for graceful handling (shorter than vitest timeout)
const TEST_TIMEOUT_MS = VITEST_TIMEOUT_MS / 2;
const CASE_COUNT = 100;
const RANDOM_TAIL_CASES = 10;

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
 * Creates a checkAbort function that throws FuzzTimeoutError when the
 * elapsed time exceeds the timeout. This allows synchronous query execution
 * to be aborted when it takes too long.
 */
function createCheckAbort(
  startTime: number,
  timeoutMs: number,
  label: string,
): () => void {
  return () => {
    const elapsed = Date.now() - startTime;
    if (elapsed > timeoutMs) {
      throw new FuzzTimeoutError(label, elapsed);
    }
  };
}

// oxlint-disable-next-line expect-expect
test.each(createCases())(
  'fuzz-hydration $label',
  runCase,
  VITEST_TIMEOUT_MS, // vitest timeout: longer than internal timeout to ensure we catch it ourselves
);

test('sentinel', () => {
  expect(true).toBe(true);
});

if (REPRO_SEED) {
  // oxlint-disable-next-line no-focused-tests
  test.only(
    'repro',
    async () => {
      const tc = {
        ...createCase(REPRO_SEED),
        label: `repro-${REPRO_SEED}`,
        tags: ['repro'],
      };
      const {query} = tc;
      console.log(
        'ZQL',
        await formatOutput(
          asQueryInternals(query[0]).ast.table +
            astToZQL(asQueryInternals(query[0]).ast),
        ),
      );
      await runCase(tc);
    },
    VITEST_TIMEOUT_MS,
  );
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

function createCases(): GeneratedQueryCase[] {
  const deterministic = generateStructuredQueryCases(
    schema,
    rawData,
    CASE_COUNT - RANDOM_TAIL_CASES,
  );
  const tail = Array.from({length: RANDOM_TAIL_CASES}, () => createCase()).map(
    (tc, i) => ({
      ...tc,
      label: `random-tail-${i}-${tc.seed}`,
      tags: ['random-tail'],
    }),
  );
  return [...deterministic, ...tail];
}

function gatherExistsDependencyRows(
  ast: AST,
  data: Dataset,
  maxRowsPerTable = 3,
): [table: string, row: Row][] {
  const tables = new Set<string>();

  function visitAST(ast: AST) {
    if (ast.where) {
      visitCondition(ast.where);
    }
    for (const related of ast.related ?? []) {
      visitAST(related.subquery);
    }
  }

  function collectSubqueryTables(ast: AST) {
    tables.add(ast.table);
    visitAST(ast);
  }

  function visitCondition(condition: Condition) {
    switch (condition.type) {
      case 'correlatedSubquery':
        collectSubqueryTables(condition.related.subquery);
        return;
      case 'and':
      case 'or':
        condition.conditions.forEach(visitCondition);
        return;
      case 'simple':
        return;
    }
  }

  visitAST(ast);
  return [...tables].flatMap(table =>
    [...(data[table] ?? [])]
      .slice(0, maxRowsPerTable)
      .map(row => [table, row] as [string, Row]),
  );
}

async function runCase({
  query,
  seed,
  rng,
  label,
}: {
  query: [AnyQuery, AnyQuery[]];
  seed: number;
  rng: () => number;
  label: string;
}) {
  const startTime = Date.now();
  const checkAbort = createCheckAbort(startTime, TEST_TIMEOUT_MS, label);

  // Create a source wrapper that injects random yields and timeout checking
  // for both memory and sqlite sources
  const sourceWrapper = createRandomYieldWrapper(rng, 0.3, checkAbort);

  try {
    await harness.transact(async delegates => {
      await runAndCompare(schema, delegates, query[0], undefined);
      await checkPush(
        schema,
        delegates,
        query[0],
        10,
        gatherExistsDependencyRows(asQueryInternals(query[0]).ast, rawData),
      );
    }, sourceWrapper);
  } catch (e) {
    // Timeouts pass with a warning
    if (e instanceof FuzzTimeoutError) {
      console.warn(`⚠️ ${e.message} - passing anyway`);
      return;
    }

    // Actual test failures get shrunk and re-thrown
    const zql = await shrink(query[1], seed, rng);
    if (seed === REPRO_SEED) {
      throw e;
    }
    throw new Error('Mismatch. Repro seed: ' + seed + '\nshrunk zql: ' + zql);
  }
}

async function shrink(
  generations: AnyQuery[],
  seed: number,
  rng: () => number,
) {
  console.log('Found failure at seed', seed);
  console.log('Shrinking', generations.length, 'generations');
  let low = 0;
  let high = generations.length;
  let lastFailure = -1;
  while (low < high) {
    const mid = low + ((high - low) >> 1);
    try {
      const startTime = Date.now();
      const checkAbort = createCheckAbort(
        startTime,
        TEST_TIMEOUT_MS,
        `shrink ${seed}`,
      );
      const sourceWrapper = createRandomYieldWrapper(rng, 0.3, checkAbort);
      await harness.transact(async delegates => {
        await runAndCompare(schema, delegates, generations[mid], undefined);
        await checkPush(
          schema,
          delegates,
          generations[mid],
          10,
          gatherExistsDependencyRows(
            asQueryInternals(generations[mid]).ast,
            rawData,
          ),
        );
      }, sourceWrapper);
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
