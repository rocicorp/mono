/* eslint-disable @typescript-eslint/no-explicit-any */
import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {bootstrap, runAndCompare} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';
import {test} from 'vitest';
import {generateQuery} from '../../../zql/src/query/test/query-gen.ts';
import '../helpers/comparePg.ts';
import {ast, QueryImpl} from '../../../zql/src/query/query-impl.ts';
import {ZPGQuery} from '../../../zero-pg/src/query.ts';
import type {AnyQuery} from '../../../zql/src/query/test/util.ts';
import {astToZQL} from '../../../ast-to-zql/src/ast-to-zql.ts';
import {formatOutput} from '../../../ast-to-zql/src/format.ts';

const pgContent = await getChinook();

// Set this to reproduce a specific failure.
const REPRO_SEED = undefined;

const harness = await bootstrap({
  suiteName: 'frontend_analysis',
  zqlSchema: schema,
  pgContent,
});

test.each(Array.from({length: 30}, () => createCase()))(
  'fuzz-hydration $seed',
  runCase,
);

if (REPRO_SEED) {
  // eslint-disable-next-line no-only-tests/no-only-tests
  test.only('repro', async () => {
    const {query} = createCase(REPRO_SEED);
    // eslint-disable-next-line no-console
    console.log(
      'ZQL',
      await formatOutput(ast(query).table + astToZQL(ast(query))),
    );
    await runCase({query, seed: REPRO_SEED});
  });
}

function createCase(seed?: number | undefined) {
  seed = seed ?? Date.now() ^ (Math.random() * 0x100000000);
  const randomizer = generateMersenne53Randomizer(seed);
  const rng = () => randomizer.next();
  const faker = new Faker({
    locale: en,
    randomizer,
  });
  return {
    seed,
    query: generateQuery(
      schema,
      {},
      rng,
      faker,
      harness.delegates.pg.serverSchema,
    ),
  };
}

async function runCase({query, seed}: {query: AnyQuery; seed: number}) {
  // reconstruct the generated query
  // for zql, zqlite and pg
  const zql = new QueryImpl(
    harness.delegates.memory,
    schema,
    ast(query).table as keyof typeof schema.tables,
    ast(query),
    query.format,
  );
  const zqlite = new QueryImpl(
    harness.delegates.sqlite,
    schema,
    ast(query).table as keyof typeof schema.tables,
    ast(query),
    query.format,
  );
  const pg = new ZPGQuery(
    schema,
    harness.delegates.pg.serverSchema,
    ast(query).table as keyof typeof schema.tables,
    harness.delegates.pg.transaction,
    ast(query),
    query.format,
  );

  try {
    await runAndCompare(
      schema,
      {
        memory: zql,
        pg,
        sqlite: zqlite,
      },
      undefined,
    );
  } catch (e) {
    if (seed === REPRO_SEED) {
      throw e;
    }
    throw new Error('mismatch. repro seed: ' + seed);
  }
}
