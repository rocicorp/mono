/* eslint-disable @typescript-eslint/no-explicit-any */
import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {bootstrap, runAndCompare} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';
import {test} from 'vitest';
import {generateQuery} from '../../../zql/src/query/test/query-gen.ts';
import '../helpers/comparePg.ts';
import {ast} from '../../../zql/src/query/query-impl.ts';
import type {AnyQuery} from '../../../zql/src/query/test/util.ts';
import {astToZQL} from '../../../ast-to-zql/src/ast-to-zql.ts';
import {formatOutput} from '../../../ast-to-zql/src/format.ts';
import {staticToRunnable} from '../helpers/static.ts';

const pgContent = await getChinook();

// Set this to reproduce a specific failure.
const REPRO_SEED = undefined;

const harness = await bootstrap({
  suiteName: 'chinook_fuzz_hydration',
  zqlSchema: schema,
  pgContent,
});

test.each(Array.from({length: 1000}, () => createCase()))(
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
  try {
    await runAndCompare(
      schema,
      staticToRunnable({
        query,
        schema,
        harness,
      }),
      undefined,
    );
  } catch (e) {
    if (seed === REPRO_SEED) {
      throw e;
    }
    throw new Error('mismatch. repro seed: ' + seed);
  }
}
