import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {beforeEach, expect, test} from 'vitest';
import {generateQuery} from '../../../zql/src/query/test/query-gen.ts';
import {ast, type AnyQuery} from '../../../zql/src/query/query-impl.ts';
import {getChinookSchemaOnly} from '../chinook/get-deps.ts';
import {bootstrap, type QueryInstances} from '../helpers/runner.ts';
import {schema} from '../chinook/schema.ts';
import type {AnyStaticQuery} from '../../../zql/src/query/test/util.ts';
import {staticToRunnable} from '../helpers/static.ts';
import {formatOutput} from '../../../ast-to-zql/src/format.ts';
import {astToZQL} from '../../../ast-to-zql/src/ast-to-zql.ts';
import {createPushScript} from '../helpers/create-push-script.ts';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import {dangerouslyClear} from '../../../zql/src/ivm/source.ts';
import {staticQuery} from '../../../zql/src/query/static-query.ts';

const pgContent = await getChinookSchemaOnly();

const harness = await bootstrap({
  suiteName: 'fuzz_push',
  zqlSchema: schema,
  pgContent,
});

beforeEach(() => {
  for (const table of Object.values(schema.tables)) {
    const source = must(harness.delegates.memory.getSource(table.name));
    source[dangerouslyClear]();
  }
});

// Set this to reproduce a specific failure.
const REPRO_SEED = undefined;
test.each(
  Array.from({length: REPRO_SEED ? 1 : 100}, () =>
    createCase(REPRO_SEED),
  ).concat([
    manual(
      staticQuery(schema, 'invoice').related('customer', q =>
        q.where('company', 'ILIKE', 'foo').orderBy('company', 'desc').limit(2),
      ),
      1791595304,
    ),
  ]),
)('fuzz-push $seed', runCase);

function manual(query: AnyQuery, seed = 0) {
  return {
    seed,
    query,
  };
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
    const runnableQueries = staticToRunnable({
      query: query as AnyStaticQuery,
      schema,
      harness,
    });

    // Materialize zql and zqlite
    // run push script
    // materialize on each push
    // compare results
    await runScriptAndCompare(query, runnableQueries, seed);
  } catch (e) {
    if (seed === REPRO_SEED) {
      throw e;
    }
    const zql = await formatOutput(ast(query).table + astToZQL(ast(query)));
    throw new Error('Mismatch. Repro seed: ' + seed + '\nzql: ' + zql);
  }
}

async function runScriptAndCompare(
  origQuery: AnyQuery,
  queries: QueryInstances,
  seed: number,
) {
  const randomizer = generateMersenne53Randomizer(seed);
  const rng = () => randomizer.next();
  const faker = new Faker({
    locale: en,
    randomizer,
  });

  const script = createPushScript(rng, faker, schema, ast(origQuery));

  // const sqliteDelegate = harness.delegates.sqlite;
  const memoryDelegate = harness.delegates.memory;

  // const sqliteView = queries.sqlite.materialize();
  const memoryView = queries.memory.materialize();

  for (const [tableName, change] of script) {
    // const sqliteSource = sqliteDelegate.getSource(tableName);
    const memorySource = memoryDelegate.getSource(tableName);
    assert(memorySource, 'missing source');

    // sqliteSource.push(change);
    memorySource.push(change);

    // const [sqliteHydrate, memoryHydrate] = await Promise.all([
    //   queries.sqlite.run(),
    //   queries.memory.run(),
    // ]);
    const memoryHydrate = await queries.memory.run();

    // expect(sqliteHydrate).toEqual(memoryHydrate);
    // expect(sqliteView.data).toEqual(sqliteHydrate);
    expect(memoryView.data).toEqual(memoryHydrate);
  }

  memoryView.destroy();
}
