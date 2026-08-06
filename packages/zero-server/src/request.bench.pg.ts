/* oxlint-disable no-console */

import postgres from 'postgres';
import {expect} from 'vitest';
import {createManualBenchmarkRecorder} from '../../shared/src/bench.ts';
import {
  getClientsTableDefinition,
  getMutationsTableDefinition,
} from '../../zero-cache/src/services/change-source/pg/schema/shard.ts';
import {
  getConnectionURI,
  type PgTest,
  test,
} from '../../zero-cache/src/test/db.ts';
import type {SchemaValue} from '../../zero-types/src/schema-value.ts';
import type {Schema, TableSchema} from '../../zero-types/src/schema.ts';
import {
  defineMutatorsWithType,
  mustGetMutator,
} from '../../zql/src/mutate/mutator-registry.ts';
import {defineMutatorWithType} from '../../zql/src/mutate/mutator.ts';
import {createBuilder} from '../../zql/src/query/create-builder.ts';
import {
  defineQueriesWithType,
  defineQueryWithType,
  mustGetQuery,
} from '../../zql/src/query/query-registry.ts';
import {zeroPostgresJS} from './adapters/postgresjs.ts';
import {handleMutateRequest} from './process-mutations.ts';
import {handleQueryRequest} from './queries/process-queries.ts';

const APPLICATION_TABLES = 517;
const APPLICATION_COLUMNS = 8_762;
const SCHEMA_TABLES = 75;
const SCHEMA_COLUMNS = 1_306;
const UPSTREAM_SCHEMA = 'zero_0';
const WARMUP_REPS = 5;
const REPS = 50;
const benchmarkRecorder = createManualBenchmarkRecorder();

test(
  'mutation latency with cold and warmed server schema',
  {timeout: 300_000},
  async ({testDBs}: PgTest) => {
    const pg = await testDBs.create('zero_server_mutation_benchmark');
    const role = `zero_server_bench_${process.pid}`;
    const password = `zero_server_bench_${process.pid}`;
    const schema = makeBenchmarkSchema();
    const mutators = defineMutatorsWithType<typeof schema>()({
      benchmark: {
        noop: defineMutatorWithType<typeof schema>()<undefined>(() =>
          Promise.resolve(),
        ),
      },
    });
    const firstMutationWarmups: number[] = [];
    const firstMutationSamples: number[] = [];
    const warmedMutationWarmups: number[] = [];
    const warmedMutationSamples: number[] = [];
    let appPG: postgres.Sql | undefined;
    let roleCreated = false;

    try {
      await pg.unsafe(`
        ${makeBenchmarkApplicationSQL()}
        CREATE SCHEMA "${UPSTREAM_SCHEMA}";
        ${getClientsTableDefinition(UPSTREAM_SCHEMA)}
        ${getMutationsTableDefinition(UPSTREAM_SCHEMA)}
      `);
      await pg.unsafe(`CREATE ROLE "${role}" LOGIN PASSWORD '${password}'`);
      roleCreated = true;
      await pg.unsafe(`
        GRANT USAGE ON SCHEMA public, "${UPSTREAM_SCHEMA}" TO "${role}";
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public, "${UPSTREAM_SCHEMA}" TO "${role}";
      `);

      const databaseURL = new URL(getConnectionURI(pg));
      databaseURL.username = role;
      databaseURL.password = password;
      const benchmarkPG = postgres(databaseURL.href, {
        max: 1,
        prepare: false,
        connection: {['application_name']: 'zero-server-mutation-bench'},
      });
      appPG = benchmarkPG;
      await benchmarkPG`SELECT 1`;

      const warmedDBProvider = zeroPostgresJS(schema, benchmarkPG);
      const runMutation = async (
        dbProvider: typeof warmedDBProvider,
        profile: string,
        rep: number | 'prime',
      ) => {
        const start = performance.now();
        const response = await handleMutateRequest({
          dbProvider,
          handler: transact =>
            transact((tx, name, args) => {
              const mutator = mustGetMutator(mutators, name);
              return mutator.fn({args, tx});
            }),
          query: {
            schema: UPSTREAM_SCHEMA,
            appID: 'zero_server_mutation_benchmark',
          },
          body: {
            clientGroupID: `${profile}-client-group`,
            mutations: [
              {
                type: 'custom',
                id: 1,
                clientID: `${profile}-client-${rep}`,
                name: 'benchmark.noop',
                args: [],
                timestamp: 1,
              },
            ],
            pushVersion: 1,
            schemaVersion: 1,
            timestamp: 1,
            requestID: `${profile}-request-${rep}`,
          },
          userID: null,
          logLevel: 'error',
        });
        const elapsed = performance.now() - start;

        expect(response).toEqual({
          kind: 'MutateResponse',
          mutations: [
            {
              id: {clientID: `${profile}-client-${rep}`, id: 1},
              result: {},
            },
          ],
          userID: null,
        });
        return elapsed;
      };

      const measureMutationProfile = async (
        profile: string,
        getDBProvider: () => typeof warmedDBProvider,
        warmups: number[],
        samples: number[],
      ) => {
        for (let rep = 0; rep < WARMUP_REPS + REPS; rep++) {
          const elapsed = await runMutation(getDBProvider(), profile, rep);
          (rep < WARMUP_REPS ? warmups : samples).push(elapsed);
        }
      };

      // Each provider owns a new CRUDMutatorFactory with no cached server
      // schema, matching the first request served by a new Zero server.
      await measureMutationProfile(
        'first-mutation',
        () => zeroPostgresJS(schema, benchmarkPG),
        firstMutationWarmups,
        firstMutationSamples,
      );

      // Prime the persistent provider outside the measured samples so every
      // warmed mutation reuses the cached server schema.
      await runMutation(warmedDBProvider, 'warmed-mutation', 'prime');
      await measureMutationProfile(
        'warmed-mutation',
        () => warmedDBProvider,
        warmedMutationWarmups,
        warmedMutationSamples,
      );
    } finally {
      try {
        await appPG?.end();
      } finally {
        try {
          if (roleCreated) {
            await pg.unsafe(
              `DROP OWNED BY "${role}"; DROP ROLE IF EXISTS "${role}";`,
            );
          }
        } finally {
          await testDBs.drop(pg);
        }
      }
    }

    expect(firstMutationSamples).toHaveLength(REPS);
    expect(warmedMutationSamples).toHaveLength(REPS);
    benchmarkRecorder.recordLatency(
      'zero-server first mutation with uncached server schema',
      firstMutationSamples,
    );
    benchmarkRecorder.recordLatency(
      'zero-server mutation with cached server schema',
      warmedMutationSamples,
    );
    logRawSamples(
      'zero-server-first-mutation-raw-samples',
      firstMutationWarmups,
      firstMutationSamples,
    );
    logRawSamples(
      'zero-server-warmed-mutation-raw-samples',
      warmedMutationWarmups,
      warmedMutationSamples,
    );
  },
);

test('warmed query request latency', async () => {
  const schema = makeBenchmarkSchema();
  const builder = createBuilder(schema);
  const queries = defineQueriesWithType<typeof schema>()({
    benchmark: {
      noop: defineQueryWithType<typeof schema>()(
        () => builder[benchmarkTableName(0)],
      ),
    },
  });
  const warmups: number[] = [];
  const samples: number[] = [];

  const runQuery = async (rep: number | 'prime') => {
    const id = `query-request-${rep}`;
    const start = performance.now();
    const response = await handleQueryRequest({
      handler: (name, args) => {
        const query = mustGetQuery(queries, name);
        return query.fn({args});
      },
      schema,
      query: {
        schema: UPSTREAM_SCHEMA,
        appID: 'zero_server_request_benchmark',
      },
      body: [
        'transform',
        [
          {
            id,
            name: 'benchmark.noop',
            args: [],
          },
        ],
      ],
      userID: null,
      logLevel: 'error',
    });
    const elapsed = performance.now() - start;

    expect(response).toEqual({
      kind: 'QueryResponse',
      queries: [
        {
          id,
          name: 'benchmark.noop',
          ast: {table: benchmarkTableName(0)},
        },
      ],
      userID: null,
    });
    return elapsed;
  };

  await runQuery('prime');
  for (let rep = 0; rep < WARMUP_REPS + REPS; rep++) {
    const elapsed = await runQuery(rep);
    (rep < WARMUP_REPS ? warmups : samples).push(elapsed);
  }

  expect(samples).toHaveLength(REPS);
  benchmarkRecorder.recordLatency('zero-server handleQueryRequest', samples);
  logRawSamples(
    'zero-server-query-request-raw-samples',
    warmups,
    samples,
    false,
  );
});

function logRawSamples(
  type: string,
  warmups: readonly number[],
  samples: readonly number[],
  includeApplicationCatalog = true,
) {
  console.log(
    JSON.stringify({
      type,
      fixture: {
        ...(includeApplicationCatalog
          ? {
              applicationTables: APPLICATION_TABLES,
              applicationColumns: APPLICATION_COLUMNS,
            }
          : {}),
        schemaTables: SCHEMA_TABLES,
        schemaColumns: SCHEMA_COLUMNS,
      },
      warmups,
      samples,
    }),
  );
}

function makeBenchmarkSchema(): Schema {
  const tables: Record<string, TableSchema> = {};
  let columns = 0;
  for (let tableIndex = 0; tableIndex < SCHEMA_TABLES; tableIndex++) {
    const name = benchmarkTableName(tableIndex);
    const count = columnCount(tableIndex);
    columns += count;
    tables[name] = {
      name,
      columns: makeColumns(count),
      primaryKey: ['id'],
    };
  }
  if (columns !== SCHEMA_COLUMNS) {
    throw new Error(
      `Expected ${SCHEMA_COLUMNS} schema columns, got ${columns}`,
    );
  }
  return {tables, relationships: {}};
}

function makeBenchmarkApplicationSQL(): string {
  const statements: string[] = [];
  let columns = 0;
  for (let tableIndex = 0; tableIndex < APPLICATION_TABLES; tableIndex++) {
    const count = columnCount(tableIndex);
    columns += count;
    const columnDefinitions = ['"id" TEXT PRIMARY KEY'];
    for (let columnIndex = 1; columnIndex < count; columnIndex++) {
      columnDefinitions.push(`"column_${columnIndex}" TEXT NOT NULL`);
    }
    statements.push(
      `CREATE TABLE "${benchmarkTableName(tableIndex)}" (${columnDefinitions.join(', ')})`,
    );
  }
  if (columns !== APPLICATION_COLUMNS) {
    throw new Error(
      `Expected ${APPLICATION_COLUMNS} fixture columns, got ${columns}`,
    );
  }
  return `${statements.join(';\n')};`;
}

function makeColumns(count: number): Record<string, SchemaValue> {
  const columns: Record<string, SchemaValue> = {id: {type: 'string'}};
  for (let columnIndex = 1; columnIndex < count; columnIndex++) {
    columns[`column_${columnIndex}`] = {type: 'string'};
  }
  return columns;
}

function columnCount(tableIndex: number): number {
  if (tableIndex < SCHEMA_TABLES) {
    return tableIndex < 31 ? 18 : 17;
  }
  return tableIndex - SCHEMA_TABLES < 384 ? 17 : 16;
}

function benchmarkTableName(tableIndex: number): string {
  return `app_table_${tableIndex.toString().padStart(3, '0')}`;
}
