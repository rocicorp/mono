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
import {zeroPostgresJS} from './adapters/postgresjs.ts';
import {handleMutateRequest} from './process-mutations.ts';

const APPLICATION_TABLES = 517;
const APPLICATION_COLUMNS = 8_762;
const SCHEMA_TABLES = 75;
const SCHEMA_COLUMNS = 1_306;
const UPSTREAM_SCHEMA = 'zero_0';
const WARMUP_REPS = 5;
const REPS = 50;
const benchmarkRecorder = createManualBenchmarkRecorder();

test(
  'first mutation with uncached server schema',
  {timeout: 300_000},
  async ({testDBs}: PgTest) => {
    const pg = await testDBs.create('zero_server_first_mutation_benchmark');
    const role = `zero_server_bench_${process.pid}`;
    const password = `zero_server_bench_${process.pid}`;
    const schema = makeBenchmarkSchema();
    const warmups: number[] = [];
    const samples: number[] = [];
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
      appPG = postgres(databaseURL.href, {
        max: 1,
        prepare: false,
        connection: {['application_name']: 'zero-server-first-mutation-bench'},
      });
      await appPG`SELECT 1`;

      for (let rep = 0; rep < WARMUP_REPS + REPS; rep++) {
        // Each provider owns a new CRUDMutatorFactory with no cached server
        // schema, matching the first request served by a new Zero server.
        const dbProvider = zeroPostgresJS(schema, appPG);
        const start = performance.now();
        const response = await handleMutateRequest({
          dbProvider,
          handler: transact => transact(() => Promise.resolve()),
          query: {
            schema: UPSTREAM_SCHEMA,
            appID: 'zero_server_first_mutation_benchmark',
          },
          body: {
            clientGroupID: 'first-mutation-client-group',
            mutations: [
              {
                type: 'custom',
                id: 1,
                clientID: `first-mutation-client-${rep}`,
                name: 'benchmark.noop',
                args: [{}],
                timestamp: 1,
              },
            ],
            pushVersion: 1,
            schemaVersion: 1,
            timestamp: 1,
            requestID: `first-mutation-request-${rep}`,
          },
          userID: null,
          logLevel: 'error',
        });
        const elapsed = performance.now() - start;

        expect('kind' in response && response.kind === 'MutateResponse').toBe(
          true,
        );
        (rep < WARMUP_REPS ? warmups : samples).push(elapsed);
      }
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

    expect(samples).toHaveLength(REPS);
    benchmarkRecorder.recordLatency(
      'zero-server first mutation with uncached server schema',
      samples,
    );
    console.log(
      JSON.stringify({
        type: 'zero-server-first-mutation-raw-samples',
        fixture: {
          applicationTables: APPLICATION_TABLES,
          applicationColumns: APPLICATION_COLUMNS,
          schemaTables: SCHEMA_TABLES,
          schemaColumns: SCHEMA_COLUMNS,
        },
        warmups,
        samples,
      }),
    );
  },
);

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
