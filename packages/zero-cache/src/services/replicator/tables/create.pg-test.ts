import {
  afterAll,
  afterEach,
  beforeEach,
  describe,
  expect,
  test,
} from '@jest/globals';
import type postgres from 'postgres';
import {testDBs} from '../../../test/db.js';
import {createTableStatement} from './create.js';
import {getPublicationInfo} from './published.js';
import type {TableSpec} from './specs.js';

describe('tables/create', () => {
  type Case = {
    name: string;
    tableSpec: TableSpec;
  };

  const cases: Case[] = [
    {
      name: 'zero clients',
      tableSpec: {
        schema: 'public',
        name: 'clients',
        columns: {
          ['client_id']: {
            dataType: 'varchar',
            characterMaximumLength: 180,
            columnDefault: null,
            notNull: true,
          },
          ['last_mutation_id']: {
            dataType: 'int8',
            characterMaximumLength: null,
            columnDefault: null,
            notNull: true,
          },
        },
        primaryKey: ['client_id'],
      },
    },
    {
      name: 'types and array types and defaults',
      tableSpec: {
        schema: 'public',
        name: 'users',
        columns: {
          ['user_id']: {
            dataType: 'int4',
            characterMaximumLength: null,
            columnDefault: null,
            notNull: true,
          },
          handle: {
            characterMaximumLength: 40,
            columnDefault: "'@foo'::text",
            dataType: 'varchar',
            notNull: false,
          },
          address: {
            characterMaximumLength: null,
            columnDefault: null,
            dataType: 'text[]',
            notNull: false,
          },
          ['timez']: {
            dataType: 'timestamptz[]',
            characterMaximumLength: null,
            columnDefault: null,
            notNull: false,
          },
          ['bigint_array']: {
            characterMaximumLength: null,
            columnDefault: null,
            dataType: 'int8[]',
            notNull: false,
          },
          ['bool_array']: {
            characterMaximumLength: null,
            columnDefault: null,
            dataType: 'bool[]',
            notNull: false,
          },
          ['real_array']: {
            characterMaximumLength: null,
            columnDefault: null,
            dataType: 'float4[]',
            notNull: false,
          },
          ['int_array']: {
            dataType: 'int4[]',
            characterMaximumLength: null,
            columnDefault: "'{1,2,3}'::integer[]",
            notNull: false,
          },
          ['json_val']: {
            dataType: 'jsonb',
            characterMaximumLength: null,
            columnDefault: null,
            notNull: false,
          },
        },
        primaryKey: ['user_id'],
      },
    },
  ];

  let db: postgres.Sql;
  beforeEach(async () => {
    db = await testDBs.create('create_tables_test');
    await db`CREATE PUBLICATION zero_all FOR ALL TABLES`;
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  afterAll(async () => {
    await testDBs.end();
  });

  for (const c of cases) {
    test(c.name, async () => {
      await db.unsafe(createTableStatement(c.tableSpec));

      const published = await getPublicationInfo(db, 'zero_');
      expect(
        published.tables[`${c.tableSpec.schema}.${c.tableSpec.name}`],
      ).toEqual(c.tableSpec);
    });
  }
});
