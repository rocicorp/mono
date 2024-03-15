import {
  afterAll,
  afterEach,
  beforeEach,
  describe,
  expect,
  test,
} from '@jest/globals';
import type postgres from 'postgres';
import {TestDBs} from '../../../test/db.js';
import {getPublishedTables} from './published.js';
import type {TableSpec} from './specs.js';

describe('tables/published', () => {
  type Case = {
    name: string;
    setupQuery: string;
    expectedResult: Record<string, TableSpec>;
  };

  const cases: Case[] = [
    {
      name: 'Nothing published',
      setupQuery: `CREATE SCHEMA zero`,
      expectedResult: {},
    },
    {
      name: 'zero.clients',
      setupQuery: `
      CREATE SCHEMA zero;
      CREATE PUBLICATION zero_all FOR TABLES IN SCHEMA zero;
      CREATE TABLE zero.clients (
        client_id VARCHAR (180) PRIMARY KEY,
        last_mutation_id BIGINT
      );
      `,
      expectedResult: {
        ['zero.clients']: {
          schema: 'zero',
          name: 'clients',
          columns: {
            ['client_id']: {
              dataType: 'character varying',
              characterMaximumLength: 180,
              columnDefault: null,
            },
            ['last_mutation_id']: {
              dataType: 'bigint',
              characterMaximumLength: null,
              columnDefault: null,
            },
          },
          primaryKey: ['client_id'],
        },
      },
    },
    {
      name: 'types and array types',
      setupQuery: `
      CREATE SCHEMA test;
      CREATE TABLE test.users (
        user_id INTEGER PRIMARY KEY,
        handle text,
        address text[],
        timez TIMESTAMPTZ[],
        bigint_array BIGINT[],
        bool_array BOOL[],
        real_array REAL[],
        int_array INTEGER[] DEFAULT '{1, 2, 3}',
        json_val JSONB
      );
      CREATE PUBLICATION zero_data FOR TABLE test.users;
      `,
      expectedResult: {
        ['test.users']: {
          schema: 'test',
          name: 'users',
          columns: {
            ['user_id']: {
              dataType: 'integer',
              characterMaximumLength: null,
              columnDefault: null,
            },
            handle: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'text',
            },
            address: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'text[]',
            },
            ['timez']: {
              dataType: 'timestamp with time zone[]',
              characterMaximumLength: null,
              columnDefault: null,
            },
            ['bigint_array']: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'bigint[]',
            },
            ['bool_array']: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'boolean[]',
            },
            ['real_array']: {
              characterMaximumLength: null,
              columnDefault: null,
              dataType: 'real[]',
            },
            ['int_array']: {
              dataType: 'integer[]',
              characterMaximumLength: null,
              columnDefault: "'{1,2,3}'::integer[]",
            },
            ['json_val']: {
              dataType: 'jsonb',
              characterMaximumLength: null,
              columnDefault: null,
            },
          },
          primaryKey: ['user_id'],
        },
      },
    },
    {
      name: 'column subset',
      setupQuery: `
      CREATE SCHEMA test;
      CREATE TABLE test.users (
        user_id INTEGER PRIMARY KEY,
        password VARCHAR (50),  -- This will not be published
        timez TIMESTAMPTZ,
        bigint_val BIGINT,
        bool_val BOOL,
        real_val REAL,
        int_array INTEGER[] DEFAULT '{1, 2, 3}',
        json_val JSONB
      );
      CREATE PUBLICATION zero_data FOR TABLE test.users (user_id, timez, int_array, json_val);
      `,
      expectedResult: {
        ['test.users']: {
          schema: 'test',
          name: 'users',
          columns: {
            ['user_id']: {
              dataType: 'integer',
              characterMaximumLength: null,
              columnDefault: null,
            },
            ['timez']: {
              dataType: 'timestamp with time zone',
              characterMaximumLength: null,
              columnDefault: null,
            },
            ['int_array']: {
              dataType: 'integer[]',
              characterMaximumLength: null,
              columnDefault: "'{1,2,3}'::integer[]",
            },
            ['json_val']: {
              dataType: 'jsonb',
              characterMaximumLength: null,
              columnDefault: null,
            },
          },
          primaryKey: ['user_id'],
        },
      },
    },
    {
      name: 'primary key columns',
      setupQuery: `
      CREATE SCHEMA test;
      CREATE TABLE test.issues (
        issue_id INTEGER,
        description TEXT,
        org_id INTEGER,
        component_id INTEGER,
        PRIMARY KEY (org_id, component_id, issue_id)
      );
      CREATE PUBLICATION zero_keys FOR ALL TABLES;
      `,
      expectedResult: {
        ['test.issues']: {
          schema: 'test',
          name: 'issues',
          columns: {
            ['issue_id']: {
              dataType: 'integer',
              characterMaximumLength: null,
              columnDefault: null,
            },
            ['description']: {
              dataType: 'text',
              characterMaximumLength: null,
              columnDefault: null,
            },
            ['org_id']: {
              dataType: 'integer',
              characterMaximumLength: null,
              columnDefault: null,
            },
            ['component_id']: {
              dataType: 'integer',
              characterMaximumLength: null,
              columnDefault: null,
            },
          },
          primaryKey: ['org_id', 'component_id', 'issue_id'],
        },
      },
    },
    {
      name: 'multiple schemas',
      setupQuery: `
      CREATE SCHEMA test;
      CREATE TABLE test.issues (
        issue_id INTEGER,
        description TEXT,
        org_id INTEGER,
        component_id INTEGER,
        PRIMARY KEY (org_id, component_id, issue_id)
      );
      CREATE TABLE test.users (
        user_id INTEGER PRIMARY KEY,
        password TEXT,
        handle TEXT DEFAULT 'foo'
      );
      CREATE PUBLICATION zero_tables FOR TABLE test.issues, TABLE test.users (user_id, handle);

      CREATE SCHEMA zero;
      CREATE PUBLICATION zero_meta FOR TABLES IN SCHEMA zero;

      CREATE TABLE zero.clients (
        client_id VARCHAR (180) PRIMARY KEY,
        last_mutation_id BIGINT
      );
      `,
      expectedResult: {
        ['test.issues']: {
          schema: 'test',
          name: 'issues',
          columns: {
            ['issue_id']: {
              dataType: 'integer',
              characterMaximumLength: null,
              columnDefault: null,
            },
            ['description']: {
              dataType: 'text',
              characterMaximumLength: null,
              columnDefault: null,
            },
            ['org_id']: {
              dataType: 'integer',
              characterMaximumLength: null,
              columnDefault: null,
            },
            ['component_id']: {
              dataType: 'integer',
              characterMaximumLength: null,
              columnDefault: null,
            },
          },
          primaryKey: ['org_id', 'component_id', 'issue_id'],
        },
        ['test.users']: {
          schema: 'test',
          name: 'users',
          columns: {
            ['user_id']: {
              dataType: 'integer',
              characterMaximumLength: null,
              columnDefault: null,
            },
            ['handle']: {
              dataType: 'text',
              characterMaximumLength: null,
              columnDefault: "'foo'::text",
            },
          },
          primaryKey: ['user_id'],
        },
        ['zero.clients']: {
          schema: 'zero',
          name: 'clients',
          columns: {
            ['client_id']: {
              dataType: 'character varying',
              characterMaximumLength: 180,
              columnDefault: null,
            },
            ['last_mutation_id']: {
              dataType: 'bigint',
              characterMaximumLength: null,
              columnDefault: null,
            },
          },
          primaryKey: ['client_id'],
        },
      },
    },
  ];

  const testDBs = new TestDBs();
  let db: postgres.Sql;
  beforeEach(async () => {
    db = await testDBs.create('published_tables_test');
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  afterAll(async () => {
    await testDBs.end();
  });

  for (const c of cases) {
    test(c.name, async () => {
      await db.unsafe(c.setupQuery);

      const tables = await getPublishedTables(db, 'zero_');
      expect(tables).toEqual(c.expectedResult);
    });
  }
});
