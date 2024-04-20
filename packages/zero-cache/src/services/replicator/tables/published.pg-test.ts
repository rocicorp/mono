import type postgres from 'postgres';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from '../../../test/db.js';
import {PublicationInfo, getPublicationInfo} from './published.js';

describe('tables/published', () => {
  type Case = {
    name: string;
    setupQuery: string;
    expectedResult: PublicationInfo;
  };

  const cases: Case[] = [
    {
      name: 'Nothing published',
      setupQuery: `CREATE SCHEMA zero`,
      expectedResult: {
        publications: [],
        tables: [],
      },
    },
    {
      name: 'zero.clients',
      setupQuery: `
      CREATE SCHEMA zero;
      CREATE PUBLICATION zero_all FOR TABLES IN SCHEMA zero;
      CREATE TABLE zero.clients (
        "clientID" VARCHAR (180) PRIMARY KEY,
        "lastMutationID" BIGINT
      );
      `,
      expectedResult: {
        publications: [
          {
            pubname: 'zero_all',
            pubinsert: true,
            pubupdate: true,
            pubdelete: true,
            pubtruncate: true,
          },
        ],
        tables: [
          {
            schema: 'zero',
            name: 'clients',
            columns: {
              clientID: {
                dataType: 'varchar',
                characterMaximumLength: 180,
                columnDefault: null,
                notNull: true,
              },
              lastMutationID: {
                dataType: 'int8',
                characterMaximumLength: null,
                columnDefault: null,
                notNull: false,
              },
            },
            primaryKey: ['clientID'],
          },
        ],
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
        publications: [
          {
            pubname: 'zero_data',
            pubinsert: true,
            pubupdate: true,
            pubdelete: true,
            pubtruncate: true,
          },
        ],
        tables: [
          {
            schema: 'test',
            name: 'users',
            columns: {
              ['user_id']: {
                dataType: 'int4',
                characterMaximumLength: null,
                columnDefault: null,
                notNull: true,
              },
              handle: {
                characterMaximumLength: null,
                columnDefault: null,
                dataType: 'text',
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
        ],
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
        publications: [
          {
            pubname: 'zero_data',
            pubinsert: true,
            pubupdate: true,
            pubdelete: true,
            pubtruncate: true,
          },
        ],
        tables: [
          {
            schema: 'test',
            name: 'users',
            columns: {
              ['user_id']: {
                dataType: 'int4',
                characterMaximumLength: null,
                columnDefault: null,
                notNull: true,
              },
              ['timez']: {
                dataType: 'timestamptz',
                characterMaximumLength: null,
                columnDefault: null,
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
        ],
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
        publications: [
          {
            pubname: 'zero_keys',
            pubinsert: true,
            pubupdate: true,
            pubdelete: true,
            pubtruncate: true,
          },
        ],
        tables: [
          {
            schema: 'test',
            name: 'issues',
            columns: {
              ['issue_id']: {
                dataType: 'int4',
                characterMaximumLength: null,
                columnDefault: null,
                notNull: true,
              },
              ['description']: {
                dataType: 'text',
                characterMaximumLength: null,
                columnDefault: null,
                notNull: false,
              },
              ['org_id']: {
                dataType: 'int4',
                characterMaximumLength: null,
                columnDefault: null,
                notNull: true,
              },
              ['component_id']: {
                dataType: 'int4',
                characterMaximumLength: null,
                columnDefault: null,
                notNull: true,
              },
            },
            primaryKey: ['org_id', 'component_id', 'issue_id'],
          },
        ],
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
        "clientID" VARCHAR (180) PRIMARY KEY,
        "lastMutationID" BIGINT
      );
      `,
      expectedResult: {
        publications: [
          {
            pubname: 'zero_meta',
            pubinsert: true,
            pubupdate: true,
            pubdelete: true,
            pubtruncate: true,
          },
          {
            pubname: 'zero_tables',
            pubinsert: true,
            pubupdate: true,
            pubdelete: true,
            pubtruncate: true,
          },
        ],
        tables: [
          {
            schema: 'test',
            name: 'issues',
            columns: {
              ['issue_id']: {
                dataType: 'int4',
                characterMaximumLength: null,
                columnDefault: null,
                notNull: true,
              },
              ['description']: {
                dataType: 'text',
                characterMaximumLength: null,
                columnDefault: null,
                notNull: false,
              },
              ['org_id']: {
                dataType: 'int4',
                characterMaximumLength: null,
                columnDefault: null,
                notNull: true,
              },
              ['component_id']: {
                dataType: 'int4',
                characterMaximumLength: null,
                columnDefault: null,
                notNull: true,
              },
            },
            primaryKey: ['org_id', 'component_id', 'issue_id'],
          },
          {
            schema: 'test',
            name: 'users',
            columns: {
              ['user_id']: {
                dataType: 'int4',
                characterMaximumLength: null,
                columnDefault: null,
                notNull: true,
              },
              ['handle']: {
                dataType: 'text',
                characterMaximumLength: null,
                columnDefault: "'foo'::text",
                notNull: false,
              },
            },
            primaryKey: ['user_id'],
          },
          {
            schema: 'zero',
            name: 'clients',
            columns: {
              clientID: {
                dataType: 'varchar',
                characterMaximumLength: 180,
                columnDefault: null,
                notNull: true,
              },
              lastMutationID: {
                dataType: 'int8',
                characterMaximumLength: null,
                columnDefault: null,
                notNull: false,
              },
            },
            primaryKey: ['clientID'],
          },
        ],
      },
    },
  ];

  let db: postgres.Sql;
  beforeEach(async () => {
    db = await testDBs.create('published_tables_test');
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  for (const c of cases) {
    test(c.name, async () => {
      await db.unsafe(c.setupQuery);

      const tables = await getPublicationInfo(db, 'zero_');
      expect(tables).toEqual(c.expectedResult);
    });
  }
});
