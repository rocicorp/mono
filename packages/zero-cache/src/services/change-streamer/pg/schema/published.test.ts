import type postgres from 'postgres';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from '../../../../test/db.js';
import {type PublicationInfo, getPublicationInfo} from './published.js';

describe('tables/published', () => {
  type Case = {
    name: string;
    setupQuery: string;
    expectedResult?: PublicationInfo;
    expectedError?: string;
  };

  const cases: Case[] = [
    {
      name: 'Nothing published',
      setupQuery: `CREATE SCHEMA zero`,
      expectedResult: {
        publications: [],
        tables: [],
        indices: [],
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
                pos: 1,
                dataType: 'varchar',
                characterMaximumLength: 180,
                notNull: true,
                dflt: null,
              },
              lastMutationID: {
                pos: 2,
                dataType: 'int8',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
            },
            primaryKey: ['clientID'],
            publications: {['zero_all']: {rowFilter: null}},
          },
        ],
        indices: [],
      },
    },
    {
      name: 'types and array types',
      setupQuery: `
      CREATE SCHEMA test;
      CREATE TABLE test.users (
        user_id INTEGER PRIMARY KEY,
        handle text DEFAULT null,
        address text[],
        boolean BOOL DEFAULT 'false',
        int int8 DEFAULT 2147483647,
        flt FLOAT8 DEFAULT 123.456,
        bigint int8 DEFAULT 2147483648,
        timez TIMESTAMPTZ[],
        bigint_array BIGINT[],
        bool_array BOOL[] DEFAULT '{true,false}',
        real_array REAL[],
        int_array INTEGER[],
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
                pos: 1,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: true,
                dflt: null,
              },
              handle: {
                pos: 2,
                characterMaximumLength: null,
                dataType: 'text',
                notNull: false,
                dflt: null,
              },
              address: {
                pos: 3,
                characterMaximumLength: null,
                dataType: 'text[]',
                notNull: false,
                dflt: null,
              },
              boolean: {
                pos: 4,
                characterMaximumLength: null,
                dataType: 'bool',
                notNull: false,
                dflt: 'false',
              },
              int: {
                pos: 5,
                characterMaximumLength: null,
                dataType: 'int8',
                notNull: false,
                dflt: '2147483647',
              },
              flt: {
                pos: 6,
                characterMaximumLength: null,
                dataType: 'float8',
                notNull: false,
                dflt: '123.456',
              },
              bigint: {
                pos: 7,
                characterMaximumLength: null,
                dataType: 'int8',
                notNull: false,
                dflt: "'2147483648'::bigint",
              },
              timez: {
                pos: 8,
                dataType: 'timestamptz[]',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
              ['bigint_array']: {
                pos: 9,
                characterMaximumLength: null,
                dataType: 'int8[]',
                notNull: false,
                dflt: null,
              },
              ['bool_array']: {
                pos: 10,
                characterMaximumLength: null,
                dataType: 'bool[]',
                notNull: false,
                dflt: "'{t,f}'::boolean[]",
              },
              ['real_array']: {
                pos: 11,
                characterMaximumLength: null,
                dataType: 'float4[]',
                notNull: false,
                dflt: null,
              },
              ['int_array']: {
                pos: 12,
                dataType: 'int4[]',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
              ['json_val']: {
                pos: 13,
                dataType: 'jsonb',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
            },
            primaryKey: ['user_id'],
            publications: {['zero_data']: {rowFilter: null}},
          },
        ],
        indices: [],
      },
    },
    {
      name: 'row filter',
      setupQuery: `
      CREATE SCHEMA test;
      CREATE TABLE test.users (
        user_id INTEGER PRIMARY KEY,
        org_id INTEGER,
        handle text
      );
      CREATE PUBLICATION zero_data FOR TABLE test.users WHERE (org_id = 123);
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
                pos: 1,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: true,
                dflt: null,
              },
              ['org_id']: {
                pos: 2,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
              handle: {
                pos: 3,
                characterMaximumLength: null,
                dataType: 'text',
                notNull: false,
                dflt: null,
              },
            },
            primaryKey: ['user_id'],
            publications: {['zero_data']: {rowFilter: '(org_id = 123)'}},
          },
        ],
        indices: [],
      },
    },
    {
      name: 'multiple row filters',
      setupQuery: `
      CREATE SCHEMA test;
      CREATE TABLE test.users (
        user_id INTEGER PRIMARY KEY,
        org_id INTEGER,
        handle text
      );
      CREATE PUBLICATION zero_one FOR TABLE test.users WHERE (org_id = 123);
      CREATE PUBLICATION zero_two FOR TABLE test.users (org_id, handle, user_id) WHERE (org_id = 456);
      `,
      expectedResult: {
        publications: [
          {
            pubname: 'zero_one',
            pubinsert: true,
            pubupdate: true,
            pubdelete: true,
            pubtruncate: true,
          },
          {
            pubname: 'zero_two',
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
                pos: 1,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: true,
                dflt: null,
              },
              ['org_id']: {
                pos: 2,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
              handle: {
                pos: 3,
                characterMaximumLength: null,
                dataType: 'text',
                notNull: false,
                dflt: null,
              },
            },
            primaryKey: ['user_id'],
            publications: {
              ['zero_one']: {rowFilter: '(org_id = 123)'},
              ['zero_two']: {rowFilter: '(org_id = 456)'},
            },
          },
        ],
        indices: [],
      },
    },
    {
      name: 'multiple row filters with unconditional',
      setupQuery: `
      CREATE SCHEMA test;
      CREATE TABLE test.users (
        user_id INTEGER PRIMARY KEY,
        org_id INTEGER,
        handle text
      );
      CREATE PUBLICATION zero_one FOR TABLE test.users WHERE (org_id = 123);
      CREATE PUBLICATION zero_two FOR TABLE test.users (org_id, handle, user_id);
      `,
      expectedResult: {
        publications: [
          {
            pubname: 'zero_one',
            pubinsert: true,
            pubupdate: true,
            pubdelete: true,
            pubtruncate: true,
          },
          {
            pubname: 'zero_two',
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
                pos: 1,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: true,
                dflt: null,
              },
              ['org_id']: {
                pos: 2,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
              handle: {
                pos: 3,
                characterMaximumLength: null,
                dataType: 'text',
                notNull: false,
                dflt: null,
              },
            },
            primaryKey: ['user_id'],
            publications: {
              ['zero_one']: {rowFilter: '(org_id = 123)'},
              ['zero_two']: {rowFilter: null},
            },
          },
        ],
        indices: [],
      },
    },
    {
      name: 'multiple row filters with conflicting columns',
      setupQuery: `
      CREATE SCHEMA test;
      CREATE TABLE test.users (
        user_id INTEGER PRIMARY KEY,
        org_id INTEGER,
        handle text
      );
      CREATE PUBLICATION zero_one FOR TABLE test.users WHERE (org_id = 123);
      CREATE PUBLICATION zero_two FOR TABLE test.users (org_id, user_id);
      `,
      expectedError:
        'Error: Table users is exported with different columns: [user_id,org_id,handle] vs [user_id,org_id]',
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
        int_array INTEGER[],
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
                pos: 1,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: true,
                dflt: null,
              },
              ['timez']: {
                pos: 3,
                dataType: 'timestamptz',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
              ['int_array']: {
                pos: 7,
                dataType: 'int4[]',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
              ['json_val']: {
                pos: 8,
                dataType: 'jsonb',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
            },
            primaryKey: ['user_id'],
            publications: {['zero_data']: {rowFilter: null}},
          },
        ],
        indices: [],
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
                pos: 1,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: true,
                dflt: null,
              },
              ['description']: {
                pos: 2,
                dataType: 'text',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
              ['org_id']: {
                pos: 3,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: true,
                dflt: null,
              },
              ['component_id']: {
                pos: 4,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: true,
                dflt: null,
              },
            },
            primaryKey: ['org_id', 'component_id', 'issue_id'],
            publications: {['zero_keys']: {rowFilter: null}},
          },
        ],
        indices: [],
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
      CREATE PUBLICATION _zero_meta FOR TABLES IN SCHEMA zero;

      CREATE TABLE zero.clients (
        "clientID" VARCHAR (180) PRIMARY KEY,
        "lastMutationID" BIGINT
      );
      `,
      expectedResult: {
        publications: [
          {
            pubname: '_zero_meta',
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
                pos: 1,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: true,
                dflt: null,
              },
              ['description']: {
                pos: 2,
                dataType: 'text',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
              ['org_id']: {
                pos: 3,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: true,
                dflt: null,
              },
              ['component_id']: {
                pos: 4,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: true,
                dflt: null,
              },
            },
            primaryKey: ['org_id', 'component_id', 'issue_id'],
            publications: {['zero_tables']: {rowFilter: null}},
          },
          {
            schema: 'test',
            name: 'users',
            columns: {
              ['user_id']: {
                pos: 1,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: true,
                dflt: null,
              },
              ['handle']: {
                pos: 3,
                dataType: 'text',
                characterMaximumLength: null,
                notNull: false,
                dflt: "'foo'::text",
              },
            },
            primaryKey: ['user_id'],
            publications: {['zero_tables']: {rowFilter: null}},
          },
          {
            schema: 'zero',
            name: 'clients',
            columns: {
              clientID: {
                pos: 1,
                dataType: 'varchar',
                characterMaximumLength: 180,
                notNull: true,
                dflt: null,
              },
              lastMutationID: {
                pos: 2,
                dataType: 'int8',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
            },
            primaryKey: ['clientID'],
            publications: {['_zero_meta']: {rowFilter: null}},
          },
        ],
        indices: [],
      },
    },
    {
      name: 'indices',
      setupQuery: `
      CREATE SCHEMA test;
      CREATE TABLE test.issues (
        issue_id INTEGER PRIMARY KEY,
        org_id INTEGER,
        component_id INTEGER
      );
      CREATE INDEX issues_org_id ON test.issues (org_id);
      CREATE INDEX issues_component_id ON test.issues (component_id);
      CREATE PUBLICATION zero_data FOR TABLE test.issues;
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
            name: 'issues',
            columns: {
              ['issue_id']: {
                pos: 1,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: true,
                dflt: null,
              },
              ['org_id']: {
                pos: 2,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
              ['component_id']: {
                pos: 3,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
            },
            primaryKey: ['issue_id'],
            publications: {['zero_data']: {rowFilter: null}},
          },
        ],
        indices: [
          {
            schemaName: 'test',
            tableName: 'issues',
            name: 'issues_component_id',
            columns: ['component_id'],
            unique: false,
          },
          {
            schemaName: 'test',
            tableName: 'issues',
            name: 'issues_org_id',
            columns: ['org_id'],
            unique: false,
          },
        ],
      },
    },
    {
      name: 'unique indices',
      setupQuery: `
      CREATE SCHEMA test;
      CREATE TABLE test.issues (
        issue_id INTEGER PRIMARY KEY,
        org_id INTEGER,
        component_id INTEGER
      );
      CREATE UNIQUE INDEX issues_org_id ON test.issues (org_id);
      CREATE UNIQUE INDEX issues_component_id ON test.issues (component_id);
      CREATE PUBLICATION zero_data FOR TABLE test.issues;
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
            name: 'issues',
            columns: {
              ['issue_id']: {
                pos: 1,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: true,
                dflt: null,
              },
              ['org_id']: {
                pos: 2,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
              ['component_id']: {
                pos: 3,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
            },
            primaryKey: ['issue_id'],
            publications: {['zero_data']: {rowFilter: null}},
          },
        ],
        indices: [
          {
            schemaName: 'test',
            tableName: 'issues',
            name: 'issues_component_id',
            columns: ['component_id'],
            unique: true,
          },
          {
            schemaName: 'test',
            tableName: 'issues',
            name: 'issues_org_id',
            columns: ['org_id'],
            unique: true,
          },
        ],
      },
    },
    {
      name: 'compound indices',
      setupQuery: `
      CREATE SCHEMA test;
      CREATE TABLE test.foo (
        id INTEGER PRIMARY KEY,
        a INTEGER,
        b INTEGER
      );
      CREATE INDEX foo_a_b ON test.foo (a, b);
      CREATE INDEX foo_b_a ON test.foo (b, a);
      CREATE PUBLICATION zero_data FOR TABLE test.foo;
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
            name: 'foo',
            columns: {
              ['id']: {
                pos: 1,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: true,
                dflt: null,
              },
              ['a']: {
                pos: 2,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
              ['b']: {
                pos: 3,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
            },
            primaryKey: ['id'],
            publications: {['zero_data']: {rowFilter: null}},
          },
        ],
        indices: [
          {
            schemaName: 'test',
            tableName: 'foo',
            name: 'foo_a_b',
            columns: ['a', 'b'],
            unique: false,
          },
          {
            schemaName: 'test',
            tableName: 'foo',
            name: 'foo_b_a',
            columns: ['b', 'a'],
            unique: false,
          },
        ],
      },
    },
    {
      name: 'indices after column rename',
      setupQuery: `
      CREATE SCHEMA test;
      CREATE TABLE test.foo (
        id INTEGER PRIMARY KEY,
        a INTEGER,
        b INTEGER
      );
      CREATE INDEX foo_a_b ON test.foo (a, b);
      CREATE INDEX foo_b_a ON test.foo (b, a);
      CREATE PUBLICATION zero_data FOR TABLE test.foo;

      ALTER TABLE test.foo RENAME a to az;
      ALTER TABLE test.foo RENAME b to bz;
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
            name: 'foo',
            columns: {
              ['id']: {
                pos: 1,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: true,
                dflt: null,
              },
              ['az']: {
                pos: 2,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
              ['bz']: {
                pos: 3,
                dataType: 'int4',
                characterMaximumLength: null,
                notNull: false,
                dflt: null,
              },
            },
            primaryKey: ['id'],
            publications: {['zero_data']: {rowFilter: null}},
          },
        ],
        indices: [
          {
            schemaName: 'test',
            tableName: 'foo',
            name: 'foo_a_b',
            columns: ['az', 'bz'],
            unique: false,
          },
          {
            schemaName: 'test',
            tableName: 'foo',
            name: 'foo_b_a',
            columns: ['bz', 'az'],
            unique: false,
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

      // Make sure both lookup methods work (e.g. name prefix and list membership)
      for (const restrictions of [
        undefined,
        [
          'zero_all',
          'zero_data',
          'zero_one',
          'zero_two',
          'zero_keys',
          '_zero_meta',
          'zero_tables',
        ],
      ])
        try {
          const tables = await getPublicationInfo(db, restrictions);
          expect(tables).toEqual(c.expectedResult);
        } catch (e) {
          if (c.expectedError) {
            expect(c.expectedError).toMatch(String(e));
          } else {
            throw e;
          }
        }
    });
  }
});
