import type postgres from 'postgres';
import {beforeEach, describe, expect} from 'vitest';
import * as PostgresTypeClass from '../../../../db/postgres-type-class-enum.ts';
import {test, type PgTest} from '../../../../test/db.ts';
import {getPublicationInfo, type PublicationInfo} from './published.ts';

describe('tables/published', () => {
  let db: postgres.Sql;
  let pgVersion: number;

  beforeEach<PgTest>(async ({testDBs}) => {
    db = await testDBs.create('published_tables_test');
    [{pgVersion}] =
      await db`SELECT current_setting('server_version_num')::int as "pgVersion"`;
    return () => testDBs.drop(db);
  });

  async function runAndExpectIndexes(
    setupQuery: string,
    expectedResult: Omit<PublicationInfo, 'indexes'>,
  ) {
    await db.unsafe(setupQuery);

    const tables = await getPublicationInfo(
      db,
      expectedResult.publications.map(p => p.pubname),
    );
    expect(tables).toMatchObject(expectedResult);
    for (const t of tables.tables) {
      expect(t.columns.excluded).toBeUndefined();
    }
    // Return an expectation on the stringified indexes to preserve field
    // ordering, which is used to define the order of the indexed columns.
    return expect(JSON.stringify(tables.indexes, null, 2));
  }

  async function runAndExpectError(setupQuery: string) {
    await db.unsafe(setupQuery);

    try {
      await getPublicationInfo(db, [
        'zero_all',
        'zero_data',
        'zero_one',
        'zero_two',
        'zero_keys',
        '_zero_meta',
        'zero_tables',
      ]);
      throw Error('expected error');
    } catch (e) {
      return expect(e);
    }
  }

  test('zero.clients', async () => {
    (
      await runAndExpectIndexes(
        `
      CREATE SCHEMA zero;
      CREATE PUBLICATION zero_all FOR TABLES IN SCHEMA zero;
      CREATE TABLE zero.clients (
        "clientID" VARCHAR (180) PRIMARY KEY,
        "lastMutationID" BIGINT
      );`,
        {
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
              oid: expect.any(Number),
              schema: 'zero',
              schemaOID: expect.any(Number),
              name: 'clients',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['clientID'],
              columns: {
                clientID: {
                  pos: 1,
                  dataType: 'varchar',
                  typeOID: 1043,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: 180,
                  notNull: true,
                  dflt: null,
                },
                lastMutationID: {
                  pos: 2,
                  dataType: 'int8',
                  typeOID: 20,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
              },
              primaryKey: ['clientID'],
              publications: {['zero_all']: {rowFilter: null}},
            },
          ],
        },
      )
    ).toMatchInlineSnapshot(`
      "[
        {
          "schema": "zero",
          "tableName": "clients",
          "name": "clients_pkey",
          "unique": true,
          "isPrimaryKey": true,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "clientID": "ASC"
          }
        }
      ]"
    `);
  });

  test('types and array types', async () => {
    (
      await runAndExpectIndexes(
        `
        CREATE TYPE DAY_OF_WEEK AS ENUM ('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun');
        CREATE SCHEMA test;
        CREATE TABLE test.users (
          user_id INTEGER PRIMARY KEY,
          handle text DEFAULT null,
          address text[],
          boolean BOOL DEFAULT 'false',
          int int8 DEFAULT (2147483647 + 2),
          flt FLOAT8 DEFAULT 123.456,
          bigint int8 DEFAULT 2147483648,
          timez TIMESTAMPTZ[],
          bigint_array BIGINT[],
          bool_array BOOL[] DEFAULT '{true,false}',
          real_array REAL[],
          int_array INTEGER[],
          json_val JSONB,
          day DAY_OF_WEEK,
          excluded INTEGER GENERATED ALWAYS AS (user_id + 1) STORED
        );
        CREATE PUBLICATION zero_data FOR TABLE test.users;`,
        {
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
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'users',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['user_id'],
              columns: {
                ['user_id']: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                handle: {
                  pos: 2,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  dataType: 'text',
                  typeOID: 25,
                  notNull: false,
                  dflt: null,
                },
                address: {
                  pos: 3,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  dataType: 'text[]',
                  typeOID: 1009,
                  notNull: false,
                  dflt: null,
                },
                boolean: {
                  pos: 4,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  dataType: 'bool',
                  typeOID: 16,
                  notNull: false,
                  dflt: 'false',
                },
                int: {
                  pos: 5,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  dataType: 'int8',
                  typeOID: 20,
                  notNull: false,
                  dflt: '(2147483647 + 2)',
                },
                flt: {
                  pos: 6,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  dataType: 'float8',
                  typeOID: 701,
                  notNull: false,
                  dflt: '123.456',
                },
                bigint: {
                  pos: 7,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  dataType: 'int8',
                  typeOID: 20,
                  notNull: false,
                  dflt: "'2147483648'::bigint",
                },
                timez: {
                  pos: 8,
                  dataType: 'timestamptz[]',
                  typeOID: 1185,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
                ['bigint_array']: {
                  pos: 9,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  dataType: 'int8[]',
                  typeOID: 1016,
                  notNull: false,
                  dflt: null,
                },
                ['bool_array']: {
                  pos: 10,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  dataType: 'bool[]',
                  typeOID: 1000,
                  notNull: false,
                  dflt: "'{t,f}'::boolean[]",
                },
                ['real_array']: {
                  pos: 11,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  dataType: 'float4[]',
                  typeOID: 1021,
                  notNull: false,
                  dflt: null,
                },
                ['int_array']: {
                  pos: 12,
                  dataType: 'int4[]',
                  typeOID: 1007,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
                ['json_val']: {
                  pos: 13,
                  dataType: 'jsonb',
                  typeOID: 3802,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
                ['day']: {
                  pos: 14,
                  dataType: 'day_of_week',
                  typeOID: expect.any(Number),
                  pgTypeClass: PostgresTypeClass.Enum,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
              },
              primaryKey: ['user_id'],
              publications: {['zero_data']: {rowFilter: null}},
            },
          ],
        },
      )
    ).toMatchInlineSnapshot(`
      "[
        {
          "schema": "test",
          "tableName": "users",
          "name": "users_pkey",
          "unique": true,
          "isPrimaryKey": true,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "user_id": "ASC"
          }
        }
      ]"
    `);
  });

  test('row filter', async () => {
    (
      await runAndExpectIndexes(
        `
      CREATE SCHEMA test;
      CREATE TABLE test.users (
        user_id INTEGER PRIMARY KEY,
        org_id INTEGER,
        handle text
      );
      CREATE PUBLICATION zero_data FOR TABLE test.users WHERE (org_id = 123);`,
        {
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
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'users',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['user_id'],
              columns: {
                ['user_id']: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['org_id']: {
                  pos: 2,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
                handle: {
                  pos: 3,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  dataType: 'text',
                  typeOID: 25,
                  notNull: false,
                  dflt: null,
                },
              },
              primaryKey: ['user_id'],
              publications: {['zero_data']: {rowFilter: '(org_id = 123)'}},
            },
          ],
        },
      )
    ).toMatchInlineSnapshot(`
      "[
        {
          "schema": "test",
          "tableName": "users",
          "name": "users_pkey",
          "unique": true,
          "isPrimaryKey": true,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "user_id": "ASC"
          }
        }
      ]"
    `);
  });

  test('multple row filters', async () => {
    (
      await runAndExpectIndexes(
        `
      CREATE SCHEMA test;
      CREATE TABLE test.users (
        user_id INTEGER PRIMARY KEY,
        org_id INTEGER,
        handle text
      );
      CREATE PUBLICATION zero_one FOR TABLE test.users WHERE (org_id = 123);
      CREATE PUBLICATION zero_two FOR TABLE test.users (org_id, handle, user_id) WHERE (org_id = 456);`,
        {
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
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'users',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['user_id'],
              columns: {
                ['user_id']: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['org_id']: {
                  pos: 2,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
                handle: {
                  pos: 3,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  dataType: 'text',
                  typeOID: 25,
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
        },
      )
    ).toMatchInlineSnapshot(`
      "[
        {
          "schema": "test",
          "tableName": "users",
          "name": "users_pkey",
          "unique": true,
          "isPrimaryKey": true,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "user_id": "ASC"
          }
        }
      ]"
    `);
  });

  test('multiple row filters with unconditional', async () => {
    (
      await runAndExpectIndexes(
        `
      CREATE SCHEMA test;
      CREATE TABLE test.users (
        user_id INTEGER PRIMARY KEY,
        org_id INTEGER,
        handle text
      );
      CREATE PUBLICATION zero_one FOR TABLE test.users WHERE (org_id = 123);
      CREATE PUBLICATION zero_two FOR TABLE test.users (org_id, handle, user_id);`,
        {
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
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'users',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['user_id'],
              columns: {
                ['user_id']: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['org_id']: {
                  pos: 2,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
                handle: {
                  pos: 3,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  dataType: 'text',
                  typeOID: 25,
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
        },
      )
    ).toMatchInlineSnapshot(`
      "[
        {
          "schema": "test",
          "tableName": "users",
          "name": "users_pkey",
          "unique": true,
          "isPrimaryKey": true,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "user_id": "ASC"
          }
        }
      ]"
    `);
  });

  test('multiple row filters with conflicting columns', async () => {
    (
      await runAndExpectError(`
      CREATE SCHEMA test;
      CREATE TABLE test.users (
        user_id INTEGER PRIMARY KEY,
        org_id INTEGER,
        handle text
      );
      CREATE PUBLICATION zero_one FOR TABLE test.users WHERE (org_id = 123);
      CREATE PUBLICATION zero_two FOR TABLE test.users (org_id, user_id);`)
    ).toMatchInlineSnapshot(
      `[Error: Table users is exported with different columns: [user_id,org_id,handle] vs [user_id,org_id]]`,
    );
  });

  test('column subset', async () => {
    (
      await runAndExpectIndexes(
        `
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
      CREATE PUBLICATION zero_data FOR TABLE test.users (user_id, timez, int_array, json_val);`,
        {
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
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'users',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['user_id'],
              columns: {
                ['user_id']: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['timez']: {
                  pos: 3,
                  dataType: 'timestamptz',
                  typeOID: 1184,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
                ['int_array']: {
                  pos: 7,
                  dataType: 'int4[]',
                  typeOID: 1007,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
                ['json_val']: {
                  pos: 8,
                  dataType: 'jsonb',
                  typeOID: 3802,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
              },
              primaryKey: ['user_id'],
              publications: {['zero_data']: {rowFilter: null}},
            },
          ],
        },
      )
    ).toMatchInlineSnapshot(`
      "[
        {
          "schema": "test",
          "tableName": "users",
          "name": "users_pkey",
          "unique": true,
          "isPrimaryKey": true,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "user_id": "ASC"
          }
        }
      ]"
    `);
  });

  test('primary key columns', async () => {
    (
      await runAndExpectIndexes(
        `
      CREATE SCHEMA test;
      CREATE TABLE test.issues (
        issue_id INTEGER,
        description TEXT,
        org_id INTEGER,
        component_id INTEGER,
        PRIMARY KEY (org_id, component_id, issue_id)
      );
      CREATE PUBLICATION zero_keys FOR ALL TABLES;`,
        {
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
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'issues',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['org_id', 'component_id', 'issue_id'],
              columns: {
                ['issue_id']: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['description']: {
                  pos: 2,
                  dataType: 'text',
                  typeOID: 25,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
                ['org_id']: {
                  pos: 3,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['component_id']: {
                  pos: 4,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
              },
              primaryKey: ['org_id', 'component_id', 'issue_id'],
              publications: {['zero_keys']: {rowFilter: null}},
            },
          ],
        },
      )
    ).toMatchInlineSnapshot(`
      "[
        {
          "schema": "test",
          "tableName": "issues",
          "name": "issues_pkey",
          "unique": true,
          "isPrimaryKey": true,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "org_id": "ASC",
            "component_id": "ASC",
            "issue_id": "ASC"
          }
        }
      ]"
    `);
  });

  test('missing primary key', async () => {
    (
      await runAndExpectIndexes(
        `
      CREATE SCHEMA test;
      CREATE TABLE test.issues (
        issue_id INTEGER NOT NULL,
        description TEXT,
        org_id INTEGER NOT NULL,
        component_id INTEGER NOT NULL
      );
      CREATE PUBLICATION zero_keys FOR ALL TABLES;`,
        {
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
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'issues',
              replicaIdentity: 'd',
              replicaIdentityColumns: [],
              columns: {
                ['issue_id']: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['description']: {
                  pos: 2,
                  dataType: 'text',
                  typeOID: 25,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
                ['org_id']: {
                  pos: 3,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['component_id']: {
                  pos: 4,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
              },
              primaryKey: [],
              publications: {['zero_keys']: {rowFilter: null}},
            },
          ],
        },
      )
    ).toMatchInlineSnapshot(`"[]"`);
  });

  test('multiple schemas', async () => {
    (
      await runAndExpectIndexes(
        `
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
      );`,
        {
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
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'issues',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['org_id', 'component_id', 'issue_id'],
              columns: {
                ['issue_id']: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['description']: {
                  pos: 2,
                  dataType: 'text',
                  typeOID: 25,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
                ['org_id']: {
                  pos: 3,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['component_id']: {
                  pos: 4,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
              },
              primaryKey: ['org_id', 'component_id', 'issue_id'],
              publications: {['zero_tables']: {rowFilter: null}},
            },
            {
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'users',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['user_id'],
              columns: {
                ['user_id']: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['handle']: {
                  pos: 3,
                  dataType: 'text',
                  typeOID: 25,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: "'foo'::text",
                },
              },
              primaryKey: ['user_id'],
              publications: {['zero_tables']: {rowFilter: null}},
            },
            {
              oid: expect.any(Number),
              schema: 'zero',
              schemaOID: expect.any(Number),
              name: 'clients',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['clientID'],
              columns: {
                clientID: {
                  pos: 1,
                  dataType: 'varchar',
                  typeOID: 1043,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: 180,
                  notNull: true,
                  dflt: null,
                },
                lastMutationID: {
                  pos: 2,
                  dataType: 'int8',
                  typeOID: 20,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
              },
              primaryKey: ['clientID'],
              publications: {['_zero_meta']: {rowFilter: null}},
            },
          ],
        },
      )
    ).toMatchInlineSnapshot(`
      "[
        {
          "schema": "test",
          "tableName": "issues",
          "name": "issues_pkey",
          "unique": true,
          "isPrimaryKey": true,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "org_id": "ASC",
            "component_id": "ASC",
            "issue_id": "ASC"
          }
        },
        {
          "schema": "test",
          "tableName": "users",
          "name": "users_pkey",
          "unique": true,
          "isPrimaryKey": true,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "user_id": "ASC"
          }
        },
        {
          "schema": "zero",
          "tableName": "clients",
          "name": "clients_pkey",
          "unique": true,
          "isPrimaryKey": true,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "clientID": "ASC"
          }
        }
      ]"
    `);
  });

  test('indexes that INCLUDE generated', async () => {
    (
      await runAndExpectIndexes(
        `
      CREATE SCHEMA test;
      CREATE TABLE test.issues (
        issue_id INTEGER PRIMARY KEY,
        org_id INTEGER,
        component_id INTEGER,
        stored INTEGER GENERATED ALWAYS AS (issue_id + 1) STORED
      );
      CREATE INDEX issues_org_id ON test.issues (org_id);
      -- Indexes with INCLUDE'd generated columns are fine, as INCLUDE'd
      -- columns are ignored by the replica.
      CREATE INDEX issues_component_id ON test.issues (component_id) INCLUDE (stored);
      CREATE PUBLICATION zero_data FOR TABLE test.issues;
      CREATE PUBLICATION zero_two FOR TABLE test.issues;`,
        {
          publications: [
            {
              pubname: 'zero_data',
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
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'issues',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['issue_id'],
              columns: {
                ['issue_id']: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['org_id']: {
                  pos: 2,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
                ['component_id']: {
                  pos: 3,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
              },
              primaryKey: ['issue_id'],
              publications: {
                ['zero_data']: {rowFilter: null},
                ['zero_two']: {rowFilter: null},
              },
            },
          ],
        },
      )
    ).toMatchInlineSnapshot(`
      "[
        {
          "schema": "test",
          "tableName": "issues",
          "name": "issues_component_id",
          "unique": false,
          "isPrimaryKey": false,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "component_id": "ASC"
          }
        },
        {
          "schema": "test",
          "tableName": "issues",
          "name": "issues_org_id",
          "unique": false,
          "isPrimaryKey": false,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "org_id": "ASC"
          }
        },
        {
          "schema": "test",
          "tableName": "issues",
          "name": "issues_pkey",
          "unique": true,
          "isPrimaryKey": true,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "issue_id": "ASC"
          }
        }
      ]"
    `);
  });

  test('unique indexes', async () => {
    (
      await runAndExpectIndexes(
        `
      CREATE SCHEMA test;
      CREATE TABLE test.issues (
        issue_id INTEGER PRIMARY KEY,
        org_id INTEGER UNIQUE,
        component_id INTEGER
      );
      CREATE UNIQUE INDEX issues_component_id ON test.issues (component_id);
      CREATE PUBLICATION zero_data FOR TABLE test.issues;`,
        {
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
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'issues',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['issue_id'],
              columns: {
                ['issue_id']: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['org_id']: {
                  pos: 2,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
                ['component_id']: {
                  pos: 3,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
              },
              primaryKey: ['issue_id'],
              publications: {['zero_data']: {rowFilter: null}},
            },
          ],
        },
      )
    ).toMatchInlineSnapshot(`
      "[
        {
          "schema": "test",
          "tableName": "issues",
          "name": "issues_component_id",
          "unique": true,
          "isPrimaryKey": false,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "component_id": "ASC"
          }
        },
        {
          "schema": "test",
          "tableName": "issues",
          "name": "issues_org_id_key",
          "unique": true,
          "isPrimaryKey": false,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "org_id": "ASC"
          }
        },
        {
          "schema": "test",
          "tableName": "issues",
          "name": "issues_pkey",
          "unique": true,
          "isPrimaryKey": true,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "issue_id": "ASC"
          }
        }
      ]"
    `);
  });

  test('replica identity index', async () => {
    (
      await runAndExpectIndexes(
        `
      CREATE SCHEMA test;
      CREATE TABLE test.issues (
        issue_id INTEGER NOT NULL,
        org_id INTEGER NOT NULL,
        component_id INTEGER
      );
      CREATE UNIQUE INDEX issues_key_idx ON test.issues (org_id, issue_id);
      ALTER TABLE test.issues REPLICA IDENTITY USING INDEX issues_key_idx;
      CREATE PUBLICATION zero_data FOR TABLE test.issues;`,
        {
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
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'issues',
              replicaIdentity: 'i',
              replicaIdentityColumns: ['org_id', 'issue_id'],
              columns: {
                ['issue_id']: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['org_id']: {
                  pos: 2,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['component_id']: {
                  pos: 3,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
              },
              primaryKey: [],
              publications: {['zero_data']: {rowFilter: null}},
            },
          ],
        },
      )
    ).toMatchInlineSnapshot(`
      "[
        {
          "schema": "test",
          "tableName": "issues",
          "name": "issues_key_idx",
          "unique": true,
          "isPrimaryKey": false,
          "isReplicaIdentity": true,
          "isImmediate": true,
          "columns": {
            "org_id": "ASC",
            "issue_id": "ASC"
          }
        }
      ]"
    `);
  });

  test('replica identity index without index', async () => {
    (
      await runAndExpectIndexes(
        /*sql*/ `
      CREATE SCHEMA test;
      CREATE TABLE test.issues (
        issue_id INTEGER NOT NULL,
        org_id INTEGER NOT NULL,
        component_id INTEGER
      );
      CREATE UNIQUE INDEX issues_key_idx ON test.issues (org_id, issue_id);
      ALTER TABLE test.issues REPLICA IDENTITY USING INDEX issues_key_idx;
      CREATE PUBLICATION zero_data FOR TABLE test.issues;
      DROP INDEX issues_key_idx;`,
        {
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
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'issues',
              replicaIdentity: 'i',
              replicaIdentityColumns: [],
              columns: {
                ['issue_id']: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['org_id']: {
                  pos: 2,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['component_id']: {
                  pos: 3,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
              },
              primaryKey: [],
              publications: {['zero_data']: {rowFilter: null}},
            },
          ],
        },
      )
    ).toMatchInlineSnapshot(`"[]"`);
  });

  test('replica identity full', async () => {
    (
      await runAndExpectIndexes(
        `
      CREATE SCHEMA test;
      CREATE TABLE test.issues (
        issue_id INTEGER NOT NULL,
        org_id INTEGER NOT NULL,
        component_id INTEGER,
        foo_bar TEXT
      );
      ALTER TABLE test.issues REPLICA IDENTITY FULL;
      CREATE PUBLICATION zero_data FOR TABLE test.issues;`,
        {
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
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'issues',
              replicaIdentity: 'f',
              replicaIdentityColumns: [
                'component_id',
                'foo_bar',
                'issue_id',
                'org_id',
              ],
              columns: {
                ['issue_id']: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['org_id']: {
                  pos: 2,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['component_id']: {
                  pos: 3,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
                ['foo_bar']: {
                  pos: 4,
                  dataType: 'text',
                  typeOID: 25,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
              },
              primaryKey: [],
              publications: {['zero_data']: {rowFilter: null}},
            },
          ],
        },
      )
    ).toMatchInlineSnapshot(`"[]"`);
  });

  test('compound indexes', async () => {
    (
      await runAndExpectIndexes(
        `
      CREATE SCHEMA test;
      CREATE TABLE test.foo (
        id INTEGER PRIMARY KEY,
        a INTEGER,
        b INTEGER
      );
      CREATE INDEX foo_a_b ON test.foo (a ASC, b DESC);
      CREATE INDEX foo_b_a ON test.foo (b DESC, a DESC);
      CREATE PUBLICATION zero_data FOR TABLE test.foo;
      CREATE PUBLICATION zero_two FOR TABLE test.foo;`,
        {
          publications: [
            {
              pubname: 'zero_data',
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
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'foo',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['id'],
              columns: {
                ['id']: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['a']: {
                  pos: 2,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
                [PostgresTypeClass.Base]: {
                  pos: 3,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
              },
              primaryKey: ['id'],
              publications: {
                ['zero_data']: {rowFilter: null},
                ['zero_two']: {rowFilter: null},
              },
            },
          ],
        },
      )
    ).toMatchInlineSnapshot(`
      "[
        {
          "schema": "test",
          "tableName": "foo",
          "name": "foo_a_b",
          "unique": false,
          "isPrimaryKey": false,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "a": "ASC",
            "b": "DESC"
          }
        },
        {
          "schema": "test",
          "tableName": "foo",
          "name": "foo_b_a",
          "unique": false,
          "isPrimaryKey": false,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "b": "DESC",
            "a": "DESC"
          }
        },
        {
          "schema": "test",
          "tableName": "foo",
          "name": "foo_pkey",
          "unique": true,
          "isPrimaryKey": true,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "id": "ASC"
          }
        }
      ]"
    `);
  });

  test('ignores irrelevant indexes', async () => {
    (
      await runAndExpectIndexes(
        `
      CREATE SCHEMA test;
      CREATE TABLE test.issues (
        issue_id INTEGER PRIMARY KEY,
        org_id INTEGER CHECK (org_id > 0),
        component_id INTEGER,
        excluded INTEGER GENERATED ALWAYS AS (issue_id + 1) STORED
      );
      CREATE TABLE test.users (
        user_id INTEGER PRIMARY KEY,
        birthday TIMESTAMPTZ
      );
      CREATE INDEX idx_with_expression ON test.issues (org_id, (component_id + 1));
      CREATE INDEX partial_idx ON test.issues (component_id) WHERE org_id > 1000;
      CREATE INDEX idx_with_gen ON test.issues (issue_id, org_id, component_id, excluded);
      CREATE INDEX birthday_idx ON test.users (user_id, birthday);
      CREATE PUBLICATION zero_data FOR TABLE test.issues, TABLE test.users (user_id);`,
        {
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
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'issues',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['issue_id'],
              columns: {
                ['issue_id']: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['org_id']: {
                  pos: 2,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
                ['component_id']: {
                  pos: 3,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
              },
              primaryKey: ['issue_id'],
              publications: {['zero_data']: {rowFilter: null}},
            },
            {
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'users',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['user_id'],
              columns: {
                ['user_id']: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
              },
              primaryKey: ['user_id'],
              publications: {['zero_data']: {rowFilter: null}},
            },
          ],
        },
      )
    ).toMatchInlineSnapshot(`
      "[
        {
          "schema": "test",
          "tableName": "issues",
          "name": "issues_pkey",
          "unique": true,
          "isPrimaryKey": true,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "issue_id": "ASC"
          }
        },
        {
          "schema": "test",
          "tableName": "users",
          "name": "users_pkey",
          "unique": true,
          "isPrimaryKey": true,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "user_id": "ASC"
          }
        }
      ]"
    `);
  });

  test('includes generated columns for pg 18+', async ({skip}) => {
    if (pgVersion < 180000) {
      skip();
    }
    (
      await runAndExpectIndexes(
        /*sql*/ `
      CREATE SCHEMA test;
      CREATE TABLE test.issues (
        issue_id INTEGER PRIMARY KEY,
        org_id INTEGER CHECK (org_id > 0),
        component_id INTEGER,
        included INTEGER GENERATED ALWAYS AS (issue_id + 1) STORED
      );
      CREATE TABLE test.users (
        user_id INTEGER PRIMARY KEY,
        birthday TIMESTAMPTZ
      );
      CREATE INDEX idx_with_expression ON test.issues (org_id, (component_id + 1));
      CREATE INDEX partial_idx ON test.issues (component_id) WHERE org_id > 1000;
      CREATE INDEX idx_with_gen ON test.issues (issue_id, org_id, component_id, included);
      CREATE INDEX birthday_idx ON test.users (user_id, birthday);
      CREATE PUBLICATION zero_data FOR TABLE test.issues, TABLE test.users (user_id) 
             WITH (publish_generated_columns=stored);`,
        {
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
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'issues',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['issue_id'],
              columns: {
                issue_id: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                org_id: {
                  pos: 2,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
                component_id: {
                  pos: 3,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
                included: {
                  pos: 4,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: '(issue_id + 1)',
                },
              },
              primaryKey: ['issue_id'],
              publications: {['zero_data']: {rowFilter: null}},
            },
            {
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'users',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['user_id'],
              columns: {
                ['user_id']: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
              },
              primaryKey: ['user_id'],
              publications: {['zero_data']: {rowFilter: null}},
            },
          ],
        },
      )
    ).toMatchInlineSnapshot(`
      "[
        {
          "schema": "test",
          "tableName": "issues",
          "name": "idx_with_gen",
          "unique": false,
          "isPrimaryKey": false,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "issue_id": "ASC",
            "org_id": "ASC",
            "component_id": "ASC",
            "included": "ASC"
          }
        },
        {
          "schema": "test",
          "tableName": "issues",
          "name": "issues_pkey",
          "unique": true,
          "isPrimaryKey": true,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "issue_id": "ASC"
          }
        },
        {
          "schema": "test",
          "tableName": "users",
          "name": "users_pkey",
          "unique": true,
          "isPrimaryKey": true,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "user_id": "ASC"
          }
        }
      ]"
    `);
  });

  test('indices after column rename', async () => {
    (
      await runAndExpectIndexes(
        `
      CREATE SCHEMA test;
      CREATE TABLE test.foo (
        id INTEGER PRIMARY KEY,
        a INTEGER,
        b INTEGER
      );
      CREATE INDEX foo_a_b ON test.foo (a, b);
      CREATE INDEX foo_b_a ON test.foo (b DESC, a DESC);
      CREATE PUBLICATION zero_data FOR TABLE test.foo;

      ALTER TABLE test.foo RENAME a to az;
      ALTER TABLE test.foo RENAME b to bz;`,
        {
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
              oid: expect.any(Number),
              schema: 'test',
              schemaOID: expect.any(Number),
              name: 'foo',
              replicaIdentity: 'd',
              replicaIdentityColumns: ['id'],
              columns: {
                ['id']: {
                  pos: 1,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: true,
                  dflt: null,
                },
                ['az']: {
                  pos: 2,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
                ['bz']: {
                  pos: 3,
                  dataType: 'int4',
                  typeOID: 23,
                  pgTypeClass: PostgresTypeClass.Base,
                  characterMaximumLength: null,
                  notNull: false,
                  dflt: null,
                },
              },
              primaryKey: ['id'],
              publications: {['zero_data']: {rowFilter: null}},
            },
          ],
        },
      )
    ).toMatchInlineSnapshot(`
      "[
        {
          "schema": "test",
          "tableName": "foo",
          "name": "foo_a_b",
          "unique": false,
          "isPrimaryKey": false,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "az": "ASC",
            "bz": "ASC"
          }
        },
        {
          "schema": "test",
          "tableName": "foo",
          "name": "foo_b_a",
          "unique": false,
          "isPrimaryKey": false,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "bz": "DESC",
            "az": "DESC"
          }
        },
        {
          "schema": "test",
          "tableName": "foo",
          "name": "foo_pkey",
          "unique": true,
          "isPrimaryKey": true,
          "isReplicaIdentity": false,
          "isImmediate": true,
          "columns": {
            "id": "ASC"
          }
        }
      ]"
    `);
  });
});
