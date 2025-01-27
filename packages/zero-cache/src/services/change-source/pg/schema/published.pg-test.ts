import type postgres from 'postgres';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {assert} from '../../../../../../shared/src/asserts.ts';
import * as PostgresTypeClass from '../../../../db/postgres-type-class-enum.ts';
import {testDBs} from '../../../../test/db.ts';
import {type PublicationInfo, getPublicationInfo} from './published.ts';

describe('tables/published', () => {
  type Case = {
    name: string;
    setupQuery: string;
    expectedResult?: PublicationInfo;
    expectedError?: string;
  };

  const cases: Case[] = [
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
            oid: expect.any(Number),
            schema: 'zero',
            name: 'clients',
            replicaIdentity: 'd',
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
        indexes: [
          {
            name: 'clients_pkey',
            schema: 'zero',
            tableName: 'clients',
            columns: {clientID: 'ASC'},
            unique: true,
          },
        ],
      },
    },
    {
      name: 'types and array types',
      setupQuery: `
      CREATE TYPE DAY_OF_WEEK AS ENUM ('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun');
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
        json_val JSONB,
        day DAY_OF_WEEK
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
            oid: expect.any(Number),
            schema: 'test',
            name: 'users',
            replicaIdentity: 'd',
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
                dflt: '2147483647',
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
        indexes: [
          {
            schema: 'test',
            tableName: 'users',
            name: 'users_pkey',
            columns: {['user_id']: 'ASC'},
            unique: true,
            isReplicaIdentity: false,
            isImmediate: true,
          },
        ],
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
            oid: expect.any(Number),
            schema: 'test',
            name: 'users',
            replicaIdentity: 'd',
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
        indexes: [
          {
            schema: 'test',
            tableName: 'users',
            name: 'users_pkey',
            columns: {['user_id']: 'ASC'},
            unique: true,
            isReplicaIdentity: false,
            isImmediate: true,
          },
        ],
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
            oid: expect.any(Number),
            schema: 'test',
            name: 'users',
            replicaIdentity: 'd',
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
        indexes: [
          {
            schema: 'test',
            tableName: 'users',
            name: 'users_pkey',
            columns: {['user_id']: 'ASC'},
            unique: true,
            isReplicaIdentity: false,
            isImmediate: true,
          },
        ],
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
            oid: expect.any(Number),
            schema: 'test',
            name: 'users',
            replicaIdentity: 'd',
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
        indexes: [
          {
            schema: 'test',
            tableName: 'users',
            name: 'users_pkey',
            columns: {['user_id']: 'ASC'},
            unique: true,
            isReplicaIdentity: false,
            isImmediate: true,
          },
        ],
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
            oid: expect.any(Number),
            schema: 'test',
            name: 'users',
            replicaIdentity: 'd',
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
        indexes: [
          {
            schema: 'test',
            tableName: 'users',
            name: 'users_pkey',
            columns: {['user_id']: 'ASC'},
            unique: true,
            isReplicaIdentity: false,
            isImmediate: true,
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
            oid: expect.any(Number),
            schema: 'test',
            name: 'issues',
            replicaIdentity: 'd',
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
        indexes: [
          {
            schema: 'test',
            tableName: 'issues',
            name: 'issues_pkey',
            columns: {['issue_id']: 'ASC'},
            unique: true,
            isReplicaIdentity: false,
            isImmediate: true,
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
            oid: expect.any(Number),
            schema: 'test',
            name: 'issues',
            replicaIdentity: 'd',
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
            name: 'users',
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
            name: 'clients',
            replicaIdentity: 'd',
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
        indexes: [
          {
            schema: 'test',
            tableName: 'issues',
            name: 'issues_pkey',
            columns: {
              ['component_id']: 'ASC',
              ['issue_id']: 'ASC',
              ['org_id']: 'ASC',
            },
            unique: true,
            isReplicaIdentity: false,
            isImmediate: true,
          },
          {
            schema: 'test',
            tableName: 'users',
            name: 'users_pkey',
            columns: {['user_id']: 'ASC'},
            unique: true,
            isReplicaIdentity: false,
            isImmediate: true,
          },
          {
            schema: 'zero',
            tableName: 'clients',
            name: 'clients_pkey',
            columns: {clientID: 'ASC'},
            unique: true,
            isReplicaIdentity: false,
            isImmediate: true,
          },
        ],
      },
    },
    {
      name: 'indexes',
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
      CREATE PUBLICATION zero_two FOR TABLE test.issues;
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
            name: 'issues',
            replicaIdentity: 'd',
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
        indexes: [
          {
            schema: 'test',
            tableName: 'issues',
            name: 'issues_component_id',
            columns: {['component_id']: 'ASC'},
            unique: false,
            isReplicaIdentity: false,
            isImmediate: true,
          },
          {
            schema: 'test',
            tableName: 'issues',
            name: 'issues_org_id',
            columns: {['org_id']: 'ASC'},
            unique: false,
            isReplicaIdentity: false,
            isImmediate: true,
          },
          {
            schema: 'test',
            tableName: 'issues',
            name: 'issues_pkey',
            columns: {['issue_id']: 'ASC'},
            unique: true,
            isReplicaIdentity: false,
            isImmediate: true,
          },
        ],
      },
    },
    {
      name: 'unique indexes',
      setupQuery: `
      CREATE SCHEMA test;
      CREATE TABLE test.issues (
        issue_id INTEGER PRIMARY KEY,
        org_id INTEGER UNIQUE,
        component_id INTEGER
      );
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
            oid: expect.any(Number),
            schema: 'test',
            name: 'issues',
            replicaIdentity: 'd',
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
        indexes: [
          {
            schema: 'test',
            tableName: 'issues',
            name: 'issues_component_id',
            columns: {['component_id']: 'ASC'},
            unique: true,
            isReplicaIdentity: false,
            isImmediate: true,
          },
          {
            schema: 'test',
            tableName: 'issues',
            name: 'issues_org_id_key',
            columns: {['org_id']: 'ASC'},
            unique: true,
            isReplicaIdentity: false,
            isImmediate: true,
          },
          {
            schema: 'test',
            tableName: 'issues',
            name: 'issues_pkey',
            columns: {['issue_id']: 'ASC'},
            unique: true,
            isReplicaIdentity: false,
            isImmediate: true,
          },
        ],
      },
    },
    {
      name: 'replica identity index',
      setupQuery: `
      CREATE SCHEMA test;
      CREATE TABLE test.issues (
        issue_id INTEGER NOT NULL,
        org_id INTEGER NOT NULL,
        component_id INTEGER
      );
      CREATE UNIQUE INDEX issues_key_idx ON test.issues (org_id, issue_id);
      ALTER TABLE test.issues REPLICA IDENTITY USING INDEX issues_key_idx;
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
            oid: expect.any(Number),
            schema: 'test',
            name: 'issues',
            replicaIdentity: 'i',
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
        indexes: [
          {
            schema: 'test',
            tableName: 'issues',
            name: 'issues_key_idx',
            columns: {
              ['org_id']: 'ASC',
              ['issue_id']: 'ASC',
            },
            unique: true,
            isReplicaIdentity: true,
            isImmediate: true,
          },
        ],
      },
    },
    {
      name: 'compound indexes',
      setupQuery: `
      CREATE SCHEMA test;
      CREATE TABLE test.foo (
        id INTEGER PRIMARY KEY,
        a INTEGER,
        b INTEGER
      );
      CREATE INDEX foo_a_b ON test.foo (a ASC, b DESC);
      CREATE INDEX foo_b_a ON test.foo (b DESC, a DESC);
      CREATE PUBLICATION zero_data FOR TABLE test.foo;
      CREATE PUBLICATION zero_two FOR TABLE test.foo;
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
            name: 'foo',
            replicaIdentity: 'd',
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
        indexes: [
          {
            schema: 'test',
            tableName: 'foo',
            name: 'foo_a_b',
            columns: {
              a: 'ASC',
              b: 'DESC',
            },
            unique: false,
            isReplicaIdentity: false,
            isImmediate: true,
          },
          {
            schema: 'test',
            tableName: 'foo',
            name: 'foo_b_a',
            columns: {
              b: 'DESC',
              a: 'DESC',
            },
            unique: false,
            isReplicaIdentity: false,
            isImmediate: true,
          },
          {
            schema: 'test',
            tableName: 'foo',
            name: 'foo_pkey',
            columns: {id: 'ASC'},
            unique: true,
            isReplicaIdentity: false,
            isImmediate: true,
          },
        ],
      },
    },
    {
      name: 'ignores irrelevant indexes',
      setupQuery: `
      CREATE SCHEMA test;
      CREATE TABLE test.issues (
        issue_id INTEGER PRIMARY KEY,
        org_id INTEGER CHECK (org_id > 0),
        component_id INTEGER
      );
      CREATE INDEX idx_with_expression ON test.issues (org_id, (component_id + 1));
      CREATE INDEX partial_idx ON test.issues (component_id) WHERE org_id > 1000;
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
            oid: expect.any(Number),
            schema: 'test',
            name: 'issues',
            replicaIdentity: 'd',
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
        indexes: [
          {
            schema: 'test',
            tableName: 'issues',
            name: 'issues_pkey',
            columns: {['issue_id']: 'ASC'},
            unique: true,
            isReplicaIdentity: false,
            isImmediate: true,
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
      CREATE INDEX foo_b_a ON test.foo (b DESC, a DESC);
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
            oid: expect.any(Number),
            schema: 'test',
            name: 'foo',
            replicaIdentity: 'd',
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
        indexes: [
          {
            schema: 'test',
            tableName: 'foo',
            name: 'foo_a_b',
            columns: {
              az: 'ASC',
              bz: 'ASC',
            },
            unique: false,
            isReplicaIdentity: false,
            isImmediate: true,
          },
          {
            schema: 'test',
            tableName: 'foo',
            name: 'foo_b_a',
            columns: {
              bz: 'DESC',
              az: 'DESC',
            },
            unique: false,
            isReplicaIdentity: false,
            isImmediate: true,
          },
          {
            schema: 'test',
            tableName: 'foo',
            name: 'foo_pkey',
            columns: {id: 'ASC'},
            unique: true,
            isReplicaIdentity: false,
            isImmediate: true,
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

      try {
        const tables = await getPublicationInfo(
          db,
          c.expectedResult
            ? c.expectedResult.publications.map(p => p.pubname)
            : [
                'zero_all',
                'zero_data',
                'zero_one',
                'zero_two',
                'zero_keys',
                '_zero_meta',
                'zero_tables',
              ],
        );
        assert(c.expectedResult);
        expect(tables).toMatchObject(c.expectedResult);
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
