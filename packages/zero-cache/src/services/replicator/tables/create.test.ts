import Database from 'better-sqlite3';
import type postgres from 'postgres';
import {afterAll, afterEach, beforeEach, describe, expect, test} from 'vitest';
import {stripCommentsAndWhitespace} from 'zero-cache/src/db/query-test-util.js';
import {testDBs} from '../../../test/db.js';
import {createTableStatement} from './create.js';
import {listTables} from './list.js';
import {getPublicationInfo} from './published.js';
import type {TableSpec} from './specs.js';

describe('tables/create', () => {
  type Case = {
    name: string;
    srcTableSpec: TableSpec;
    createStatement: string;
    liteTableSpec: TableSpec;
    dstTableSpec?: TableSpec;
  };

  const cases: Case[] = [
    {
      name: 'zero clients',
      srcTableSpec: {
        schema: 'public',
        name: 'clients',
        columns: {
          clientID: {
            dataType: 'varchar',
            characterMaximumLength: 180,
            notNull: true,
          },
          lastMutationID: {
            dataType: 'int8',
            characterMaximumLength: null,
            notNull: true,
          },
        },
        primaryKey: ['clientID'],
      },
      createStatement: `
      CREATE TABLE "public"."clients" (
        "clientID" varchar(180) NOT NULL,
        "lastMutationID" int8 NOT NULL,
        PRIMARY KEY ("clientID")
      );`,
      dstTableSpec: {
        schema: 'public',
        name: 'clients',
        columns: {
          clientID: {
            dataType: 'varchar',
            characterMaximumLength: 180,
            notNull: true,
          },
          lastMutationID: {
            dataType: 'int8',
            characterMaximumLength: null,
            notNull: true,
          },
        },
        primaryKey: ['clientID'],
      },
      liteTableSpec: {
        schema: '',
        name: 'clients',
        columns: {
          clientID: {
            dataType: 'varchar(180)',
            characterMaximumLength: null,
            notNull: true,
          },
          lastMutationID: {
            dataType: 'int8',
            characterMaximumLength: null,
            notNull: true,
          },
        },
        primaryKey: ['clientID'],
      },
    },
    {
      name: 'table name with dot',
      srcTableSpec: {
        schema: '',
        name: 'zero.clients',
        columns: {
          clientID: {
            dataType: 'varchar',
            characterMaximumLength: 180,
            notNull: true,
          },
          lastMutationID: {
            dataType: 'int8',
            characterMaximumLength: null,
            notNull: true,
          },
        },
        primaryKey: ['clientID'],
      },
      createStatement: `
      CREATE TABLE "zero.clients" (
        "clientID" varchar(180) NOT NULL,
        "lastMutationID" int8 NOT NULL,
        PRIMARY KEY ("clientID")
      );`,
      dstTableSpec: {
        schema: 'public',
        name: 'zero.clients',
        columns: {
          clientID: {
            dataType: 'varchar',
            characterMaximumLength: 180,
            notNull: true,
          },
          lastMutationID: {
            dataType: 'int8',
            characterMaximumLength: null,
            notNull: true,
          },
        },
        primaryKey: ['clientID'],
      },
      liteTableSpec: {
        schema: '',
        name: 'zero.clients',
        columns: {
          clientID: {
            dataType: 'varchar(180)',
            characterMaximumLength: null,
            notNull: true,
          },
          lastMutationID: {
            dataType: 'int8',
            characterMaximumLength: null,
            notNull: true,
          },
        },
        primaryKey: ['clientID'],
      },
    },
    {
      name: 'types and array types and defaults',
      srcTableSpec: {
        schema: 'public',
        name: 'users',
        columns: {
          ['user_id']: {
            dataType: 'int4',
            characterMaximumLength: null,
            notNull: true,
          },
          handle: {
            characterMaximumLength: 40,
            dataType: 'varchar',
            notNull: false,
          },
          address: {
            characterMaximumLength: null,
            dataType: 'text[]',
            notNull: false,
          },
          ['timez']: {
            dataType: 'timestamptz[]',
            characterMaximumLength: null,
            notNull: false,
          },
          ['bigint_array']: {
            characterMaximumLength: null,
            dataType: 'int8[]',
            notNull: false,
          },
          ['bool_array']: {
            characterMaximumLength: null,
            dataType: 'bool[]',
            notNull: false,
          },
          ['real_array']: {
            characterMaximumLength: null,
            dataType: 'float4[]',
            notNull: false,
          },
          ['int_array']: {
            dataType: 'int4[]',
            characterMaximumLength: null,
            notNull: false,
          },
          ['json_val']: {
            dataType: 'jsonb',
            characterMaximumLength: null,
            notNull: false,
          },
        },
        primaryKey: ['user_id'],
      },
      createStatement: `
      CREATE TABLE "public"."users" (
         "user_id" int4 NOT NULL,
         "handle" varchar(40),
         "address" text[],
         "timez" timestamptz[],
         "bigint_array" int8[],
         "bool_array" bool[],
         "real_array" float4[],
         "int_array" int4[],
         "json_val" jsonb,
         PRIMARY KEY ("user_id")
      );`,
      dstTableSpec: {
        schema: 'public',
        name: 'users',
        columns: {
          ['user_id']: {
            dataType: 'int4',
            characterMaximumLength: null,
            notNull: true,
          },
          handle: {
            characterMaximumLength: 40,
            dataType: 'varchar',
            notNull: false,
          },
          address: {
            characterMaximumLength: null,
            dataType: 'text[]',
            notNull: false,
          },
          ['timez']: {
            dataType: 'timestamptz[]',
            characterMaximumLength: null,
            notNull: false,
          },
          ['bigint_array']: {
            characterMaximumLength: null,
            dataType: 'int8[]',
            notNull: false,
          },
          ['bool_array']: {
            characterMaximumLength: null,
            dataType: 'bool[]',
            notNull: false,
          },
          ['real_array']: {
            characterMaximumLength: null,
            dataType: 'float4[]',
            notNull: false,
          },
          ['int_array']: {
            dataType: 'int4[]',
            characterMaximumLength: null,
            notNull: false,
          },
          ['json_val']: {
            dataType: 'jsonb',
            characterMaximumLength: null,
            notNull: false,
          },
        },
        primaryKey: ['user_id'],
      },
      liteTableSpec: {
        schema: '',
        name: 'users',
        columns: {
          ['user_id']: {
            dataType: 'int4',
            characterMaximumLength: null,
            notNull: true,
          },
          handle: {
            characterMaximumLength: null,
            dataType: 'varchar(40)',
            notNull: false,
          },
          address: {
            characterMaximumLength: null,
            dataType: 'text[]',
            notNull: false,
          },
          ['timez']: {
            dataType: 'timestamptz[]',
            characterMaximumLength: null,
            notNull: false,
          },
          ['bigint_array']: {
            characterMaximumLength: null,
            dataType: 'int8[]',
            notNull: false,
          },
          ['bool_array']: {
            characterMaximumLength: null,
            dataType: 'bool[]',
            notNull: false,
          },
          ['real_array']: {
            characterMaximumLength: null,
            dataType: 'float4[]',
            notNull: false,
          },
          ['int_array']: {
            dataType: 'int4[]',
            characterMaximumLength: null,
            notNull: false,
          },
          ['json_val']: {
            dataType: 'jsonb',
            characterMaximumLength: null,
            notNull: false,
          },
        },
        primaryKey: ['user_id'],
      },
    },
  ];

  describe('pg', () => {
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
        const createStatement = createTableStatement(c.srcTableSpec);
        expect(stripCommentsAndWhitespace(createStatement)).toBe(
          stripCommentsAndWhitespace(c.createStatement),
        );
        await db.unsafe(createStatement);

        const published = await getPublicationInfo(db);
        expect(published.tables).toEqual(
          expect.arrayContaining([
            {...(c.dstTableSpec ?? c.srcTableSpec), filterConditions: []},
          ]),
        );
      });
    }
  });

  describe('sqlite', () => {
    let db: Database.Database;

    beforeEach(() => {
      db = new Database(':memory:');
    });

    for (const c of cases) {
      test(c.name, async () => {
        db.exec(
          createTableStatement({
            ...c.srcTableSpec,
            schema: '',
          }),
        );

        const tables = await listTables(db);
        expect(tables).toEqual(expect.arrayContaining([c.liteTableSpec]));
      });
    }
  });
});
