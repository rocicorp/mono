import type postgres from 'postgres';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {afterAll, afterEach, beforeEach, describe, expect, test} from 'vitest';
import {listTables} from 'zero-cache/src/db/lite-tables.js';
import {stripCommentsAndWhitespace} from 'zero-cache/src/db/query-test-util.js';
import {testDBs} from 'zero-cache/src/test/db.js';
import type {TableSpec} from 'zero-cache/src/types/specs.js';
import {Database} from 'zqlite/src/db.js';
import {createTableStatement} from './create.js';
import {getPublicationInfo} from './published.js';

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
            pos: 1,
            dataType: 'varchar',
            characterMaximumLength: 180,
            notNull: true,
          },
          lastMutationID: {
            pos: 2,
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
            pos: 1,
            dataType: 'varchar',
            characterMaximumLength: 180,
            notNull: true,
          },
          lastMutationID: {
            pos: 2,
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
            pos: 1,
            dataType: 'varchar(180)',
            characterMaximumLength: null,
            notNull: true,
          },
          lastMutationID: {
            pos: 2,
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
            pos: 1,
            dataType: 'varchar',
            characterMaximumLength: 180,
            notNull: true,
          },
          lastMutationID: {
            pos: 2,
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
            pos: 1,
            dataType: 'varchar',
            characterMaximumLength: 180,
            notNull: true,
          },
          lastMutationID: {
            pos: 2,
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
            pos: 1,
            dataType: 'varchar(180)',
            characterMaximumLength: null,
            notNull: true,
          },
          lastMutationID: {
            pos: 2,
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
            pos: 1,
            dataType: 'int4',
            characterMaximumLength: null,
            notNull: true,
          },
          handle: {
            pos: 2,
            characterMaximumLength: 40,
            dataType: 'varchar',
            notNull: false,
          },
          address: {
            pos: 3,
            characterMaximumLength: null,
            dataType: 'text[]',
            notNull: false,
          },
          ['timez']: {
            pos: 4,
            dataType: 'timestamptz[]',
            characterMaximumLength: null,
            notNull: false,
          },
          ['bigint_array']: {
            pos: 5,
            characterMaximumLength: null,
            dataType: 'int8[]',
            notNull: false,
          },
          ['bool_array']: {
            pos: 6,
            characterMaximumLength: null,
            dataType: 'bool[]',
            notNull: false,
          },
          ['real_array']: {
            pos: 7,
            characterMaximumLength: null,
            dataType: 'float4[]',
            notNull: false,
          },
          ['int_array']: {
            pos: 8,
            dataType: 'int4[]',
            characterMaximumLength: null,
            notNull: false,
          },
          ['json_val']: {
            pos: 9,
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
            pos: 1,
            dataType: 'int4',
            characterMaximumLength: null,
            notNull: true,
          },
          handle: {
            pos: 2,
            characterMaximumLength: 40,
            dataType: 'varchar',
            notNull: false,
          },
          address: {
            pos: 3,
            characterMaximumLength: null,
            dataType: 'text[]',
            notNull: false,
          },
          ['timez']: {
            pos: 4,
            dataType: 'timestamptz[]',
            characterMaximumLength: null,
            notNull: false,
          },
          ['bigint_array']: {
            pos: 5,
            characterMaximumLength: null,
            dataType: 'int8[]',
            notNull: false,
          },
          ['bool_array']: {
            pos: 6,
            characterMaximumLength: null,
            dataType: 'bool[]',
            notNull: false,
          },
          ['real_array']: {
            pos: 7,
            characterMaximumLength: null,
            dataType: 'float4[]',
            notNull: false,
          },
          ['int_array']: {
            pos: 8,
            dataType: 'int4[]',
            characterMaximumLength: null,
            notNull: false,
          },
          ['json_val']: {
            pos: 9,
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
            pos: 1,
            dataType: 'int4',
            characterMaximumLength: null,
            notNull: true,
          },
          handle: {
            pos: 2,
            characterMaximumLength: null,
            dataType: 'varchar(40)',
            notNull: false,
          },
          address: {
            pos: 3,
            characterMaximumLength: null,
            dataType: 'text[]',
            notNull: false,
          },
          ['timez']: {
            pos: 4,
            dataType: 'timestamptz[]',
            characterMaximumLength: null,
            notNull: false,
          },
          ['bigint_array']: {
            pos: 5,
            characterMaximumLength: null,
            dataType: 'int8[]',
            notNull: false,
          },
          ['bool_array']: {
            pos: 6,
            characterMaximumLength: null,
            dataType: 'bool[]',
            notNull: false,
          },
          ['real_array']: {
            pos: 7,
            characterMaximumLength: null,
            dataType: 'float4[]',
            notNull: false,
          },
          ['int_array']: {
            pos: 8,
            dataType: 'int4[]',
            characterMaximumLength: null,
            notNull: false,
          },
          ['json_val']: {
            pos: 9,
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
        expect(published.tables).toEqual([
          {
            ...(c.dstTableSpec ?? c.srcTableSpec),
            publications: {['zero_all']: {rowFilter: null}},
          },
        ]);
      });
    }
  });

  describe('sqlite', () => {
    let db: Database;

    beforeEach(() => {
      db = new Database(createSilentLogContext(), ':memory:');
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
