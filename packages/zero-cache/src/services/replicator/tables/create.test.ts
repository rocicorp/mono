import {Database} from 'better-sqlite3';
import type postgres from 'postgres';
import {afterAll, afterEach, beforeEach, describe, expect, test} from 'vitest';
import {DbFile} from 'zero-cache/src/test/lite.js';
import {stripCommentsAndWhitespace} from 'zero-cache/src/zql/query-test-util.js';
import {testDBs} from '../../../test/db.js';
import {createTableStatementIgnoringNotNullConstraint} from './create.js';
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
            columnDefault: null,
            notNull: true,
          },
          lastMutationID: {
            dataType: 'int8',
            characterMaximumLength: null,
            columnDefault: null,
            notNull: true,
          },
        },
        primaryKey: ['clientID'],
      },
      createStatement: `
      CREATE TABLE "public"."clients" (
        "clientID" varchar(180),
        "lastMutationID" int8,
        PRIMARY KEY ("clientID")
      );`,
      dstTableSpec: {
        schema: 'public',
        name: 'clients',
        columns: {
          clientID: {
            dataType: 'varchar',
            characterMaximumLength: 180,
            columnDefault: null,
            notNull: true, // NOT NULL by virtue of being a PRIMARY KEY
          },
          lastMutationID: {
            dataType: 'int8',
            characterMaximumLength: null,
            columnDefault: null,
            notNull: false, // NOT NULL constraint is ignored
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
            columnDefault: null,
            notNull: false,
          },
          lastMutationID: {
            dataType: 'int8',
            characterMaximumLength: null,
            columnDefault: null,
            notNull: false, // NOT NULL constraint is ignored
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
            columnDefault: null,
            notNull: true,
          },
          lastMutationID: {
            dataType: 'int8',
            characterMaximumLength: null,
            columnDefault: null,
            notNull: true,
          },
        },
        primaryKey: ['clientID'],
      },
      createStatement: `
      CREATE TABLE "zero.clients" (
        "clientID" varchar(180),
        "lastMutationID" int8,
        PRIMARY KEY ("clientID")
      );`,
      dstTableSpec: {
        schema: 'public',
        name: 'zero.clients',
        columns: {
          clientID: {
            dataType: 'varchar',
            characterMaximumLength: 180,
            columnDefault: null,
            notNull: true, // NOT NULL by virtue of being a PRIMARY KEY
          },
          lastMutationID: {
            dataType: 'int8',
            characterMaximumLength: null,
            columnDefault: null,
            notNull: false, // NOT NULL constraint is ignored
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
            columnDefault: null,
            notNull: false,
          },
          lastMutationID: {
            dataType: 'int8',
            characterMaximumLength: null,
            columnDefault: null,
            notNull: false, // NOT NULL constraint is ignored
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
            columnDefault: null,
            notNull: true,
          },
          handle: {
            characterMaximumLength: 40,
            columnDefault: "'@foo'",
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
            columnDefault: "'{1,2,3}'",
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
      createStatement: `
      CREATE TABLE "public"."users" (
         "user_id" int4,
         "handle" varchar(40) DEFAULT '@foo',
         "address" text[],
         "timez" timestamptz[],
         "bigint_array" int8[],
         "bool_array" bool[],
         "real_array" float4[],
         "int_array" int4[] DEFAULT '{1,2,3}',
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
            columnDefault: null,
            notNull: true,
          },
          handle: {
            characterMaximumLength: 40,
            columnDefault: "'@foo'::character varying",
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
      liteTableSpec: {
        schema: '',
        name: 'users',
        columns: {
          ['user_id']: {
            dataType: 'int4',
            characterMaximumLength: null,
            columnDefault: null,
            notNull: false,
          },
          handle: {
            characterMaximumLength: null,
            columnDefault: "'@foo'",
            dataType: 'varchar(40)',
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
            columnDefault: "'{1,2,3}'",
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
        const createStatement = createTableStatementIgnoringNotNullConstraint(
          c.srcTableSpec,
        );
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
    let dbFile: DbFile;
    let db: Database;

    beforeEach(() => {
      dbFile = new DbFile('create-tables');
      db = dbFile.connect();
    });

    afterEach(async () => {
      await dbFile.unlink();
    });

    for (const c of cases) {
      test(c.name, async () => {
        db.exec(
          createTableStatementIgnoringNotNullConstraint({
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
