import type postgres from 'postgres';
import {afterAll, afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.js';
import {Database} from '../../../zqlite/src/db.js';
import {getPublicationInfo} from '../services/change-source/pg/schema/published.js';
import {testDBs} from '../test/db.js';
import {createTableStatement} from './create.js';
import {listTables} from './lite-tables.js';
import {mapPostgresToLite} from './pg-to-lite.js';
import * as PostgresTypeClass from './postgres-type-class-enum.js';
import {stripCommentsAndWhitespace} from './query-test-util.js';
import {type LiteTableSpec, type TableSpec} from './specs.js';

describe('tables/create', () => {
  type Case = {
    name: string;
    srcTableSpec: TableSpec;
    createStatement: string;
    liteTableSpec: LiteTableSpec;
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
            dflt: null,
          },
          lastMutationID: {
            pos: 2,
            dataType: 'int8',
            characterMaximumLength: null,
            notNull: true,
            dflt: null,
          },
        },
        primaryKey: ['clientID'],
      },
      createStatement: `
      CREATE TABLE "public"."clients" (
        "clientID" "varchar"(180) NOT NULL,
        "lastMutationID" "int8" NOT NULL,
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
            dflt: null,
          },
          lastMutationID: {
            pos: 2,
            dataType: 'int8',
            characterMaximumLength: null,
            notNull: true,
            dflt: null,
          },
        },
        primaryKey: ['clientID'],
      },
      liteTableSpec: {
        name: 'clients',
        columns: {
          clientID: {
            pos: 1,
            dataType: 'varchar|NOT_NULL',
            characterMaximumLength: null,
            notNull: false,
            dflt: null,
          },
          lastMutationID: {
            pos: 2,
            dataType: 'int8|NOT_NULL',
            characterMaximumLength: null,
            notNull: false,
            dflt: null,
          },
          ['_0_version']: {
            pos: 3,
            dataType: 'TEXT',
            characterMaximumLength: null,
            dflt: null,
            notNull: false,
          },
        },
      },
    },
    {
      name: 'table name with dot',
      srcTableSpec: {
        schema: 'public',
        name: 'zero.clients',
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
            notNull: true,
            dflt: null,
          },
        },
        primaryKey: ['clientID'],
      },
      createStatement: `
      CREATE TABLE "public"."zero.clients" (
        "clientID" "varchar"(180) NOT NULL,
        "lastMutationID" "int8" NOT NULL,
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
            dflt: null,
          },
          lastMutationID: {
            pos: 2,
            dataType: 'int8',
            characterMaximumLength: null,
            notNull: true,
            dflt: null,
          },
        },
        primaryKey: ['clientID'],
      },
      liteTableSpec: {
        name: 'zero.clients',
        columns: {
          clientID: {
            pos: 1,
            dataType: 'varchar|NOT_NULL',
            characterMaximumLength: null,
            notNull: false,
            dflt: null,
          },
          lastMutationID: {
            pos: 2,
            dataType: 'int8|NOT_NULL',
            characterMaximumLength: null,
            notNull: false,
            dflt: null,
          },
          ['_0_version']: {
            characterMaximumLength: null,
            dataType: 'TEXT',
            dflt: null,
            notNull: false,
            pos: 3,
          },
        },
      },
    },
    {
      name: 'types and defaults',
      srcTableSpec: {
        schema: 'public',
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
            characterMaximumLength: 40,
            dataType: 'varchar',
            notNull: false,
            dflt: null,
          },
          rank: {
            pos: 3,
            characterMaximumLength: null,
            dataType: 'int8',
            notNull: false,
            dflt: '1',
          },
          admin: {
            pos: 4,
            dataType: 'bool',
            characterMaximumLength: null,
            notNull: false,
            dflt: 'false',
          },
          bigint: {
            pos: 5,
            characterMaximumLength: null,
            dataType: 'int8',
            notNull: false,
            dflt: "'2147483648'::bigint",
          },
          enumnum: {
            pos: 6,
            characterMaximumLength: null,
            dataType: 'my_type',
            pgTypeClass: PostgresTypeClass.Enum,
            notNull: false,
            dflt: null,
          },
        },
        primaryKey: ['user_id'],
      },
      createStatement: `
      CREATE TABLE "public"."users" (
         "user_id" "int4" NOT NULL,
         "handle" "varchar"(40),
         "rank" "int8" DEFAULT 1,
         "admin" "bool" DEFAULT false,
         "bigint" "int8" DEFAULT '2147483648'::bigint,
         "enumnum" "my_type",
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
            dflt: null,
          },
          handle: {
            pos: 2,
            characterMaximumLength: 40,
            dataType: 'varchar',
            notNull: false,
            dflt: null,
          },
          rank: {
            pos: 3,
            characterMaximumLength: null,
            dataType: 'int8',
            notNull: false,
            dflt: '1',
          },
          admin: {
            pos: 4,
            dataType: 'bool',
            characterMaximumLength: null,
            notNull: false,
            dflt: 'false',
          },
          bigint: {
            pos: 5,
            characterMaximumLength: null,
            dataType: 'int8',
            notNull: false,
            dflt: "'2147483648'::bigint",
          },
          enumnum: {
            pos: 6,
            characterMaximumLength: null,
            dataType: 'my_type',
            pgTypeClass: PostgresTypeClass.Enum,
            notNull: false,
            dflt: null,
          },
        },
        primaryKey: ['user_id'],
      },
      liteTableSpec: {
        name: 'users',
        columns: {
          ['user_id']: {
            pos: 1,
            dataType: 'int4|NOT_NULL',
            characterMaximumLength: null,
            notNull: false,
            dflt: null,
          },
          handle: {
            pos: 2,
            characterMaximumLength: null,
            dataType: 'varchar',
            notNull: false,
            dflt: null,
          },
          rank: {
            pos: 3,
            characterMaximumLength: null,
            dataType: 'int8',
            notNull: false,
            dflt: null,
          },
          admin: {
            pos: 4,
            dataType: 'bool',
            characterMaximumLength: null,
            notNull: false,
            dflt: null,
          },
          bigint: {
            pos: 5,
            characterMaximumLength: null,
            dataType: 'int8',
            notNull: false,
            dflt: null,
          },
          enumnum: {
            pos: 6,
            characterMaximumLength: null,
            dataType: 'my_type|TEXT_ENUM',
            notNull: false,
            dflt: null,
          },
          ['_0_version']: {
            characterMaximumLength: null,
            dataType: 'TEXT',
            dflt: null,
            notNull: false,
            pos: 7,
          },
        },
      },
    },
  ];

  describe('pg', () => {
    let db: postgres.Sql;
    beforeEach(async () => {
      db = await testDBs.create('create_tables_test');
      await db`
      CREATE PUBLICATION zero_all FOR ALL TABLES;
      CREATE TYPE my_type AS ENUM ('foo', 'bar', 'baz');
      `.simple();
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

        const published = await getPublicationInfo(db, ['zero_all']);
        expect(published.tables).toMatchObject([
          {
            ...(c.dstTableSpec ?? c.srcTableSpec),
            oid: expect.any(Number),
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
      test(c.name, () => {
        const liteTableSpec = mapPostgresToLite(c.srcTableSpec);
        db.exec(createTableStatement(liteTableSpec));

        const tables = listTables(db);
        expect(tables).toEqual(expect.arrayContaining([c.liteTableSpec]));
      });
    }
  });
});
