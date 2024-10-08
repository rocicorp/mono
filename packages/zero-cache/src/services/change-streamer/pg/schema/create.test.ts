import type postgres from 'postgres';
import {createSilentLogContext} from '../../../../../../shared/src/logging-test-utils.js';
import {afterAll, afterEach, beforeEach, describe, expect, test} from 'vitest';
import {listTables} from '../../../../db/lite-tables.js';
import {stripCommentsAndWhitespace} from '../../../../db/query-test-util.js';
import {testDBs} from '../../../../test/db.js';
import type {TableSpec} from '../../../../types/specs.js';
import {Database} from '../../../../../../zqlite/src/db.js';
import {createTableStatement} from './create.js';
import {mapPostgresToLite} from './lite.js';
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
        schema: '',
        name: 'clients',
        columns: {
          clientID: {
            pos: 1,
            dataType: 'TEXT',
            characterMaximumLength: null,
            notNull: true,
            dflt: null,
          },
          lastMutationID: {
            pos: 2,
            dataType: 'INTEGER',
            characterMaximumLength: null,
            notNull: true,
            dflt: null,
          },
          ['_0_version']: {
            pos: 3,
            dataType: 'TEXT',
            characterMaximumLength: null,
            dflt: null,
            notNull: true,
          },
        },
        primaryKey: ['clientID'],
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
        schema: '',
        name: 'zero.clients',
        columns: {
          clientID: {
            pos: 1,
            dataType: 'TEXT',
            characterMaximumLength: null,
            notNull: true,
            dflt: null,
          },
          lastMutationID: {
            pos: 2,
            dataType: 'INTEGER',
            characterMaximumLength: null,
            notNull: true,
            dflt: null,
          },
          ['_0_version']: {
            characterMaximumLength: null,
            dataType: 'TEXT',
            dflt: null,
            notNull: true,
            pos: 3,
          },
        },
        primaryKey: ['clientID'],
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
        },
        primaryKey: ['user_id'],
      },
      createStatement: `
      CREATE TABLE "public"."users" (
         "user_id" int4 NOT NULL,
         "handle" varchar(40),
         "rank" int8,
         "admin" bool,
         "bigint" int8,
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
        },
        primaryKey: ['user_id'],
      },
      liteTableSpec: {
        schema: '',
        name: 'users',
        columns: {
          ['user_id']: {
            pos: 1,
            dataType: 'INTEGER',
            characterMaximumLength: null,
            notNull: true,
            dflt: null,
          },
          handle: {
            pos: 2,
            characterMaximumLength: null,
            dataType: 'TEXT',
            notNull: false,
            dflt: null,
          },
          rank: {
            pos: 3,
            characterMaximumLength: null,
            dataType: 'INTEGER',
            notNull: false,
            dflt: null,
          },
          admin: {
            pos: 4,
            dataType: 'BOOL',
            characterMaximumLength: null,
            notNull: false,
            dflt: null,
          },
          bigint: {
            pos: 5,
            characterMaximumLength: null,
            dataType: 'INTEGER',
            notNull: false,
            dflt: null,
          },
          ['_0_version']: {
            characterMaximumLength: null,
            dataType: 'TEXT',
            dflt: null,
            notNull: true,
            pos: 6,
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
      test(c.name, () => {
        const liteTableSpec = mapPostgresToLite(c.srcTableSpec);
        db.exec(createTableStatement(liteTableSpec));

        const tables = listTables(db);
        expect(tables).toEqual(expect.arrayContaining([c.liteTableSpec]));
      });
    }
  });
});
