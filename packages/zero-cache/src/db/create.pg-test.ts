import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {test} from '../test/db.ts';
import {createLiteTableStatement, liteColumnDef} from './create.ts';
import {listTables} from './lite-tables.ts';
import {mapPostgresToLite} from './pg-to-lite.ts';
import * as PostgresTypeClass from './postgres-type-class-enum.ts';
import {type ColumnSpec, type LiteTableSpec, type TableSpec} from './specs.ts';

describe('tables/create', () => {
  type Case = {
    name: string;
    createStatement: string;
    liteTableSpec: LiteTableSpec;
    dstTableSpec: TableSpec;
  };

  const cases: Case[] = [
    {
      name: 'zero clients',
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
            elemPgTypeClass: null,
            dflt: null,
          },
          lastMutationID: {
            pos: 2,
            dataType: 'int8',
            characterMaximumLength: null,
            notNull: true,
            elemPgTypeClass: null,
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
            elemPgTypeClass: null,
            dflt: null,
          },
          lastMutationID: {
            pos: 2,
            dataType: 'int8|NOT_NULL',
            characterMaximumLength: null,
            notNull: false,
            elemPgTypeClass: null,
            dflt: null,
          },
          ['_0_version']: {
            pos: 3,
            dataType: 'TEXT',
            characterMaximumLength: null,
            dflt: null,
            notNull: false,
            elemPgTypeClass: null,
          },
        },
      },
    },
    {
      name: 'table name with dot',
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
            elemPgTypeClass: null,
            dflt: null,
          },
          lastMutationID: {
            pos: 2,
            dataType: 'int8',
            characterMaximumLength: null,
            notNull: true,
            elemPgTypeClass: null,
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
            elemPgTypeClass: null,
            dflt: null,
          },
          lastMutationID: {
            pos: 2,
            dataType: 'int8|NOT_NULL',
            characterMaximumLength: null,
            notNull: false,
            elemPgTypeClass: null,
            dflt: null,
          },
          ['_0_version']: {
            characterMaximumLength: null,
            dataType: 'TEXT',
            dflt: null,
            notNull: false,
            elemPgTypeClass: null,
            pos: 3,
          },
        },
      },
    },
    {
      name: 'types and defaults',
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
            elemPgTypeClass: null,
            dflt: null,
          },
          handle: {
            pos: 2,
            characterMaximumLength: 40,
            dataType: 'varchar',
            notNull: false,
            elemPgTypeClass: null,
            dflt: null,
          },
          rank: {
            pos: 3,
            characterMaximumLength: null,
            dataType: 'int8',
            notNull: false,
            elemPgTypeClass: null,
            dflt: '1',
          },
          admin: {
            pos: 4,
            dataType: 'bool',
            characterMaximumLength: null,
            notNull: false,
            elemPgTypeClass: null,
            dflt: 'false',
          },
          bigint: {
            pos: 5,
            characterMaximumLength: null,
            dataType: 'int8',
            notNull: false,
            elemPgTypeClass: null,
            dflt: "'2147483648'::bigint",
          },
          enumnum: {
            pos: 6,
            characterMaximumLength: null,
            dataType: 'my_type',
            pgTypeClass: PostgresTypeClass.Enum,
            notNull: false,
            elemPgTypeClass: null,
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
            elemPgTypeClass: null,
            dflt: null,
          },
          handle: {
            pos: 2,
            characterMaximumLength: null,
            dataType: 'varchar',
            notNull: false,
            elemPgTypeClass: null,
            dflt: null,
          },
          rank: {
            pos: 3,
            characterMaximumLength: null,
            dataType: 'int8',
            notNull: false,
            elemPgTypeClass: null,
            dflt: null,
          },
          admin: {
            pos: 4,
            dataType: 'bool',
            characterMaximumLength: null,
            notNull: false,
            elemPgTypeClass: null,
            dflt: null,
          },
          bigint: {
            pos: 5,
            characterMaximumLength: null,
            dataType: 'int8',
            notNull: false,
            elemPgTypeClass: null,
            dflt: null,
          },
          enumnum: {
            pos: 6,
            characterMaximumLength: null,
            dataType: 'my_type|TEXT_ENUM',
            notNull: false,
            elemPgTypeClass: null,
            dflt: null,
          },
          ['_0_version']: {
            characterMaximumLength: null,
            dataType: 'TEXT',
            dflt: null,
            notNull: false,
            elemPgTypeClass: null,
            pos: 7,
          },
        },
      },
    },
    {
      name: 'array types',
      createStatement: `
      CREATE TABLE "public"."array_table" (
        "id" "int4" NOT NULL,
        "tags" "varchar"[],
        "nums" "int4"[],
        "enums" "my_type"[],
        PRIMARY KEY ("id")
      );`,
      dstTableSpec: {
        schema: 'public',
        name: 'array_table',
        columns: {
          id: {
            pos: 1,
            dataType: 'int4',
            characterMaximumLength: null,
            notNull: true,
            elemPgTypeClass: null,
            dflt: null,
          },
          tags: {
            pos: 2,
            dataType: 'varchar[]',
            characterMaximumLength: null,
            notNull: false,
            elemPgTypeClass: 'b',
            dflt: null,
          },
          nums: {
            pos: 3,
            dataType: 'int4[]',
            characterMaximumLength: null,
            notNull: false,
            elemPgTypeClass: 'b',
            dflt: null,
          },
          enums: {
            pos: 4,
            dataType: 'my_type[]',
            characterMaximumLength: null,
            notNull: false,
            elemPgTypeClass: 'e',
            dflt: null,
          },
        },
        primaryKey: ['id'],
      },
      liteTableSpec: {
        name: 'array_table',
        columns: {
          id: {
            pos: 1,
            dataType: 'int4|NOT_NULL',
            characterMaximumLength: null,
            notNull: false,
            elemPgTypeClass: null,
            dflt: null,
          },
          tags: {
            pos: 2,
            dataType: 'varchar[]',
            characterMaximumLength: null,
            notNull: false,
            elemPgTypeClass: 'b',
            dflt: null,
          },
          nums: {
            pos: 3,
            dataType: 'int4[]',
            characterMaximumLength: null,
            notNull: false,
            elemPgTypeClass: 'b',
            dflt: null,
          },
          enums: {
            pos: 4,
            dataType: 'my_type[]|TEXT_ENUM',
            characterMaximumLength: null,
            notNull: false,
            elemPgTypeClass: 'e',
            dflt: null,
          },
          ['_0_version']: {
            pos: 5,
            dataType: 'TEXT',
            characterMaximumLength: null,
            dflt: null,
            notNull: false,
            elemPgTypeClass: null,
          },
        },
      },
    },
    {
      name: 'multi-dimensional array types',
      createStatement: `
      CREATE TABLE "public"."multidim_array_table" (
        "id" "int4" NOT NULL,
        "matrix" "int4"[][],
        "enum_matrix" "my_type"[][],
        "text_3d" "text"[][][],
        PRIMARY KEY ("id")
      );`,
      dstTableSpec: {
        schema: 'public',
        name: 'multidim_array_table',
        columns: {
          id: {
            pos: 1,
            dataType: 'int4',
            characterMaximumLength: null,
            notNull: true,
            elemPgTypeClass: null,
            dflt: null,
          },
          matrix: {
            pos: 2,
            dataType: 'int4[][]',
            characterMaximumLength: null,
            notNull: false,
            elemPgTypeClass: 'b',
            dflt: null,
          },
          enum_matrix: {
            pos: 3,
            dataType: 'my_type[][]',
            characterMaximumLength: null,
            notNull: false,
            elemPgTypeClass: 'e',
            dflt: null,
          },
          text_3d: {
            pos: 4,
            dataType: 'text[][][]',
            characterMaximumLength: null,
            notNull: false,
            elemPgTypeClass: 'b',
            dflt: null,
          },
        },
        primaryKey: ['id'],
      },
      liteTableSpec: {
        name: 'multidim_array_table',
        columns: {
          id: {
            pos: 1,
            dataType: 'int4|NOT_NULL',
            characterMaximumLength: null,
            notNull: false,
            elemPgTypeClass: null,
            dflt: null,
          },
          matrix: {
            pos: 2,
            dataType: 'int4[][]',
            characterMaximumLength: null,
            notNull: false,
            elemPgTypeClass: 'b',
            dflt: null,
          },
          enum_matrix: {
            pos: 3,
            dataType: 'my_type[][]|TEXT_ENUM',
            characterMaximumLength: null,
            notNull: false,
            elemPgTypeClass: 'e',
            dflt: null,
          },
          text_3d: {
            pos: 4,
            dataType: 'text[][][]',
            characterMaximumLength: null,
            notNull: false,
            elemPgTypeClass: 'b',
            dflt: null,
          },
          ['_0_version']: {
            pos: 5,
            dataType: 'TEXT',
            characterMaximumLength: null,
            dflt: null,
            notNull: false,
            elemPgTypeClass: null,
          },
        },
      },
    },
  ];

  describe('sqlite', () => {
    let db: Database;

    beforeEach(() => {
      db = new Database(createSilentLogContext(), ':memory:');
    });

    test.each(cases)('$name', c => {
      const liteTableSpec = mapPostgresToLite(c.dstTableSpec);
      db.exec(createLiteTableStatement(liteTableSpec));

      const tables = listTables(db);
      expect(tables).toEqual(expect.arrayContaining([c.liteTableSpec]));
    });
  });

  // Regression tests for array type SQL generation bug
  // Original issue: Legacy data with "text[]|TEXT_ARRAY" was generating malformed SQL like:
  //   SQLite: "text[]|TEXT_ARRAY"[] (attribute not removed + double brackets)
  describe('columnDef - legacy array format handling', () => {
    test('handles legacy text[]|TEXT_ARRAY format for SQLite', () => {
      const spec = {
        pos: 1,
        dataType: 'text[]|TEXT_ARRAY', // Legacy format
        characterMaximumLength: null,
        notNull: false,
        dflt: null,
        elemPgTypeClass: PostgresTypeClass.Base,
      } as const;

      // SQLite should get "text[]" (not "text[]|TEXT_ARRAY"[])
      const sqliteResult = liteColumnDef(spec);
      expect(sqliteResult).toBe('"text[]"');
    });

    test('handles legacy text|TEXT_ARRAY format', () => {
      const spec = {
        pos: 1,
        dataType: 'text|TEXT_ARRAY', // Legacy format without brackets
        characterMaximumLength: null,
        notNull: false,
        dflt: null,
        elemPgTypeClass: PostgresTypeClass.Base,
      } as const;

      // SQLite should get "text[]"
      const sqliteResult = liteColumnDef(spec);
      expect(sqliteResult).toBe('"text[]"');
    });

    test('handles legacy text|TEXT_ARRAY[] format', () => {
      const spec = {
        pos: 1,
        dataType: 'text|TEXT_ARRAY[]', // Legacy format with trailing []
        characterMaximumLength: null,
        notNull: false,
        dflt: null,
        elemPgTypeClass: PostgresTypeClass.Base,
      } as const;

      // SQLite should get "text[]"
      const sqliteResult = liteColumnDef(spec);
      expect(sqliteResult).toBe('"text[]"');
    });

    test('handles legacy text[]|TEXT_ARRAY[] format', () => {
      const spec = {
        pos: 1,
        dataType: 'text[]|TEXT_ARRAY[]', // Legacy format with both [] and trailing []
        characterMaximumLength: null,
        notNull: false,
        dflt: null,
        elemPgTypeClass: PostgresTypeClass.Base,
      } satisfies ColumnSpec;

      // SQLite should get "text[]"
      const sqliteResult = liteColumnDef(spec);
      expect(sqliteResult).toBe('"text[]"');
    });

    test('handles new text[] format', () => {
      const spec = {
        pos: 1,
        dataType: 'text[]', // New format (no |TEXT_ARRAY)
        characterMaximumLength: null,
        notNull: false,
        dflt: null,
        elemPgTypeClass: PostgresTypeClass.Base,
      } as const;

      // SQLite should get "text[]"
      const sqliteResult = liteColumnDef(spec);
      expect(sqliteResult).toBe('"text[]"');
    });

    test('handles new text[][] format', () => {
      const spec = {
        pos: 1,
        dataType: 'text[][]', // New format (no |TEXT_ARRAY)
        characterMaximumLength: null,
        notNull: false,
        dflt: null,
        elemPgTypeClass: PostgresTypeClass.Base,
      } as const;

      // SQLite should get "text[]"
      const sqliteResult = liteColumnDef(spec);
      expect(sqliteResult).toBe('"text[][]"');
    });

    test('handles new text|NOT_NULL[] format', () => {
      const spec = {
        pos: 1,
        dataType: 'text|NOT_NULL[]', // New format with attributes
        characterMaximumLength: null,
        notNull: false,
        dflt: null,
        elemPgTypeClass: PostgresTypeClass.Base,
      } as const;

      // SQLite should get "text|NOT_NULL[]"
      const sqliteResult = liteColumnDef(spec);
      expect(sqliteResult).toBe('"text|NOT_NULL[]"');
    });
  });
});
