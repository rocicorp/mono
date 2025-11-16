import {beforeAll, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {
  ClientSchema,
  TableSchema,
} from '../../../../zero-protocol/src/client-schema.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {computeZqlSpecs} from '../../db/lite-tables.ts';
import type {LiteAndZqlSpec, LiteTableSpec} from '../../db/specs.ts';
import type {ShardID} from '../../types/shards.ts';
import {checkClientSchema} from './client-schema.ts';

describe('client schemas', () => {
  const tableSpecs = new Map<string, LiteAndZqlSpec>();
  const fullTables = new Map<string, LiteTableSpec>();

  const SHARD_ID: ShardID = {appID: 'zero', shardNum: 0};

  beforeAll(() => {
    const lc = createSilentLogContext();
    const db = new Database(lc, ':memory:');
    db.exec(/* sql */ `
      CREATE TABLE foo(
        id "text|NOT_NULL",
        a "int|NOT_NULL",
        b bool,
        c json,
        d timestamp,
        e timestamptz,
        f date,
        notSyncedToClient custom_pg_type,
        _0_version TEXT
      );
      CREATE UNIQUE INDEX foo_pkey ON foo (id ASC);
      CREATE UNIQUE INDEX foo_id_a_key ON foo (id ASC, a DESC);

      CREATE TABLE bar(
        id "text|NOT_NULL",
        d int,
        e bool,
        f json,
        _0_version TEXT
      );
      CREATE UNIQUE INDEX bar_pkey ON bar (id ASC);

      CREATE TABLE nopk(
        id "text|NOT_NULL",
        d int,
        e bool,
        f json,
        _0_version TEXT
      );
      CREATE INDEX not_unique ON nopk (id ASC);
      CREATE UNIQUE INDEX nullable ON nopk (d ASC);

      -- Not the full internal tables. Just declared here to confirm that
      -- they do not show up in the error messages.
      CREATE TABLE "zero.permissions" (lock bool PRIMARY KEY);
      CREATE TABLE "zero_0.clients" (clientGroupID TEXT PRIMARY KEY);
      `);
    computeZqlSpecs(lc, db, tableSpecs, fullTables);
  });

  test.each([
    [
      {
        tables: {
          bar: {
            columns: {
              id: {type: 'string'},
              d: {type: 'number'},
            },
            primaryKey: ['id'],
          },
        },
      } satisfies ClientSchema,
    ],
    [
      {
        tables: {
          bar: {
            columns: {
              id: {type: 'string'},
              d: {type: 'number'},
            },
            primaryKey: ['id'],
          },
          foo: {
            columns: {
              id: {type: 'string'},
              c: {type: 'json'},
            },
            primaryKey: ['id'],
          },
        },
      } satisfies ClientSchema,
    ],
    [
      {
        tables: {
          bar: {
            columns: {
              e: {type: 'boolean'},
              id: {type: 'string'},
              f: {type: 'json'},
              d: {type: 'number'},
            },
            primaryKey: ['id'],
          },
          foo: {
            columns: {
              c: {type: 'json'},
              id: {type: 'string'},
              a: {type: 'number'},
              b: {type: 'boolean'},
            },
            primaryKey: ['id'],
          },
        },
      } satisfies ClientSchema,
    ],
    [
      {
        tables: {
          bar: {
            columns: {
              e: {type: 'boolean'},
              id: {type: 'string'},
              f: {type: 'json'},
              d: {type: 'number'},
            },
            primaryKey: ['id'],
          },
          foo: {
            columns: {
              c: {type: 'json'},
              id: {type: 'string'},
              a: {type: 'number'},
              b: {type: 'boolean'},
              d: {type: 'number'},
              e: {type: 'number'},
              f: {type: 'number'},
            },
            primaryKey: ['id'],
          },
        },
      } satisfies ClientSchema,
    ],
  ] as [ClientSchema][])('subset okay: %o', clientSchema => {
    checkClientSchema(SHARD_ID, clientSchema, tableSpecs, fullTables);
  });

  test('missing tables with non-public schema', () => {
    expect(() =>
      checkClientSchema(
        SHARD_ID,
        {
          tables: {
            ['yyy.zzz']: {
              columns: {
                id: {type: 'string'},
              },
              primaryKey: ['id'],
            },
          },
        },
        tableSpecs,
        fullTables,
      ),
    ).toThrowErrorMatchingInlineSnapshot(
      `[ProtocolError: The "yyy.zzz" table does not exist or is not one of the replicated tables: "bar","foo". Note that zero does not sync tables from non-public schemas by default. Make sure you have defined a custom ZERO_APP_PUBLICATION to sync tables from non-public schemas.]`,
    );
  });

  test('missing tables, missing columns', () => {
    expect(() =>
      checkClientSchema(
        SHARD_ID,
        {
          tables: {
            bar: {
              columns: {
                e: {type: 'boolean'},
                id: {type: 'string'},
                f: {type: 'json'},
                d: {type: 'number'},
                zzz: {type: 'number'},
              },
              primaryKey: ['id'],
            },
            foo: {
              columns: {
                c: {type: 'json'},
                id: {type: 'string'},
                a: {type: 'number'},
                b: {type: 'boolean'},
              },
              primaryKey: ['id'],
            },
            yyy: {
              columns: {
                id: {type: 'string'},
              },
              primaryKey: ['id'],
            },
          },
        },
        tableSpecs,
        fullTables,
      ),
    ).toThrowErrorMatchingInlineSnapshot(
      `
      [ProtocolError: The "yyy" table does not exist or is not one of the replicated tables: "bar","foo".
      The "bar"."zzz" column does not exist or is not one of the replicated columns: "d","e","f","id".]
    `,
    );
  });

  test('column not synced to client', () => {
    expect(() =>
      checkClientSchema(
        SHARD_ID,
        {
          tables: {
            foo: {
              columns: {
                c: {type: 'json'},
                id: {type: 'string'},
                a: {type: 'number'},
                b: {type: 'boolean'},
                notSyncedToClient: {type: 'json'},
              },
              primaryKey: ['id'],
            },
          },
        },
        tableSpecs,
        fullTables,
      ),
    ).toThrowErrorMatchingInlineSnapshot(
      `[ProtocolError: The "foo"."notSyncedToClient" column cannot be synced because it is of an unsupported data type "custom_pg_type"]`,
    );
  });

  test('column data type mismatch', () => {
    expect(() =>
      checkClientSchema(
        SHARD_ID,
        {
          tables: {
            foo: {
              columns: {
                c: {type: 'json'},
                id: {type: 'string'},
                a: {type: 'string'},
                b: {type: 'number'},
                d: {type: 'number'},
                e: {type: 'number'},
                f: {type: 'number'},
              },
              primaryKey: ['id'],
            },
          },
        },
        tableSpecs,
        fullTables,
      ),
    ).toThrowErrorMatchingInlineSnapshot(
      `
          [ProtocolError: The "foo"."a" column's upstream type "number" does not match the client type "string"
          The "foo"."b" column's upstream type "boolean" does not match the client type "number"]
        `,
    );
  });

  test('table missing primary key', () => {
    expect(() =>
      checkClientSchema(
        SHARD_ID,
        {
          tables: {
            nopk: {
              columns: {
                id: {type: 'string'},
              },
              primaryKey: ['id'],
            },
            foo: {
              columns: {
                c: {type: 'json'},
                id: {type: 'string'},
                a: {type: 'number'},
                b: {type: 'boolean'},
              },
              primaryKey: ['id'],
            },
          },
        },
        tableSpecs,
        fullTables,
      ),
    ).toThrowErrorMatchingInlineSnapshot(
      `[ProtocolError: The "nopk" table is missing a primary key or non-null unique index and thus cannot be synced to the client]`,
    );
  });

  test('client schema missing primary key', () => {
    expect(() =>
      checkClientSchema(
        SHARD_ID,
        {
          tables: {
            foo: {
              columns: {
                c: {type: 'json'},
                id: {type: 'string'},
                a: {type: 'number'},
                b: {type: 'boolean'},
              },
            } as unknown as TableSchema, // force cast, missing primary key
          },
        },
        tableSpecs,
        fullTables,
      ),
    ).toThrowErrorMatchingInlineSnapshot(
      `[ProtocolError: The "foo" table's client schema does not specify a primary key.]`,
    );
  });

  test.each([
    [
      {
        tables: {
          foo: {
            columns: {
              c: {type: 'json'},
              id: {type: 'string'},
              a: {type: 'number'},
              b: {type: 'boolean'},
              d: {type: 'number'},
              e: {type: 'number'},
              f: {type: 'number'},
            },
            primaryKey: ['id'],
          },
        },
      } satisfies ClientSchema,
    ],
    [
      {
        tables: {
          foo: {
            columns: {
              c: {type: 'json'},
              id: {type: 'string'},
              a: {type: 'number'},
              b: {type: 'boolean'},
              d: {type: 'number'},
              e: {type: 'number'},
              f: {type: 'number'},
            },
            primaryKey: ['id', 'a'],
          },
        },
      } satisfies ClientSchema,
    ],
    [
      {
        tables: {
          foo: {
            columns: {
              c: {type: 'json'},
              id: {type: 'string'},
              a: {type: 'number'},
              b: {type: 'boolean'},
              d: {type: 'number'},
              e: {type: 'number'},
              f: {type: 'number'},
            },
            primaryKey: ['a', 'id'],
          },
        },
      } satisfies ClientSchema,
    ],
  ] as [ClientSchema][])(
    'all unique indexes can be primary key: %o',
    clientSchema => {
      checkClientSchema(SHARD_ID, clientSchema, tableSpecs, fullTables);
    },
  );

  test('table with wrong primary key', () => {
    expect(() =>
      checkClientSchema(
        SHARD_ID,
        {
          tables: {
            foo: {
              columns: {
                c: {type: 'json'},
                id: {type: 'string'},
                a: {type: 'number'},
                b: {type: 'boolean'},
              },
              primaryKey: ['a'],
            },
          },
        },
        tableSpecs,
        fullTables,
      ),
    ).toThrowErrorMatchingInlineSnapshot(
      `[ProtocolError: The "foo" table's primaryKey <a> is not associated with a non-null unique index.]`,
    );

    expect(() =>
      checkClientSchema(
        SHARD_ID,
        {
          tables: {
            foo: {
              columns: {
                c: {type: 'json'},
                id: {type: 'string'},
                a: {type: 'number'},
                b: {type: 'boolean'},
              },
              primaryKey: ['id', 'a', 'b'],
            },
          },
        },
        tableSpecs,
        fullTables,
      ),
    ).toThrowErrorMatchingInlineSnapshot(
      `[ProtocolError: The "foo" table's primaryKey <id,a,b> is not associated with a non-null unique index.]`,
    );
  });

  test('nothing synced', () => {
    expect(() =>
      checkClientSchema(
        SHARD_ID,
        {
          tables: {
            foo: {
              columns: {
                c: {type: 'json'},
                id: {type: 'string'},
                a: {type: 'number'},
                b: {type: 'boolean'},
              },
              primaryKey: ['id'],
            },
          },
        },
        new Map(),
        new Map(),
      ),
    ).toThrowErrorMatchingInlineSnapshot(
      `[ProtocolError: No tables have been synced from upstream. Please check that the ZERO_UPSTREAM_DB has been properly set.]`,
    );
  });
});
