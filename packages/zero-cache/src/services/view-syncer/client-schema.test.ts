import {beforeAll, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
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
        a int,
        b bool,
        c json,
        notSyncedToClient custom_pg_type,
        _0_version TEXT
      );
      CREATE UNIQUE INDEX foo_pkey ON foo (id ASC);

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
          },
        },
      },
    ],
    [
      {
        tables: {
          bar: {
            columns: {
              id: {type: 'string'},
              d: {type: 'number'},
            },
          },
          foo: {
            columns: {
              id: {type: 'string'},
              c: {type: 'json'},
            },
          },
        },
      },
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
          },
          foo: {
            columns: {
              c: {type: 'json'},
              id: {type: 'string'},
              a: {type: 'number'},
              b: {type: 'boolean'},
            },
          },
        },
      },
    ],
  ] as [ClientSchema][])('subset okay: %o', clientSchema => {
    checkClientSchema(SHARD_ID, clientSchema, tableSpecs, fullTables);
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
            },
            foo: {
              columns: {
                c: {type: 'json'},
                id: {type: 'string'},
                a: {type: 'number'},
                b: {type: 'boolean'},
              },
            },
            yyy: {
              columns: {
                id: {type: 'string'},
              },
            },
          },
        },
        tableSpecs,
        fullTables,
      ),
    ).toThrowErrorMatchingInlineSnapshot(
      `[Error: {"kind":"SchemaVersionNotSupported","message":"The \\"yyy\\" table does not exist or is not one of the replicated tables: \\"bar\\",\\"foo\\".\\nThe \\"bar\\".\\"zzz\\" column does not exist or is not one of the replicated columns: \\"d\\",\\"e\\",\\"f\\",\\"id\\"."}]`,
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
            },
          },
        },
        tableSpecs,
        fullTables,
      ),
    ).toThrowErrorMatchingInlineSnapshot(
      `[Error: {"kind":"SchemaVersionNotSupported","message":"The \\"foo\\".\\"notSyncedToClient\\" column cannot be synced because it is of an unsupported data type \\"custom_pg_type\\""}]`,
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
              },
            },
          },
        },
        tableSpecs,
        fullTables,
      ),
    ).toThrowErrorMatchingInlineSnapshot(
      `[Error: {"kind":"SchemaVersionNotSupported","message":"The \\"foo\\".\\"a\\" column's upstream type \\"number\\" does not match the client type \\"string\\"\\nThe \\"foo\\".\\"b\\" column's upstream type \\"boolean\\" does not match the client type \\"number\\""}]`,
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
            },
            foo: {
              columns: {
                c: {type: 'json'},
                id: {type: 'string'},
                a: {type: 'number'},
                b: {type: 'boolean'},
              },
            },
          },
        },
        tableSpecs,
        fullTables,
      ),
    ).toThrowErrorMatchingInlineSnapshot(
      `[Error: {"kind":"SchemaVersionNotSupported","message":"The \\"nopk\\" table is missing a primary key or non-null unique index and thus cannot be synced to the client"}]`,
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
            },
          },
        },
        new Map(),
        new Map(),
      ),
    ).toThrowErrorMatchingInlineSnapshot(
      `[Error: {"kind":"Internal","message":"No tables have been synced from upstream. Please check that the ZERO_UPSTREAM_DB has been properly set."}]`,
    );
  });
});
