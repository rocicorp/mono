import {LogContext} from '@rocicorp/logger';
import {afterAll, beforeAll, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.js';
import {Queue} from '../../../../../shared/src/queue.js';
import type {Database} from '../../../../../zqlite/src/db.js';
import {listIndexes, listTables} from '../../../db/lite-tables.js';
import type {LiteIndexSpec, LiteTableSpec} from '../../../db/specs.js';
import {getConnectionURI, testDBs} from '../../../test/db.js';
import {DbFile, expectMatchingObjectsInTables} from '../../../test/lite.js';
import type {JSONValue} from '../../../types/bigint-json.js';
import type {PostgresDB} from '../../../types/pg.js';
import type {Source} from '../../../types/streams.js';
import type {MessageProcessor} from '../../replicator/incremental-sync.js';
import {createMessageProcessor} from '../../replicator/test-utils.js';
import type {DownstreamChange} from '../change-streamer.js';
import type {DataChange} from '../schema/change.js';
import {initializeChangeSource} from './change-source.js';

const SHARD_ID = 'change_source_end_to_mid_test_id';

/**
 * End-to-mid test. This covers:
 *
 * - Executing a DDL or DML statement on upstream postgres.
 * - Verifying the resulting Change messages in the ChangeStream.
 * - Applying the changes to the replica with a MessageProcessor
 * - Verifying the resulting SQLite schema and/or data on the replica.
 */
describe('change-source/pg/end-to-mid-test', () => {
  let lc: LogContext;
  let upstream: PostgresDB;
  let replicaDbFile: DbFile;
  let replica: Database;
  let changes: Source<DownstreamChange>;
  let downstream: Queue<DownstreamChange>;
  let replicator: MessageProcessor;

  beforeAll(async () => {
    lc = createSilentLogContext();
    upstream = await testDBs.create('change_source_end_to_mid_test_upstream');
    replicaDbFile = new DbFile('change_source_end_to_mid_test_replica');
    replica = replicaDbFile.connect(lc);

    const upstreamURI = getConnectionURI(upstream);
    await upstream.unsafe(`
    CREATE TABLE foo(
      id TEXT PRIMARY KEY,
      int INT4,
      big BIGINT,
      flt FLOAT8,
      bool BOOLEAN,
      timea TIMESTAMPTZ,
      timeb TIMESTAMPTZ,
      date DATE,
      time TIME,
      json JSON,
      jsonb JSONB
    );

    CREATE SCHEMA test;

    CREATE PUBLICATION zero_some_public FOR TABLE foo (id, int);
    CREATE PUBLICATION zero_all_test FOR TABLES IN SCHEMA test;
    `);

    const source = (
      await initializeChangeSource(
        lc,
        upstreamURI,
        {id: SHARD_ID, publications: ['zero_some_public', 'zero_all_test']},
        replicaDbFile.path,
      )
    ).changeSource;
    const stream = await source.startStream('00');

    changes = stream.changes;
    downstream = drainToQueue(changes);
    replicator = createMessageProcessor(replica);
  });

  afterAll(async () => {
    changes?.cancel();
    await testDBs.drop(upstream);
    await replicaDbFile.unlink();
  });

  function drainToQueue(
    sub: Source<DownstreamChange>,
  ): Queue<DownstreamChange> {
    const queue = new Queue<DownstreamChange>();
    void (async () => {
      for await (const msg of sub) {
        void queue.enqueue(msg);
      }
    })();
    return queue;
  }

  async function nextTransaction(): Promise<DataChange[]> {
    const data: DataChange[] = [];
    for (;;) {
      const change = await downstream.dequeue();
      replicator.processMessage(lc, change);

      switch (change[0]) {
        case 'begin':
          break;
        case 'data':
          data.push(change[1]);
          break;
        case 'commit':
          return data;
        default:
          change satisfies never;
      }
    }
  }

  test.each([
    [
      'create table',
      'CREATE TABLE test.bar (id INT8 PRIMARY KEY);',
      [{tag: 'create-table'}],
      {['test.bar']: []},
      [
        {
          name: 'test.bar',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: null,
              notNull: true,
              pos: 1,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 2,
            },
          },
          primaryKey: ['id'],
        },
      ],
      [],
    ],
    [
      'add column',
      'ALTER TABLE test.bar ADD name INT8;',
      [{tag: 'add-column'}],
      {['test.bar']: []},
      [
        {
          columns: {
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 2,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: null,
              notNull: true,
              pos: 1,
            },
            name: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: null,
              notNull: false,
              pos: 3,
            },
          },
          name: 'test.bar',
          primaryKey: ['id'],
        },
      ],
      [],
    ],
    [
      'rename column',
      'ALTER TABLE test.bar RENAME name TO handle;',
      [{tag: 'update-column'}],
      {['test.bar']: []},
      [
        {
          columns: {
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 2,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: null,
              notNull: true,
              pos: 1,
            },
            handle: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: null,
              notNull: false,
              pos: 3,
            },
          },
          name: 'test.bar',
          primaryKey: ['id'],
        },
      ],
      [],
    ],
    [
      'change column data type',
      'ALTER TABLE test.bar ALTER handle TYPE TEXT;',
      [{tag: 'update-column'}],
      {['test.bar']: []},
      [
        {
          columns: {
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 2,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: null,
              notNull: true,
              pos: 1,
            },
            handle: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 3,
            },
          },
          name: 'test.bar',
          primaryKey: ['id'],
        },
      ],
      [],
    ],
    [
      'add unique column to automatically generate index',
      'ALTER TABLE test.bar ADD username TEXT UNIQUE;',
      [{tag: 'add-column'}, {tag: 'create-index'}],
      {['test.bar']: []},
      [
        {
          columns: {
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 2,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: null,
              notNull: true,
              pos: 1,
            },
            handle: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 3,
            },
            username: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 4,
            },
          },
          name: 'test.bar',
          primaryKey: ['id'],
        },
      ],
      [
        {
          name: 'test.bar_username_key',
          tableName: 'test.bar',
          columns: {username: 'ASC'},
          unique: true,
        },
      ],
    ],
    [
      'rename unique column with associated index',
      'ALTER TABLE test.bar RENAME username TO login;',
      [{tag: 'update-column'}],
      {['test.bar']: []},
      [
        {
          columns: {
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 2,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: null,
              notNull: true,
              pos: 1,
            },
            handle: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 3,
            },
            login: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 4,
            },
          },
          name: 'test.bar',
          primaryKey: ['id'],
        },
      ],
      [
        {
          name: 'test.bar_username_key',
          tableName: 'test.bar',
          columns: {login: 'ASC'},
          unique: true,
        },
      ],
    ],
    [
      'retype unique column with associated index',
      'ALTER TABLE test.bar ALTER login TYPE VARCHAR(180);',
      [{tag: 'update-column'}],
      {['test.bar']: []},
      [
        {
          columns: {
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 2,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: null,
              notNull: true,
              pos: 1,
            },
            handle: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 3,
            },
            login: {
              characterMaximumLength: null,
              dataType: 'varchar',
              dflt: null,
              notNull: false,
              pos: 4,
            },
          },
          name: 'test.bar',
          primaryKey: ['id'],
        },
      ],
      [
        {
          name: 'test.bar_username_key',
          tableName: 'test.bar',
          columns: {login: 'ASC'},
          unique: true,
        },
      ],
    ],
    [
      'drop column with index',
      'ALTER TABLE test.bar DROP login;',
      [{tag: 'drop-index'}, {tag: 'drop-column'}],
      {['test.bar']: []},
      [
        {
          columns: {
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 2,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: null,
              notNull: true,
              pos: 1,
            },
            handle: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 3,
            },
          },
          name: 'test.bar',
          primaryKey: ['id'],
        },
      ],
      [],
    ],
    [
      'add unpublished column',
      'ALTER TABLE foo ADD "newInt" INT4;',
      [], // no DDL event published
      {},
      [
        // the view of "foo" is unchanged.
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 1,
            },
            int: {
              characterMaximumLength: null,
              dataType: 'int4',
              dflt: null,
              notNull: false,
              pos: 2,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 3,
            },
          },
          primaryKey: ['id'],
        },
      ],
      [],
    ],
    [
      'alter publication add and drop column',
      'ALTER PUBLICATION zero_some_public SET TABLE foo (id, "newInt");',
      [
        // Since it is an ALTER PUBLICATION command, we should correctly get
        // a drop and an add, and not a rename.
        {
          tag: 'drop-column',
          table: {schema: 'public', name: 'foo'},
          column: 'int',
        },
        {
          tag: 'add-column',
          table: {schema: 'public', name: 'foo'},
        },
      ],
      {foo: []},
      [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 1,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 2,
            },
            newInt: {
              characterMaximumLength: null,
              dataType: 'int4',
              dflt: null,
              notNull: false,
              pos: 3,
            },
          },
          primaryKey: ['id'],
        },
      ],
      [],
    ],
    [
      'alter publication add multiple columns',
      'ALTER PUBLICATION zero_some_public SET TABLE foo (id, "newInt", int, flt);',
      [
        {
          tag: 'add-column',
          table: {schema: 'public', name: 'foo'},
        },
        {
          tag: 'add-column',
          table: {schema: 'public', name: 'foo'},
        },
      ],
      {foo: []},
      [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 1,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 2,
            },
            newInt: {
              characterMaximumLength: null,
              dataType: 'int4',
              dflt: null,
              notNull: false,
              pos: 3,
            },
            flt: {
              characterMaximumLength: null,
              dataType: 'float8',
              dflt: null,
              notNull: false,
              pos: 4,
            },
            int: {
              characterMaximumLength: null,
              dataType: 'int4',
              dflt: null,
              notNull: false,
              pos: 5,
            },
          },
          primaryKey: ['id'],
        },
      ],
      [],
    ],
    [
      'create unpublished table with indexes',
      'CREATE TABLE public.boo (id INT8 PRIMARY KEY, name TEXT UNIQUE);',
      [],
      {},
      [],
      [],
    ],
    [
      'alter publication introduces table with indexes and changes columns',
      'ALTER PUBLICATION zero_some_public SET TABLE foo (id, flt), boo;',
      [
        {tag: 'drop-column'},
        {tag: 'drop-column'},
        {tag: 'create-table'},
        {tag: 'create-index'},
      ],
      {foo: []},
      [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 1,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 2,
            },
            flt: {
              characterMaximumLength: null,
              dataType: 'float8',
              dflt: null,
              notNull: false,
              pos: 3,
            },
          },
          primaryKey: ['id'],
        },
        {
          name: 'boo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: null,
              notNull: true,
              pos: 1,
            },
            name: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 2,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 3,
            },
          },
          primaryKey: ['id'],
        },
      ],
      [
        {
          name: 'boo_name_key',
          tableName: 'boo',
          columns: {name: 'ASC'},
          unique: true,
        },
      ],
    ],
    [
      'create index',
      'CREATE INDEX foo_flt ON foo (flt DESC);',
      [{tag: 'create-index'}],
      {foo: []},
      [],
      [
        {
          name: 'foo_flt',
          tableName: 'foo',
          columns: {flt: 'DESC'},
          unique: false,
        },
      ],
    ],
    [
      'drop index',
      'DROP INDEX foo_flt;',
      [
        {
          tag: 'drop-index',
          id: {schema: 'public', name: 'foo_flt'},
        },
      ],
      {foo: []},
      [],
      [],
    ],
    [
      'remove table (with indexes) from publication',
      `ALTER PUBLICATION zero_some_public DROP TABLE boo`,
      [
        {
          tag: 'drop-index',
          id: {schema: 'public', name: 'boo_name_key'},
        },
        {
          tag: 'drop-table',
          id: {schema: 'public', name: 'boo'},
        },
      ],
      {},
      [],
      [],
    ],
    [
      'data types',
      `
      ALTER PUBLICATION zero_some_public SET TABLE foo (
        id, int, big, flt, bool, timea, date, json, jsonb);

      INSERT INTO foo (id, int, big, flt, bool, timea, date, json, jsonb)
         VALUES (
          'abc', 
          -2, 
          9007199254740993, 
          3.45, 
          true, 
          '2019-01-12T00:30:35.381101032Z', 
          'April 12, 2003',
          '[{"foo":"bar","bar":"foo"},123]',
          '{"far": 456, "boo" : {"baz": 123}}'
        );
      `,
      [
        {tag: 'add-column'},
        {tag: 'add-column'},
        {tag: 'add-column'},
        {tag: 'add-column'},
        {tag: 'add-column'},
        {tag: 'add-column'},
        {tag: 'add-column'},
        {
          tag: 'insert',
          new: {
            id: 'abc',
            int: -2,
            big: 9007199254740993n,
            bool: true,
            timea: 1547253035381.101,
            date: 1050105600000,
            json: [{foo: 'bar', bar: 'foo'}, 123],
            jsonb: {boo: {baz: 123}, far: 456},
          },
        },
      ],
      {
        foo: [
          {
            id: 'abc',
            int: -2n,
            big: 9007199254740993n,
            flt: 3.45,
            bool: 1n,
            timea: 1547253035381.101,
            date: 1050105600000n,
            json: '[{"foo":"bar","bar":"foo"},123]',
            jsonb: '{"boo":{"baz":123},"far":456}',
            ['_0_version']: expect.stringMatching(/[a-z0-9]+/),
          },
        ],
      },
      [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 1,
            },
            flt: {
              characterMaximumLength: null,
              dataType: 'float8',
              dflt: null,
              notNull: false,
              pos: 3,
            },
            big: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: null,
              notNull: false,
              pos: 4,
            },
            bool: {
              characterMaximumLength: null,
              dataType: 'bool',
              dflt: null,
              notNull: false,
              pos: 5,
            },
            date: {
              characterMaximumLength: null,
              dataType: 'date',
              dflt: null,
              notNull: false,
              pos: 6,
            },
            int: {
              characterMaximumLength: null,
              dataType: 'int4',
              dflt: null,
              notNull: false,
              pos: 7,
            },
            json: {
              characterMaximumLength: null,
              dataType: 'json',
              dflt: null,
              notNull: false,
              pos: 8,
            },
            jsonb: {
              characterMaximumLength: null,
              dataType: 'jsonb',
              dflt: null,
              notNull: false,
              pos: 9,
            },
            timea: {
              characterMaximumLength: null,
              dataType: 'timestamptz',
              dflt: null,
              notNull: false,
              pos: 10,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 2,
            },
          },
          primaryKey: ['id'],
        },
      ],
      [],
    ],
  ] satisfies [
    name: string,
    statements: string,
    changes: Partial<DataChange>[],
    expectedData: Record<string, JSONValue>,
    expectedTables: LiteTableSpec[],
    expectedIndexes: LiteIndexSpec[],
  ][])(
    '%s',
    async (
      _name,
      stmts,
      changes,
      expectedData,
      expectedTables,
      expectedIndexes,
    ) => {
      await upstream.unsafe(stmts);
      const transaction = await nextTransaction();
      expect(transaction.length).toBe(changes.length);

      transaction.forEach((change, i) => {
        expect(change).toMatchObject(changes[i]);
      });

      expectMatchingObjectsInTables(replica, expectedData, 'bigint');

      const tables = listTables(replica);
      for (const table of expectedTables) {
        expect(tables).toContainEqual(table);
      }
      const indexes = listIndexes(replica);
      for (const index of expectedIndexes) {
        expect(indexes).toContainEqual(index);
      }
    },
  );
});
