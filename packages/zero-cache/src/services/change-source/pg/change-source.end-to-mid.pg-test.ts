import {LogContext} from '@rocicorp/logger';
import {afterAll, beforeAll, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../../shared/src/queue.ts';
import type {Database} from '../../../../../zqlite/src/db.ts';
import {listIndexes, listTables} from '../../../db/lite-tables.ts';
import type {LiteIndexSpec, LiteTableSpec} from '../../../db/specs.ts';
import {getConnectionURI, testDBs} from '../../../test/db.ts';
import {DbFile, expectMatchingObjectsInTables} from '../../../test/lite.ts';
import {type JSONValue} from '../../../types/bigint-json.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import type {Source} from '../../../types/streams.ts';
import type {ChangeProcessor} from '../../replicator/change-processor.ts';
import {createChangeProcessor} from '../../replicator/test-utils.ts';
import type {DataChange} from '../protocol/current/data.ts';
import type {ChangeStreamMessage} from '../protocol/current/downstream.ts';
import {initializePostgresChangeSource} from './change-source.ts';

const APP_ID = 'orez';

/**
 * End-to-mid test. This covers:
 *
 * - Executing a DDL or DML statement on upstream postgres.
 * - Verifying the resulting Change messages in the ChangeStream.
 * - Applying the changes to the replica with a MessageProcessor
 * - Verifying the resulting SQLite schema and/or data on the replica.
 */
describe('change-source/pg/end-to-mid-test', {timeout: 30000}, () => {
  let lc: LogContext;
  let upstream: PostgresDB;
  let replicaDbFile: DbFile;
  let replica: Database;
  let changes: Source<ChangeStreamMessage>;
  let downstream: Queue<ChangeStreamMessage | 'timeout'>;
  let replicator: ChangeProcessor;

  beforeAll(async () => {
    lc = createSilentLogContext();
    upstream = await testDBs.create('change_source_end_to_mid_test_upstream');
    replicaDbFile = new DbFile('change_source_end_to_mid_test_replica');
    replica = replicaDbFile.connect(lc);

    const upstreamURI = getConnectionURI(upstream);
    await upstream.unsafe(`
    CREATE TYPE ENUMZ AS ENUM ('1', '2', '3');
    CREATE TABLE foo(
      id TEXT NOT NULL,
      int INT4,
      big BIGINT,
      flt FLOAT8,
      bool BOOLEAN,
      timea TIMESTAMPTZ,
      timeb TIMESTAMPTZ,
      date DATE,
      time TIME,
      json JSON,
      jsonb JSONB,
      numz ENUMZ,
      uuid UUID
    );

    CREATE SCHEMA IF NOT EXISTS my;

    CREATE UNIQUE INDEX foo_key ON foo (id);
    CREATE PUBLICATION zero_some_public FOR TABLE foo (id, int);
    CREATE PUBLICATION zero_all_test FOR TABLES IN SCHEMA my;
    `);

    const source = (
      await initializePostgresChangeSource(
        lc,
        upstreamURI,
        {
          appID: APP_ID,
          publications: ['zero_some_public', 'zero_all_test'],
          shardNum: 0,
        },
        replicaDbFile.path,
        {tableCopyWorkers: 5, rowBatchSize: 10000},
      )
    ).changeSource;
    const stream = await source.startStream('00');

    changes = stream.changes;
    downstream = drainToQueue(changes);
    replicator = createChangeProcessor(replica);
  }, 30000);

  afterAll(async () => {
    changes?.cancel();
    await testDBs.drop(upstream);
    replicaDbFile.delete();
  });

  function drainToQueue(
    sub: Source<ChangeStreamMessage>,
  ): Queue<ChangeStreamMessage | 'timeout'> {
    const queue = new Queue<ChangeStreamMessage | 'timeout'>();
    void (async () => {
      for await (const msg of sub) {
        queue.enqueue(msg);
      }
    })();
    return queue;
  }

  async function nextTransaction(): Promise<DataChange[]> {
    const data: DataChange[] = [];
    for (;;) {
      const change = await downstream.dequeue('timeout', 2000);
      if (change === 'timeout') {
        throw new Error('timed out waiting for change');
      }
      const [type] = change;
      if (type !== 'control' && type !== 'status') {
        replicator.processMessage(lc, change);
      }

      switch (type) {
        case 'begin':
          break;
        case 'data':
          data.push(change[1]);
          break;
        case 'commit':
        case 'rollback':
        case 'control':
        case 'status':
          return data;
        default:
          change satisfies never;
      }
    }
  }

  test.each([
    [
      'create table',
      `CREATE TABLE my.baz (
        id INT8 CONSTRAINT baz_pkey PRIMARY KEY,
        gen INT8 GENERATED ALWAYS AS (id + 1) STORED  -- Should be excluded
       );`,
      [{tag: 'create-table'}, {tag: 'create-index'}],
      {['my.baz']: []},
      [
        {
          name: 'my.baz',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'int8|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 2,
            },
          },
        },
      ],
      [
        {
          columns: {id: 'ASC'},
          name: 'my.baz_pkey',
          tableName: 'my.baz',
          unique: true,
        },
      ],
    ],
    [
      'rename table',
      'ALTER TABLE my.baz RENAME TO bar;',
      [{tag: 'rename-table'}],
      {['my.bar']: []},
      [
        {
          name: 'my.bar',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'int8|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 2,
            },
          },
        },
      ],
      [
        {
          columns: {id: 'ASC'},
          name: 'my.baz_pkey',
          tableName: 'my.bar',
          unique: true,
        },
      ],
    ],
    [
      'add column',
      'ALTER TABLE my.bar ADD name INT8;',
      [{tag: 'add-column'}],
      {['my.bar']: []},
      [
        {
          columns: {
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 2,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'int8|NOT_NULL',
              dflt: null,
              notNull: false,
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
          name: 'my.bar',
        },
      ],
      [],
    ],
    [
      'rename column',
      'ALTER TABLE my.bar RENAME name TO handle;',
      [{tag: 'update-column'}],
      {['my.bar']: []},
      [
        {
          columns: {
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 2,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'int8|NOT_NULL',
              dflt: null,
              notNull: false,
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
          name: 'my.bar',
        },
      ],
      [],
    ],
    [
      'change column data type',
      'ALTER TABLE my.bar ALTER handle TYPE TEXT;',
      [{tag: 'update-column'}],
      {['my.bar']: []},
      [
        {
          columns: {
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 2,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'int8|NOT_NULL',
              dflt: null,
              notNull: false,
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
          name: 'my.bar',
        },
      ],
      [],
    ],
    [
      'change the primary key',
      `
      ALTER TABLE my.bar DROP CONSTRAINT baz_pkey;
      ALTER TABLE my.bar ADD PRIMARY KEY (handle);
      `,
      [
        {tag: 'drop-index'},
        {
          tag: 'update-column',
          old: {
            name: 'handle',
            spec: {dataType: 'text', notNull: false, pos: expect.any(Number)},
          },
          new: {
            name: 'handle',
            spec: {dataType: 'text', notNull: true, pos: expect.any(Number)},
          },
        },
        {tag: 'create-index'},
      ],
      {['my.bar']: []},
      [
        {
          columns: {
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 2,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'int8|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            handle: {
              characterMaximumLength: null,
              dataType: 'text|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 3,
            },
          },
          name: 'my.bar',
        },
      ],
      [
        {
          columns: {handle: 'ASC'},
          name: 'my.bar_pkey',
          tableName: 'my.bar',
          unique: true,
        },
      ],
    ],
    [
      'add unique column to automatically generate index',
      'ALTER TABLE my.bar ADD username TEXT UNIQUE;',
      [{tag: 'add-column'}, {tag: 'create-index'}],
      {['my.bar']: []},
      [
        {
          columns: {
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 2,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'int8|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            handle: {
              characterMaximumLength: null,
              dataType: 'text|NOT_NULL',
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
          name: 'my.bar',
        },
      ],
      [
        {
          name: 'my.bar_username_key',
          tableName: 'my.bar',
          columns: {username: 'ASC'},
          unique: true,
        },
      ],
    ],
    [
      'rename unique column with associated index',
      'ALTER TABLE my.bar RENAME username TO login;',
      [{tag: 'update-column'}],
      {['my.bar']: []},
      [
        {
          columns: {
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 2,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'int8|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            handle: {
              characterMaximumLength: null,
              dataType: 'text|NOT_NULL',
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
          name: 'my.bar',
        },
      ],
      [
        {
          name: 'my.bar_username_key',
          tableName: 'my.bar',
          columns: {login: 'ASC'},
          unique: true,
        },
      ],
    ],
    [
      'retype unique column with associated index',
      'ALTER TABLE my.bar ALTER login TYPE VARCHAR(180);',
      [{tag: 'update-column'}],
      {['my.bar']: []},
      [
        {
          columns: {
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 2,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'int8|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            handle: {
              characterMaximumLength: null,
              dataType: 'text|NOT_NULL',
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
          name: 'my.bar',
        },
      ],
      [
        {
          name: 'my.bar_username_key',
          tableName: 'my.bar',
          columns: {login: 'ASC'},
          unique: true,
        },
      ],
    ],
    [
      'change column default and set not null',
      `
       ALTER TABLE my.bar ALTER login SET DEFAULT floor(10000 * random())::text;
       ALTER TABLE my.bar ALTER login SET NOT NULL;`,
      [{tag: 'update-column'}],
      {['my.bar']: []},
      [
        {
          columns: {
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 2,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'int8|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            handle: {
              characterMaximumLength: null,
              dataType: 'text|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 3,
            },
            login: {
              characterMaximumLength: null,
              dataType: 'varchar|NOT_NULL',
              dflt: null, // defaults should be ignored for update-column
              notNull: false,
              pos: 4,
            },
          },
          name: 'my.bar',
        },
      ],
      [
        {
          name: 'my.bar_username_key',
          tableName: 'my.bar',
          columns: {login: 'ASC'},
          unique: true,
        },
      ],
    ],
    [
      'drop column with index',
      'ALTER TABLE my.bar DROP login;',
      [{tag: 'drop-index'}, {tag: 'drop-column'}],
      {['my.bar']: []},
      [
        {
          columns: {
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 2,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'int8|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            handle: {
              characterMaximumLength: null,
              dataType: 'text|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 3,
            },
          },
          name: 'my.bar',
        },
      ],
      [],
    ],
    [
      'add multiple columns',
      'ALTER TABLE my.bar ADD foo TEXT, ADD bar TEXT;',
      [{tag: 'add-column'}, {tag: 'add-column'}],
      {['my.bar']: []},
      [
        {
          columns: {
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 2,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'int8|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            handle: {
              characterMaximumLength: null,
              dataType: 'text|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 3,
            },
            bar: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 4,
            },
            foo: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 5,
            },
          },
          name: 'my.bar',
        },
      ],
      [],
    ],
    [
      'alter, add, and drop columns',
      'ALTER TABLE my.bar ALTER foo SET NOT NULL, ADD boo TEXT, DROP bar;',
      [{tag: 'drop-column'}, {tag: 'update-column'}, {tag: 'add-column'}],
      {['my.bar']: []},
      [
        {
          columns: {
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 2,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'int8|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            handle: {
              characterMaximumLength: null,
              dataType: 'text|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 3,
            },
            foo: {
              characterMaximumLength: null,
              dataType: 'text|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 4,
            },
            boo: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 5,
            },
          },
          name: 'my.bar',
        },
      ],
      [],
    ],
    [
      'rename schema',
      'ALTER SCHEMA my RENAME TO your;',
      [{tag: 'drop-index'}, {tag: 'rename-table'}, {tag: 'create-index'}],
      {['your.bar']: []},
      [
        {
          columns: {
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 2,
            },
            id: {
              characterMaximumLength: null,
              dataType: 'int8|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            handle: {
              characterMaximumLength: null,
              dataType: 'text|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 3,
            },
            foo: {
              characterMaximumLength: null,
              dataType: 'text|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 4,
            },
            boo: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 5,
            },
          },
          name: 'your.bar',
        },
      ],
      [
        {
          columns: {handle: 'ASC'},
          name: 'your.bar_pkey',
          tableName: 'your.bar',
          unique: true,
        },
      ],
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
              dataType: 'text|NOT_NULL',
              dflt: null,
              notNull: false,
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
              notNull: false,
              pos: 3,
            },
          },
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
              dataType: 'text|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
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
              dataType: 'text|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
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
        {tag: 'create-index'},
      ],
      {foo: []},
      [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'text|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
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
        },
        {
          name: 'boo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'int8|NOT_NULL',
              dflt: null,
              notNull: false,
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
              notNull: false,
              pos: 3,
            },
          },
        },
      ],
      [
        {
          name: 'boo_name_key',
          tableName: 'boo',
          columns: {name: 'ASC'},
          unique: true,
        },
        {
          name: 'boo_pkey',
          tableName: 'boo',
          columns: {id: 'ASC'},
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
          tag: 'drop-index',
          id: {schema: 'public', name: 'boo_pkey'},
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
        id, int, big, flt, bool, timea, date, json, jsonb, numz, uuid);

      INSERT INTO foo (id, int, big, flt, bool, timea, date, json, jsonb, numz, uuid)
         VALUES (
          'abc', 
          -2, 
          9007199254740993, 
          3.45, 
          true, 
          '2019-01-12T00:30:35.381101032Z', 
          'April 12, 2003',
          '[{"foo":"bar","bar":"foo"},123]',
          '{"far": 456, "boo" : {"baz": 123}}',
          '2',
          'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'
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
            json: '[{"foo":"bar","bar":"foo"},123]',
            jsonb: '{"boo": {"baz": 123}, "far": 456}',
            numz: '2',
            uuid: 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
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
            jsonb: '{"boo": {"baz": 123}, "far": 456}',
            numz: '2', // Verifies TEXT affinity
            uuid: 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
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
              dataType: 'text|NOT_NULL',
              dflt: null,
              notNull: false,
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
            numz: {
              characterMaximumLength: null,
              dataType: 'enumz|TEXT_ENUM',
              dflt: null,
              notNull: false,
              pos: 10,
            },
            timea: {
              characterMaximumLength: null,
              dataType: 'timestamptz',
              dflt: null,
              notNull: false,
              pos: 11,
            },
            uuid: {
              characterMaximumLength: null,
              dataType: 'uuid',
              dflt: null,
              notNull: false,
              pos: 12,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 2,
            },
          },
        },
      ],
      [],
    ],
    [
      'no primary key',
      `
      CREATE TABLE nopk (a TEXT NOT NULL, b TEXT);
      ALTER PUBLICATION zero_some_public ADD TABLE nopk;

      INSERT INTO nopk (a, b) VALUES ('foo', 'bar');
      `,
      [
        {tag: 'create-table'},
        {
          tag: 'insert',
          relation: {
            schema: 'public',
            name: 'nopk',
            replicaIdentity: 'default',
            keyColumns: [], // Note: This means is will be replicated to SQLite but not synced to clients.
          },
        },
      ],
      {nopk: [{a: 'foo', b: 'bar'}]},
      [
        {
          name: 'nopk',
          columns: {
            a: {
              characterMaximumLength: null,
              dataType: 'text|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            b: {
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
              notNull: false,
              pos: 3,
            },
          },
        },
      ],
      [],
    ],
    [
      'resumptive replication',
      `
      CREATE TABLE existing (a TEXT PRIMARY KEY, b TEXT);
      INSERT INTO existing (a, b) VALUES ('c', 'd');
      INSERT INTO existing (a, b) VALUES ('e', 'f');

      CREATE TABLE existing_full (a TEXT PRIMARY KEY, b TEXT);
      ALTER TABLE existing_full REPLICA IDENTITY FULL;
      INSERT INTO existing_full (a, b) VALUES ('c', 'd');
      INSERT INTO existing_full (a, b) VALUES ('e', 'f');

      ALTER PUBLICATION zero_some_public ADD TABLE existing;
      ALTER PUBLICATION zero_some_public ADD TABLE existing_full;
      UPDATE existing SET a = a;
      UPDATE existing_full SET a = a;
      `,
      [
        {tag: 'create-table'},
        {tag: 'create-index'},
        {tag: 'create-table'},
        {tag: 'create-index'},
        {tag: 'update'},
        {tag: 'update'},
        {tag: 'update'},
        {tag: 'update'},
      ],
      {
        existing: [
          {a: 'c', b: 'd'},
          {a: 'e', b: 'f'},
        ],
        ['existing_full']: [
          {a: 'c', b: 'd'},
          {a: 'e', b: 'f'},
        ],
      },
      [
        {
          name: 'existing',
          columns: {
            a: {
              characterMaximumLength: null,
              dataType: 'text|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            b: {
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
              notNull: false,
              pos: 3,
            },
          },
        },
        {
          name: 'existing_full',
          columns: {
            a: {
              characterMaximumLength: null,
              dataType: 'text|NOT_NULL',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            b: {
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
              notNull: false,
              pos: 3,
            },
          },
        },
      ],
      [
        {
          columns: {a: 'ASC'},
          name: 'existing_pkey',
          tableName: 'existing',
          unique: true,
        },
        {
          columns: {a: 'ASC'},
          name: 'existing_full_pkey',
          tableName: 'existing_full',
          unique: true,
        },
      ],
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
      expect(transaction).toMatchObject(changes);

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
