/**
 * The Postgres and SQLite change logs interpret one fold
 * (`replicator/change-log-cookies.ts`), and they have to agree on every
 * transition forever: the failure mode when they drift is a backfill that is
 * silently never re-requested, on a column that is then silently never
 * populated. This drives the same change sequence through both writers and
 * requires the resulting cookie sets to be identical.
 *
 */

import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {type PgTest, test} from '../../test/db.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {SchemaChange} from '../change-source/protocol/current/data.ts';
import type {
  ChangeStreamData,
  Commit,
} from '../change-source/protocol/current/downstream.ts';
import type {UpstreamStatusMessage} from '../change-source/protocol/current/status.ts';
import {
  CREATE_CHANGE_LOG_COOKIE_SCHEMA,
  readCookies,
  type BackfillCookie,
  type CookieSet,
  type TableMetadataCookie,
} from '../replicator/change-log-cookies.ts';
import {CREATE_CHANGE_LOG_STREAM_SCHEMA} from '../replicator/change-log-db.ts';
import {ChangeLogStreamWriter} from '../replicator/change-log-stream-writer.ts';
import {serializeChangeStreamData} from './change-log-codec.ts';
import {ensureReplicationConfig, setupCDCTables} from './schema/tables.ts';
import {Storer, type TuningOptions} from './storer.ts';

const lc = createSilentLogContext();

const APP_ID = 'cookies';
const SHARD_NUM = 1;
const CDC_SCHEMA = `${APP_ID}_${SHARD_NUM}/cdc`;
const REPLICA_VERSION = '00';

const opts: TuningOptions = {
  backPressureLimitHeapProportion: 0.04,
  statementTimeoutMs: 20_000,
  changeLogBatchSize: 2000,
};

describe('change-streamer/change-log cookie parity', () => {
  let db: PostgresDB;
  let storer: Storer;
  let done: Promise<void>;
  let changeLog: Database;
  let watermark: number;

  beforeEach<PgTest>(async ({testDBs}) => {
    db = await testDBs.create('change_log_cookie_parity', {
      typeOpts: {sendStringAsJson: true},
    });
    const shard = {appID: APP_ID, shardNum: SHARD_NUM};
    await db.begin(tx => setupCDCTables(lc, tx, shard));
    await ensureReplicationConfig(
      lc,
      db,
      {
        replicaVersion: REPLICA_VERSION,
        publications: [],
        watermark: REPLICA_VERSION,
      },
      shard,
      true,
    );

    storer = new Storer(
      lc,
      shard,
      'task-id',
      'change-streamer:12345',
      'ws',
      db,
      REPLICA_VERSION,
      (_: Commit | UpstreamStatusMessage) => {},
      () => {},
      opts,
    );
    await storer.assumeOwnership();
    done = storer.run();

    changeLog = new Database(lc, ':memory:');
    changeLog.exec(CREATE_CHANGE_LOG_STREAM_SCHEMA);
    changeLog.exec(CREATE_CHANGE_LOG_COOKIE_SCHEMA);
    watermark = 1;

    return async () => {
      changeLog.close();
      await testDBs.drop(db);
      void storer?.stop();
      await done;
    };
  });

  /**
   * Drives one transaction carrying `changes` through both writers, the way
   * their respective stream loops do.
   */
  async function transaction(...changes: SchemaChange[]): Promise<void> {
    const wm = String(++watermark).padStart(2, '0');
    const messages: ChangeStreamData[] = [
      ['begin', {tag: 'begin'}, {commitWatermark: wm}],
      ...changes.map((change): ChangeStreamData => ['data', change]),
      ['commit', {tag: 'commit'}, {watermark: wm}],
    ];

    messages.forEach(message => storer.store(wm, message));

    const writer = new ChangeLogStreamWriter(new StatementRunner(changeLog));
    writer.begin(wm, serializeChangeStreamData(messages[0]));
    changes.forEach((change, i) =>
      writer.append(serializeChangeStreamData(messages[i + 1]), change),
    );
    writer.commit(wm, serializeChangeStreamData(messages.at(-1)!), 1_000);

    await storer.allProcessed();
  }

  /** The cookie set the Postgres change log holds, in the SQLite log's shape. */
  async function pgCookies(): Promise<CookieSet> {
    const [tableMetadata, backfilling] = await Promise.all([
      db<TableMetadataCookie[]>`
        SELECT "schema", "table", "metadata" FROM ${db(CDC_SCHEMA)}."tableMetadata"
          ORDER BY "schema", "table"`,
      db<BackfillCookie[]>`
        SELECT "schema", "table", "column", "backfill" FROM ${db(CDC_SCHEMA)}."backfilling"
          ORDER BY "schema", "table", "column"`,
    ]);
    return {
      tableMetadata: [...tableMetadata],
      backfilling: [...backfilling],
    };
  }

  /**
   * Both stores agree, and the agreed-on set is what was expected. The second
   * half matters: two interpreters of the same fold agree vacuously if the fold
   * itself does nothing.
   */
  async function expectCookies(expected: CookieSet) {
    const pg = await pgCookies();
    expect(pg).toEqual(readCookies(changeLog));
    expect(pg).toEqual(expected);
  }

  test('all eight transitions', async () => {
    // 1. create-table, carrying metadata and two in-flight backfills.
    await transaction({
      tag: 'create-table',
      spec: {schema: 'my', name: 'foo', columns: {}},
      metadata: {rowKey: {type: 'index', columns: ['a', 'b']}},
      backfill: {
        a: {fooID: 987, barID: 'zoo'},
        b: {fooID: 843, barID: 'ozz'},
      },
    });
    await expectCookies({
      tableMetadata: [
        {
          schema: 'my',
          table: 'foo',
          metadata: {rowKey: {type: 'index', columns: ['a', 'b']}},
        },
      ],
      backfilling: [
        {
          schema: 'my',
          table: 'foo',
          column: 'a',
          backfill: {fooID: 987, barID: 'zoo'},
        },
        {
          schema: 'my',
          table: 'foo',
          column: 'b',
          backfill: {fooID: 843, barID: 'ozz'},
        },
      ],
    });

    // A table on a non-`public` schema with backfills and *no* metadata row:
    // legal, since `metadata` is optional, and the case the replica-derived
    // path (C3) cannot reconstruct without an identity of its own.
    await transaction({
      tag: 'create-table',
      spec: {schema: 'your', name: 'bar', columns: {}},
      backfill: {c: {fooID: 5}},
    });

    // 2. update-table-metadata.
    // 3. add-column, carrying both a metadata update and a backfill.
    await transaction(
      {
        tag: 'update-table-metadata',
        table: {schema: 'my', name: 'foo'},
        old: {rowKey: {type: 'index', columns: ['a', 'b']}},
        new: {rowKey: {type: 'default', columns: ['id']}},
      },
      {
        tag: 'add-column',
        table: {schema: 'my', name: 'foo'},
        tableMetadata: {rowKey: {type: 'default', columns: ['id']}},
        column: {name: 'd', spec: {pos: 4, dataType: 'text'}},
        backfill: {fooID: 123, barID: 'baz'},
      },
    );
    await expectCookies({
      tableMetadata: [
        {
          schema: 'my',
          table: 'foo',
          metadata: {rowKey: {type: 'default', columns: ['id']}},
        },
      ],
      backfilling: [
        {
          schema: 'my',
          table: 'foo',
          column: 'a',
          backfill: {fooID: 987, barID: 'zoo'},
        },
        {
          schema: 'my',
          table: 'foo',
          column: 'b',
          backfill: {fooID: 843, barID: 'ozz'},
        },
        {
          schema: 'my',
          table: 'foo',
          column: 'd',
          backfill: {fooID: 123, barID: 'baz'},
        },
        {schema: 'your', table: 'bar', column: 'c', backfill: {fooID: 5}},
      ],
    });

    // 4. update-column (a rename), and a spec-only update that must move
    //    nothing. 5. drop-column.
    await transaction(
      {
        tag: 'update-column',
        table: {schema: 'my', name: 'foo'},
        old: {name: 'a', spec: {pos: 1, dataType: 'text'}},
        new: {name: 'z', spec: {pos: 1, dataType: 'text'}},
      },
      {
        tag: 'update-column',
        table: {schema: 'my', name: 'foo'},
        old: {name: 'b', spec: {pos: 2, dataType: 'text'}},
        new: {name: 'b', spec: {pos: 2, dataType: 'int4'}},
      },
      {
        tag: 'drop-column',
        table: {schema: 'my', name: 'foo'},
        column: 'd',
      },
    );
    await expectCookies({
      tableMetadata: [
        {
          schema: 'my',
          table: 'foo',
          metadata: {rowKey: {type: 'default', columns: ['id']}},
        },
      ],
      backfilling: [
        {
          schema: 'my',
          table: 'foo',
          column: 'b',
          backfill: {fooID: 843, barID: 'ozz'},
        },
        {
          schema: 'my',
          table: 'foo',
          column: 'z',
          backfill: {fooID: 987, barID: 'zoo'},
        },
        {schema: 'your', table: 'bar', column: 'c', backfill: {fooID: 5}},
      ],
    });

    // 6. backfill-completed. `rowKey` columns are excluded from `columns` but
    //    are backfilled with them, so both are cleared; the table's metadata is
    //    not, since it is what the table's *next* BackfillRequest must carry.
    await transaction({
      tag: 'backfill-completed',
      relation: {
        schema: 'my',
        name: 'foo',
        rowKey: {columns: ['b'], type: 'index'},
      },
      columns: ['z'],
      watermark: '07',
    });
    await expectCookies({
      tableMetadata: [
        {
          schema: 'my',
          table: 'foo',
          metadata: {rowKey: {type: 'default', columns: ['id']}},
        },
      ],
      backfilling: [
        {schema: 'your', table: 'bar', column: 'c', backfill: {fooID: 5}},
      ],
    });

    // 7. rename-table, which moves both cookies and only the renamed table's.
    await transaction({
      tag: 'rename-table',
      old: {schema: 'my', name: 'foo'},
      new: {schema: 'renamed', name: 'foo'},
    });
    await expectCookies({
      tableMetadata: [
        {
          schema: 'renamed',
          table: 'foo',
          metadata: {rowKey: {type: 'default', columns: ['id']}},
        },
      ],
      backfilling: [
        {schema: 'your', table: 'bar', column: 'c', backfill: {fooID: 5}},
      ],
    });

    // 8. drop-table, which clears both.
    await transaction(
      {tag: 'drop-table', id: {schema: 'renamed', name: 'foo'}},
      {tag: 'drop-table', id: {schema: 'your', name: 'bar'}},
    );
    await expectCookies({tableMetadata: [], backfilling: []});
  });

  test('index changes and metadata-free schema changes move nothing', async () => {
    await transaction({
      tag: 'create-table',
      spec: {schema: 'my', name: 'foo', columns: {}},
      metadata: {rowKey: {type: 'default', columns: ['id']}},
    });
    await transaction(
      {
        tag: 'create-index',
        spec: {
          schema: 'my',
          name: 'foo_idx',
          tableName: 'foo',
          unique: false,
          columns: {id: 'ASC'},
        },
      },
      {tag: 'drop-index', id: {schema: 'my', name: 'foo_idx'}},
      // A change source that does not support backfill sends neither field.
      {
        tag: 'add-column',
        table: {schema: 'my', name: 'foo'},
        column: {name: 'e', spec: {pos: 5, dataType: 'text'}},
      },
    );

    await expectCookies({
      tableMetadata: [
        {
          schema: 'my',
          table: 'foo',
          metadata: {rowKey: {type: 'default', columns: ['id']}},
        },
      ],
      backfilling: [],
    });
  });

  test('an aborted transaction leaves neither store holding its cookies', async () => {
    await transaction({
      tag: 'create-table',
      spec: {schema: 'my', name: 'foo', columns: {}},
      backfill: {a: {fooID: 1}},
    });

    // The stream is interrupted after the schema change and before the commit:
    // Postgres aborts its transaction pool, SQLite rolls its transaction back.
    const wm = String(++watermark).padStart(2, '0');
    const change: SchemaChange = {
      tag: 'add-column',
      table: {schema: 'my', name: 'foo'},
      column: {name: 'b', spec: {pos: 2, dataType: 'text'}},
      backfill: {fooID: 2},
    };
    storer.store(wm, ['begin', {tag: 'begin'}, {commitWatermark: wm}]);
    storer.store(wm, ['data', change]);

    const writer = new ChangeLogStreamWriter(new StatementRunner(changeLog));
    writer.begin(
      wm,
      serializeChangeStreamData([
        'begin',
        {tag: 'begin'},
        {commitWatermark: wm},
      ]),
    );
    writer.append(serializeChangeStreamData(['data', change]), change);
    writer.rollback();

    storer.abort();
    await storer.allProcessed();

    await expectCookies({
      tableMetadata: [],
      backfilling: [
        {schema: 'my', table: 'foo', column: 'a', backfill: {fooID: 1}},
      ],
    });
  });

  test('several cookie moves in one transaction stay in stream order', async () => {
    // The storer flushes its buffered changeLog batch before a schema change so
    // that the cookie DML stays in stream order relative to the rows; SQLite
    // gets that from statement order. Several cookie-moving changes in one
    // transaction is what would catch either one losing it.
    await transaction(
      {
        tag: 'create-table',
        spec: {schema: 'my', name: 'foo', columns: {}},
        backfill: {a: {fooID: 1}, b: {fooID: 2}},
      },
      {
        tag: 'backfill-completed',
        relation: {schema: 'my', name: 'foo', rowKey: {columns: ['a']}},
        columns: [],
        watermark: '07',
      },
      {
        tag: 'add-column',
        table: {schema: 'my', name: 'foo'},
        column: {name: 'c', spec: {pos: 3, dataType: 'text'}},
        backfill: {fooID: 3},
      },
    );

    // `a` completed, `b` never did, `c` started after the completion.
    await expectCookies({
      tableMetadata: [],
      backfilling: [
        {schema: 'my', table: 'foo', column: 'b', backfill: {fooID: 2}},
        {schema: 'my', table: 'foo', column: 'c', backfill: {fooID: 3}},
      ],
    });
  });
});
