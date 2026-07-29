import {LogContext} from '@rocicorp/logger';
import {describe, expect, test} from 'vitest';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {
  extractChangeSubstring,
  serializeChangeStreamData,
} from '../change-streamer/change-log-codec.ts';
import {ChangeLogTransactionHasher} from '../change-streamer/change-log-transaction-hash.ts';
import {
  CHANGE_LOG_STREAM_TABLE,
  readReplicaAnchor,
  reconcileChangeLog,
  type ReconcileResult,
} from './change-log-db.ts';
import {initReplicationState} from './schema/replication-state.ts';
import {
  getReplicaChangeLogInfo,
  getSQLiteChangeLogInfo,
  getSQLiteChangeLogStartupInfo,
  logSQLiteChangeLogStartup,
  recordSQLiteChangeLogReconcile,
  SQLiteChangeLogObserver,
} from './sqlite-change-log-observability.ts';

function setupReplica() {
  const sink = new TestLogSink();
  const lc = new LogContext('debug', undefined, sink);
  const db = new Database(lc, ':memory:');
  db.exec(/*sql*/ `
    CREATE TABLE "_zero.versionHistory" (
      "dataVersion" INTEGER NOT NULL,
      "schemaVersion" INTEGER NOT NULL,
      "minSafeVersion" INTEGER NOT NULL,
      "lock" INTEGER PRIMARY KEY DEFAULT 1 CHECK ("lock" = 1)
    );
    INSERT INTO "_zero.versionHistory"
      ("dataVersion", "schemaVersion", "minSafeVersion")
      VALUES (15, 15, 0);
  `);
  initReplicationState(db, ['zero_data'], '02');
  // The change log is a separate database as of replica schema v15. Seed it at
  // the replica head the way the writer's startup reconciliation does.
  const changeLog = new Database(lc, ':memory:');
  reconcileChangeLog(lc, changeLog, readReplicaAnchor(db));
  return {
    db,
    changeLog,
    lc,
    sink,
    [Symbol.dispose]() {
      changeLog.close();
      db.close();
    },
  };
}

function receive(
  observer: SQLiteChangeLogObserver,
  data: ChangeStreamData,
): void {
  observer.messageReceived(data, serializeChangeStreamData(data));
}

function twoRowTransactionHash(watermark: string): string {
  const begin: ChangeStreamData = [
    'begin',
    {tag: 'begin'},
    {commitWatermark: watermark},
  ];
  const commit: ChangeStreamData = ['commit', {tag: 'commit'}, {watermark}];
  const hasher = new ChangeLogTransactionHasher();
  hasher.add({
    watermark,
    pos: 0,
    tag: 'begin',
    change: extractChangeSubstring(serializeChangeStreamData(begin), 'begin'),
    precommit: null,
  });
  hasher.add({
    watermark,
    pos: 1,
    tag: 'commit',
    change: extractChangeSubstring(serializeChangeStreamData(commit), 'commit'),
    precommit: watermark,
  });
  return hasher.digest();
}

describe('SQLite change-log observability', () => {
  test('reads and logs startup state', () => {
    using fixture = setupReplica();
    const info = getSQLiteChangeLogInfo(fixture.db, fixture.changeLog);

    expect(info).toMatchObject({
      schemaVersion: 15,
      stateWatermark: '02',
      seedWatermark: '02',
      headWatermark: '02',
      rows: 2,
      estimatedBytes: expect.any(Number),
    });
    expect(info.estimatedBytes).toBeGreaterThan(0);

    logSQLiteChangeLogStartup(fixture.lc, 'backup', true, info);
    expect(fixture.sink.messages.at(-1)).toEqual([
      'info',
      undefined,
      [
        'SQLite change-log startup',
        {
          sqliteChangeLog: {
            fileMode: 'backup',
            writerEnabled: true,
            schemaVersion: 15,
            seedWatermark: '02',
            headWatermark: '02',
            stateWatermark: '02',
          },
        },
      ],
    ]);
  });

  test('startup info skips the row/byte aggregate', () => {
    using fixture = setupReplica();
    const startupInfo = getSQLiteChangeLogStartupInfo(
      fixture.db,
      fixture.changeLog,
    );

    // Same schema/state/head as the full read, but no table-scanning totals.
    expect(startupInfo).toEqual({
      schemaVersion: 15,
      stateWatermark: '02',
      seedWatermark: '02',
      headWatermark: '02',
    });
    const {
      rows: _rows,
      estimatedBytes: _bytes,
      ...head
    } = getSQLiteChangeLogInfo(fixture.db, fixture.changeLog);
    expect(startupInfo).toEqual(head);
  });

  // A disabled writer opens no change-log database, so it reports the replica's
  // own state and no head. The replica has not carried a copy of the log since
  // schema v15, so there is nothing else it could report.
  test('a disabled writer reports replica state without a head', () => {
    using fixture = setupReplica();
    const info = getReplicaChangeLogInfo(fixture.db);

    expect(info).toEqual({
      schemaVersion: 15,
      stateWatermark: '02',
      seedWatermark: '02',
    });

    logSQLiteChangeLogStartup(fixture.lc, 'serving', false, info);
    expect(fixture.sink.messages.at(-1)).toEqual([
      'info',
      undefined,
      [
        'SQLite change-log startup',
        {
          sqliteChangeLog: {
            fileMode: 'serving',
            writerEnabled: false,
            schemaVersion: 15,
            seedWatermark: '02',
            headWatermark: undefined,
            stateWatermark: '02',
          },
        },
      ],
    ]);
  });

  test('startup info throws on an unseeded change log', () => {
    using fixture = setupReplica();
    // The writer path runs after reconciliation, which cannot leave the log
    // empty, so this is the assertion that catches a caller reading the log
    // before it has been reconciled.
    fixture.changeLog.prepare(`DELETE FROM "${CHANGE_LOG_STREAM_TABLE}"`).run();

    expect(() =>
      getSQLiteChangeLogStartupInfo(fixture.db, fixture.changeLog),
    ).toThrow('SQLite change log must contain its seed transaction');
  });

  test('reports temporary head skew without an invariant failure', () => {
    using fixture = setupReplica();
    fixture.db
      .prepare(`UPDATE "_zero.replicationState" SET "stateVersion" = '06'`)
      .run();
    const observer = new SQLiteChangeLogObserver(
      fixture.lc,
      getSQLiteChangeLogInfo(fixture.db, fixture.changeLog),
    );

    expect(observer.state()).toMatchObject({
      receivedHead: '06',
      sqliteHead: '02',
      headLag: 4,
      rows: 2,
      rollbacks: 0,
      invariantFailures: 0,
    });

    const begin: ChangeStreamData = [
      'begin',
      {tag: 'begin'},
      {commitWatermark: '07'},
    ];
    const commit: ChangeStreamData = [
      'commit',
      {tag: 'commit'},
      {watermark: '07'},
    ];
    receive(observer, begin);
    observer.messageProcessed(begin, null, 1);
    receive(observer, commit);
    expect(observer.state()).toMatchObject({
      receivedHead: '07',
      sqliteHead: '02',
      headLag: 5,
      invariantFailures: 0,
    });

    observer.messageProcessed(
      commit,
      {
        watermark: '07',
        completedBackfill: undefined,
        schemaUpdated: false,
        changeLogUpdated: false,
        changeLogStream: {
          rows: 2,
          estimatedBytes: 100,
          hash: twoRowTransactionHash('07'),
        },
      },
      2,
    );
    expect(observer.state()).toMatchObject({
      receivedHead: '07',
      sqliteHead: '07',
      headLag: 0,
      rows: 4,
      estimatedBytes:
        getSQLiteChangeLogInfo(fixture.db, fixture.changeLog).estimatedBytes +
        100,
      invariantFailures: 0,
      hashMatches: 1,
      hashMismatches: 0,
      hashUnpaired: 0,
    });
  });

  test('counts upstream, interrupted, and failed transaction rollbacks', () => {
    using fixture = setupReplica();
    const observer = new SQLiteChangeLogObserver(
      fixture.lc,
      getSQLiteChangeLogInfo(fixture.db, fixture.changeLog),
    );

    observer.messageProcessed(
      ['begin', {tag: 'begin'}, {commitWatermark: '03'}],
      null,
      1,
    );
    observer.messageProcessed(['rollback', {tag: 'rollback'}], null, 1);
    observer.messageProcessed(
      ['begin', {tag: 'begin'}, {commitWatermark: '04'}],
      null,
      1,
    );
    observer.abort();
    observer.messageFailed(
      ['begin', {tag: 'begin'}, {commitWatermark: '05'}],
      new Error('write failed'),
      1,
    );

    expect(observer.state()).toMatchObject({
      rollbacks: 3,
      invariantFailures: 0,
    });
  });

  test('counts writer invariant errors and malformed commit results', () => {
    using fixture = setupReplica();
    const observer = new SQLiteChangeLogObserver(
      fixture.lc,
      getSQLiteChangeLogInfo(fixture.db, fixture.changeLog),
    );
    const invariantError = new Error('stream position mismatch');
    invariantError.name = 'ChangeLogStreamInvariantError';
    observer.messageFailed(
      ['begin', {tag: 'begin'}, {commitWatermark: '03'}],
      invariantError,
      1,
    );
    const begin: ChangeStreamData = [
      'begin',
      {tag: 'begin'},
      {commitWatermark: '04'},
    ];
    const commit: ChangeStreamData = [
      'commit',
      {tag: 'commit'},
      {watermark: '04'},
    ];
    receive(observer, begin);
    observer.messageProcessed(begin, null, 1);
    receive(observer, commit);
    observer.messageProcessed(
      commit,
      {
        watermark: '04',
        completedBackfill: undefined,
        schemaUpdated: false,
        changeLogUpdated: false,
      },
      1,
    );

    expect(observer.state().invariantFailures).toBe(2);
    expect(observer.state().hashUnpaired).toBe(1);
  });

  test('counts and logs a transaction hash mismatch', () => {
    using fixture = setupReplica();
    const observer = new SQLiteChangeLogObserver(
      fixture.lc,
      getSQLiteChangeLogInfo(fixture.db, fixture.changeLog),
    );
    const begin: ChangeStreamData = [
      'begin',
      {tag: 'begin'},
      {commitWatermark: '03'},
    ];
    const commit: ChangeStreamData = [
      'commit',
      {tag: 'commit'},
      {watermark: '03'},
    ];
    receive(observer, begin);
    observer.messageProcessed(begin, null, 1);
    receive(observer, commit);
    observer.messageProcessed(
      commit,
      {
        watermark: '03',
        completedBackfill: undefined,
        schemaUpdated: false,
        changeLogUpdated: false,
        changeLogStream: {
          rows: 2,
          estimatedBytes: 100,
          hash: '0'.repeat(64),
        },
      },
      1,
    );

    expect(observer.state()).toMatchObject({
      hashMatches: 0,
      hashMismatches: 1,
      hashUnpaired: 0,
    });
    expect(fixture.sink.messages.at(-1)).toEqual([
      'error',
      {component: 'sqlite-change-log-observer'},
      [
        'SQLite change-log transaction hash mismatch',
        {
          sqliteChangeLogHashComparison: {
            watermark: '03',
            sourceHash: twoRowTransactionHash('03').slice(0, 16),
            sqliteHash: '0'.repeat(16),
          },
        },
      ],
    ]);
  });
});

describe('replicator/sqlite-change-log reconcile reporting', () => {
  // Every ReconcileResult, including each wipe reason: `gap` in steady state
  // means log-first ordering is not holding, and is the one on the alert list.
  const cases: [name: string, result: ReconcileResult][] = [
    ['consistent', {action: 'none', head: '05'}],
    ['phantom truncation', {action: 'truncated', head: '05', rows: 27}],
    ['wipe: created', {action: 'reseeded', head: '05', reason: 'created'}],
    [
      'wipe: schema-mismatch',
      {action: 'reseeded', head: '05', reason: 'schema-mismatch'},
    ],
    [
      'wipe: replica-version-mismatch',
      {action: 'reseeded', head: '05', reason: 'replica-version-mismatch'},
    ],
    ['wipe: gap', {action: 'reseeded', head: '05', reason: 'gap'}],
  ];

  test.each(cases)('%s', (_name, result) => {
    const sink = new TestLogSink();
    const lc = new LogContext('debug', undefined, sink);

    recordSQLiteChangeLogReconcile(lc, result);

    expect(sink.messages.at(-1)).toEqual([
      'debug',
      undefined,
      ['SQLite change-log reconciliation', {sqliteChangeLogReconcile: result}],
    ]);
  });
});
