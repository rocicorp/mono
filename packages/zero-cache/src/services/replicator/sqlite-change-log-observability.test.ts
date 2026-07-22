import {LogContext} from '@rocicorp/logger';
import {describe, expect, test} from 'vitest';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {initReplicationState} from './schema/replication-state.ts';
import {
  getSQLiteChangeLogInfo,
  logSQLiteChangeLogStartup,
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
      VALUES (14, 14, 0);
  `);
  initReplicationState(db, ['zero_data'], '02');
  return {
    db,
    lc,
    sink,
    [Symbol.dispose]() {
      db.close();
    },
  };
}

describe('SQLite change-log observability', () => {
  test('reads and logs startup state', () => {
    using fixture = setupReplica();
    const info = getSQLiteChangeLogInfo(fixture.db);

    expect(info).toMatchObject({
      schemaVersion: 14,
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
            schemaVersion: 14,
            seedWatermark: '02',
            headWatermark: '02',
            stateWatermark: '02',
          },
        },
      ],
    ]);
  });

  test('reports temporary head skew without an invariant failure', () => {
    using fixture = setupReplica();
    fixture.db
      .prepare(`UPDATE "_zero.replicationState" SET "stateVersion" = '06'`)
      .run();
    const observer = new SQLiteChangeLogObserver(
      fixture.lc,
      getSQLiteChangeLogInfo(fixture.db),
    );

    expect(observer.state()).toMatchObject({
      receivedHead: '06',
      sqliteHead: '02',
      headLag: 4,
      rows: 2,
      rollbacks: 0,
      invariantFailures: 0,
    });

    observer.messageProcessed(
      ['begin', {tag: 'begin'}, {commitWatermark: '07'}],
      null,
      1,
    );
    observer.messageReceived(['commit', {tag: 'commit'}, {watermark: '07'}]);
    expect(observer.state()).toMatchObject({
      receivedHead: '07',
      sqliteHead: '02',
      headLag: 5,
      invariantFailures: 0,
    });

    observer.messageProcessed(
      ['commit', {tag: 'commit'}, {watermark: '07'}],
      {
        watermark: '07',
        completedBackfill: undefined,
        schemaUpdated: false,
        changeLogUpdated: false,
        changeLogStream: {rows: 2, estimatedBytes: 100},
      },
      2,
    );
    expect(observer.state()).toMatchObject({
      receivedHead: '07',
      sqliteHead: '07',
      headLag: 0,
      rows: 4,
      estimatedBytes: getSQLiteChangeLogInfo(fixture.db).estimatedBytes + 100,
      invariantFailures: 0,
    });
  });

  test('counts upstream, interrupted, and failed transaction rollbacks', () => {
    using fixture = setupReplica();
    const observer = new SQLiteChangeLogObserver(
      fixture.lc,
      getSQLiteChangeLogInfo(fixture.db),
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
      getSQLiteChangeLogInfo(fixture.db),
    );
    const invariantError = new Error('stream position mismatch');
    invariantError.name = 'ChangeLogStreamInvariantError';
    observer.messageFailed(
      ['begin', {tag: 'begin'}, {commitWatermark: '03'}],
      invariantError,
      1,
    );
    observer.messageProcessed(
      ['begin', {tag: 'begin'}, {commitWatermark: '04'}],
      null,
      1,
    );
    observer.messageReceived(['commit', {tag: 'commit'}, {watermark: '04'}]);
    observer.messageProcessed(
      ['commit', {tag: 'commit'}, {watermark: '04'}],
      {
        watermark: '04',
        completedBackfill: undefined,
        schemaUpdated: false,
        changeLogUpdated: false,
      },
      1,
    );

    expect(observer.state().invariantFailures).toBe(2);
  });
});
