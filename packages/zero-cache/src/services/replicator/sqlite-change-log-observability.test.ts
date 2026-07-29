import {existsSync, statSync} from 'node:fs';
import {LogContext} from '@rocicorp/logger';
import {afterEach, describe, expect, test} from 'vitest';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {DbFile} from '../../test/lite.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {
  extractChangeSubstring,
  serializeChangeStreamData,
} from '../change-streamer/change-log-codec.ts';
import {ChangeLogTransactionHasher} from '../change-streamer/change-log-transaction-hash.ts';
import {
  CHANGE_LOG_DB_SCHEMA_VERSION,
  CHANGE_LOG_META_TABLE,
  CHANGE_LOG_STREAM_TABLE,
  changeLogFileName,
  deleteChangeLogDB,
  openChangeLogDB,
  openChangeLogDBForWriting,
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
  sqliteFileBytes,
  SQLiteChangeLogObserver,
} from './sqlite-change-log-observability.ts';

function setupReplica() {
  const sink = new TestLogSink();
  const lc = new LogContext('debug', undefined, sink);
  const db = new Database(lc, ':memory:');
  // The replica's schema version is deliberately not the change log's. Nothing
  // here reads it — it is present so the tests below can show that the reported
  // schema version comes from the log rather than from this table.
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
      // The change log's own version, from its meta table — not the replica's
      // 15, which the two databases now version independently.
      schemaVersion: CHANGE_LOG_DB_SCHEMA_VERSION,
      stateWatermark: '02',
      seedWatermark: '02',
      headWatermark: '02',
      rows: 2,
      estimatedBytes: expect.any(Number),
    });
    expect(info.schemaVersion).not.toBe(15);
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
            schemaVersion: CHANGE_LOG_DB_SCHEMA_VERSION,
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
      schemaVersion: CHANGE_LOG_DB_SCHEMA_VERSION,
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
  // own state and neither a schema version nor a head. The replica has not
  // carried a copy of the log since schema v15, so there is nothing else it
  // could report.
  test('a disabled writer reports replica state without a head', () => {
    using fixture = setupReplica();
    const info = getReplicaChangeLogInfo(fixture.db);

    expect(info).toEqual({
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
            schemaVersion: undefined,
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

  // Invariant 11: the log's replicaVersion always equals the replica's.
  // Reconciliation guarantees it by wiping, so reaching the observability seam
  // with a mismatch means the log was never reconciled against this replica.
  test('startup info throws on a log anchored to another replica', () => {
    using fixture = setupReplica();
    fixture.changeLog
      .prepare(`UPDATE "${CHANGE_LOG_META_TABLE}" SET "replicaVersion" = '01'`)
      .run();

    expect(() =>
      getSQLiteChangeLogStartupInfo(fixture.db, fixture.changeLog),
    ).toThrow('SQLite change log is anchored to replica version 01, not 02');
  });

  test('startup info throws on a log with no meta row', () => {
    using fixture = setupReplica();
    fixture.changeLog.prepare(`DELETE FROM "${CHANGE_LOG_META_TABLE}"`).run();

    expect(() =>
      getSQLiteChangeLogStartupInfo(fixture.db, fixture.changeLog),
    ).toThrow('SQLite change log must have a meta row');
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

describe('SQLite change-log observability on disk', () => {
  let dbFile: DbFile | undefined;

  afterEach(() => {
    if (dbFile) {
      deleteChangeLogDB(dbFile.path);
      dbFile.delete();
      dbFile = undefined;
    }
  });

  function setupFiles() {
    const sink = new TestLogSink();
    const lc = new LogContext('debug', undefined, sink);
    dbFile = new DbFile('sqlite-change-log-observability');
    const replica = dbFile.connect(lc);
    initReplicationState(replica, ['zero_data'], '02');
    const {db: changeLog} = openChangeLogDBForWriting(
      lc,
      dbFile.path,
      readReplicaAnchor(replica),
    );
    return {
      lc,
      sink,
      replica,
      changeLog,
      file: dbFile.path,
      [Symbol.dispose]() {
        changeLog.close();
        replica.close();
      },
    };
  }

  // Metrics are gathered from the replicator process, which holds the log
  // read-only while the write worker owns the writable handle. A read-only
  // SQLite connection cannot open a write transaction, so a successful read
  // through one is the assertion that gathering costs the writer nothing.
  test('gathers info through a read-only change-log handle', () => {
    using files = setupFiles();
    files.changeLog.close();
    using changeLog = openChangeLogDB(files.lc, files.file, {readonly: true});

    expect(getSQLiteChangeLogInfo(files.replica, changeLog)).toMatchObject({
      schemaVersion: CHANGE_LOG_DB_SCHEMA_VERSION,
      stateWatermark: '02',
      headWatermark: '02',
      rows: 2,
    });
    // The handle the read went through would have rejected any write.
    expect(() =>
      changeLog.prepare(`DELETE FROM "${CHANGE_LOG_STREAM_TABLE}"`).run(),
    ).toThrow(/readonly/);
  });

  test('file bytes sum the database and its wal sidecars', async () => {
    using files = setupFiles();
    // Written but not checkpointed, so the sidecars hold bytes the main file
    // does not — which is the whole reason the gauge is a sum.
    files.changeLog
      .prepare(/*sql*/ `
        INSERT INTO "${CHANGE_LOG_STREAM_TABLE}"
          ("watermark", "pos", "change") VALUES ('03', 0, ?)
      `)
      .run('x'.repeat(100_000));

    const log = changeLogFileName(files.file);
    const paths = [log, `${log}-wal`, `${log}-wal2`].filter(p => existsSync(p));
    const expected = paths.reduce((sum, p) => sum + statSync(p).size, 0);

    expect(paths.length).toBeGreaterThan(1);
    expect(await sqliteFileBytes(files.lc, log)).toBe(expected);
    expect(expected).toBeGreaterThan(statSync(log).size);
  });

  test('file bytes count an absent database as zero, silently', async () => {
    using files = setupFiles();

    // The change log does not exist until the writer creates it, and which
    // sidecars exist depends on the journal mode, so neither is a warning.
    expect(await sqliteFileBytes(files.lc, `${files.file}-nonexistent`)).toBe(
      0,
    );
    expect(files.sink.messages.filter(([level]) => level === 'warn')).toEqual(
      [],
    );
  });

  test('file bytes warn on a stat failure that is not a missing file', async () => {
    using files = setupFiles();
    // A path under a regular file: ENOTDIR, not ENOENT. Absence is expected
    // and silent; anything else says something is wrong with the environment.
    const bad = `${files.file}/under-a-file`;

    expect(await sqliteFileBytes(files.lc, bad)).toBe(0);
    // Unordered: the three files are stat'd concurrently.
    expect(
      files.sink.messages
        .filter(([level]) => level === 'warn')
        .map(([, , args]) => String(args[0]))
        .sort((a, b) => (a < b ? -1 : a > b ? 1 : 0)),
    ).toEqual([
      `unable to stat ${bad} for size metrics`,
      `unable to stat ${bad}-wal for size metrics`,
      `unable to stat ${bad}-wal2 for size metrics`,
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
