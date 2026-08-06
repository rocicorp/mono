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
import {EMPTY_COOKIE_SET} from './change-log-cookies.ts';
import {
  CHANGE_LOG_DB_SCHEMA_VERSION,
  CHANGE_LOG_META_TABLE,
  CHANGE_LOG_STREAM_TABLE,
  changeLogFileName,
  deleteChangeLogDB,
  openChangeLogDB,
  openChangeLogDBForWriting,
  reconcileChangeLog,
  type ChangeLogAnchor,
  type ReconcileResult,
} from './change-log-db.ts';
import {
  getSQLiteChangeLogInfo,
  getSQLiteChangeLogStartupInfo,
  logSQLiteChangeLogStartup,
  recordSQLiteChangeLogReconcile,
  sqliteFileBytes,
  SQLiteChangeLogObserver,
  type ChangeLogCommit,
} from './sqlite-change-log-observability.ts';

const ANCHOR: ChangeLogAnchor = {
  identity: {epoch: null, generation: '01', replicaID: '1777575698286'},
  resumeWatermark: '02',
  nowMs: 1_700_000_000_000,
  cookies: EMPTY_COOKIE_SET,
};

/**
 * A reconciled change log, with no replica beside it: everything reported here
 * comes from the log itself.
 */
function setupChangeLog() {
  const sink = new TestLogSink();
  const lc = new LogContext('debug', undefined, sink);
  const changeLog = new Database(lc, ':memory:');
  reconcileChangeLog(lc, changeLog, ANCHOR);
  return {
    changeLog,
    lc,
    sink,
    [Symbol.dispose]() {
      changeLog.close();
    },
  };
}

function receive(
  observer: SQLiteChangeLogObserver,
  data: ChangeStreamData,
): void {
  observer.messageReceived(data, serializeChangeStreamData(data));
}

function commitResult(
  watermark: string,
  hash: string,
  estimatedBytes = 100,
): ChangeLogCommit {
  return {
    watermark,
    stats: {rows: 2, estimatedBytes, hash, cookieMutations: []},
    logCommitMs: 0.009,
  };
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
    using fixture = setupChangeLog();
    const info = getSQLiteChangeLogInfo(fixture.changeLog);

    expect(info).toEqual({
      epoch: null,
      generation: '01',
      replicaID: '1777575698286',
      schemaVersion: CHANGE_LOG_DB_SCHEMA_VERSION,
      seededAtMs: ANCHOR.nowMs,
      seedWatermark: '02',
      headWatermark: '02',
      rows: 2,
      estimatedBytes: expect.any(Number),
      cookieRows: {tableMetadata: 0, backfilling: 0},
    });
    expect(info.estimatedBytes).toBeGreaterThan(0);

    logSQLiteChangeLogStartup(fixture.lc, '/data/replica.db-change-log', info);
    expect(fixture.sink.messages.at(-1)).toEqual([
      'info',
      undefined,
      [
        'SQLite change-log startup',
        {
          sqliteChangeLog: {
            file: '/data/replica.db-change-log',
            epoch: null,
            generation: '01',
            replicaID: '1777575698286',
            schemaVersion: CHANGE_LOG_DB_SCHEMA_VERSION,
            seedWatermark: '02',
            seededAtMs: ANCHOR.nowMs,
            headWatermark: '02',
          },
        },
      ],
    ]);
  });

  test('startup info skips the row/byte aggregate', () => {
    using fixture = setupChangeLog();
    const startupInfo = getSQLiteChangeLogStartupInfo(fixture.changeLog);

    // Same meta and head as the full read, but no table-scanning totals.
    expect(startupInfo).toEqual({
      epoch: null,
      generation: '01',
      replicaID: '1777575698286',
      schemaVersion: CHANGE_LOG_DB_SCHEMA_VERSION,
      seededAtMs: ANCHOR.nowMs,
      seedWatermark: '02',
      headWatermark: '02',
    });
    const {
      rows: _rows,
      estimatedBytes: _bytes,
      cookieRows: _cookieRows,
      ...head
    } = getSQLiteChangeLogInfo(fixture.changeLog);
    expect(startupInfo).toEqual(head);
  });

  test('startup info throws on an unseeded change log', () => {
    using fixture = setupChangeLog();
    // The writer reads this after reconciliation, which cannot leave the log
    // empty, so this is the assertion that catches a caller reading the log
    // before it has been reconciled.
    fixture.changeLog.prepare(`DELETE FROM "${CHANGE_LOG_STREAM_TABLE}"`).run();

    expect(() => getSQLiteChangeLogStartupInfo(fixture.changeLog)).toThrow(
      'the SQLite change log must contain its seed transaction',
    );
  });

  test('startup info throws on a log with no meta row', () => {
    using fixture = setupChangeLog();
    fixture.changeLog.prepare(`DELETE FROM "${CHANGE_LOG_META_TABLE}"`).run();

    expect(() => getSQLiteChangeLogStartupInfo(fixture.changeLog)).toThrow(
      'the SQLite change log has no meta row',
    );
  });

  // The log commits before the forward, so the received head trails its own
  // head except *within* a transaction, which is the window this measures.
  test('reports intra-transaction head skew without an invariant failure', () => {
    using fixture = setupChangeLog();
    const observer = new SQLiteChangeLogObserver(
      fixture.lc,
      getSQLiteChangeLogInfo(fixture.changeLog),
    );

    expect(observer.state()).toMatchObject({
      receivedHead: '02',
      sqliteHead: '02',
      headLag: 0,
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
      commitResult('07', twoRowTransactionHash('07')),
      2,
    );
    expect(observer.state()).toMatchObject({
      receivedHead: '07',
      sqliteHead: '07',
      headLag: 0,
      rows: 4,
      estimatedBytes:
        getSQLiteChangeLogInfo(fixture.changeLog).estimatedBytes + 100,
      invariantFailures: 0,
      hashMatches: 1,
      hashMismatches: 0,
      hashUnpaired: 0,
    });
  });

  test('counts upstream, interrupted, and failed transaction rollbacks', () => {
    using fixture = setupChangeLog();
    const observer = new SQLiteChangeLogObserver(
      fixture.lc,
      getSQLiteChangeLogInfo(fixture.changeLog),
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
    using fixture = setupChangeLog();
    const observer = new SQLiteChangeLogObserver(
      fixture.lc,
      getSQLiteChangeLogInfo(fixture.changeLog),
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
    observer.messageProcessed(commit, null, 1);

    expect(observer.state().invariantFailures).toBe(2);
    expect(observer.state().hashUnpaired).toBe(1);
  });

  // A missed truncate-above surfaces as a constraint violation on the writer's
  // plain INSERT, which fail-soft would otherwise absorb quietly. This is the
  // seam that keeps it visible.
  test('records a reported invariant failure', () => {
    using fixture = setupChangeLog();
    const observer = new SQLiteChangeLogObserver(
      fixture.lc,
      getSQLiteChangeLogInfo(fixture.changeLog),
    );

    observer.invariantFailure('constraint violated', {tag: 'begin'});

    expect(observer.state().invariantFailures).toBe(1);
    expect(fixture.sink.messages.at(-1)).toEqual([
      'error',
      {component: 'sqlite-change-log-observer'},
      ['constraint violated', {tag: 'begin'}],
    ]);
  });

  test('restates its totals when reconciliation moves the head', () => {
    using fixture = setupChangeLog();
    const observer = new SQLiteChangeLogObserver(
      fixture.lc,
      getSQLiteChangeLogInfo(fixture.changeLog),
    );
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
    observer.messageProcessed(
      commit,
      commitResult('07', twoRowTransactionHash('07')),
      1,
    );
    expect(observer.state()).toMatchObject({sqliteHead: '07', rows: 4});

    // A reconnect truncated '07' away, so the observer's head and totals are
    // restated from the log rather than left describing rows that are gone.
    reconcileChangeLog(fixture.lc, fixture.changeLog, ANCHOR);
    observer.reconciled(getSQLiteChangeLogInfo(fixture.changeLog));

    expect(observer.state()).toMatchObject({
      receivedHead: '02',
      sqliteHead: '02',
      headLag: 0,
      rows: 2,
    });
  });

  test('counts and logs a transaction hash mismatch', () => {
    using fixture = setupChangeLog();
    const observer = new SQLiteChangeLogObserver(
      fixture.lc,
      getSQLiteChangeLogInfo(fixture.changeLog),
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
    observer.messageProcessed(commit, commitResult('03', '0'.repeat(64)), 1);

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
    const {db: changeLog} = openChangeLogDBForWriting(lc, dbFile.path, ANCHOR);
    return {
      lc,
      sink,
      changeLog,
      file: dbFile.path,
      [Symbol.dispose]() {
        changeLog.close();
      },
    };
  }

  // The writer owns the writable handle, so anything else — a metrics scrape, a
  // catchup read — holds the log read-only. A read-only SQLite connection cannot
  // open a write transaction, so a successful read through one is the assertion
  // that gathering costs the writer nothing.
  test('gathers info through a read-only change-log handle', () => {
    using files = setupFiles();
    files.changeLog.close();
    using changeLog = openChangeLogDB(files.lc, files.file, {readonly: true});

    expect(getSQLiteChangeLogInfo(changeLog)).toMatchObject({
      schemaVersion: CHANGE_LOG_DB_SCHEMA_VERSION,
      seedWatermark: '02',
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
    const bad = `${changeLogFileName(files.file)}/under-a-file`;

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
  // means the log did not lead the resume watermark, and is the one on the
  // alert list.
  const cases: [name: string, result: ReconcileResult][] = [
    ['consistent', {action: 'none', head: '05', cookiesStale: false}],
    [
      'phantom truncation',
      {action: 'truncated', head: '05', rows: 27, cookiesStale: true},
    ],
    [
      'wipe: created',
      {action: 'reseeded', head: '05', reason: 'created', cookiesStale: true},
    ],
    [
      'wipe: schema-mismatch',
      {
        action: 'reseeded',
        head: '05',
        reason: 'schema-mismatch',
        cookiesStale: true,
      },
    ],
    [
      'wipe: identity-mismatch',
      {
        action: 'reseeded',
        head: '05',
        reason: 'identity-mismatch',
        cookiesStale: true,
      },
    ],
    [
      'wipe: gap',
      {action: 'reseeded', head: '05', reason: 'gap', cookiesStale: true},
    ],
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
