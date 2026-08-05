import {existsSync, writeFileSync} from 'node:fs';
import {PG_LOCK_NOT_AVAILABLE} from '@drdgvhbh/postgres-error-codes';
import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import postgres from 'postgres';
import {beforeEach, describe, expect, vi, type Mock} from 'vitest';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {assert} from '../../../../shared/src/asserts.ts';
import {BigIntJSON, stringify} from '../../../../shared/src/bigint-json.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {expectTables, test, type PgTest} from '../../test/db.ts';
import {DbFile} from '../../test/lite.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription, type Result} from '../../types/subscription.ts';
import type {ChangeSource} from '../change-source/change-source.ts';
import type {
  ChangeStreamData,
  ChangeStreamMessage,
} from '../change-source/protocol/current/downstream.ts';
import type {UpstreamStatusMessage} from '../change-source/protocol/current/status.ts';
import {exitAfter} from '../life-cycle.ts';
import type {LitestreamVersion} from '../litestream/metrics.ts';
import {
  changeLogFileName,
  deleteChangeLogDB,
  openChangeLogDB,
  readChangeLogHead,
  reconcileChangeLog,
  type ChangeLogAnchor,
} from '../replicator/change-log-db.ts';
import {ChangeLogStreamWriter} from '../replicator/change-log-stream-writer.ts';
import {ReplicationStatusPublisher} from '../replicator/replication-status.ts';
import {
  getSubscriptionState,
  initReplicationState,
  updateReplicationWatermark,
  type SubscriptionState,
} from '../replicator/schema/replication-state.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {serializeChangeStreamData} from './change-log-codec.ts';
import {
  initializeStreamer,
  type SQLiteCatchupOptions,
  type TuningOptions,
} from './change-streamer-service.ts';
import {
  PROTOCOL_VERSION,
  type ChangeStreamerService,
  type Downstream,
} from './change-streamer.ts';
import * as ErrorType from './error-type-enum.ts';
import {Forwarder} from './forwarder.ts';
import {initChangeStreamerSchema} from './schema/init.ts';
import {AutoResetSignal, ensureReplicationConfig} from './schema/tables.ts';
import type {SnapshotMessage} from './snapshot.ts';
import {SQLiteChangeLogCatchup} from './sqlite-change-log-catchup.ts';
import {SQLiteChangeLogReader} from './sqlite-change-log-reader.ts';
import {SQLiteChangeLogWriter} from './sqlite-change-log-writer.ts';
import {PurgeLocker, Storer} from './storer.ts';

const opts: TuningOptions = {
  backPressureLimitHeapProportion: 0.04,
  flowControlConsensusPaddingSeconds: 1,
  statementTimeoutMs: 20_000,
  changeLogBatchSize: 2000,
};

describe('change-streamer/service', () => {
  let lc: LogContext;
  let replicaConfig: SubscriptionState;
  let sql: PostgresDB;
  let streamer: ChangeStreamerService;
  let changes: Subscription<ChangeStreamMessage>;
  let acks: Queue<UpstreamStatusMessage>;
  let streamerDone: Promise<void>;
  let logSink: TestLogSink;

  // vi.useFakeTimers() does not play well with the postgres client.
  // Inject a manual mock instead.
  let setTimeoutFn: Mock<typeof setTimeout>;

  const REPLICA_VERSION = '01';
  const shard = {appID: 'zoro', shardNum: 3};

  beforeEach<PgTest>(async ({testDBs}) => {
    logSink = new TestLogSink();
    lc = new LogContext('debug', undefined, logSink);

    sql = await testDBs.create('change_streamer_test_change_db', {
      typeOpts: {sendStringAsJson: true},
    });

    const replica = new Database(lc, ':memory:');
    initReplicationState(replica, ['zero_data'], REPLICA_VERSION);
    replicaConfig = getSubscriptionState(new StatementRunner(replica));

    changes = Subscription.create();
    acks = new Queue();
    setTimeoutFn = vi.fn();

    await initChangeStreamerSchema(lc, sql, shard);
    streamer = await initializeStreamer(
      lc,
      shard,
      'task-id',
      'change.streamer:12345',
      'ws',
      sql,
      {
        startStream: () =>
          Promise.resolve({
            initialWatermark: '02',
            changes,
            acks: {push: status => acks.enqueue(status)},
          }),
        startLagReporter: () =>
          Promise.resolve({firstCommitTimeMs: 100, nextSendTimeMs: 123}),
        stop: () => Promise.resolve(),
      },
      ReplicationStatusPublisher.forTesting(),
      replicaConfig,
      null,
      null,
      true,
      opts,
      setTimeoutFn as unknown as typeof setTimeout,
    );
    streamerDone = streamer.run();

    return async () => {
      await streamer.stop();
      await testDBs.drop(sql);
    };
  });

  function drainToQueue(sub: Source<string>): Queue<Downstream> {
    const queue = new Queue<Downstream>();
    void (async () => {
      for await (const msg of sub) {
        queue.enqueue(BigIntJSON.parse(msg) as Downstream);
      }
    })();
    return queue;
  }

  function drainSnapshotMessages(
    sub: Source<SnapshotMessage>,
  ): Queue<SnapshotMessage> {
    const queue = new Queue<SnapshotMessage>();
    void (async () => {
      for await (const msg of sub) {
        queue.enqueue(msg);
      }
    })();
    return queue;
  }

  // Creates and runs a second ChangeStreamerImpl configured with a
  // `backupURL`, i.e. with snapshot reservations enabled. Callers that use
  // this must first `await streamer.stop()` to release the default
  // (backup-less) streamer's ownership of the change DB.
  async function newBackupStreamer(
    backupURL: string,
    source: Partial<ChangeSource> = {},
    litestreamVersion: LitestreamVersion = 'legacy',
  ): Promise<ChangeStreamerService> {
    const backupStreamer = await initializeStreamer(
      lc,
      shard,
      'task-id',
      'change.streamer:12345',
      'ws',
      sql,
      {
        startStream: () =>
          Promise.resolve({
            initialWatermark: '02',
            changes: Subscription.create(),
            acks: {push: () => {}},
          }),
        startLagReporter: () => Promise.resolve(null),
        stop: () => Promise.resolve(),
        ...source,
      } satisfies ChangeSource,
      ReplicationStatusPublisher.forTesting(),
      replicaConfig,
      {backupURL, litestreamVersion},
      null,
      true,
      opts,
      setTimeoutFn as unknown as typeof setTimeout,
    );
    void backupStreamer.run();
    return backupStreamer;
  }

  async function nextChange(sub: Queue<Downstream>) {
    const down = await sub.dequeue();
    assert(down[0] !== 'error', `Unexpected error ${stringify(down)}`);
    return down[1];
  }

  async function expectStreamerToExitNormallyWithoutErrorLog(
    deleteChangeDBObjects: () => Promise<unknown>,
  ) {
    const exit = vi
      .spyOn(process, 'exit')
      .mockImplementation(() => undefined as never);
    try {
      const done = exitAfter(lc, () => streamerDone);
      await deleteChangeDBObjects();
      changes.cancel(new Error('disconnected'));
      await done;
      expect(exit).toHaveBeenCalledWith(0);
      expect(logSink.messages.filter(([level]) => level === 'error')).toEqual(
        [],
      );
    } finally {
      exit.mockRestore();
    }
  }

  async function verifyNoMoreChanges(sub: Queue<Downstream>) {
    const down = await sub.dequeue(
      ['error', {type: 0, message: 'timed-out'}],
      50,
    );
    expect(down).toEqual(['error', {type: 0, message: 'timed-out'}]);
  }

  async function expectAcks(...watermarks: string[]) {
    for (const watermark of watermarks) {
      expect((await acks.dequeue())[2].watermark).toBe(watermark);
    }
  }

  const messages = new ReplicationMessages({foo: 'id'});

  /**
   * The anchor a writer beside `replica` reconciles against. The catchup tests
   * drive the log from outside the change-streamer so that its content is
   * controlled independently of the stream, which is what lets them exercise a
   * head the streamer has not reached; `replica`'s replication state stands in
   * for the watermark such a stream would resume from.
   */
  function anchorFor(replica: Database): ChangeLogAnchor {
    const {stateVersion, replicaVersion} = replica
      .prepare(/*sql*/ `
        SELECT state."stateVersion", config."replicaVersion"
          FROM "_zero.replicationState" AS state,
               "_zero.replicationConfig" AS config
      `)
      .get<{stateVersion: string; replicaVersion: string}>();
    return {
      identity: {epoch: null, generation: replicaVersion, replicaID: null},
      resumeWatermark: stateVersion,
      nowMs: Date.now(),
    };
  }

  /** The change-log database beside `replicaFile`, seeded and reconciled. */
  function createChangeLogDB(replicaFile: DbFile, replica: Database): Database {
    const changeLog = openChangeLogDB(lc, replicaFile.path, {readonly: false});
    reconcileChangeLog(lc, changeLog, anchorFor(replica));
    return changeLog;
  }

  /** A replica whose `-change-log` sibling the SQLite catchup tests build. */
  function createCatchupReplica(name: string): {
    file: DbFile;
    replica: Database;
  } {
    const file = new DbFile(name);
    const replica = file.connect(lc);
    replica.pragma('journal_mode = wal');
    initReplicationState(replica, ['zero_data'], REPLICA_VERSION);
    return {file, replica};
  }

  /** Restarts the streamer with SQLite catchup wired to `sqliteCatchup`. */
  async function restartWithSQLiteCatchup(
    sqliteCatchup: SQLiteCatchupOptions,
  ): Promise<void> {
    await streamer.stop();
    await streamerDone;

    changes = Subscription.create();
    acks = new Queue();
    streamer = await initializeStreamer(
      lc,
      shard,
      'task-id',
      'change.streamer:12345',
      'ws',
      sql,
      {
        startStream: () =>
          Promise.resolve({
            initialWatermark: '02',
            changes,
            acks: {push: status => acks.enqueue(status)},
          }),
        startLagReporter: () =>
          Promise.resolve({firstCommitTimeMs: 100, nextSendTimeMs: 123}),
        stop: () => Promise.resolve(),
      },
      ReplicationStatusPublisher.forTesting(),
      replicaConfig,
      null,
      null,
      true,
      {...opts, sqliteCatchup},
      setTimeoutFn as unknown as typeof setTimeout,
    );
    streamerDone = streamer.run();
  }

  /**
   * Restarts the streamer with the change-log writer enabled, i.e. with the
   * production topology: this process writes the log from its own stream loop,
   * and can serve catchup from what it wrote.
   */
  async function restartWithInlineChangeLogWriter(
    logFile: DbFile,
    sqliteCatchup?: Partial<SQLiteCatchupOptions>,
    sqliteChangeLogPurge?: TuningOptions['sqliteChangeLogPurge'],
    backupURL?: string,
  ): Promise<void> {
    await streamer.stop();
    await streamerDone;

    changes = Subscription.create();
    acks = new Queue();
    streamer = await initializeStreamer(
      lc,
      shard,
      'task-id',
      'change.streamer:12345',
      'ws',
      sql,
      {
        startStream: () =>
          Promise.resolve({
            initialWatermark: REPLICA_VERSION,
            changes,
            acks: {push: status => acks.enqueue(status)},
          }),
        startLagReporter: () => null,
        stop: () => Promise.resolve(),
      },
      ReplicationStatusPublisher.forTesting(),
      replicaConfig,
      backupURL === undefined ? null : {backupURL, litestreamVersion: 'legacy'},
      null,
      true,
      {
        ...opts,
        // No replica beside it: the writer's anchor is the watermark its stream
        // connection resumes from, which is Postgres's `lastWatermark`.
        sqliteChangeLogWriter: {
          replicaFile: logFile.path,
          identity: {
            epoch: null,
            generation: REPLICA_VERSION,
            replicaID: 'replica-id',
          },
        },
        ...(sqliteChangeLogPurge === undefined ? {} : {sqliteChangeLogPurge}),
        ...(sqliteCatchup === undefined
          ? {}
          : {
              sqliteCatchup: {
                changeLogFile: changeLogFileName(logFile.path),
                readBatchRows: 2,
                barrierTimeoutMs: 1_000,
                ...sqliteCatchup,
              },
            }),
      },
      setTimeoutFn as unknown as typeof setTimeout,
    );
    streamerDone = streamer.run();
  }

  /** The oldest watermark retained in the SQLite change log beside `logFile`. */
  function sqliteMinWatermark(logFile: DbFile): string {
    using log = openChangeLogDB(lc, logFile.path, {readonly: true});
    return log
      .prepare(/*sql*/ `
        SELECT min("watermark") AS "watermark"
          FROM "_zero.changeLogStream"
      `)
      .get<{watermark: string}>().watermark;
  }

  /** A serving subscriber, i.e. one eligible for SQLite catchup. */
  function subscribeServing(id: string): Promise<Source<string>> {
    return streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: `${id}-task`,
      id,
      mode: 'serving',
      watermark: REPLICA_VERSION,
      replicaVersion: REPLICA_VERSION,
      initial: true,
      logsChangeStream: false,
    });
  }

  /**
   * Appends a transaction to the log the way the writer does, and advances the
   * replica's watermark with it so that {@link anchorFor} keeps agreeing with
   * the log's head.
   */
  function appendSQLiteTransaction(
    replica: Database,
    changeLog: Database,
    watermark: string,
    data: readonly ChangeStreamData[],
  ): void {
    const runner = new StatementRunner(replica);
    const writer = new ChangeLogStreamWriter(new StatementRunner(changeLog));
    const begin: ChangeStreamData = [
      'begin',
      messages.begin(),
      {commitWatermark: watermark},
    ];
    const commit: ChangeStreamData = ['commit', messages.commit(), {watermark}];

    runner.beginImmediate();
    try {
      writer.begin(watermark, serializeChangeStreamData(begin));
      for (const message of data) {
        writer.append(serializeChangeStreamData(message), message[1].tag);
      }
      writer.commit(watermark, serializeChangeStreamData(commit), Date.now());
      updateReplicationWatermark(runner, watermark);
      runner.commit();
    } catch (e) {
      runner.rollback();
      writer.rollback();
      throw e;
    }
  }

  /**
   * The whole of slice 7I, end to end: the change-streamer writes the SQLite
   * change log from its own stream loop, commits it before forwarding each
   * transaction's `commit`, and can then serve a subscriber's catchup from it —
   * with the barrier released by the writer's commit rather than by any
   * subscriber's ACK.
   */
  test('the change-streamer writes the log inline and serves catchup from it', async () => {
    const logFile = new DbFile('sqlite-change-log-inline-writer');
    await restartWithInlineChangeLogWriter(logFile, {
      barrierPollIntervalMs: 10,
      shouldUse: ctx => ctx.id === 'from-sqlite',
    });

    const liveSub = await subscribeServing('live-observer');
    const live = drainToQueue(liveSub);
    expect(await nextChange(live)).toMatchObject({tag: 'status'});

    try {
      changes.push(['begin', messages.begin(), {commitWatermark: '06'}]);
      changes.push(['data', messages.insert('foo', {id: 'hello'})]);
      changes.push(['commit', messages.commit(), {watermark: '06'}]);

      expect(await nextChange(live)).toMatchObject({tag: 'begin'});
      expect(await nextChange(live)).toMatchObject({
        tag: 'insert',
        new: {id: 'hello'},
      });
      expect(await nextChange(live)).toMatchObject({tag: 'commit'});
      // Invariant 2, at the earliest moment it can be observed: a subscriber
      // that has the `commit` implies the log already committed it, because the
      // log's commit precedes the forward.
      {
        using log = openChangeLogDB(lc, logFile.path, {readonly: true});
        expect(readChangeLogHead(log)).toBe('06');
      }

      // PG serving and the upstream ACK are unchanged.
      await expectAcks('06');
      expect(
        await sql`SELECT watermark FROM "zoro_3/cdc"."changeLog"
                    WHERE watermark = '06' ORDER BY pos`.values(),
      ).toEqual([['06'], ['06'], ['06']]);

      // The log holds the seed transaction at the watermark the stream resumed
      // from, then the transaction that was just forwarded.
      using changeLog = openChangeLogDB(lc, logFile.path, {readonly: true});
      expect(
        changeLog
          .prepare(/*sql*/ `
            SELECT "watermark", "pos", json_extract("change", '$.tag') AS "tag",
                   "precommit"
              FROM "_zero.changeLogStream" ORDER BY "watermark", "pos"
          `)
          .all(),
      ).toEqual([
        {watermark: '01', pos: 0, tag: 'begin', precommit: null},
        {watermark: '01', pos: 1, tag: 'commit', precommit: '01'},
        {watermark: '06', pos: 0, tag: 'begin', precommit: null},
        {watermark: '06', pos: 1, tag: 'insert', precommit: null},
        {watermark: '06', pos: 2, tag: 'commit', precommit: '06'},
      ]);
      expect(
        changeLog
          .prepare(/*sql*/ `
            SELECT "epoch", "generation", "replicaID", "schemaVersion",
                   "seedWatermark"
              FROM "_zero.changeLogMeta"
          `)
          .get(),
      ).toEqual({
        epoch: null,
        generation: REPLICA_VERSION,
        replicaID: 'replica-id',
        schemaVersion: 2,
        seedWatermark: REPLICA_VERSION,
      });

      // And a subscriber selected for SQLite is served from it -- from the
      // SQLite reader itself, not a silent PG fallback delivering the same
      // messages.
      const readerRead = vi.spyOn(SQLiteChangeLogReader.prototype, 'read');
      const sqliteSub = await subscribeServing('from-sqlite');
      const fromSQLite = drainToQueue(sqliteSub);
      expect(await nextChange(fromSQLite)).toMatchObject({tag: 'status'});
      expect(await nextChange(fromSQLite)).toMatchObject({tag: 'begin'});
      expect(await nextChange(fromSQLite)).toMatchObject({
        tag: 'insert',
        new: {id: 'hello'},
      });
      expect(await nextChange(fromSQLite)).toMatchObject({tag: 'commit'});
      expect(readerRead).toHaveBeenCalled();
      readerRead.mockRestore();
      sqliteSub.cancel();
    } finally {
      liveSub.cancel();
      await streamer.stop();
      await streamerDone;
      deleteChangeLogDB(logFile.path);
      logFile.delete();
    }
  });

  test('SQLite cleanup continues independently after PG reaches the floor', async () => {
    const logFile = new DbFile('sqlite-change-log-independent-purge');
    await restartWithInlineChangeLogWriter(logFile, undefined, {
      retentionMs: 1,
      batchRows: 2,
      maxBatchesPerPass: 1,
      now: () => Date.now() + 60_000,
      yieldFn: () => Promise.resolve(),
    });

    const watermarks = ['03', '04', '05', '06', '07', '08', '09'];
    try {
      for (const watermark of watermarks) {
        changes.push(['begin', messages.begin(), {commitWatermark: watermark}]);
        changes.push(['commit', messages.commit(), {watermark}]);
      }
      await expectAcks(...watermarks);

      expect(sqliteMinWatermark(logFile)).toBe(REPLICA_VERSION);

      setTimeoutFn.mockClear();
      const pgPurge = vi.spyOn(Storer.prototype, 'purgeRecordsBefore');
      try {
        streamer.trackBackupWatermark('08');
        const initial = setTimeoutFn.mock.calls.slice(0, 2);
        expect(initial.map(call => call[1])).toEqual([30_000, 30_000]);
        await Promise.all(
          initial.map(call => call[0]() as unknown as Promise<void>),
        );

        let timerIndex = initial.length;
        let sqlitePasses = 1;
        while (sqliteMinWatermark(logFile) !== '08') {
          const call = setTimeoutFn.mock.calls[timerIndex];
          assert(call, `cleanup timer ${timerIndex} was not scheduled`);
          expect(call[1]).toBe(0);
          timerIndex++;
          sqlitePasses++;
          await (call[0]() as unknown as Promise<void>);

          // PG reaches the floor in the first pass. Later level-triggered
          // passes belong solely to SQLite's bounded drain.
          expect(pgPurge).toHaveBeenCalledTimes(1);
          expect(sqlitePasses).toBeLessThan(10);
        }

        expect(sqlitePasses).toBeGreaterThan(1);
        expect(
          await sql`SELECT min(watermark) FROM "zoro_3/cdc"."changeLog"`.values(),
        ).toEqual([['08']]);
        expect(setTimeoutFn).toHaveBeenCalledTimes(timerIndex);
      } finally {
        pgPurge.mockRestore();
      }
    } finally {
      await streamer.stop();
      await streamerDone;
      deleteChangeLogDB(logFile.path);
      logFile.delete();
    }
  });

  /**
   * The SQLite floor's live constraint is level-triggered, not edge-triggered:
   * backup monitors never resend an unchanged floor, so once a laggard's ACK
   * holds a purge below the backup watermark, only the coordinator's own
   * deferred retries can finish the job when the laggard catches up.
   */
  test('a laggard ACK holds the SQLite floor until its subscriber catches up', async () => {
    const logFile = new DbFile('sqlite-change-log-laggard-purge');
    await restartWithInlineChangeLogWriter(logFile, undefined, {
      retentionMs: 1,
      batchRows: 100,
      now: () => Date.now() + 60_000,
      yieldFn: () => Promise.resolve(),
    });

    const watermarks = ['03', '04', '05', '06', '07', '08'];
    try {
      for (const watermark of watermarks) {
        changes.push(['begin', messages.begin(), {commitWatermark: watermark}]);
        changes.push(['commit', messages.commit(), {watermark}]);
      }
      await expectAcks(...watermarks);
      expect(sqliteMinWatermark(logFile)).toBe(REPLICA_VERSION);

      // A serving subscriber whose ACK ('04') trails the backup watermark.
      const laggard = await streamer.subscribe({
        protocolVersion: PROTOCOL_VERSION,
        taskID: 'laggard-task',
        id: 'laggard',
        mode: 'serving',
        watermark: '04',
        replicaVersion: REPLICA_VERSION,
        initial: true,
        logsChangeStream: false,
      });

      let fired = 0;
      const fireNextTimer = async () => {
        const call = setTimeoutFn.mock.calls[fired];
        assert(call, `cleanup timer ${fired} was not scheduled`);
        expect(call[1]).toBe(30_000);
        fired++;
        await (call[0]() as unknown as Promise<void>);
      };

      setTimeoutFn.mockClear();
      streamer.trackBackupWatermark('08');

      // One PG and one SQLite cleanup pass: both purge up to the laggard's
      // ACK and stop there.
      await fireNextTimer();
      await fireNextTimer();
      expect(sqliteMinWatermark(logFile)).toBe('04');

      // Both loops re-armed themselves even though the SQLite pass found
      // nothing left below its floor: the floor has not reached the backup
      // watermark, so cleanup must keep re-evaluating.
      await fireNextTimer();
      await fireNextTimer();
      expect(sqliteMinWatermark(logFile)).toBe('04');

      // The laggard consumes through '08', advancing its ACK ...
      const msgs = drainToQueue(laggard);
      for (;;) {
        const msg = await msgs.dequeue();
        if (msg[0] === 'commit' && msg[2].watermark === '08') {
          break;
        }
      }
      // ... and the still-armed retries drain both change logs to the backup
      // watermark without another trackBackupWatermark() call.
      let passes = 0;
      while (sqliteMinWatermark(logFile) !== '08') {
        await fireNextTimer();
        expect(++passes).toBeLessThan(10);
      }
      while (fired < setTimeoutFn.mock.calls.length) {
        await fireNextTimer();
      }
      expect(
        await sql`SELECT min(watermark) FROM "zoro_3/cdc"."changeLog"`.values(),
      ).toEqual([['08']]);
      // Both floors reached the backup watermark: the loops disarmed.
      expect(setTimeoutFn).toHaveBeenCalledTimes(fired);
    } finally {
      await streamer.stop();
      await streamerDone;
      deleteChangeLogDB(logFile.path);
      logFile.delete();
    }
  });

  /**
   * The reservation/purge wiring end to end: `startSnapshotReservation`
   * pauses the SQLite purge scheduler, and the reservation's close routes
   * through `SnapshotReservations`' onClose to the scheduler's resume — so a
   * restoring follower's advertised bounds stay servable, and the log is not
   * pinned once the restore is over.
   */
  test('a snapshot reservation pauses the SQLite purge until it closes', async () => {
    const logFile = new DbFile('sqlite-change-log-reservation-pause');
    await restartWithInlineChangeLogWriter(
      logFile,
      undefined,
      {
        retentionMs: 1,
        batchRows: 100,
        now: () => Date.now() + 60_000,
        yieldFn: () => Promise.resolve(),
      },
      's3://foo/bar',
    );

    const watermarks = ['03', '04', '05', '06', '07', '08'];
    try {
      for (const watermark of watermarks) {
        changes.push(['begin', messages.begin(), {commitWatermark: watermark}]);
        changes.push(['commit', messages.commit(), {watermark}]);
      }
      await expectAcks(...watermarks);
      expect(sqliteMinWatermark(logFile)).toBe(REPLICA_VERSION);

      // A view-syncer reserves a snapshot while it restores from backup.
      const reservation =
        await streamer.startSnapshotReservation('view-syncer-1');
      const snapshots = drainSnapshotMessages(reservation);

      setTimeoutFn.mockClear();
      streamer.trackBackupWatermark('08');
      expect(await snapshots.dequeue()).toEqual([
        'status',
        {
          tag: 'status',
          backupURL: 's3://foo/bar',
          replicaVersion: REPLICA_VERSION,
          minWatermark: '08',
        },
      ]);

      let fired = 0;
      const fireNextTimer = async () => {
        const call = setTimeoutFn.mock.calls[fired];
        assert(call, `cleanup timer ${fired} was not scheduled`);
        expect(call[1]).toBe(30_000);
        fired++;
        await (call[0]() as unknown as Promise<void>);
      };

      // The confirmed reservation does not hold the PG purge below the
      // backup watermark ...
      await fireNextTimer();
      expect(
        await sql`SELECT min(watermark) FROM "zoro_3/cdc"."changeLog"`.values(),
      ).toEqual([['08']]);
      // ... but it pauses the SQLite purge outright: each pass declines and
      // re-arms rather than invalidating the advertised snapshot bounds.
      await fireNextTimer();
      expect(sqliteMinWatermark(logFile)).toBe(REPLICA_VERSION);
      await fireNextTimer();
      expect(sqliteMinWatermark(logFile)).toBe(REPLICA_VERSION);

      // Closing the reservation resumes purging: the next armed pass drains
      // the SQLite log to the floor.
      reservation.cancel();
      await fireNextTimer();
      expect(sqliteMinWatermark(logFile)).toBe('08');
      // Cleanup reached the backup watermark: nothing further is armed.
      expect(setTimeoutFn).toHaveBeenCalledTimes(fired);
    } finally {
      await streamer.stop();
      await streamerDone;
      deleteChangeLogDB(logFile.path);
      logFile.delete();
    }
  });

  /**
   * Invariant 1 in its assertable form, against the real stream loop rather
   * than a model of it: no `await` separates `#storer.store()` from the log's
   * commit, and the commit precedes the forward of that transaction's `commit`
   * message. The microtask queued at `store()` is the await-detector — if the
   * loop yields between the two calls, the microtask runs and the label flips.
   */
  test('the log commits in the tick that stores the commit, before the forward', async () => {
    const events: string[] = [];
    let microtaskRanSinceStore = false;

    const realStore = Storer.prototype.store;
    const storeSpy = vi
      .spyOn(Storer.prototype, 'store')
      .mockImplementation(function (this: Storer, watermark, data) {
        if (data[0] === 'commit') {
          events.push(`store:${watermark}`);
          microtaskRanSinceStore = false;
          queueMicrotask(() => {
            microtaskRanSinceStore = true;
          });
        }
        return realStore.call(this, watermark, data);
      });
    const realWrite = SQLiteChangeLogWriter.prototype.write;
    const writeSpy = vi
      .spyOn(SQLiteChangeLogWriter.prototype, 'write')
      .mockImplementation(function (this: SQLiteChangeLogWriter, change, json) {
        realWrite.call(this, change, json);
        if (change[0] === 'commit') {
          events.push(
            `log-commit:${change[2].watermark}:` +
              (microtaskRanSinceStore ? 'after-an-await' : 'same-tick'),
          );
        }
      });
    const realForward = Forwarder.prototype.forward;
    const forwardSpy = vi
      .spyOn(Forwarder.prototype, 'forward')
      .mockImplementation(function (this: Forwarder, entry) {
        if (entry[1] === 'commit') {
          events.push(`forward:${entry[0]}`);
        }
        return realForward.call(this, entry);
      });

    const logFile = new DbFile('sqlite-change-log-ordering');
    await restartWithInlineChangeLogWriter(logFile);

    try {
      changes.push(['begin', messages.begin(), {commitWatermark: '06'}]);
      changes.push(['data', messages.insert('foo', {id: 'first'})]);
      changes.push(['commit', messages.commit(), {watermark: '06'}]);
      changes.push(['begin', messages.begin(), {commitWatermark: '08'}]);
      changes.push(['data', messages.insert('foo', {id: 'second'})]);
      changes.push(['commit', messages.commit(), {watermark: '08'}]);
      // The upstream ACK follows the storer's Postgres commit, which is behind
      // everything the events record.
      await expectAcks('06', '08');

      expect(events).toEqual([
        'store:06',
        'log-commit:06:same-tick',
        'forward:06',
        'store:08',
        'log-commit:08:same-tick',
        'forward:08',
      ]);
    } finally {
      storeSpy.mockRestore();
      writeSpy.mockRestore();
      forwardSpy.mockRestore();
      await streamer.stop();
      await streamerDone;
      deleteChangeLogDB(logFile.path);
      logFile.delete();
    }
  });

  /**
   * The in-process form of truncate-above, driven by production machinery: a
   * storer commit fails, the stream is re-established, and its resume watermark
   * is behind the log's head. The writer inserts with a plain `INSERT`, so an
   * un-truncated overlap is a constraint violation on the re-delivery.
   */
  test('an in-process reconnect below the log head truncates rather than colliding', async () => {
    await streamer.stop();
    await streamerDone;

    const logFile = new DbFile('sqlite-change-log-reconnect');
    const first = Subscription.create<ChangeStreamMessage>();
    const second = Subscription.create<ChangeStreamMessage>();
    const {promise: reconnected, resolve: didReconnect} = resolver<true>();
    streamer = await initializeStreamer(
      lc,
      shard,
      'task-id',
      'change.streamer:12345',
      'ws',
      sql,
      {
        startStream: vi
          .fn()
          .mockImplementationOnce(() =>
            Promise.resolve({
              initialWatermark: REPLICA_VERSION,
              changes: first,
              acks: {push: () => {}},
            }),
          )
          .mockImplementationOnce(() => {
            didReconnect(true);
            return Promise.resolve({
              initialWatermark: REPLICA_VERSION,
              changes: second,
              acks: {push: () => {}},
            });
          })
          .mockImplementation(() => resolver().promise),
        startLagReporter: () => null,
        stop: () => Promise.resolve(),
      },
      ReplicationStatusPublisher.forTesting(),
      replicaConfig,
      null,
      null,
      true,
      {
        ...opts,
        sqliteChangeLogWriter: {
          replicaFile: logFile.path,
          identity: {
            epoch: null,
            generation: REPLICA_VERSION,
            replicaID: 'replica-id',
          },
        },
      },
      // The real timer, because this test depends on the reconnect backoff
      // actually firing.
    );
    streamerDone = streamer.run();

    const head = () => {
      using changeLog = openChangeLogDB(lc, logFile.path, {readonly: true});
      return changeLog
        .prepare(
          `SELECT max("watermark") AS "head" FROM "_zero.changeLogStream"`,
        )
        .get<{head: string | null}>().head;
    };

    try {
      // Makes the storer's commit of '05' fail, so Postgres never records it
      // while the log already has. `pos` 3 is where the commit row of the
      // four-message transaction below lands.
      await sql`INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change)
        VALUES ('05', 3, ${{conflicting: 'entry'}})`;

      first.push(['begin', messages.begin(), {commitWatermark: '05'}]);
      first.push(['data', messages.insert('foo', {id: 'logged-not-stored'})]);
      first.push(['data', messages.insert('foo', {id: 'also-not-stored'})]);
      first.push(['commit', messages.commit(), {watermark: '05'}]);

      // The reconnect re-reads its resume watermark, finds it behind the log's
      // head, and truncates '05' away before the new stream starts.
      expect(await reconnected).toBe(true);
      expect(head()).toBe(REPLICA_VERSION);

      // The resumed stream re-delivers '05'. Without the truncation this would
      // violate PRIMARY KEY ("watermark","pos").
      await sql`DELETE FROM "zoro_3/cdc"."changeLog"
                  WHERE watermark = '05' AND pos = 3`;
      second.push(['begin', messages.begin(), {commitWatermark: '05'}]);
      second.push(['data', messages.insert('foo', {id: 'redelivered'})]);
      second.push(['data', messages.insert('foo', {id: 'redelivered-too'})]);
      second.push(['commit', messages.commit(), {watermark: '05'}]);

      await vi.waitFor(() => expect(head()).toBe('05'));
      // Fail-soft would have deleted the file on a constraint violation, so the
      // head above already proves there was none. Assert it directly too, since
      // the whole point of the carve-out is that it is reported rather than
      // absorbed.
      expect(
        logSink.messages
          .filter(([level]) => level === 'error')
          .map(([, , args]) => String(args[0]))
          .filter(m => m.includes('constraint')),
      ).toEqual([]);
    } finally {
      first.cancel();
      second.cancel();
      await streamer.stop();
      await streamerDone;
      deleteChangeLogDB(logFile.path);
      logFile.delete();
    }
  });

  test('SQLite required head tracks forwarded transaction boundaries', async () => {
    await streamer.stop();
    await streamerDone;

    const catchupReplicaFile = new DbFile('sqlite-catchup-service-integration');
    const catchupReplica = catchupReplicaFile.connect(lc);
    catchupReplica.pragma('journal_mode = wal');
    initReplicationState(catchupReplica, ['zero_data'], REPLICA_VERSION);
    const catchupChangeLog = createChangeLogDB(
      catchupReplicaFile,
      catchupReplica,
    );

    changes = Subscription.create();
    acks = new Queue();
    streamer = await initializeStreamer(
      lc,
      shard,
      'task-id',
      'change.streamer:12345',
      'ws',
      sql,
      {
        startStream: () =>
          Promise.resolve({
            initialWatermark: '02',
            changes,
            acks: {push: status => acks.enqueue(status)},
          }),
        startLagReporter: () =>
          Promise.resolve({firstCommitTimeMs: 100, nextSendTimeMs: 123}),
        stop: () => Promise.resolve(),
      },
      ReplicationStatusPublisher.forTesting(),
      replicaConfig,
      null,
      null,
      true,
      {
        ...opts,
        sqliteCatchup: {
          changeLogFile: changeLogFileName(catchupReplicaFile.path),
          readBatchRows: 2,
          barrierTimeoutMs: 1_000,
          // Nothing here acks the change log, so the barrier relies on its
          // backstop poll. Keep it short rather than waiting out the default.
          barrierPollIntervalMs: 10,
          shouldUse: ctx => ctx.id !== 'live-observer',
        },
      },
      setTimeoutFn as unknown as typeof setTimeout,
    );
    streamerDone = streamer.run();

    try {
      const liveSub = await streamer.subscribe({
        protocolVersion: PROTOCOL_VERSION,
        taskID: 'live-task',
        id: 'live-observer',
        mode: 'serving',
        watermark: REPLICA_VERSION,
        replicaVersion: REPLICA_VERSION,
        initial: true,
        logsChangeStream: false,
      });
      const live = drainToQueue(liveSub);
      expect(await nextChange(live)).toMatchObject({tag: 'status'});

      const insert04: ChangeStreamData = [
        'data',
        messages.insert('foo', {id: 'committed'}),
      ];
      changes.push(['begin', messages.begin(), {commitWatermark: '04'}]);
      changes.push(insert04);
      expect(await nextChange(live)).toMatchObject({tag: 'begin'});
      expect(await nextChange(live)).toMatchObject({
        tag: 'insert',
        new: {id: 'committed'},
      });

      const commitSub = await streamer.subscribe({
        protocolVersion: PROTOCOL_VERSION,
        taskID: 'commit-task',
        id: 'commit-catchup',
        mode: 'serving',
        watermark: REPLICA_VERSION,
        replicaVersion: REPLICA_VERSION,
        initial: true,
        logsChangeStream: false,
      });
      const committed = drainToQueue(commitSub);

      changes.push(['commit', messages.commit(), {watermark: '04'}]);
      expect(await nextChange(live)).toMatchObject({tag: 'commit'});
      appendSQLiteTransaction(catchupReplica, catchupChangeLog, '04', [
        insert04,
      ]);

      expect(await nextChange(committed)).toMatchObject({tag: 'status'});
      expect(await nextChange(committed)).toMatchObject({tag: 'begin'});
      expect(await nextChange(committed)).toMatchObject({
        tag: 'insert',
        new: {id: 'committed'},
      });
      expect(await nextChange(committed)).toMatchObject({tag: 'commit'});

      changes.push(['begin', messages.begin(), {commitWatermark: '06'}]);
      changes.push(['data', messages.insert('foo', {id: 'rolled-back'})]);
      expect(await nextChange(live)).toMatchObject({tag: 'begin'});
      expect(await nextChange(live)).toMatchObject({
        tag: 'insert',
        new: {id: 'rolled-back'},
      });

      const rollbackSub = await streamer.subscribe({
        protocolVersion: PROTOCOL_VERSION,
        taskID: 'rollback-task',
        id: 'rollback-catchup',
        mode: 'serving',
        watermark: '04',
        replicaVersion: REPLICA_VERSION,
        initial: true,
        logsChangeStream: false,
      });
      const rolledBack = drainToQueue(rollbackSub);

      changes.push(['rollback', messages.rollback()]);
      expect(await nextChange(live)).toMatchObject({tag: 'rollback'});
      expect(await nextChange(rolledBack)).toMatchObject({tag: 'status'});

      const insert08: ChangeStreamData = [
        'data',
        messages.insert('foo', {id: 'after-rollback'}),
      ];
      changes.push(['begin', messages.begin(), {commitWatermark: '08'}]);
      changes.push(insert08);
      changes.push(['commit', messages.commit(), {watermark: '08'}]);
      expect(await nextChange(rolledBack)).toMatchObject({tag: 'begin'});
      expect(await nextChange(rolledBack)).toMatchObject({
        tag: 'insert',
        new: {id: 'after-rollback'},
      });
      expect(await nextChange(rolledBack)).toMatchObject({tag: 'commit'});
      expect(await nextChange(live)).toMatchObject({tag: 'begin'});
      expect(await nextChange(live)).toMatchObject({
        tag: 'insert',
        new: {id: 'after-rollback'},
      });
      expect(await nextChange(live)).toMatchObject({tag: 'commit'});
      appendSQLiteTransaction(catchupReplica, catchupChangeLog, '08', [
        insert08,
      ]);

      changes.push(['begin', messages.begin(), {commitWatermark: '0a'}]);
      changes.push(['data', messages.insert('foo', {id: 'interrupted'})]);
      expect(await nextChange(live)).toMatchObject({tag: 'begin'});
      expect(await nextChange(live)).toMatchObject({
        tag: 'insert',
        new: {id: 'interrupted'},
      });

      const interruptedSub = await streamer.subscribe({
        protocolVersion: PROTOCOL_VERSION,
        taskID: 'interrupted-task',
        id: 'interrupted-catchup',
        mode: 'serving',
        watermark: '08',
        replicaVersion: REPLICA_VERSION,
        initial: true,
        logsChangeStream: false,
      });
      const interrupted = drainToQueue(interruptedSub);

      changes.end();
      expect(await nextChange(live)).toMatchObject({tag: 'rollback'});
      expect(await nextChange(interrupted)).toMatchObject({tag: 'status'});
      await verifyNoMoreChanges(interrupted);

      liveSub.cancel();
      commitSub.cancel();
      rollbackSub.cancel();
      interruptedSub.cancel();
    } finally {
      await streamer.stop();
      catchupChangeLog.close();
      catchupReplica.close();
      deleteChangeLogDB(catchupReplicaFile.path);
      catchupReplicaFile.delete();
    }
  });

  /**
   * The barrier, with the writer in-process. A subscriber that registers
   * mid-transaction must wait for that transaction rather than be served a
   * truncated view of it, and the wait must end on the writer's own commit: the
   * poll interval here is far beyond the test timeout, and no subscriber acks
   * this log any more.
   */
  test("the writer's commit releases the SQLite barrier", async () => {
    // The wiring this test exists for: the writer's onCommit callback notifies
    // the barrier, now that no subscriber's ACK advances this log. Asserted via
    // spy because the mid-transaction wait below can also be satisfied on its
    // first plan() read — the required head resolves at forward time, after the
    // log's commit — which would leave a broken notification path undetected.
    const onChangeLogCommit = vi.spyOn(
      SQLiteChangeLogCatchup.prototype,
      'onChangeLogCommit',
    );
    // Pins the source: the catchup below must be served by the SQLite reader.
    // A silent PG fallback delivers the same messages with the same timing,
    // since PG catchup also withholds them until the storer commits.
    const readerRead = vi.spyOn(SQLiteChangeLogReader.prototype, 'read');

    const logFile = new DbFile('sqlite-change-log-barrier');
    await restartWithInlineChangeLogWriter(logFile, {
      barrierTimeoutMs: 60_000,
      barrierPollIntervalMs: 60_000,
      shouldUse: ctx => ctx.id === 'from-sqlite',
    });

    const liveSub = await subscribeServing('live-observer');
    const live = drainToQueue(liveSub);
    expect(await nextChange(live)).toMatchObject({tag: 'status'});

    try {
      changes.push(['begin', messages.begin(), {commitWatermark: '06'}]);
      changes.push(['data', messages.insert('foo', {id: 'mid-transaction'})]);
      expect(await nextChange(live)).toMatchObject({tag: 'begin'});
      expect(await nextChange(live)).toMatchObject({
        tag: 'insert',
        new: {id: 'mid-transaction'},
      });

      // Registered while '06' is in flight, so its required head is '06'.
      const sqliteSub = await subscribeServing('from-sqlite');
      const fromSQLite = drainToQueue(sqliteSub);
      // The barrier holds the subscription: not even the status message is
      // delivered until the required head is readable.
      await sleep(50);
      expect(fromSQLite.size()).toBe(0);

      changes.push(['commit', messages.commit(), {watermark: '06'}]);
      expect(await nextChange(live)).toMatchObject({tag: 'commit'});

      // Released by the writer's commit rather than by a poll.
      expect(await nextChange(fromSQLite)).toMatchObject({tag: 'status'});
      expect(await nextChange(fromSQLite)).toMatchObject({tag: 'begin'});
      expect(await nextChange(fromSQLite)).toMatchObject({
        tag: 'insert',
        new: {id: 'mid-transaction'},
      });
      expect(await nextChange(fromSQLite)).toMatchObject({tag: 'commit'});
      // The commit's notification reached the barrier, and the messages above
      // came out of the SQLite reader.
      expect(onChangeLogCommit).toHaveBeenCalledWith('06');
      expect(readerRead).toHaveBeenCalled();
      sqliteSub.cancel();
    } finally {
      onChangeLogCommit.mockRestore();
      readerRead.mockRestore();
      liveSub.cancel();
      await streamer.stop();
      await streamerDone;
      deleteChangeLogDB(logFile.path);
      logFile.delete();
    }
  });

  /**
   * §3.7 at the service level: a write failure costs catchup reach, never the
   * shard's replication. The writer disables itself, deletes the file, and --
   * because the reader is cached on the service -- closes it, so no in-process
   * reader is left serving an unlinked inode while every new open sees nothing.
   */
  test('a mid-stream writer failure fails soft without stopping replication', async () => {
    const readerClose = vi.spyOn(SQLiteChangeLogReader.prototype, 'close');
    const logFile = new DbFile('sqlite-change-log-fail-soft');
    await restartWithInlineChangeLogWriter(logFile, {
      barrierPollIntervalMs: 10,
      shouldUse: ctx => ctx.id.startsWith('from-sqlite'),
    });

    const liveSub = await subscribeServing('live-observer');
    const live = drainToQueue(liveSub);
    expect(await nextChange(live)).toMatchObject({tag: 'status'});

    try {
      changes.push(['begin', messages.begin(), {commitWatermark: '06'}]);
      changes.push(['data', messages.insert('foo', {id: 'logged'})]);
      changes.push(['commit', messages.commit(), {watermark: '06'}]);
      expect(await nextChange(live)).toMatchObject({tag: 'begin'});
      expect(await nextChange(live)).toMatchObject({tag: 'insert'});
      expect(await nextChange(live)).toMatchObject({tag: 'commit'});

      // A subscriber served from SQLite, so that the service holds the cached
      // reader the fail-soft below must close.
      const sqliteSub = await subscribeServing('from-sqlite');
      const fromSQLite = drainToQueue(sqliteSub);
      expect(await nextChange(fromSQLite)).toMatchObject({tag: 'status'});
      expect(await nextChange(fromSQLite)).toMatchObject({tag: 'begin'});
      expect(await nextChange(fromSQLite)).toMatchObject({
        tag: 'insert',
        new: {id: 'logged'},
      });
      expect(await nextChange(fromSQLite)).toMatchObject({tag: 'commit'});
      expect(readerClose).not.toHaveBeenCalled();

      // Breaks the log under the writer: its next insert fails with an error
      // the file's contents cannot explain, i.e. the generic fail-soft class.
      {
        using saboteur = openChangeLogDB(lc, logFile.path, {readonly: false});
        saboteur.exec(`DROP TABLE "_zero.changeLogStream"`);
      }

      changes.push(['begin', messages.begin(), {commitWatermark: '08'}]);
      changes.push(['data', messages.insert('foo', {id: 'after-failure'})]);
      changes.push(['commit', messages.commit(), {watermark: '08'}]);

      // Replication continues, for the live subscriber and for the one that
      // was served from SQLite alike.
      for (const sub of [live, fromSQLite]) {
        expect(await nextChange(sub)).toMatchObject({tag: 'begin'});
        expect(await nextChange(sub)).toMatchObject({
          tag: 'insert',
          new: {id: 'after-failure'},
        });
        expect(await nextChange(sub)).toMatchObject({tag: 'commit'});
      }

      // The writer disabled itself, deleted the file, and closed the reader.
      expect(existsSync(changeLogFileName(logFile.path))).toBe(false);
      expect(readerClose).toHaveBeenCalled();
      expect(
        logSink.messages
          .filter(([level]) => level === 'error')
          .map(([, , args]) => String(args[0]))
          .join('\n'),
      ).toContain('error writing to the SQLite change log');

      // A subscriber arriving after the failure declines to SQLite -- the file
      // is gone -- and is served everything from PG.
      const afterSub = await subscribeServing('from-sqlite-after');
      const after = drainToQueue(afterSub);
      expect(await nextChange(after)).toMatchObject({tag: 'status'});
      expect(await nextChange(after)).toMatchObject({tag: 'begin'});
      expect(await nextChange(after)).toMatchObject({
        tag: 'insert',
        new: {id: 'logged'},
      });
      expect(await nextChange(after)).toMatchObject({tag: 'commit'});
      expect(await nextChange(after)).toMatchObject({tag: 'begin'});
      expect(await nextChange(after)).toMatchObject({
        tag: 'insert',
        new: {id: 'after-failure'},
      });
      expect(await nextChange(after)).toMatchObject({tag: 'commit'});

      afterSub.cancel();
      sqliteSub.cancel();
    } finally {
      readerClose.mockRestore();
      liveSub.cancel();
      await streamer.stop();
      await streamerDone;
      deleteChangeLogDB(logFile.path);
      logFile.delete();
    }
  });

  // The exclusion outlives its original reason -- the writer is no longer a
  // subscriber, so nothing can wait on its own ACK -- because a replicator that
  // predates the writer's move still sets the parameter. Slice 11 lifts it.
  test('backup subscribers and legacy change-log writers cannot select SQLite catchup', async () => {
    await streamer.stop();
    await streamerDone;

    const catchupReplicaFile = new DbFile('sqlite-catchup-writer-integration');
    const catchupReplica = catchupReplicaFile.connect(lc);
    catchupReplica.pragma('journal_mode = wal');
    initReplicationState(catchupReplica, ['zero_data'], REPLICA_VERSION);
    const catchupChangeLog = createChangeLogDB(
      catchupReplicaFile,
      catchupReplica,
    );

    changes = Subscription.create();
    acks = new Queue();
    const shouldUse = vi.fn(() => true);
    streamer = await initializeStreamer(
      lc,
      shard,
      'task-id',
      'change.streamer:12345',
      'ws',
      sql,
      {
        startStream: () =>
          Promise.resolve({
            initialWatermark: '02',
            changes,
            acks: {push: status => acks.enqueue(status)},
          }),
        startLagReporter: () =>
          Promise.resolve({firstCommitTimeMs: 100, nextSendTimeMs: 123}),
        stop: () => Promise.resolve(),
      },
      ReplicationStatusPublisher.forTesting(),
      replicaConfig,
      null,
      null,
      true,
      {
        ...opts,
        sqliteCatchup: {
          changeLogFile: changeLogFileName(catchupReplicaFile.path),
          readBatchRows: 2,
          barrierTimeoutMs: 60_000,
          barrierPollIntervalMs: 60_000,
          // Deliberately attempts to select every subscriber. Eligibility
          // checks must run first and cannot be overridden by this selector.
          shouldUse,
        },
      },
      setTimeoutFn as unknown as typeof setTimeout,
    );
    streamerDone = streamer.run();

    try {
      const observerSub = await streamer.subscribe({
        protocolVersion: PROTOCOL_VERSION,
        taskID: 'observer-task',
        id: 'observer',
        mode: 'serving',
        watermark: REPLICA_VERSION,
        replicaVersion: REPLICA_VERSION,
        initial: true,
        logsChangeStream: false,
      });
      const observed = drainToQueue(observerSub);
      expect(await nextChange(observed)).toMatchObject({tag: 'status'});

      changes.push(['begin', messages.begin(), {commitWatermark: '04'}]);
      changes.push(['data', messages.insert('foo', {id: 'forwarded'})]);
      changes.push(['commit', messages.commit(), {watermark: '04'}]);
      // Establishes a forwarded head, which is what the writer would then be
      // made to wait for.
      expect(await nextChange(observed)).toMatchObject({tag: 'begin'});

      shouldUse.mockClear();
      const backupSub = await streamer.subscribe({
        protocolVersion: PROTOCOL_VERSION,
        taskID: 'backup-task',
        id: 'backup-reader',
        mode: 'backup',
        watermark: REPLICA_VERSION,
        replicaVersion: REPLICA_VERSION,
        initial: true,
        logsChangeStream: false,
      });
      expect(shouldUse).not.toHaveBeenCalled();
      const backed = drainToQueue(backupSub);

      // Backup subscribers retain the PG catchup/recovery policy.
      expect(await nextChange(backed)).toMatchObject({tag: 'status'});
      expect(await nextChange(backed)).toMatchObject({tag: 'begin'});
      expect(await nextChange(backed)).toMatchObject({
        tag: 'insert',
        new: {id: 'forwarded'},
      });
      expect(await nextChange(backed)).toMatchObject({tag: 'commit'});

      shouldUse.mockClear();
      const writerSub = await streamer.subscribe({
        protocolVersion: PROTOCOL_VERSION,
        taskID: 'task-id',
        id: 'change-log-writer',
        // A single-node canonical writer has serving mode, so the independent
        // logsChangeStream guard remains necessary.
        mode: 'serving',
        watermark: REPLICA_VERSION,
        replicaVersion: REPLICA_VERSION,
        initial: true,
        logsChangeStream: true,
      });
      expect(shouldUse).not.toHaveBeenCalled();
      const written = drainToQueue(writerSub);

      // Served from PG instead of waiting on itself.
      expect(await nextChange(written)).toMatchObject({tag: 'status'});
      expect(await nextChange(written)).toMatchObject({tag: 'begin'});
      expect(await nextChange(written)).toMatchObject({
        tag: 'insert',
        new: {id: 'forwarded'},
      });
      expect(await nextChange(written)).toMatchObject({tag: 'commit'});
      expect(logSink.messages).toContainEqual([
        'warn',
        expect.anything(),
        [
          expect.stringContaining(
            'not serving legacy change-log writer change-log-writer',
          ),
        ],
      ]);

      observerSub.cancel();
      backupSub.cancel();
      writerSub.cancel();
    } finally {
      await streamer.stop();
      catchupChangeLog.close();
      catchupReplica.close();
      deleteChangeLogDB(catchupReplicaFile.path);
      catchupReplicaFile.delete();
    }
  });

  test('a change-streamer that starts before the change log serves from PG, then from SQLite', async () => {
    // The replicator has not created the log yet, which is the normal ordering
    // when both processes start at once.
    const {file: catchupReplicaFile, replica: catchupReplica} =
      createCatchupReplica('sqlite-catchup-startup-race');
    await restartWithSQLiteCatchup({
      changeLogFile: changeLogFileName(catchupReplicaFile.path),
      readBatchRows: 2,
      barrierTimeoutMs: 1_000,
      barrierPollIntervalMs: 10,
      shouldUse: () => true,
    });

    let catchupChangeLog: Database | undefined;
    try {
      changes.push(['begin', messages.begin(), {commitWatermark: '04'}]);
      changes.push(['data', messages.insert('foo', {id: 'from-pg'})]);
      changes.push(['commit', messages.commit(), {watermark: '04'}]);
      // SQLite catchup is not eligible until the stream has started and the
      // forwarded head is known, which the ACK of this commit establishes.
      await expectAcks('04');

      const pgSub = await subscribeServing('before-change-log');
      const fromPG = drainToQueue(pgSub);
      expect(await nextChange(fromPG)).toMatchObject({tag: 'status'});
      expect(await nextChange(fromPG)).toMatchObject({tag: 'begin'});
      expect(await nextChange(fromPG)).toMatchObject({
        tag: 'insert',
        new: {id: 'from-pg'},
      });
      expect(await nextChange(fromPG)).toMatchObject({tag: 'commit'});

      expect(logSink.messages).toContainEqual([
        'debug',
        expect.anything(),
        [
          expect.stringContaining(
            'serving before-change-log from PG catchup: cannot read',
          ),
          // A readonly handle cannot create the file, so an absent log arrives
          // as an open error rather than as an empty database.
          expect.objectContaining({message: expect.stringContaining('unable')}),
        ],
      ]);
      // Startup ordering is not an incident: nothing is warned about until the
      // log has been unavailable for longer than the threshold.
      expect(
        logSink.messages.filter(
          ([level, , args]) =>
            level === 'warn' && String(args[0]).includes('PG catchup'),
        ),
      ).toEqual([]);

      // The replicator creates and seeds the log, then applies the transaction
      // to it. Its payload differs from the forwarded one only so that the next
      // subscriber's changes say which log served it.
      catchupChangeLog = createChangeLogDB(catchupReplicaFile, catchupReplica);
      appendSQLiteTransaction(catchupReplica, catchupChangeLog, '04', [
        ['data', messages.insert('foo', {id: 'from-sqlite'})],
      ]);

      const sqliteSub = await subscribeServing('after-change-log');
      const fromSQLite = drainToQueue(sqliteSub);
      expect(await nextChange(fromSQLite)).toMatchObject({tag: 'status'});
      expect(await nextChange(fromSQLite)).toMatchObject({tag: 'begin'});
      expect(await nextChange(fromSQLite)).toMatchObject({
        tag: 'insert',
        new: {id: 'from-sqlite'},
      });
      expect(await nextChange(fromSQLite)).toMatchObject({tag: 'commit'});

      pgSub.cancel();
      sqliteSub.cancel();
    } finally {
      await streamer.stop();
      catchupChangeLog?.close();
      catchupReplica.close();
      deleteChangeLogDB(catchupReplicaFile.path);
      catchupReplicaFile.delete();
    }
  });

  test('a change log with no changes declines, without caching the failure', async () => {
    const {file: catchupReplicaFile, replica: catchupReplica} =
      createCatchupReplica('sqlite-catchup-empty-log');
    // The file exists but the writer has not reconciled it into a stream table.
    const catchupChangeLog = openChangeLogDB(lc, catchupReplicaFile.path, {
      readonly: false,
    });
    catchupChangeLog.pragma('journal_mode = wal');

    // Each declined attempt must close the handle it opened, since the next
    // subscription opens another one.
    const readerClose = vi.spyOn(SQLiteChangeLogReader.prototype, 'close');
    await restartWithSQLiteCatchup({
      changeLogFile: changeLogFileName(catchupReplicaFile.path),
      readBatchRows: 2,
      barrierTimeoutMs: 1_000,
      barrierPollIntervalMs: 10,
      shouldUse: () => true,
    });

    try {
      changes.push(['begin', messages.begin(), {commitWatermark: '04'}]);
      changes.push(['data', messages.insert('foo', {id: 'from-pg'})]);
      changes.push(['commit', messages.commit(), {watermark: '04'}]);
      // SQLite catchup is not eligible until the stream has started and the
      // forwarded head is known, which the ACK of this commit establishes.
      await expectAcks('04');

      const noTableSub = await subscribeServing('no-stream-table');
      const noTable = drainToQueue(noTableSub);
      expect(await nextChange(noTable)).toMatchObject({tag: 'status'});
      expect(await nextChange(noTable)).toMatchObject({tag: 'begin'});
      expect(await nextChange(noTable)).toMatchObject({
        tag: 'insert',
        new: {id: 'from-pg'},
      });
      expect(await nextChange(noTable)).toMatchObject({tag: 'commit'});
      expect(readerClose).toHaveBeenCalledTimes(1);

      // A stream table with no rows is equally unserviceable.
      reconcileChangeLog(lc, catchupChangeLog, anchorFor(catchupReplica));
      catchupChangeLog.prepare(`DELETE FROM "_zero.changeLogStream"`).run();

      const noRowsSub = await subscribeServing('no-rows');
      const noRows = drainToQueue(noRowsSub);
      expect(await nextChange(noRows)).toMatchObject({tag: 'status'});
      expect(await nextChange(noRows)).toMatchObject({tag: 'begin'});
      expect(await nextChange(noRows)).toMatchObject({
        tag: 'insert',
        new: {id: 'from-pg'},
      });
      expect(await nextChange(noRows)).toMatchObject({tag: 'commit'});
      // Constructed again rather than remembered as unusable, and closed again.
      expect(readerClose).toHaveBeenCalledTimes(2);

      // Reconciling an emptied log reseeds it at the replica head, after which
      // the writer's transactions make it servable.
      reconcileChangeLog(lc, catchupChangeLog, anchorFor(catchupReplica));
      appendSQLiteTransaction(catchupReplica, catchupChangeLog, '04', [
        ['data', messages.insert('foo', {id: 'from-sqlite'})],
      ]);

      const servedSub = await subscribeServing('served-from-sqlite');
      const served = drainToQueue(servedSub);
      expect(await nextChange(served)).toMatchObject({tag: 'status'});
      expect(await nextChange(served)).toMatchObject({tag: 'begin'});
      expect(await nextChange(served)).toMatchObject({
        tag: 'insert',
        new: {id: 'from-sqlite'},
      });
      expect(await nextChange(served)).toMatchObject({tag: 'commit'});
      // The reader that can serve is retained by the catchup coordinator.
      expect(readerClose).toHaveBeenCalledTimes(2);

      noTableSub.cancel();
      noRowsSub.cancel();
      servedSub.cancel();
    } finally {
      await streamer.stop();
      readerClose.mockRestore();
      catchupChangeLog.close();
      catchupReplica.close();
      deleteChangeLogDB(catchupReplicaFile.path);
      catchupReplicaFile.delete();
    }
  });

  test('an unreadable change log declines rather than failing the subscription', async () => {
    const {file: catchupReplicaFile, replica: catchupReplica} =
      createCatchupReplica('sqlite-catchup-corrupt-log');
    writeFileSync(
      changeLogFileName(catchupReplicaFile.path),
      'not a SQLite database',
    );

    await restartWithSQLiteCatchup({
      changeLogFile: changeLogFileName(catchupReplicaFile.path),
      readBatchRows: 2,
      barrierTimeoutMs: 1_000,
      barrierPollIntervalMs: 10,
      shouldUse: () => true,
      // A zero threshold makes the first decline the "still unavailable" case,
      // which is the one that warrants a warning.
      notReadyWarnThresholdMs: 0,
    });

    try {
      changes.push(['begin', messages.begin(), {commitWatermark: '04'}]);
      changes.push(['data', messages.insert('foo', {id: 'from-pg'})]);
      changes.push(['commit', messages.commit(), {watermark: '04'}]);
      // SQLite catchup is not eligible until the stream has started and the
      // forwarded head is known, which the ACK of this commit establishes.
      await expectAcks('04');

      const sub = await subscribeServing('corrupt-change-log');
      const served = drainToQueue(sub);
      expect(await nextChange(served)).toMatchObject({tag: 'status'});
      expect(await nextChange(served)).toMatchObject({tag: 'begin'});
      expect(await nextChange(served)).toMatchObject({
        tag: 'insert',
        new: {id: 'from-pg'},
      });
      expect(await nextChange(served)).toMatchObject({tag: 'commit'});

      expect(logSink.messages).toContainEqual([
        'warn',
        expect.anything(),
        [
          expect.stringContaining(
            'serving corrupt-change-log from PG catchup: cannot read',
          ),
          expect.anything(),
        ],
      ]);
      expect(logSink.messages.filter(([level]) => level === 'error')).toEqual(
        [],
      );

      sub.cancel();
    } finally {
      await streamer.stop();
      catchupReplica.close();
      deleteChangeLogDB(catchupReplicaFile.path);
      catchupReplicaFile.delete();
    }
  });

  test('immediate forwarding, transaction storage', async () => {
    const sub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid',
      mode: 'serving',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
      logsChangeStream: false,
    });
    const downstream = drainToQueue(sub);

    // Ignore lag reports from before this change stream was initialized.
    changes.push([
      'status',
      {
        ack: false,
        lagReport: {
          lastTimings: {
            sendTimeMs: 10,
            commitTimeMs: 10,
            receiveTimeMs: 15,
          },
          nextSendTimeMs: 50,
        },
      },
      {watermark: '08'},
    ]);

    changes.push(['begin', messages.begin(), {commitWatermark: '09'}]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes.push(['data', messages.insert('foo', {id: 'world'})]);
    changes.push([
      'commit',
      messages.commit({extra: 'fields'}),
      {watermark: '09'},
    ]);

    // Ignore lag reports from before this change stream was initialized.
    changes.push([
      'status',
      {
        ack: false,
        lagReport: {
          lastTimings: {
            sendTimeMs: 50,
            commitTimeMs: 50,
            receiveTimeMs: 55,
          },
          nextSendTimeMs: 100,
        },
      },
      {watermark: '0a'},
    ]);

    expect(await nextChange(downstream)).toMatchObject({
      tag: 'status',
      lagReport: {nextSendTimeMs: 123},
    });
    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'hello'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'world'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'commit',
      extra: 'fields',
    });

    changes.push(['status', {ack: false}, {watermark: '0b'}]);

    changes.push([
      'status',
      {
        ack: false,
        lagReport: {
          lastTimings: {
            sendTimeMs: 150,
            commitTimeMs: 151,
            receiveTimeMs: 152,
          },
          nextSendTimeMs: 234,
        },
      },
      {watermark: '0c'},
    ]);

    changes.push(['status', {ack: true}, {watermark: '0d'}]);

    expect(await nextChange(downstream)).toMatchObject({
      tag: 'status',
      lagReport: {
        lastTimings: {
          sendTimeMs: 150,
          commitTimeMs: 151,
          receiveTimeMs: 152,
        },
        nextSendTimeMs: 234,
      },
    });

    // Await the ACK for the single commit, then the status message with an ack.
    await expectAcks('09', '0d');

    expect(
      await sql`SELECT watermark, change->'tag' FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toMatchInlineSnapshot(`
      Result [
        [
          "01",
          "begin",
        ],
        [
          "01",
          "commit",
        ],
        [
          "09",
          "begin",
        ],
        [
          "09",
          "insert",
        ],
        [
          "09",
          "insert",
        ],
        [
          "09",
          "commit",
        ],
      ]
    `);
    await expectTables(sql, {
      ['zoro_3/cdc.replicationState']: [
        {
          lock: 1,
          owner: 'task-id',
          ownerAddress: 'change.streamer:12345',
          lastWatermark: '09',
        },
      ],
    });
  });

  test('subscriber catchup and continuation', async () => {
    // Process some changes upstream.
    changes.push(['begin', messages.begin(), {commitWatermark: '09'}]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes.push(['data', messages.insert('foo', {id: 'world'})]);
    changes.push([
      'commit',
      messages.commit({extra: 'stuff'}),
      {watermark: '09'},
    ]);

    // Subscribe to the original watermark.
    const sub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid',
      mode: 'serving',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
      logsChangeStream: false,
    });

    changes.push(['status', {ack: true}, {watermark: '0a'}]);

    // Process more upstream changes.
    changes.push(['begin', messages.begin(), {commitWatermark: '0b'}]);
    changes.push(['data', messages.delete('foo', {id: 'world'})]);
    changes.push([
      'commit',
      messages.commit({more: 'stuff'}),
      {watermark: '0b'},
    ]);

    changes.push(['status', {ack: true}, {watermark: '0d'}]);

    // Verify that all changes were sent to the subscriber ...
    const downstream = drainToQueue(sub);
    expect(await nextChange(downstream)).toMatchObject({tag: 'status'});
    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'hello'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'world'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'commit',
      extra: 'stuff',
    });
    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'delete',
      key: {id: 'world'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'commit',
      more: 'stuff',
    });

    // Two commits with intervening status messages. Note that the '0a'
    // status is superseded by '0d' (the latest known status watermark) before
    // the pg change-log persists up through '0b', so it is coalesced into
    // the '0d' ack rather than being acked on its own.
    await expectAcks('09', '0b', '0d');

    expect(
      await sql`SELECT watermark, change->'tag' FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toMatchInlineSnapshot(`
      Result [
        [
          "01",
          "begin",
        ],
        [
          "01",
          "commit",
        ],
        [
          "09",
          "begin",
        ],
        [
          "09",
          "insert",
        ],
        [
          "09",
          "insert",
        ],
        [
          "09",
          "commit",
        ],
        [
          "0b",
          "begin",
        ],
        [
          "0b",
          "delete",
        ],
        [
          "0b",
          "commit",
        ],
      ]
    `);
    await expectTables(sql, {
      ['zoro_3/cdc.replicationState']: [
        {
          lock: 1,
          owner: 'task-id',
          ownerAddress: 'change.streamer:12345',
          lastWatermark: '0b',
        },
      ],
    });
  });

  test('subscriber catchup and continuation after rollback', async () => {
    // Process some changes upstream.
    changes.push(['begin', messages.begin(), {commitWatermark: '09'}]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes.push(['data', messages.insert('foo', {id: 'world'})]);
    changes.push([
      'commit',
      messages.commit({extra: 'stuff'}),
      {watermark: '09'},
    ]);

    // Subscribe to the original watermark.
    const sub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid',
      mode: 'serving',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
      logsChangeStream: false,
    });

    // Process more upstream changes.
    changes.push(['begin', messages.begin(), {commitWatermark: '0a'}]);
    changes.push(['data', messages.delete('foo', {id: 'world'})]);
    changes.push(['rollback', messages.rollback()]);

    changes.push(['status', {ack: true}, {watermark: '0d'}]);

    // Verify that all changes were sent to the subscriber ...
    const downstream = drainToQueue(sub);
    expect(await nextChange(downstream)).toMatchObject({tag: 'status'});
    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'hello'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'world'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'commit',
      extra: 'stuff',
    });
    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'delete',
      key: {id: 'world'},
    });
    expect(await nextChange(downstream)).toMatchObject({tag: 'rollback'});

    // One commit to ACK, then the status message
    await expectAcks('09', '0d');

    // Only the changes for the committed (i.e. first) transaction are persisted.
    expect(
      await sql`SELECT watermark, change->'tag' FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toMatchInlineSnapshot(`
      Result [
        [
          "01",
          "begin",
        ],
        [
          "01",
          "commit",
        ],
        [
          "09",
          "begin",
        ],
        [
          "09",
          "insert",
        ],
        [
          "09",
          "insert",
        ],
        [
          "09",
          "commit",
        ],
      ]
    `);
    await expectTables(sql, {
      ['zoro_3/cdc.replicationState']: [
        {
          lock: 1,
          owner: 'task-id',
          ownerAddress: 'change.streamer:12345',
          lastWatermark: '09',
        },
      ],
    });
  });

  test('subscriber ahead of change log', async () => {
    // Process some changes upstream.
    changes.push(['begin', messages.begin(), {commitWatermark: '09'}]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes.push(['data', messages.insert('foo', {id: 'world'})]);
    changes.push([
      'commit',
      messages.commit({extra: 'stuff'}),
      {watermark: '09'},
    ]);

    // Subscribe to a watermark from "the future".
    const sub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid',
      mode: 'serving',
      watermark: '0b',
      replicaVersion: REPLICA_VERSION,
      initial: true,
      logsChangeStream: false,
    });

    // Process more upstream changes.
    changes.push(['begin', messages.begin(), {commitWatermark: '0b'}]);
    changes.push(['data', messages.delete('foo', {id: 'world'})]);
    changes.push([
      'commit',
      messages.commit({more: 'stuff'}),
      {watermark: '0b'},
    ]);

    // Finally something the subscriber hasn't seen.
    changes.push(['begin', messages.begin(), {commitWatermark: '0c'}]);
    changes.push(['data', messages.insert('foo', {id: 'voila'})]);
    changes.push([
      'commit',
      messages.commit({something: 'new'}),
      {watermark: '0c'},
    ]);

    // The subscriber should only see what's new to it.
    const downstream = drainToQueue(sub);
    expect(await nextChange(downstream)).toMatchObject({tag: 'status'});
    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'voila'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'commit',
      something: 'new',
    });

    await expectAcks('09', '0b', '0c');

    // Only the changes for the committed (i.e. first) transaction are persisted.
    expect(
      await sql`SELECT watermark, change->'tag' FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toMatchInlineSnapshot(`
      Result [
        [
          "01",
          "begin",
        ],
        [
          "01",
          "commit",
        ],
        [
          "09",
          "begin",
        ],
        [
          "09",
          "insert",
        ],
        [
          "09",
          "insert",
        ],
        [
          "09",
          "commit",
        ],
        [
          "0b",
          "begin",
        ],
        [
          "0b",
          "delete",
        ],
        [
          "0b",
          "commit",
        ],
        [
          "0c",
          "begin",
        ],
        [
          "0c",
          "insert",
        ],
        [
          "0c",
          "commit",
        ],
      ]
    `);
    await expectTables(sql, {
      ['zoro_3/cdc.replicationState']: [
        {
          lock: 1,
          owner: 'task-id',
          ownerAddress: 'change.streamer:12345',
          lastWatermark: '0c',
        },
      ],
    });
  });

  test('data types (forwarded and catchup)', async () => {
    const sub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid',
      mode: 'serving',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
      logsChangeStream: false,
    });
    const downstream = drainToQueue(sub);

    changes.push(['begin', messages.begin(), {commitWatermark: '09'}]);
    changes.push([
      'data',
      messages.insert('foo', {
        id: 'hello',
        int: 123456789,
        big: 987654321987654321n,
        flt: 123.456,
        bool: true,
      }),
    ]);
    changes.push([
      'commit',
      messages.commit({extra: 'info'}),
      {watermark: '09'},
    ]);

    expect(await nextChange(downstream)).toMatchObject({tag: 'status'});
    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {
        id: 'hello',
        int: 123456789,
        big: 987654321987654321n,
        flt: 123.456,
        bool: true,
      },
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'commit',
      extra: 'info',
    });

    await expectAcks('09');

    expect(
      await sql`SELECT watermark, change FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toMatchInlineSnapshot(`
      Result [
        [
          "01",
          {
            "tag": "begin",
          },
        ],
        [
          "01",
          {
            "tag": "commit",
          },
        ],
        [
          "09",
          {
            "tag": "begin",
          },
        ],
        [
          "09",
          {
            "new": {
              "big": 987654321987654321n,
              "bool": true,
              "flt": 123.456,
              "id": "hello",
              "int": 123456789,
            },
            "relation": {
              "name": "foo",
              "rowKey": {
                "columns": [
                  "id",
                ],
                "type": "default",
              },
              "schema": "public",
              "tag": "relation",
            },
            "tag": "insert",
          },
        ],
        [
          "09",
          {
            "extra": "info",
            "tag": "commit",
          },
        ],
      ]
    `);

    // Also verify when loading from the Store as opposed to direct forwarding.
    const catchupSub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid2',
      mode: 'serving',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
      logsChangeStream: false,
    });
    const catchup = drainToQueue(catchupSub);
    expect(await nextChange(catchup)).toMatchObject({tag: 'status'});
    expect(await nextChange(catchup)).toMatchObject({tag: 'begin'});
    expect(await nextChange(catchup)).toMatchObject({
      tag: 'insert',
      new: {
        id: 'hello',
        int: 123456789,
        big: 987654321987654321n,
        flt: 123.456,
        bool: true,
      },
    });
    expect(await nextChange(catchup)).toMatchObject({
      tag: 'commit',
      extra: 'info',
    });
    await expectTables(sql, {
      ['zoro_3/cdc.replicationState']: [
        {
          lock: 1,
          owner: 'task-id',
          ownerAddress: 'change.streamer:12345',
          lastWatermark: '09',
        },
      ],
    });
  });

  test('immediate subscription status', async () => {
    // Initialize the change log with entries that will be purged.
    await sql`
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('04', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('04', 1, '{"tag":"commit"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('06', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('06', 1, '{"tag":"commit"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('08', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('08', 1, '{"tag":"commit"}'::json);
      UPDATE "zoro_3/cdc"."replicationState" SET "lastWatermark" = '08';
    `.simple();

    const sub04 = drainToQueue(
      await streamer.subscribe({
        protocolVersion: PROTOCOL_VERSION,
        taskID: 'task-id',
        id: 'myid1',
        mode: 'serving',
        watermark: '04',
        replicaVersion: REPLICA_VERSION,
        initial: true,
        logsChangeStream: false,
      }),
    );
    expect(await nextChange(sub04)).toMatchObject({tag: 'status'});

    const sub08 = drainToQueue(
      await streamer.subscribe({
        protocolVersion: PROTOCOL_VERSION,
        taskID: 'task-id',
        id: 'myid1',
        mode: 'serving',
        watermark: '08',
        replicaVersion: REPLICA_VERSION,
        initial: true,
        logsChangeStream: false,
      }),
    );
    expect(await nextChange(sub08)).toMatchObject({tag: 'status'});

    const sub02 = drainToQueue(
      await streamer.subscribe({
        protocolVersion: PROTOCOL_VERSION,
        taskID: 'task-id',
        id: 'myid1',
        mode: 'serving',
        watermark: '02',
        replicaVersion: REPLICA_VERSION,
        initial: true,
        logsChangeStream: false,
      }),
    );
    expect(await sub02.dequeue()).toEqual([
      'error',
      {
        type: ErrorType.WatermarkTooOld,
        message: 'earliest supported watermark is 04 (requested 02)',
      },
    ]);
  });

  test('change log cleanup', async () => {
    // Initialize the change log with entries that will be purged.
    await sql`
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('03', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('04', 0, '{"tag":"commit"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('05', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('06', 0, '{"tag":"commit"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('07', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('08', 0, '{"tag":"commit"}'::json);
      UPDATE "zoro_3/cdc"."replicationState" SET "lastWatermark" = '08';
    `.simple();

    // Start two subscribers: one at 06 and one at 04
    const sub1 = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid1',
      mode: 'serving',
      watermark: '06',
      replicaVersion: REPLICA_VERSION,
      initial: true,
      logsChangeStream: false,
    });

    const sub2 = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid2',
      mode: 'serving',
      watermark: '04',
      replicaVersion: REPLICA_VERSION,
      initial: true,
      logsChangeStream: false,
    });

    expect(
      await sql`SELECT watermark FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toEqual([['01'], ['01'], ['03'], ['04'], ['05'], ['06'], ['07'], ['08']]);

    // Report the backup watermark as '06'. The purge floor is
    // min(backupWatermark, current subscriber acks), which is '04' (sub2).
    streamer.trackBackupWatermark('06');

    expect(setTimeoutFn).toHaveBeenCalledTimes(1);
    expect(setTimeoutFn.mock.calls[0][1]).toBe(30000);

    // The first purge should have deleted records before '04'.
    await (setTimeoutFn.mock.calls[0][0]() as unknown as Promise<void>);
    expect(
      await sql`SELECT watermark FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toEqual([['04'], ['05'], ['06'], ['07'], ['08']]);

    expect(setTimeoutFn).toHaveBeenCalledTimes(2);

    // The second purge should be a noop, because sub2 is still at '04'.
    await (setTimeoutFn.mock.calls[1][0]() as unknown as Promise<void>);
    expect(
      await sql`SELECT watermark FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toEqual([['04'], ['05'], ['06'], ['07'], ['08']]);

    // And the timer should thus be rescheduled.
    expect(setTimeoutFn).toHaveBeenCalledTimes(3);

    drainToQueue(sub1);
    for await (const json of sub2) {
      const msg: Downstream = BigIntJSON.parse(json) as Downstream;
      if (msg[0] === 'commit' && msg[2].watermark === '08') {
        // Now that sub2 has consumed past '06',
        // a purge should successfully clear records before '06'
        await (setTimeoutFn.mock.calls[2][0]() as unknown as Promise<void>);
        expect(
          await sql`SELECT watermark FROM "zoro_3/cdc"."changeLog"`.values(),
        ).toEqual([['06'], ['07'], ['08']]);
        break;
      }
    }
    // replicationState is unaffected
    await expectTables(sql, {
      ['zoro_3/cdc.replicationState']: [
        {
          lock: 1,
          owner: 'task-id',
          ownerAddress: 'change.streamer:12345',
          lastWatermark: '08',
        },
      ],
    });

    // No more timeouts should have been scheduled because the purged
    // watermark has caught up to the backup watermark.
    expect(setTimeoutFn).toHaveBeenCalledTimes(3);

    // New connections earlier than 06 should now be rejected.
    const sub3 = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid2',
      mode: 'serving',
      watermark: '04',
      replicaVersion: REPLICA_VERSION,
      initial: true,
      logsChangeStream: false,
    });

    const msgs = drainToQueue(sub3);
    expect(await msgs.dequeue()).toEqual([
      'error',
      {
        type: ErrorType.WatermarkTooOld,
        message: 'earliest supported watermark is 06 (requested 04)',
      },
    ]);
  });

  test('change log cleanup reaches the backup watermark with no subscribers', async () => {
    await sql`
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('03', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('04', 0, '{"tag":"commit"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('05', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('06', 0, '{"tag":"commit"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('07', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('08', 0, '{"tag":"commit"}'::json);
      UPDATE "zoro_3/cdc"."replicationState" SET "lastWatermark" = '08';
    `.simple();

    streamer.trackBackupWatermark('06');

    expect(setTimeoutFn).toHaveBeenCalledTimes(1);
    expect(setTimeoutFn.mock.calls[0][1]).toBe(30_000);

    await (setTimeoutFn.mock.calls[0][0]() as unknown as Promise<void>);
    expect(
      await sql`SELECT watermark FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toEqual([['06'], ['07'], ['08']]);

    // Cleanup reached the confirmed backup watermark, so it is not re-armed.
    expect(setTimeoutFn).toHaveBeenCalledTimes(1);
  });

  test('an unchanged behind-backup floor is logged only once', async () => {
    await sql`
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('03', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('04', 0, '{"tag":"commit"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('05', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('06', 0, '{"tag":"commit"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('07', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('08', 0, '{"tag":"commit"}'::json);
      UPDATE "zoro_3/cdc"."replicationState" SET "lastWatermark" = '08';
    `.simple();

    // Two idle subscribers pin the cleanup floor below the backup. The
    // level-triggered retry re-evaluates the floor every pass, and must not
    // repeat the laggard warning while the blocking floor stands still.
    const sub1 = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid1',
      mode: 'serving',
      watermark: '04',
      replicaVersion: REPLICA_VERSION,
      initial: true,
      logsChangeStream: false,
    });
    const sub2 = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid2',
      mode: 'serving',
      watermark: '06',
      replicaVersion: REPLICA_VERSION,
      initial: true,
      logsChangeStream: false,
    });

    const behindLogs = () =>
      logSink.messages.filter(
        ([, , args]) =>
          typeof args[0] === 'string' &&
          args[0].startsWith('At least one client is behind backup'),
      );
    let fired = 0;
    const fireNextTimer = async () => {
      const call = setTimeoutFn.mock.calls[fired];
      assert(call, `cleanup timer ${fired} was not scheduled`);
      expect(call[1]).toBe(30_000);
      fired++;
      await (call[0]() as unknown as Promise<void>);
    };

    setTimeoutFn.mockClear();
    streamer.trackBackupWatermark('08');

    // The first pass logs the blocking floor ('04') and purges up to it.
    await fireNextTimer();
    expect(behindLogs()).toHaveLength(1);
    expect(
      await sql`SELECT watermark FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toEqual([['04'], ['05'], ['06'], ['07'], ['08']]);

    // The retry re-evaluates the same floor: no repeated line.
    await fireNextTimer();
    expect(behindLogs()).toHaveLength(1);

    // The blocking floor moves ('04' -> '06'), still behind the backup:
    // logged anew, again only once.
    sub1.cancel();
    await fireNextTimer();
    expect(behindLogs()).toHaveLength(2);
    await fireNextTimer();
    expect(behindLogs()).toHaveLength(2);

    // With no laggard left, the floor reaches the backup watermark: purge
    // completes silently and the cleanup loop disarms.
    sub2.cancel();
    await fireNextTimer();
    expect(behindLogs()).toHaveLength(2);
    expect(
      await sql`SELECT watermark FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toEqual([['08']]);
    expect(setTimeoutFn).toHaveBeenCalledTimes(5);

    // A new backup outrunning a client is a new condition: logged again.
    const sub3 = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid3',
      mode: 'serving',
      watermark: '08',
      replicaVersion: REPLICA_VERSION,
      initial: true,
      logsChangeStream: false,
    });
    streamer.trackBackupWatermark('0a');
    await fireNextTimer();
    expect(behindLogs()).toHaveLength(3);
    await fireNextTimer();
    expect(behindLogs()).toHaveLength(3);
    sub3.cancel();
  });

  test('startSnapshotReservation throws when backups are not configured', async () => {
    // The default `streamer` fixture is initialized with a `null` backupURL.
    await expect(
      streamer.startSnapshotReservation('view-syncer-1'),
    ).rejects.toThrow('backups are not configured');
  });

  test('startSnapshotReservation withholds status until a backup watermark is tracked', async () => {
    await streamer.stop();
    const backupStreamer = await newBackupStreamer('s3://foo/bar');

    const reservation =
      await backupStreamer.startSnapshotReservation('view-syncer-1');
    const messages = drainSnapshotMessages(reservation);

    // No backup watermark has been tracked yet, so the reservation stays
    // open with no status pushed.
    const NO_MESSAGE = Symbol('no-message');
    expect(
      await messages.dequeue(NO_MESSAGE as unknown as SnapshotMessage, 50),
    ).toBe(NO_MESSAGE);

    backupStreamer.trackBackupWatermark('05');

    // The confirmed minWatermark is the backup watermark itself ('05'),
    // since it is later than the change-log's actual minimum (the initial
    // watermark, REPLICA_VERSION) -- the normal, expected case.
    expect(await messages.dequeue()).toEqual([
      'status',
      {
        tag: 'status',
        backupURL: 's3://foo/bar',
        replicaVersion: REPLICA_VERSION,
        minWatermark: '05',
      },
    ]);

    await backupStreamer.stop();
  });

  test('startSnapshotReservation immediately confirms once a backup watermark is known', async () => {
    await streamer.stop();
    const backupStreamer = await newBackupStreamer('s3://foo/bar');

    backupStreamer.trackBackupWatermark('05');

    const reservation =
      await backupStreamer.startSnapshotReservation('view-syncer-1');
    expect(await drainSnapshotMessages(reservation).dequeue()).toEqual([
      'status',
      {
        tag: 'status',
        backupURL: 's3://foo/bar',
        replicaVersion: REPLICA_VERSION,
        minWatermark: '05',
      },
    ]);

    await backupStreamer.stop();
  });

  test('subscribe() closes a pending snapshot reservation for the same taskID', async () => {
    await streamer.stop();
    const backupStreamer = await newBackupStreamer('s3://foo/bar');

    const reservation =
      await backupStreamer.startSnapshotReservation('view-syncer-1');

    await backupStreamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'view-syncer-1',
      id: 'myid1',
      mode: 'serving',
      watermark: '05',
      replicaVersion: REPLICA_VERSION,
      initial: true,
      logsChangeStream: false,
    });

    // The reservation's connection is torn down once its taskID subscribes
    // to the change stream: its iteration completes rather than hanging
    // open.
    const {done} = await reservation[Symbol.asyncIterator]().next();
    expect(done).toBe(true);

    await backupStreamer.stop();
  });

  test('a confirmed snapshot reservation does not hold back purging below the backup watermark', async () => {
    // Free up ownership of the change DB for the backup-enabled streamer
    // that this test needs (the default `streamer` fixture has no backup).
    await streamer.stop();

    await sql`
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('03', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('04', 0, '{"tag":"commit"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('05', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('06', 0, '{"tag":"commit"}'::json);
      UPDATE "zoro_3/cdc"."replicationState" SET "lastWatermark" = '06';
    `.simple();

    const backupStreamer = await newBackupStreamer('s3://foo/bar');

    // A view-syncer reserves a snapshot while it downloads the backup.
    // There are no other subscribers yet.
    const reservation =
      await backupStreamer.startSnapshotReservation('view-syncer-1');
    const messages = drainSnapshotMessages(reservation);

    backupStreamer.trackBackupWatermark('06');

    // Confirmed at the backup watermark ('06'), not the change-log's much
    // older actual minimum ('01').
    expect(await messages.dequeue()).toEqual([
      'status',
      {
        tag: 'status',
        backupURL: 's3://foo/bar',
        replicaVersion: REPLICA_VERSION,
        minWatermark: '06',
      },
    ]);
    expect(setTimeoutFn).toHaveBeenCalledTimes(1);

    // Even with no other subscribers, the confirmed reservation (rather
    // than making `current` empty and bailing out) lets the purge proceed
    // all the way to the backup watermark: it no longer pins the floor at
    // a stale minWatermark.
    await (setTimeoutFn.mock.calls[0][0]() as unknown as Promise<void>);
    expect(
      await sql`SELECT watermark FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toEqual([['06']]);

    // Purging caught all the way up to the backup watermark, so no further
    // cleanup is scheduled.
    expect(setTimeoutFn).toHaveBeenCalledTimes(1);

    await backupStreamer.stop();
  });

  test('does not confirm reservation if the change-log minWatermark has unexpectedly advanced past the backup watermark', async () => {
    await streamer.stop();

    // Simulate the change-log having been purged past what the (stale)
    // reported backup watermark claims -- not expected if cleanup logic is
    // correct, but handled defensively rather than confirming a watermark
    // that is no longer available for catchup.
    await sql`
      DELETE FROM "zoro_3/cdc"."changeLog";
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('05', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('06', 0, '{"tag":"commit"}'::json);
    `.simple();

    const backupStreamer = await newBackupStreamer('s3://foo/bar');

    const reservation =
      await backupStreamer.startSnapshotReservation('view-syncer-1');
    const messages = drainSnapshotMessages(reservation);

    // The reported backup watermark ('03') is older than the change-log's
    // actual minimum ('05').
    backupStreamer.trackBackupWatermark('03');

    const NO_MESSAGE = Symbol('no-message');
    expect(
      await messages.dequeue(NO_MESSAGE as unknown as SnapshotMessage, 50),
    ).toBe(NO_MESSAGE);

    expect(
      logSink.messages.some(
        ([level, , args]) =>
          level === 'error' &&
          args.some(
            arg => typeof arg === 'string' && arg.includes('is later than'),
          ),
      ),
    ).toBe(true);

    await backupStreamer.stop();
  });

  describe('upstream acks with a v5 backup', () => {
    // With `litestreamVersion: 'v5'`, the UpstreamAcker tracks both the pg
    // change-log (via the Storer, which persists to the real `sql` db) and
    // the backup watermark (reported via `trackBackupWatermark()`). An
    // upstream ack should only be sent once *both* have reached a watermark.
    async function newV5BackupStreamer() {
      await streamer.stop();
      const v5Changes = Subscription.create<ChangeStreamMessage>();
      const v5Acks = new Queue<UpstreamStatusMessage>();
      const backupStreamer = await newBackupStreamer(
        's3://foo/bar',
        {
          startStream: () =>
            Promise.resolve({
              initialWatermark: '02',
              changes: v5Changes,
              acks: {push: status => v5Acks.enqueue(status)},
            }),
        },
        'v5',
      );
      return {backupStreamer, v5Changes, v5Acks};
    }

    // Polls the real change db (rather than a mock) since the pg change-log
    // side of the ack gating is driven by the actual Storer flushing to it.
    async function waitForChangeLog(watermark: string) {
      for (let i = 0; i < 100; i++) {
        const rows = await sql`
          SELECT 1 FROM "zoro_3/cdc"."changeLog" WHERE watermark = ${watermark}`;
        if (rows.length) {
          return;
        }
        await sleep(10);
      }
      throw new Error(`changeLog never reached watermark ${watermark}`);
    }

    const NO_ACK = Symbol('no-ack');
    async function expectNoAck(v5Acks: Queue<UpstreamStatusMessage>) {
      expect(
        await v5Acks.dequeue(NO_ACK as unknown as UpstreamStatusMessage, 50),
      ).toBe(NO_ACK);
    }

    test('withholds the ack until the backup catches up to an already-persisted commit', async () => {
      const {backupStreamer, v5Changes, v5Acks} = await newV5BackupStreamer();

      v5Changes.push(['begin', messages.begin(), {commitWatermark: '09'}]);
      v5Changes.push(['data', messages.insert('foo', {id: 'hello'})]);
      v5Changes.push(['commit', messages.commit(), {watermark: '09'}]);

      // The pg change-log persists the commit on its own, but the backup
      // hasn't reported reaching '09' yet, so nothing should be acked.
      await waitForChangeLog('09');
      await expectNoAck(v5Acks);

      backupStreamer.trackBackupWatermark('09');
      expect((await v5Acks.dequeue())[2].watermark).toBe('09');

      await backupStreamer.stop();
    });

    test('withholds the ack until the pg change-log catches up to an already-reported backup watermark', async () => {
      const {backupStreamer, v5Changes, v5Acks} = await newV5BackupStreamer();

      // The backup races ahead of the pg change-log.
      backupStreamer.trackBackupWatermark('09');
      await expectNoAck(v5Acks);

      v5Changes.push(['begin', messages.begin(), {commitWatermark: '09'}]);
      v5Changes.push(['data', messages.insert('foo', {id: 'hello'})]);
      v5Changes.push(['commit', messages.commit(), {watermark: '09'}]);

      // Even though the backup already reported '09', the ack should wait
      // for the pg change-log to persist the commit itself.
      await waitForChangeLog('09');
      expect((await v5Acks.dequeue())[2].watermark).toBe('09');

      await backupStreamer.stop();
    });

    test('acks the min of the two watermarks across multiple commits', async () => {
      const {backupStreamer, v5Changes, v5Acks} = await newV5BackupStreamer();

      v5Changes.push(['begin', messages.begin(), {commitWatermark: '09'}]);
      v5Changes.push(['commit', messages.commit(), {watermark: '09'}]);
      v5Changes.push(['begin', messages.begin(), {commitWatermark: '0a'}]);
      v5Changes.push(['commit', messages.commit(), {watermark: '0a'}]);

      // The pg change-log races ahead to '0a', but the backup only reports
      // up through '09': the ack should stop there.
      await waitForChangeLog('0a');
      backupStreamer.trackBackupWatermark('09');
      expect((await v5Acks.dequeue())[2].watermark).toBe('09');
      await expectNoAck(v5Acks);

      // Once the backup catches up to '0a', the second commit is acked too.
      backupStreamer.trackBackupWatermark('0a');
      expect((await v5Acks.dequeue())[2].watermark).toBe('0a');

      await backupStreamer.stop();
    });
  });

  test('wrong replica version', async () => {
    const sub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid1',
      mode: 'serving',
      watermark: '06',
      replicaVersion: REPLICA_VERSION + 'foobar',
      initial: true,
      logsChangeStream: false,
    });

    const msgs = drainToQueue(sub);
    expect(await msgs.dequeue()).toEqual([
      'error',
      {
        type: ErrorType.WrongReplicaVersion,
        message: 'current replica version is 01 (requested 01foobar)',
      },
    ]);
  });

  test('retry on initial stream failure', async () => {
    const {promise: hasRetried, resolve: retried} = resolver<true>();
    const source = {
      startStream: vi
        .fn()
        .mockRejectedValueOnce('error')
        .mockImplementation(() => {
          retried(true);
          return resolver().promise;
        }),
      startLagReporter: () => null,
      stop: () => Promise.resolve(),
    } satisfies ChangeSource;
    const streamer = await initializeStreamer(
      lc,
      shard,
      'task-id',
      'change.streamer:12345',
      'ws',
      sql,
      source,
      ReplicationStatusPublisher.forTesting(),
      replicaConfig,
      null,
      null,
      true,
      opts,
    );
    void streamer.run();

    expect(await hasRetried).toBe(true);
  });

  test('shutdown if ChangeDB CDC table is missing when restarting stream', async () => {
    await expectStreamerToExitNormallyWithoutErrorLog(
      () => sql`DROP TABLE "zoro_3/cdc"."replicationState"`,
    );
  });

  test('shutdown if ChangeDB CDC schema is missing when restarting stream', async () => {
    await expectStreamerToExitNormallyWithoutErrorLog(
      () => sql`DROP SCHEMA "zoro_3/cdc" CASCADE`,
    );
  });

  test('starting point', async () => {
    const requests = new Queue<string>();
    const source = {
      startStream: vi.fn().mockImplementation(req => {
        requests.enqueue(req);
        return resolver().promise;
      }),
      startLagReporter: () => null,
      stop: () => Promise.resolve(),
    } satisfies ChangeSource;
    let streamer = await initializeStreamer(
      lc,
      shard,
      'task-id',
      'change.streamer:12345',
      'ws',
      sql,
      source,
      ReplicationStatusPublisher.forTesting(),
      replicaConfig,
      null,
      null,
      true,
      opts,
    );
    void streamer.run();

    expect(await requests.dequeue()).toBe(REPLICA_VERSION);

    await sql`
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('03', 0, '{"tag":"begin"}'::json);
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('04', 0, '{"tag":"commit"}'::json);
      UPDATE "zoro_3/cdc"."replicationState" SET "lastWatermark" = '04';
    `.simple();

    streamer = await initializeStreamer(
      lc,
      shard,
      'task-id',
      'change.streamer:12345',
      'ws',
      sql,
      source,
      ReplicationStatusPublisher.forTesting(),
      replicaConfig,
      null,
      null,
      true,
      opts,
    );
    void streamer.run();

    expect(await requests.dequeue()).toBe('04');
  });

  test('initial purge lock released', async () => {
    const purgeLocker = new PurgeLocker(lc, shard, sql);
    const lock = await purgeLocker.acquire();
    expect(lock?.minWatermark).toBe('01');

    let locked: unknown;
    try {
      // Verify that the row is locked.
      await sql`SELECT FROM "zoro_3/cdc"."changeLog" FOR UPDATE NOWAIT`;
    } catch (e) {
      locked = e;
    }
    expect(locked).toBeInstanceOf(postgres.PostgresError);
    expect((locked as postgres.PostgresError).code).toBe(PG_LOCK_NOT_AVAILABLE);

    const streamer = await initializeStreamer(
      lc,
      shard,
      'task-id',
      'change.streamer:12345',
      'ws',
      sql,
      {
        startStream: vi.fn(),
        startLagReporter: () => null,
        stop: () => Promise.resolve(),
      },
      ReplicationStatusPublisher.forTesting(),
      replicaConfig,
      null,
      lock,
      true,
      opts,
    );
    void streamer.run();

    // This should succeed once the purge lock is released.
    await sql`SELECT FROM "zoro_3/cdc"."changeLog" FOR UPDATE`;

    void streamer.stop();
  });

  test('retry on change stream error', async () => {
    const {promise: hasRetried, resolve: retried} = resolver<true>();
    const changes = Subscription.create<ChangeStreamMessage>();
    const source = {
      startStream: vi
        .fn()
        .mockImplementationOnce(() =>
          Promise.resolve({
            initialWatermark: '01',
            changes,
            acks: () => {},
          }),
        )
        .mockImplementation(() => {
          retried(true);
          return resolver().promise;
        }),
      startLagReporter: () => null,
      stop: () => Promise.resolve(),
    } satisfies ChangeSource;

    const streamer = await initializeStreamer(
      lc,
      shard,
      'task-id',
      'change.streamer:12345',
      'ws',
      sql,
      source,
      ReplicationStatusPublisher.forTesting(),
      replicaConfig,
      null,
      null,
      true,
      opts,
    );
    void streamer.run();

    changes.fail(new Error('doh'));

    expect(await hasRetried).toBe(true);
  });

  test('retry on unexpected storage error', async () => {
    const {promise: hasRetried, resolve: retried} = resolver<true>();
    const changes = Subscription.create<ChangeStreamMessage>();
    const source = {
      startStream: vi
        .fn()
        .mockImplementationOnce(() =>
          Promise.resolve({
            initialWatermark: '01',
            changes,
            acks: () => {},
          }),
        )
        .mockImplementation(() => {
          retried(true);
          return resolver().promise;
        }),
      startLagReporter: () => null,
      stop: () => Promise.resolve(),
    } satisfies ChangeSource;

    const streamer = await initializeStreamer(
      lc,
      shard,
      'task-id',
      'change.streamer:12345',
      'ws',
      sql,
      source,
      ReplicationStatusPublisher.forTesting(),
      replicaConfig,
      null,
      null,
      true,
      opts,
    );
    void streamer.run();

    // Insert unexpected data simulating that the stream and store are not in the expected state.
    await sql`INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change)
      VALUES ('05', 3, ${{conflicting: 'entry'}})`;

    changes.push(['begin', messages.begin(), {commitWatermark: '05'}]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes.push(['data', messages.insert('foo', {id: 'world'})]);
    changes.push(['commit', messages.commit(), {watermark: '05'}]);

    // The streamer should have started a new stream.
    expect(await hasRetried).toBe(true);

    // Commit should not have succeeded
    expect(
      await sql`SELECT watermark, pos FROM "zoro_3/cdc"."changeLog"`,
    ).toEqual([
      {watermark: '01', pos: 0n},
      {watermark: '01', pos: 1n},
      {watermark: '05', pos: 3n},
    ]);
  });

  test('retries at right watermark', async () => {
    const {promise: hasRetried, resolve: retried} = resolver<true>();
    const changes = Subscription.create<ChangeStreamMessage>();
    const source = {
      startStream: vi
        .fn()
        .mockImplementationOnce(() =>
          Promise.resolve({
            initialWatermark: '01',
            changes,
            acks: () => {},
          }),
        )
        .mockImplementation(() => {
          retried(true);
          return resolver().promise;
        }),
      startLagReporter: () => null,
      stop: () => Promise.resolve(),
    } satisfies ChangeSource;

    const streamer = await initializeStreamer(
      lc,
      shard,
      'task-id',
      'change.streamer:54321',
      'ws',
      sql,
      source,
      ReplicationStatusPublisher.forTesting(),
      replicaConfig,
      null,
      null,
      true,
      opts,
    );
    void streamer.run();

    // Stream down a big (1MB) transaction, which should take time to commit.
    const NEW_WATERMARK = '0g';
    const bigString = 'a'.repeat(1024);
    changes.push(['begin', {tag: 'begin'}, {commitWatermark: NEW_WATERMARK}]);
    let lastInsertProcessed: Promise<Result> | undefined;
    for (let i = 0; i < 1024; i++) {
      lastInsertProcessed = changes.push([
        'data',
        {
          tag: 'insert',
          new: {id: i, val: bigString},
          relation: {
            schema: 'public',
            name: 'foo',
            rowKey: {
              columns: ['id'],
            },
          },
        },
      ]).result;
    }
    changes.push(['commit', {tag: 'commit'}, {watermark: NEW_WATERMARK}]);

    // Wait for the last 'data' message to have been processed, which
    // means the commit was dequeued.
    await lastInsertProcessed;
    // Simulate closing the connection.
    changes.cancel();

    // Verify that the next stream starts at the NEW_WATERMARK, indicating
    // that the change-streamer waited for the last (big) commit before
    // determining the next watermark to start from.
    expect(await hasRetried).toBe(true);
    expect(source.startStream.mock.calls[1][0]).toBe(NEW_WATERMARK);
  });

  test('rolls back pending transaction when change-source dies', async () => {
    const changes1 = Subscription.create<ChangeStreamMessage>();
    const changes2 = Subscription.create<ChangeStreamMessage>();
    const source = {
      startStream: vi
        .fn()
        .mockImplementationOnce(() =>
          Promise.resolve({
            initialWatermark: '01',
            changes: changes1,
            acks: () => {},
          }),
        )
        .mockImplementationOnce(() =>
          Promise.resolve({
            initialWatermark: '01',
            changes: changes2,
            acks: () => {},
          }),
        ),
      startLagReporter: () => null,
      stop: () => Promise.resolve(),
    } satisfies ChangeSource;

    const streamer = await initializeStreamer(
      lc,
      shard,
      'task-id',
      'change.streamer:54321',
      'ws',
      sql,
      source,
      ReplicationStatusPublisher.forTesting(),
      replicaConfig,
      null,
      null,
      true,
      opts,
    );
    void streamer.run();

    const sub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid',
      mode: 'serving',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
      logsChangeStream: false,
    });
    const downstream = drainToQueue(sub);

    changes1.push(['begin', messages.begin(), {commitWatermark: '09'}]);
    changes1.push(['data', messages.insert('foo', {id: 'hello'})]);

    expect(await nextChange(downstream)).toMatchObject({tag: 'status'});
    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'hello'},
    });

    changes1.cancel(); // simulate a connection close or error.

    expect(await nextChange(downstream)).toMatchObject({tag: 'rollback'});

    changes2.push(['begin', messages.begin(), {commitWatermark: '09'}]);
    changes2.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes2.push(['data', messages.insert('foo', {id: 'world'})]);
    changes2.push([
      'commit',
      messages.commit({extra: 'fields'}),
      {watermark: '09'},
    ]);

    expect(await nextChange(downstream)).toMatchObject({tag: 'begin'});
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'hello'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'insert',
      new: {id: 'world'},
    });
    expect(await nextChange(downstream)).toMatchObject({
      tag: 'commit',
      extra: 'fields',
    });

    await streamer.stop();
  });

  test('ownership takeover before tx begins', async () => {
    changes.push(['begin', {tag: 'begin'}, {commitWatermark: '0d'}]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes.push(['commit', {tag: 'commit'}, {watermark: '0d'}]);

    // Wait for the ack of the first commit.
    await expectAcks('0d');
    // Take over ownership.
    await sql`
      UPDATE "zoro_3/cdc"."replicationState" 
        SET "owner" = 'other-task', "ownerAddress" = 'change.streamer3:7645'`;

    // The begin will read the new owner and eventually fail the transaction.
    changes.push(['begin', {tag: 'begin'}, {commitWatermark: '0f'}]);
    changes.push(['data', messages.insert('foo', {id: 'world'})]);
    changes.push(['commit', {tag: 'commit'}, {watermark: '0f'}]);

    await streamerDone;

    // Only the first changes should be committed.
    expect(
      await sql`SELECT watermark, change->'tag' FROM "zoro_3/cdc"."changeLog"`.values(),
    ).toMatchInlineSnapshot(`
      Result [
        [
          "01",
          "begin",
        ],
        [
          "01",
          "commit",
        ],
        [
          "0d",
          "begin",
        ],
        [
          "0d",
          "insert",
        ],
        [
          "0d",
          "commit",
        ],
      ]
    `);

    await expectTables(sql, {
      ['zoro_3/cdc.replicationState']: [
        {
          lock: 1,
          owner: 'other-task',
          ownerAddress: 'change.streamer3:7645',
          lastWatermark: '0d',
        },
      ],
    });
  });

  test('ownership takeover not possible during tx', async () => {
    changes.push(['begin', {tag: 'begin'}, {commitWatermark: '0d'}]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);

    // Let the next transaction begin, acquiring the lock.
    await sleep(10);

    // Verify that the lock is held.
    let result;
    try {
      result = await sql`
        SELECT owner FROM "zoro_3/cdc"."replicationState" FOR UPDATE NOWAIT`;
    } catch (e) {
      result = e;
    }
    expect(result).toMatchInlineSnapshot(
      `[PostgresError: could not obtain lock on row in relation "replicationState"]`,
    );
    // The commit should release the lock.
    changes.push(['commit', {tag: 'commit'}, {watermark: '0d'}]);

    expect(
      await sql`
      SELECT owner FROM "zoro_3/cdc"."replicationState" FOR UPDATE`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "owner": "task-id",
        },
      ]
    `);
  });

  test('reset required', async () => {
    changes.push(['control', {tag: 'reset-required'}]);
    await streamerDone;
    await expect(
      ensureReplicationConfig(lc, sql, replicaConfig, shard, true),
    ).rejects.toThrow(AutoResetSignal);
  });

  test('reset required if backup is behind', async () => {
    await sql`
      INSERT INTO "zoro_3/cdc"."changeLog" (watermark, pos, change) VALUES ('03', 0, '{"tag":"begin"}'::json);
      UPDATE "zoro_3/cdc"."replicationState" SET "lastWatermark" = '03';
    `.simple();

    void streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'backup-id',
      mode: 'backup',
      watermark: '02', // Too early
      replicaVersion: REPLICA_VERSION,
      initial: true,
      logsChangeStream: false,
    });

    await streamerDone;
    await expect(
      ensureReplicationConfig(lc, sql, replicaConfig, shard, true),
    ).rejects.toThrow(AutoResetSignal);
  });

  test('shutdown on AbortError', async () => {
    changes.fail(new AbortError());
    await streamerDone;
  });

  test('shutdown on unexpected invalid stream', async () => {
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);

    // Streamer should be shut down because of the error.
    await streamerDone;

    // Nothing should be committed
    expect(await sql`SELECT watermark FROM "zoro_3/cdc"."changeLog"`).toEqual([
      {watermark: '01'},
      {watermark: '01'},
    ]);
  });

  test('transaction aborted on unexpected termination', async () => {
    const sub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid1',
      mode: 'serving',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
      logsChangeStream: false,
    });

    const msgs = drainToQueue(sub);

    changes.push(['begin', messages.begin(), {commitWatermark: '05'}]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    changes.end();

    expect(await nextChange(msgs)).toMatchObject({tag: 'status'});
    expect(await nextChange(msgs)).toMatchObject({tag: 'begin'});
    expect(await nextChange(msgs)).toMatchObject({tag: 'insert'});
    expect(await nextChange(msgs)).toMatchObject({tag: 'rollback'});

    // No more messages should have been sent
    // No more messages should have been sent
    await verifyNoMoreChanges(msgs);
  });

  test('transaction aborted only once', async () => {
    const sub = await streamer.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task-id',
      id: 'myid1',
      mode: 'serving',
      watermark: '01',
      replicaVersion: REPLICA_VERSION,
      initial: true,
      logsChangeStream: false,
    });

    const msgs = drainToQueue(sub);

    changes.push(['begin', messages.begin(), {commitWatermark: '05'}]);
    changes.push(['data', messages.insert('foo', {id: 'hello'})]);
    // An explicit abort from change-source
    changes.push(['rollback', {tag: 'rollback'}]);
    changes.end();

    expect(await nextChange(msgs)).toMatchObject({tag: 'status'});
    expect(await nextChange(msgs)).toMatchObject({tag: 'begin'});
    expect(await nextChange(msgs)).toMatchObject({tag: 'insert'});
    expect(await nextChange(msgs)).toMatchObject({tag: 'rollback'});

    // No more messages should have been sent
    await verifyNoMoreChanges(msgs);
  });
});
