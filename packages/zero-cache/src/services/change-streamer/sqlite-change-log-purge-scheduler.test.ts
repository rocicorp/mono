import fc from 'fast-check';
import {afterEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {DbFile} from '../../test/lite.ts';
import {versionToLexi} from '../../types/lexi-version.ts';
import type {
  ChangeStreamData,
  Data,
} from '../change-source/protocol/current/downstream.ts';
import {EMPTY_COOKIE_SET} from '../replicator/change-log-cookies.ts';
import {
  CHANGE_LOG_STREAM_TABLE,
  changeLogFileName,
  deleteChangeLogDB,
  openChangeLogDBForWriting,
  type ChangeLogAnchor,
} from '../replicator/change-log-db.ts';
import {ChangeLogStreamWriter} from '../replicator/change-log-stream-writer.ts';
import {serializeChangeStreamData} from './change-log-codec.ts';
import {
  SQLiteChangeLogPurgeScheduler,
  type PurgePassResult,
  type SQLiteChangeLogPurgeSchedulerOptions,
} from './sqlite-change-log-purge-scheduler.ts';
import {SQLiteChangeLogReader} from './sqlite-change-log-reader.ts';

const lc = createSilentLogContext();

/** Watermarks in the encoding the change stream actually uses. */
function v(n: number): string {
  return versionToLexi(n);
}

const SEED_WATERMARK = v(0);
const SEED_WRITE_TIME_MS = 1_000;

/** A floor above every watermark any fixture writes. */
const NO_FLOOR = v(1_000_000);

/** A clock far enough ahead that every fixture row is past retention. */
const LONG_AGO = 10_000_000;

const files: DbFile[] = [];

afterEach(() => {
  for (const file of files.splice(0)) {
    deleteChangeLogDB(file.path);
    file.delete();
  }
});

/** Appends a transaction the way the writer does. */
function append(
  db: Database,
  watermark: string,
  changes: number,
  writeTimeMs: number,
): void {
  const writer = new ChangeLogStreamWriter(new StatementRunner(db));
  const begin: ChangeStreamData = [
    'begin',
    {tag: 'begin'},
    {commitWatermark: watermark},
  ];
  const truncate: Data = ['data', {tag: 'truncate', relations: []}];
  const commit: ChangeStreamData = ['commit', {tag: 'commit'}, {watermark}];
  try {
    writer.begin(watermark, serializeChangeStreamData(begin));
    for (let i = 0; i < changes; i++) {
      writer.append(serializeChangeStreamData(truncate), truncate[1]);
    }
    writer.commit(watermark, serializeChangeStreamData(commit), writeTimeMs);
  } catch (e) {
    writer.rollback();
    throw e;
  }
}

/**
 * Appends `count` transactions of `changes` changes each, numbered from
 * `from`, with `writeTimeMs` advancing with the watermark.
 */
function appendSeries(
  db: Database,
  from: number,
  count: number,
  changes = 1,
): void {
  for (let i = 0; i < count; i++) {
    const n = from + i;
    append(db, v(n), changes, SEED_WRITE_TIME_MS + n);
  }
}

function watermarks(db: Database): string[] {
  return db
    .prepare(/*sql*/ `
      SELECT DISTINCT "watermark" FROM "${CHANGE_LOG_STREAM_TABLE}"
      ORDER BY "watermark"
    `)
    .all<{watermark: string}>()
    .map(({watermark}) => watermark);
}

function minWatermark(db: Database): string | null {
  return db
    .prepare(/*sql*/ `
      SELECT min("watermark") AS "min" FROM "${CHANGE_LOG_STREAM_TABLE}"
    `)
    .get<{min: string | null}>().min;
}

type FakeTimers = {
  /** Pending delays, in scheduling order. */
  delays(): number[];
  /** Fires every currently pending timer. */
  fire(): void;
};

type Fixture = {
  db: Database;
  file: DbFile;
  scheduler: SQLiteChangeLogPurgeScheduler;
  timers: FakeTimers;
  setAcks(acks: string[]): void;
  setNow(nowMs: number): void;
  setConnection(db: Database | undefined): void;
};

function setup(
  opts: Partial<SQLiteChangeLogPurgeSchedulerOptions> & {
    resumeWatermark?: string;
    connected?: boolean;
  } = {},
): Fixture {
  const file = new DbFile('sqlite-change-log-purge-scheduler');
  files.push(file);
  const anchor: ChangeLogAnchor = {
    identity: {epoch: null, generation: '01', replicaID: null},
    resumeWatermark: opts.resumeWatermark ?? SEED_WATERMARK,
    nowMs: SEED_WRITE_TIME_MS,
    cookies: EMPTY_COOKIE_SET,
  };
  const {db} = openChangeLogDBForWriting(lc, file.path, anchor);

  let connection: Database | undefined =
    (opts.connected ?? true) ? db : undefined;
  let acks: string[] = [NO_FLOOR];
  let nowMs = LONG_AGO;

  const pending = new Map<number, {fn: () => void; delay: number}>();
  let nextTimer = 1;
  const setTimeoutFn = ((fn: () => void, delay?: number) => {
    const id = nextTimer++;
    pending.set(id, {fn, delay: delay ?? 0});
    return id;
  }) as unknown as typeof setTimeout;
  const clearTimeoutFn = ((id: unknown) => {
    pending.delete(id as number);
  }) as unknown as typeof clearTimeout;

  const scheduler = new SQLiteChangeLogPurgeScheduler(
    lc,
    () => connection,
    () => acks,
    {
      retentionMs: 1_000,
      batchRows: 10,
      statementTargetMs: 1_000,
      maxBatchesPerPass: 100,
      inTransactionPollMs: 1_000,
      now: () => nowMs,
      setTimeoutFn,
      clearTimeoutFn,
      yieldFn: () => Promise.resolve(),
      ...opts,
    },
  );

  return {
    db,
    file,
    scheduler,
    timers: {
      delays: () => Array.from(pending.values(), ({delay}) => delay),
      fire: () => {
        const due = [...pending.values()];
        pending.clear();
        due.forEach(({fn}) => fn());
      },
    },
    setAcks: next => {
      acks = next;
    },
    setNow: next => {
      nowMs = next;
    },
    setConnection: next => {
      connection = next;
    },
  };
}

/** Enough microtask turns for any settled work to finish, without timers. */
async function microtasks(turns = 50): Promise<void> {
  for (let i = 0; i < turns; i++) {
    await Promise.resolve();
  }
}

describe('change-streamer/sqlite-change-log-purge-scheduler', () => {
  test('drains eligible history and retains the transaction at the floor', async () => {
    const {db, scheduler, file} = setup();
    appendSeries(db, 1, 20);

    const result = await scheduler.purge(v(10));

    expect(result.stopped).toBeUndefined();
    expect(result.deletedRows).toBeGreaterThan(0);
    // Strictly below the floor: the transaction *at* the floor survives as a
    // restoring follower's catchup boundary.
    expect(minWatermark(db)).toBe(v(10));
    expect(result.probe).toBe('retained');

    using reader = new SQLiteChangeLogReader(lc, changeLogFileName(file.path));
    expect(reader.plan(v(10)).kind).toBe('range');
    expect(reader.plan(v(9)).kind).toBe('too-old');
  });

  test('the head cap retains the latest transaction under a stale-log floor', async () => {
    const {db, scheduler} = setup();
    appendSeries(db, 1, 5);

    const result = await scheduler.purge(NO_FLOOR);

    expect(result.stopped).toBeUndefined();
    expect(watermarks(db)).toEqual([v(5)]);
    // The floor ran past a frozen log; nothing at the floor was ever purged.
    expect(result.probe).toBe('ahead');
  });

  test('a floor ahead of the head stays armed for replayed commits', async () => {
    const {db, scheduler} = setup();
    appendSeries(db, 1, 5);

    // The backup outran this log: the pass drains to the head cap but must
    // stay deferred. The writer will replay commits below the unchanged
    // floor, and neither those commits nor the backup monitor (which never
    // resends an unchanged floor) re-arms cleanup — quiescence here would
    // disarm it for good.
    const ahead = await scheduler.purge(v(10));
    expect(ahead.stopped).toBeUndefined();
    expect(ahead.probe).toBe('ahead');
    expect(watermarks(db)).toEqual([v(5)]);
    expect(ahead.continuation).toBe('deferred');

    // Mid-replay: the head is still behind the floor, so cleanup stays armed
    // even though everything below the head has drained.
    appendSeries(db, 6, 3);
    const partial = await scheduler.purge(v(10));
    expect(partial.stopped).toBeUndefined();
    expect(watermarks(db)).toEqual([v(8)]);
    expect(partial.continuation).toBe('deferred');

    // Replay reaches the floor: the drain completes and cleanup disarms.
    appendSeries(db, 9, 2);
    const caughtUp = await scheduler.purge(v(10));
    expect(caughtUp.stopped).toBeUndefined();
    expect(watermarks(db)).toEqual([v(10)]);
    expect(caughtUp.probe).toBe('retained');
    expect(caughtUp.continuation).toBeUndefined();
  });

  test('bounded batches per pass request an immediate continuation', async () => {
    const {db, scheduler} = setup({
      batchRows: 4,
      maxBatchesPerPass: 2,
    });
    appendSeries(db, 1, 20);

    const first = await scheduler.purge(v(15));
    expect(first.stopped).toBe('batch-limit');
    expect(first.batches).toBe(2);
    expect(first.continuation).toBe('immediate');
    expect(minWatermark(db)).not.toBe(v(15));

    // The cleanup coordinator follows the result until the pass drains.
    let result = first;
    for (let i = 0; i < 10; i++) {
      if (result.continuation === undefined) {
        break;
      }
      result = await scheduler.purge(v(15));
    }
    expect(minWatermark(db)).toBe(v(15));
    expect(result.continuation).toBeUndefined();
  });

  test('declines only when a connected subscriber ACK is behind the floor', async () => {
    const {db, scheduler, setAcks} = setup();
    appendSeries(db, 1, 10);

    setAcks([v(3)]);
    let result = await scheduler.purge(v(8));
    expect(result.stopped).toBe('subscriber-behind');
    expect(result.deletedRows).toBe(0);
    expect(result.continuation).toBe('deferred');

    setAcks([]);
    result = await scheduler.purge(v(8));
    expect(result.stopped).toBeUndefined();
    expect(minWatermark(db)).toBe(v(8));
  });

  test('a deferred pass observes a subscriber that has caught up', async () => {
    const {db, scheduler, setAcks} = setup();
    appendSeries(db, 1, 10);

    setAcks([v(3)]);
    const declined = await scheduler.purge(v(8));
    expect(declined.stopped).toBe('subscriber-behind');
    expect(declined.continuation).toBe('deferred');

    // Subscriber ACKs are level-triggered rather than notifications. The
    // cleanup coordinator follows the deferred result with another pass.
    setAcks([v(20)]);
    const result = await scheduler.purge(v(8));
    expect(minWatermark(db)).toBe(v(8));
    expect(result.continuation).toBeUndefined();
  });

  test('a registration mid-drain is honored by the very next batch', async () => {
    const fixture = setup({
      batchRows: 6,
      yieldFn: async () => {
        // Runs between batches, where the guard must be free: a guard held
        // for the pass would deadlock this registration.
        if (!registered) {
          registered = true;
          await fixture.scheduler.cleanupGuard.runWhilePurgeBlocked(() => {
            fixture.setAcks([v(15)]);
          });
        }
      },
    });
    let registered = false;
    const {db, scheduler, file} = fixture;
    appendSeries(db, 1, 30);

    const result = await scheduler.purge(NO_FLOOR);

    // The batch after the registration saw the new ACK and declined, so the
    // range the subscriber needs survives.
    expect(registered).toBe(true);
    expect(result.stopped).toBe('subscriber-behind');
    using reader = new SQLiteChangeLogReader(lc, changeLogFileName(file.path));
    expect(reader.plan(v(15)).kind).toBe('range');
  });

  test("purges only between the writer's commits", async () => {
    const {db, scheduler, timers} = setup();
    appendSeries(db, 1, 10);

    // The writer opens a transaction, as the stream loop does mid-transaction.
    const writer = new ChangeLogStreamWriter(new StatementRunner(db));
    writer.begin(
      v(40),
      serializeChangeStreamData([
        'begin',
        {tag: 'begin'},
        {commitWatermark: v(40)},
      ]),
    );
    expect(db.inTransaction).toBe(true);

    const cycle = scheduler.purge(NO_FLOOR);
    const winner = await Promise.race([
      cycle.then(() => 'done'),
      microtasks().then(() => 'waiting'),
    ]);
    // No batch ran inside the writer's open transaction; the cycle is parked
    // on the commit notification, with the poll interval as a backstop.
    expect(winner).toBe('waiting');
    expect(watermarks(db)).toContain(SEED_WATERMARK);
    expect(timers.delays()).toEqual([1_000]);

    writer.commit(
      v(40),
      serializeChangeStreamData([
        'commit',
        {tag: 'commit'},
        {watermark: v(40)},
      ]),
      LONG_AGO - 1,
    );
    scheduler.onWriterIdle();
    const result = await cycle;

    expect(result.stopped).toBeUndefined();
    expect(watermarks(db)).toEqual([v(40)]);
  });

  test('the poll backstop finds the purge window a silent rollback opens', async () => {
    const {db, scheduler, timers} = setup();
    appendSeries(db, 1, 10);

    const writer = new ChangeLogStreamWriter(new StatementRunner(db));
    writer.begin(
      v(40),
      serializeChangeStreamData([
        'begin',
        {tag: 'begin'},
        {commitWatermark: v(40)},
      ]),
    );

    const cycle = scheduler.purge(v(8));
    await microtasks();
    expect(timers.delays()).toEqual([1_000]); // parked on the poll

    // An interrupted stream rolls back with no commit notification —
    // deliberately no onWriterIdle() here. Only the poll can find the window
    // the rollback opened.
    writer.rollback();
    timers.fire();
    const result = await cycle;

    expect(result.stopped).toBeUndefined();
    expect(minWatermark(db)).toBe(v(8));
  });

  test('an appending writer interleaves with a drain that tears an oversized transaction', async () => {
    const fixture = setup({
      batchRows: 8,
      yieldFn: () => {
        // A live write stream between batches: the guard and the connection
        // are both free, so the append must succeed mid-drain.
        appended++;
        append(fixture.db, v(100 + appended), 1, LONG_AGO - 1);
        return Promise.resolve();
      },
    });
    let appended = 0;
    const {db, scheduler} = fixture;
    // One transaction far larger than a batch, i.e. the tear (§3.5), followed
    // by a small one so the oversized transaction is not the head (which is
    // never a purge candidate).
    append(db, v(1), 60, SEED_WRITE_TIME_MS + 1);
    append(db, v(2), 1, SEED_WRITE_TIME_MS + 2);

    const result = await scheduler.purge(NO_FLOOR);

    expect(result.stopped).toBeUndefined();
    expect(result.batches).toBeGreaterThan(3);
    expect(appended).toBeGreaterThan(0);
    // The torn transaction is gone whole; the live appends all survived.
    const retained = watermarks(db);
    expect(retained).not.toContain(v(1));
    for (let i = 1; i <= appended; i++) {
      expect(retained).toContain(v(100 + i));
    }
  });

  test('reservations pause purging and resume only when every one has ended', async () => {
    const {db, scheduler} = setup();
    appendSeries(db, 1, 10);

    await scheduler.pause('task-a');
    await scheduler.pause('task-b');

    const paused = await scheduler.purge(v(8));
    expect(paused.stopped).toBe('paused');
    expect(paused.deletedRows).toBe(0);
    expect(paused.continuation).toBe('deferred');
    expect(watermarks(db)).toContain(SEED_WATERMARK);

    scheduler.resume('task-a');
    const stillPaused = await scheduler.purge(v(8));
    expect(stillPaused.stopped).toBe('paused');

    scheduler.resume('task-b');
    await scheduler.purge(v(8));
    expect(minWatermark(db)).toBe(v(8));
  });

  test('overlapping reservations from one taskID are reference-counted', async () => {
    const {db, scheduler} = setup();
    appendSeries(db, 1, 10);

    // A reservation retry can resolve its old resume after its new suspend.
    await scheduler.pause('task-a');
    await scheduler.pause('task-a');
    scheduler.resume('task-a');

    const result = await scheduler.purge(v(8));
    expect(result.stopped).toBe('paused');

    scheduler.resume('task-a');
    scheduler.resume('task-a'); // unknown once the count is drained: ignored
    expect((await scheduler.purge(v(8))).stopped).toBeUndefined();
    expect(minWatermark(db)).toBe(v(8));
  });

  test('pause resolves only once the in-flight batch has completed', async () => {
    const {db, scheduler} = setup({batchRows: 4});
    appendSeries(db, 1, 30);

    // Hold the purge mutex, as a catchup registration does, so that the
    // pass's first batch queues behind it: a batch "in flight" — dispatched
    // but not yet run — at the moment the reservation arrives.
    let releaseGuard!: () => void;
    const registration = scheduler.cleanupGuard.runWhilePurgeBlocked(
      () => new Promise<void>(resolve => (releaseGuard = resolve)),
    );
    await microtasks();

    const cycle = scheduler.purge(v(25));
    await microtasks(); // the batch's lock acquisition is now queued

    let minAtResume: string | null | undefined;
    const paused = scheduler.pause('restorer').then(() => {
      minAtResume = minWatermark(db);
    });

    // The barrier: the pause was issued after the batch was dispatched, so
    // its promise must not resolve while that batch is still pending.
    await microtasks();
    expect(minAtResume).toBeUndefined();

    releaseGuard();
    await registration;
    await paused;

    // The queued batch ran to completion *before* the pause resolved — the
    // bounds a backup monitor reads after awaiting the pause cannot be
    // invalidated by a batch that was already on its way in...
    expect(minAtResume).toBe(v(1));
    // ...and no batch runs after it: the drain is interrupted, not finished.
    const result = await cycle;
    expect(result.stopped).toBe('paused');
    expect(result.batches).toBe(1);
    expect(minWatermark(db)).toBe(v(1));
  });

  test('a batch queued behind a registration re-checks the writer under the lock', async () => {
    const {db, scheduler, timers} = setup();
    appendSeries(db, 1, 10);

    let releaseGuard!: () => void;
    const registration = scheduler.cleanupGuard.runWhilePurgeBlocked(
      () => new Promise<void>(resolve => (releaseGuard = resolve)),
    );
    await microtasks();

    const cycle = scheduler.purge(v(8));
    await microtasks(); // the batch's lock acquisition is now queued

    // The writer begins a transaction while the batch waits on the mutex —
    // everything the loop's unlocked checks established is now stale.
    const writer = new ChangeLogStreamWriter(new StatementRunner(db));
    writer.begin(
      v(40),
      serializeChangeStreamData([
        'begin',
        {tag: 'begin'},
        {commitWatermark: v(40)},
      ]),
    );

    releaseGuard();
    await registration;
    const winner = await Promise.race([
      cycle.then(() => 'done'),
      microtasks().then(() => 'waiting'),
    ]);
    // The batch retried instead of running BEGIN IMMEDIATE inside the
    // writer's open transaction: nothing was purged, and the cycle is parked
    // on the commit notification with the poll as a backstop.
    expect(winner).toBe('waiting');
    expect(watermarks(db)).toContain(SEED_WATERMARK);
    expect(timers.delays()).toEqual([1_000]);

    writer.commit(
      v(40),
      serializeChangeStreamData([
        'commit',
        {tag: 'commit'},
        {watermark: v(40)},
      ]),
      LONG_AGO - 1,
    );
    scheduler.onWriterIdle();
    const result = await cycle;
    expect(result.stopped).toBeUndefined();
    expect(minWatermark(db)).toBe(v(8));
  });

  test('a batch queued behind a registration skips a writer that failed soft', async () => {
    const {db, scheduler, setConnection} = setup();
    appendSeries(db, 1, 10);

    let releaseGuard!: () => void;
    const registration = scheduler.cleanupGuard.runWhilePurgeBlocked(
      () => new Promise<void>(resolve => (releaseGuard = resolve)),
    );
    await microtasks();

    const cycle = scheduler.purge(v(8));
    await microtasks(); // the batch's lock acquisition is now queued

    // The writer fails soft while the batch waits on the mutex.
    setConnection(undefined);

    releaseGuard();
    await registration;
    const result = await cycle;

    // The batch's locked re-check saw the connection change and retried; the
    // loop then skipped rather than purging on the stale handle.
    expect(result.stopped).toBe('writer-unavailable');
    expect(result.deletedRows).toBe(0);
    expect(result.continuation).toBe('deferred');
    expect(watermarks(db)).toContain(SEED_WATERMARK);
  });

  test('a pause lands between batches and interrupts the drain', async () => {
    const fixture = setup({
      batchRows: 4,
      yieldFn: async () => {
        if (!paused) {
          paused = true;
          await fixture.scheduler.pause('restorer');
        }
      },
    });
    let paused = false;
    const {db, scheduler} = fixture;
    appendSeries(db, 1, 30);

    const result = await scheduler.purge(v(25));
    expect(result.stopped).toBe('paused');
    expect(result.batches).toBeGreaterThan(0);
    const atPause = watermarks(db);
    expect(atPause.length).toBeGreaterThan(1); // the drain did not finish

    // Ending the reservation lets the coordinator's next pass resume the
    // interrupted drain.
    scheduler.resume('restorer');
    await scheduler.purge(v(25));
    expect(minWatermark(db)).toBe(v(25));
  });

  test('skips at debug until the writer opens the log, then picks it up', async () => {
    const {db, scheduler, setConnection} = setup({connected: false});
    appendSeries(db, 1, 10);

    const skipped = await scheduler.purge(v(8));
    expect(skipped.stopped).toBe('writer-unavailable');
    expect(skipped.probe).toBeUndefined();
    expect(skipped.continuation).toBe('deferred');
    expect(watermarks(db)).toContain(SEED_WATERMARK);

    // The change-log file appears (the writer's first reconcile): the next
    // pass picks it up without a restart.
    setConnection(db);
    await scheduler.purge(v(8));
    expect(minWatermark(db)).toBe(v(8));
  });

  test('a quiet source still drains once young history ages out', async () => {
    const {db, scheduler, setNow} = setup();
    appendSeries(db, 1, 30);
    // Only transactions written before the cutoff are eligible on the first
    // pass; the rest are within the minimum retention window.
    setNow(SEED_WRITE_TIME_MS + 20 + 1_000);

    const first = await scheduler.purge(v(25));
    expect(first.stopped).toBeUndefined();
    expect(minWatermark(db)).toBe(v(20));
    // History below the floor remains, so the coordinator gets a deferred
    // continuation even though the source is quiet.
    expect(first.continuation).toBe('deferred');

    // No new cleanup watermark arrives; a later level-triggered pass observes
    // the advanced clock and releases the rest.
    setNow(LONG_AGO);
    const second = await scheduler.purge(v(25));
    expect(minWatermark(db)).toBe(v(25));
    expect(second.continuation).toBeUndefined();
  });

  test('a stale floor stalls purge; the boundary stays servable end to end', async () => {
    const {db, scheduler, file, setNow} = setup();
    appendSeries(db, 1, 10);
    setNow(LONG_AGO);

    // The backup watermark is W: purge, then restore-then-catch-up from W.
    await scheduler.purge(v(5));
    using reader = new SQLiteChangeLogReader(lc, changeLogFileName(file.path));
    expect(reader.plan(v(5)).kind).toBe('range');

    // Live traffic continues but the backup watermark is held stale: purge
    // must stall at the floor rather than advance past it.
    appendSeries(db, 11, 10);
    setNow(LONG_AGO + 10_000);
    const stalled = await scheduler.purge(v(5));
    expect(stalled.deletedRows).toBe(0);
    expect(minWatermark(db)).toBe(v(5));
    expect(reader.plan(v(5)).kind).toBe('range');

    // The backup advances; the old boundary ages out, the new one survives.
    await scheduler.purge(v(15));
    expect(reader.plan(v(15)).kind).toBe('range');
    expect(reader.plan(v(5)).kind).toBe('too-old');
  });

  test('the probe distinguishes a cold log from a violation', async () => {
    // A log seeded above the floor never held that history: `too-old` at the
    // floor is the routine state at every fork and resumption, not an alert.
    {
      const {scheduler, setAcks} = setup({resumeWatermark: v(100)});
      setAcks([v(200)]);
      const result = await scheduler.purge(v(50));
      expect(result.stopped).toBeUndefined();
      expect(result.deletedRows).toBe(0);
      expect(result.probe).toBe('cold-log');
    }

    // A log seeded below the floor whose history at the floor is gone has
    // purged past it: the violation the probe exists to catch.
    {
      const {db, scheduler} = setup();
      appendSeries(db, 1, 20);
      // Not the purger's doing -- its own tests pin that it cannot -- but the
      // state a wrong floor would leave behind.
      db.exec(/*sql*/ `
        DELETE FROM "${CHANGE_LOG_STREAM_TABLE}" WHERE "watermark" <= '${v(10)}'
      `);
      const result = await scheduler.purge(v(10));
      expect(result.probe).toBe('violation');
    }
  });

  test('a later batch failure preserves completed work and probes the floor', async () => {
    let failDeletes = false;
    const fixture = setup({
      batchRows: 4,
      yieldFn: () => {
        if (!failDeletes) {
          failDeletes = true;
          fixture.db.exec(/*sql*/ `
            CREATE TRIGGER "fail-purge-delete"
            BEFORE DELETE ON "${CHANGE_LOG_STREAM_TABLE}"
            BEGIN
              SELECT RAISE(ABORT, 'injected purge failure');
            END
          `);
        }
        return Promise.resolve();
      },
    });
    const {db, scheduler} = fixture;
    appendSeries(db, 1, 30);

    const result = await scheduler.purge(v(25));

    expect(result.stopped).toBe('error');
    expect(result.batches).toBe(1);
    expect(result.deletedRows).toBeGreaterThan(0);
    expect(result.probe).toBe('retained');
    expect(result.continuation).toBe('deferred');
  });

  test('a failed pass reports an error and requests a retry', async () => {
    const {db, scheduler} = setup();
    appendSeries(db, 1, 10);
    db.exec(/*sql*/ `DROP TABLE "${CHANGE_LOG_STREAM_TABLE}"`);

    const result = await scheduler.purge(v(8));
    expect(result.stopped).toBe('error');
    expect(result.continuation).toBe('deferred');
  });

  test('stop ends passes and unparks a waiting batch', async () => {
    const {db, scheduler} = setup();
    appendSeries(db, 1, 10);
    const writer = new ChangeLogStreamWriter(new StatementRunner(db));
    writer.begin(
      v(40),
      serializeChangeStreamData([
        'begin',
        {tag: 'begin'},
        {commitWatermark: v(40)},
      ]),
    );

    const cycle = scheduler.purge(NO_FLOOR);
    scheduler.stop();
    const result = await cycle;
    expect(result.stopped).toBe('stopped');
    writer.rollback();

    expect((await scheduler.purge(NO_FLOOR)).stopped).toBe('stopped');
  });

  describe('property: invariants hold under arbitrary interleavings', () => {
    /** Rows per watermark: the granularity of the deletion-legality checks. */
    function rowCounts(db: Database): Map<string, number> {
      return new Map(
        db
          .prepare(/*sql*/ `
            SELECT "watermark", count(*) AS "n"
            FROM "${CHANGE_LOG_STREAM_TABLE}"
            GROUP BY "watermark"
          `)
          .all<{watermark: string; n: number}>()
          .map(({watermark, n}) => [watermark, n]),
      );
    }

    function totalRows(db: Database): number {
      return db
        .prepare(
          /*sql*/ `SELECT count(*) AS "n" FROM "${CHANGE_LOG_STREAM_TABLE}"`,
        )
        .get<{n: number}>().n;
    }

    /**
     * The floor used when a dispatch lands above every committed watermark: a
     * value no fuzzed append can reach, so the pass stays in the stale-log
     * regime (probe `ahead`) rather than being crossed by later appends,
     * which would place the floor between real watermarks and manufacture a
     * spurious `violation`. Production floors are backup watermarks, i.e.
     * monotone and always either a committed watermark or from before the log.
     */
    const FROZEN_FLOOR = 1_000_000;

    // Dispatch and pump are weighted up: they are what makes a pass start and
    // make progress, so their density decides how many deletion events land
    // inside interesting windows (open reservations, low ACKs, open
    // transactions) rather than in the drained finale.
    const command = fc.oneof(
      {
        // The cleanup coordinator supplies a new durable floor.
        arbitrary: fc.record({
          kind: fc.constant('dispatch' as const),
          floorNum: fc.integer({min: 0, max: 60}),
        }),
        weight: 3,
      },
      {
        // Releases one parked between-batch yield.
        arbitrary: fc.record({kind: fc.constant('pump' as const)}),
        weight: 3,
      },
      {
        // A whole committed transaction between the writer's commits.
        arbitrary: fc.record({
          kind: fc.constant('append' as const),
          width: fc.integer({min: 0, max: 3}),
          ageMs: fc.integer({min: -2_000, max: 5_000}),
        }),
        weight: 2,
      },
      // The writer's open-transaction window, and both ways out of it:
      // commit notifies; rollback is silent, leaving only the poll.
      fc.record({
        kind: fc.constant('begin' as const),
        width: fc.integer({min: 0, max: 3}),
      }),
      fc.record({
        kind: fc.constant('commit' as const),
        ageMs: fc.integer({min: -2_000, max: 5_000}),
      }),
      fc.record({kind: fc.constant('rollback' as const)}),
      // Snapshot reservations, including unbalanced and surplus schedules.
      fc.record({
        kind: fc.constant('pause' as const),
        task: fc.constantFrom('task-a', 'task-b'),
      }),
      fc.record({
        kind: fc.constant('resume' as const),
        task: fc.constantFrom('task-a', 'task-b'),
      }),
      // A catchup registration swapping the live ACK set under the guard.
      fc.record({
        kind: fc.constant('register' as const),
        ackNums: fc.array(fc.integer({min: 0, max: 70}), {maxLength: 2}),
      }),
      fc.record({
        kind: fc.constant('advanceClock' as const),
        ms: fc.integer({min: 0, max: 5_000}),
      }),
      fc.record({kind: fc.constant('fireTimers' as const)}),
    );

    const schedulerFuzzScenario = fc.record({
      batchRows: fc.integer({min: 1, max: 8}),
      maxBatchesPerPass: fc.integer({min: 1, max: 4}),
      seedTxs: fc.integer({min: 0, max: 8}),
      cmds: fc.array(command, {minLength: 1, maxLength: 40}),
      finalFloorNum: fc.integer({min: 0, max: 60}),
    });

    test('any interleaving preserves the gates and drains to quiescence', async () => {
      await fc.assert(
        fc.asyncProperty(
          schedulerFuzzScenario,
          async ({
            batchRows,
            maxBatchesPerPass,
            seedTxs,
            cmds,
            finalFloorNum,
          }) => {
            // The yield gate: purge parks between batches until a pump (or
            // the finale) releases it, so batch boundaries land exactly at
            // chosen command points rather than wherever the microtask queue
            // happens to put them.
            let gated = true;
            const parkedYields: Array<() => void> = [];
            const fixture = setup({
              batchRows,
              maxBatchesPerPass,
              yieldFn: () =>
                gated
                  ? new Promise<void>(resolve => parkedYields.push(resolve))
                  : Promise.resolve(),
            });
            const {db, file, scheduler, timers, setAcks, setNow} = fixture;
            try {
              appendSeries(db, 1, seedTxs);
              const committed = Array.from({length: seedTxs + 1}, (_, i) => i);
              let nextWm = seedTxs + 1;
              let openTx:
                | {
                    writer: ChangeLogStreamWriter;
                    watermark: string;
                    wmNum: number;
                  }
                | undefined;
              let nowMs = LONG_AGO;
              let acks: string[] = [];
              setAcks(acks);
              const pauses = new Map<string, number>();
              const pauseTotal = () =>
                [...pauses.values()].reduce((a, b) => a + b, 0);
              let floorNum: number | undefined;
              let insertedTotal = totalRows(db);
              const results: PurgePassResult[] = [];
              let inflight: Promise<void> | undefined;
              let settled: PurgePassResult | undefined;

              const snapFloor = (n: number) => {
                const snapped =
                  n > committed.at(-1)!
                    ? FROZEN_FLOOR
                    : committed.reduce((best, w) => (w <= n ? w : best), 0);
                return Math.max(snapped, floorNum ?? 0);
              };

              // Rows enter and leave via the writer only synchronously, so
              // measuring around each writer operation attributes every other
              // row-count change to purge batches.
              const measureInsert = (fn: () => void) => {
                const before = totalRows(db);
                fn();
                insertedTotal += totalRows(db) - before;
              };

              const checkResult = (r: PurgePassResult) => {
                // Health: the writer's windows were respected (no nested
                // BEGIN error), the connection never went away, and history
                // at or above the floor was never purged.
                expect([
                  undefined,
                  'paused',
                  'subscriber-behind',
                  'batch-limit',
                ]).toContain(r.stopped);
                expect(r.probe).not.toBe('violation');
                expect(r.batches).toBeLessThanOrEqual(maxBatchesPerPass);
                if (r.stopped === 'batch-limit') {
                  expect(r.batches).toBe(maxBatchesPerPass);
                  expect(r.continuation).toBe('immediate');
                } else if (r.stopped !== undefined) {
                  expect(r.continuation).toBe('deferred');
                }
                if (r.stopped === undefined && r.continuation === undefined) {
                  // No floor is lost: a pass reporting quiescence left
                  // nothing below both the floor and the head.
                  const wms = [...rowCounts(db).keys()];
                  const head = wms.reduce((a, b) => (a > b ? a : b));
                  for (const w of wms) {
                    expect(w >= r.floor || w === head).toBe(true);
                  }
                }
              };

              const apply = async (cmd: (typeof cmds)[number]) => {
                switch (cmd.kind) {
                  case 'dispatch':
                    // The coordinator runs one pass at a time.
                    if (inflight === undefined) {
                      floorNum = snapFloor(cmd.floorNum);
                      inflight = scheduler.purge(v(floorNum)).then(r => {
                        settled = r;
                      });
                    }
                    break;
                  case 'append':
                    if (openTx === undefined) {
                      const wm = nextWm++;
                      measureInsert(() =>
                        append(db, v(wm), cmd.width, nowMs - cmd.ageMs),
                      );
                      committed.push(wm);
                    }
                    break;
                  case 'begin':
                    if (openTx === undefined) {
                      const wmNum = nextWm++;
                      const watermark = v(wmNum);
                      const writer = new ChangeLogStreamWriter(
                        new StatementRunner(db),
                      );
                      measureInsert(() => {
                        writer.begin(
                          watermark,
                          serializeChangeStreamData([
                            'begin',
                            {tag: 'begin'},
                            {commitWatermark: watermark},
                          ]),
                        );
                        for (let i = 0; i < cmd.width; i++) {
                          writer.append(
                            serializeChangeStreamData([
                              'data',
                              {tag: 'truncate', relations: []},
                            ]),
                            {tag: 'truncate', relations: []},
                          );
                        }
                      });
                      openTx = {writer, watermark, wmNum};
                    }
                    break;
                  case 'commit':
                    if (openTx !== undefined) {
                      const {writer, watermark, wmNum} = openTx;
                      measureInsert(() =>
                        writer.commit(
                          watermark,
                          serializeChangeStreamData([
                            'commit',
                            {tag: 'commit'},
                            {watermark},
                          ]),
                          nowMs - cmd.ageMs,
                        ),
                      );
                      committed.push(wmNum);
                      openTx = undefined;
                      scheduler.onWriterIdle();
                    }
                    break;
                  case 'rollback':
                    if (openTx !== undefined) {
                      // Deliberately no onWriterIdle(): an interrupted
                      // stream's rollback is silent, and only the poll finds
                      // the window it opens.
                      const {writer} = openTx;
                      measureInsert(() => writer.rollback());
                      openTx = undefined;
                    }
                    break;
                  case 'pause':
                    pauses.set(cmd.task, (pauses.get(cmd.task) ?? 0) + 1);
                    await scheduler.pause(cmd.task);
                    break;
                  case 'resume': {
                    const count = pauses.get(cmd.task);
                    if (count !== undefined) {
                      if (count > 1) {
                        pauses.set(cmd.task, count - 1);
                      } else {
                        pauses.delete(cmd.task);
                      }
                    }
                    scheduler.resume(cmd.task);
                    break;
                  }
                  case 'register':
                    await scheduler.cleanupGuard.runWhilePurgeBlocked(() => {
                      acks = cmd.ackNums.map(v);
                      setAcks(acks);
                    });
                    break;
                  case 'advanceClock':
                    nowMs += cmd.ms;
                    setNow(nowMs);
                    break;
                  case 'fireTimers':
                    timers.fire();
                    break;
                  case 'pump':
                    parkedYields.shift()?.();
                    break;
                }
              };

              for (const cmd of cmds) {
                const before = rowCounts(db);
                const acksBefore = acks;
                const pausedBefore = pauseTotal() > 0;
                const rolledBack =
                  cmd.kind === 'rollback' ? openTx?.watermark : undefined;
                await apply(cmd);
                await microtasks();
                const after = rowCounts(db);

                const deleted = [...before.keys()].filter(
                  w => w !== rolledBack && (after.get(w) ?? 0) < before.get(w)!,
                );
                if (deleted.length > 0) {
                  // Only a purge batch deletes rows, and only legally: below
                  // the dispatched floor, below the head, and below every
                  // live subscriber ACK.
                  expect(floorNum).toBeDefined();
                  const head = [...after.keys()].reduce((a, b) =>
                    a > b ? a : b,
                  );
                  for (const w of deleted) {
                    expect(w < v(floorNum!)).toBe(true);
                    expect(w < head).toBe(true);
                  }
                  if (cmd.kind !== 'register' && acksBefore.length > 0) {
                    const minAck = acksBefore.reduce((a, b) => (a < b ? a : b));
                    for (const w of deleted) {
                      expect(w < minAck).toBe(true);
                    }
                  }
                  // Exclusion: never while a reservation was effective
                  // throughout the step.
                  expect(pausedBefore && pauseTotal() > 0).toBe(false);
                }

                if (settled !== undefined) {
                  results.push(settled);
                  checkResult(settled);
                  settled = undefined;
                  inflight = undefined;
                }
              }

              // The finale: close the writer's window, end every reservation,
              // move every ACK out of the way, age all history out, open the
              // yield gate, and drain as the level-triggered coordinator
              // would. A raised floor must always be drained to.
              if (openTx !== undefined) {
                const {writer, watermark, wmNum} = openTx;
                measureInsert(() =>
                  writer.commit(
                    watermark,
                    serializeChangeStreamData([
                      'commit',
                      {tag: 'commit'},
                      {watermark},
                    ]),
                    nowMs,
                  ),
                );
                committed.push(wmNum);
                openTx = undefined;
                scheduler.onWriterIdle();
              }
              for (const [task, count] of pauses) {
                for (let i = 0; i < count; i++) {
                  scheduler.resume(task);
                }
              }
              pauses.clear();
              acks = [];
              setAcks(acks);
              nowMs += 100_000_000;
              setNow(nowMs);
              gated = false;
              parkedYields.splice(0).forEach(release => release());
              timers.fire();
              if (inflight !== undefined) {
                await inflight;
                expect(settled).toBeDefined();
                results.push(settled!);
                checkResult(settled!);
                settled = undefined;
                inflight = undefined;
              }

              floorNum = snapFloor(finalFloorNum);
              const finalFloor = v(floorNum);
              // Two terminal states: an undefined continuation, or — under a
              // frozen floor the head can never reach — a stable no-progress
              // 'deferred' that keeps cleanup armed for replayed rows.
              const terminal = (r: PurgePassResult) =>
                r.continuation === undefined ||
                (r.stopped === undefined &&
                  r.deletedRows === 0 &&
                  r.continuation === 'deferred');
              let last: PurgePassResult;
              for (let passes = 0; ; passes++) {
                // Termination: the drain reaches quiescence in bounded passes.
                expect(passes).toBeLessThan(200);
                last = await scheduler.purge(finalFloor);
                results.push(last);
                checkResult(last);
                if (terminal(last)) {
                  break;
                }
              }
              expect(last.stopped).toBeUndefined();

              // No lost floor: however the loop exited, nothing below both
              // the final floor and the head survives.
              const finalWms = [...rowCounts(db).keys()];
              const finalHead = finalWms.reduce((a, b) => (a > b ? a : b));
              for (const w of finalWms) {
                expect(w >= finalFloor || w === finalHead).toBe(true);
              }

              // Quiescence is stable, and no timer or yield is left parked.
              const extra = await scheduler.purge(finalFloor);
              results.push(extra);
              expect(extra.deletedRows).toBe(0);
              expect(extra.continuation).toBe(last.continuation);
              expect(timers.delays()).toEqual([]);
              expect(parkedYields).toEqual([]);

              // Accounting: every row ever inserted was either deleted by
              // exactly the batches that reported it, or is still in the log.
              expect(
                results.reduce((sum, {deletedRows}) => sum + deletedRows, 0),
              ).toBe(insertedTotal - totalRows(db));
            } finally {
              scheduler.stop();
              parkedYields.splice(0).forEach(release => release());
              const index = files.indexOf(file);
              if (index >= 0) {
                files.splice(index, 1);
              }
              deleteChangeLogDB(file.path);
              file.delete();
            }
          },
        ),
        {numRuns: 1000},
      );
    }, 20_000);
  });
});
