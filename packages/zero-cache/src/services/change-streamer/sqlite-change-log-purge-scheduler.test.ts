import {afterEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {DbFile} from '../../test/lite.ts';
import {versionToLexi} from '../../types/lexi-version.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
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
  const truncate: ChangeStreamData = ['data', {tag: 'truncate', relations: []}];
  const commit: ChangeStreamData = ['commit', {tag: 'commit'}, {watermark}];
  try {
    writer.begin(watermark, serializeChangeStreamData(begin));
    for (let i = 0; i < changes; i++) {
      writer.append(serializeChangeStreamData(truncate), 'truncate');
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
      maxBatchesPerCycle: 100,
      recheckIntervalMs: 30_000,
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

  test('bounded batches per cycle, with an immediate continuation', async () => {
    const {db, scheduler, timers} = setup({
      batchRows: 4,
      maxBatchesPerCycle: 2,
    });
    appendSeries(db, 1, 20);

    const first = await scheduler.purge(v(15));
    expect(first.stopped).toBe('batch-limit');
    expect(first.batches).toBe(2);
    expect(minWatermark(db)).not.toBe(v(15));
    // The continuation is scheduled immediately -- the limit bounds one
    // cycle's bookkeeping, not the drain.
    expect(timers.delays()).toEqual([0]);

    // Repeated dispatches drain to the floor.
    for (let i = 0; i < 10; i++) {
      timers.fire();
      await scheduler.purge(v(15));
    }
    expect(minWatermark(db)).toBe(v(15));
  });

  test('declines when a subscriber ACK is behind the floor, and with no subscribers', async () => {
    const {db, scheduler, setAcks} = setup();
    appendSeries(db, 1, 10);

    setAcks([v(3)]);
    let result = await scheduler.purge(v(8));
    expect(result.stopped).toBe('subscriber-behind');
    expect(result.deletedRows).toBe(0);

    setAcks([]);
    result = await scheduler.purge(v(8));
    expect(result.stopped).toBe('no-subscribers');
    expect(result.deletedRows).toBe(0);

    // The gate is evaluated at dispatch, not at schedule: once the lagging
    // subscriber has ACKed past the floor, the same floor purges.
    setAcks([v(8), v(20)]);
    result = await scheduler.purge(v(8));
    expect(result.stopped).toBeUndefined();
    expect(minWatermark(db)).toBe(v(8));
  });

  test('a registration mid-drain is honored by the very next batch', async () => {
    const fixture = setup({
      batchRows: 6,
      yieldFn: async () => {
        // Runs between batches, where the guard must be free: a guard held
        // for the cycle would deadlock this registration.
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

  test('a floor raised by a coalesced dispatch applies to the running drain', async () => {
    const {db, scheduler} = setup({batchRows: 4});
    appendSeries(db, 1, 30);

    const first = scheduler.purge(v(10));
    const second = scheduler.purge(v(20));
    // The second dispatch coalesced into the running cycle...
    expect(second).toBe(first);
    await first;
    // ...and raised its floor.
    expect(minWatermark(db)).toBe(v(20));
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
    const {db, scheduler, timers, setNow} = setup();
    appendSeries(db, 1, 10);

    await scheduler.pause('task-a');
    await scheduler.pause('task-b');

    const paused = await scheduler.purge(v(8));
    expect(paused.stopped).toBe('paused');
    expect(paused.deletedRows).toBe(0);
    expect(watermarks(db)).toContain(SEED_WATERMARK);

    scheduler.resume('task-a');
    expect(timers.delays()).toEqual([]); // task-b still holds a reservation
    const stillPaused = await scheduler.purge(v(8));
    expect(stillPaused.stopped).toBe('paused');

    scheduler.resume('task-b');
    expect(timers.delays()).toEqual([0]);
    setNow(LONG_AGO + 1);
    timers.fire();
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
    const {db, scheduler, timers} = fixture;
    appendSeries(db, 1, 30);

    const result = await scheduler.purge(v(25));
    expect(result.stopped).toBe('paused');
    expect(result.batches).toBeGreaterThan(0);
    const atPause = watermarks(db);
    expect(atPause.length).toBeGreaterThan(1); // the drain did not finish

    // Ending the reservation resumes the interrupted drain.
    scheduler.resume('restorer');
    timers.fire();
    await scheduler.purge(v(25));
    expect(minWatermark(db)).toBe(v(25));
  });

  test('skips at debug until the writer opens the log, then picks it up', async () => {
    const {db, scheduler, timers, setConnection} = setup({connected: false});
    appendSeries(db, 1, 10);

    const skipped = await scheduler.purge(v(8));
    expect(skipped.stopped).toBe('writer-unavailable');
    expect(skipped.probe).toBeUndefined();
    expect(watermarks(db)).toContain(SEED_WATERMARK);
    expect(timers.delays()).toEqual([30_000]);

    // The change-log file appears (the writer's first reconcile): the next
    // cycle picks it up without a restart.
    setConnection(db);
    timers.fire();
    await scheduler.purge(v(8));
    expect(minWatermark(db)).toBe(v(8));
  });

  test('a quiet source still drains once young history ages out', async () => {
    const {db, scheduler, timers, setNow} = setup();
    appendSeries(db, 1, 30);
    // Only transactions written before the cutoff are eligible on the first
    // cycle; the rest are within the minimum retention window.
    setNow(SEED_WRITE_TIME_MS + 20 + 1_000);

    const first = await scheduler.purge(v(25));
    expect(first.stopped).toBeUndefined();
    expect(minWatermark(db)).toBe(v(20));
    // History below the floor remains, so the recheck timer is armed.
    expect(timers.delays()).toEqual([30_000]);

    // No new cleanup watermark ever arrives; the clock alone releases the
    // rest.
    setNow(LONG_AGO);
    timers.fire();
    await scheduler.purge(v(25));
    expect(minWatermark(db)).toBe(v(25));
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

  test('a failed cycle reports an error and re-arms instead of throwing', async () => {
    const {db, scheduler, timers} = setup();
    appendSeries(db, 1, 10);
    db.exec(/*sql*/ `DROP TABLE "${CHANGE_LOG_STREAM_TABLE}"`);

    const result = await scheduler.purge(v(8));
    expect(result.stopped).toBe('error');
    expect(timers.delays()).toEqual([30_000]);
  });

  test('stop ends cycles and unparks a waiting batch', async () => {
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
});
