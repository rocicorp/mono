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
import {serializeChangeStreamData} from '../change-streamer/change-log-codec.ts';
import type {WatermarkedChange} from '../change-streamer/change-streamer.ts';
import {SQLiteChangeLogReader} from '../change-streamer/sqlite-change-log-reader.ts';
import {EMPTY_COOKIE_SET} from './change-log-cookies.ts';
import {
  CHANGE_LOG_STREAM_TABLE,
  CHANGE_LOG_STREAM_WRITE_TIME_INDEX,
  changeLogFileName,
  deleteChangeLogDB,
  openChangeLogDB,
  openChangeLogDBForWriting,
  type ChangeLogAnchor,
} from './change-log-db.ts';
import {ChangeLogStreamWriter} from './change-log-stream-writer.ts';
import {
  SQLITE_CHANGE_LOG_COUNT_TRANSACTION_SQL,
  SQLITE_CHANGE_LOG_ELIGIBLE_COMMITS_SQL,
  SQLiteChangeLogPurger,
  type PurgeBatchOptions,
  type PurgeBatchResult,
} from './sqlite-change-log-purger.ts';

const lc = createSilentLogContext();

/** Watermarks in the encoding the change stream actually uses. */
function v(n: number): string {
  return versionToLexi(n);
}

const SEED_WATERMARK = v(0);
const SEED_WRITE_TIME_MS = 1_000;

const ANCHOR: ChangeLogAnchor = {
  identity: {epoch: null, generation: '01', replicaID: null},
  resumeWatermark: SEED_WATERMARK,
  nowMs: SEED_WRITE_TIME_MS,
  cookies: EMPTY_COOKIE_SET,
};

/** A floor above every watermark any fixture writes. */
const NO_FLOOR = v(1_000_000);
const NO_CUTOFF = Number.MAX_SAFE_INTEGER;

const files: DbFile[] = [];

afterEach(() => {
  for (const file of files.splice(0)) {
    deleteChangeLogDB(file.path);
    file.delete();
  }
});

type Log = {
  readonly db: Database;
  readonly path: string;
  readonly close: () => void;
};

/**
 * A real change-log file, seeded at {@link ANCHOR}. Real rather than
 * `:memory:` because several cases open a second connection.
 */
function createLog(): Log {
  const file = new DbFile('sqlite-change-log-purger');
  files.push(file);
  const {db} = openChangeLogDBForWriting(lc, file.path, ANCHOR);
  return {db, path: changeLogFileName(file.path), close: () => db.close()};
}

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
 * Appends `count` transactions of `changes` changes each, numbered from `from`.
 * `writeTimeMs` advances with the watermark, as the writer's `Date.now()` does.
 */
function appendSeries(
  db: Database,
  from: number,
  count: number,
  changes = 1,
): string[] {
  const watermarks: string[] = [];
  for (let i = 0; i < count; i++) {
    const n = from + i;
    append(db, v(n), changes, SEED_WRITE_TIME_MS + n);
    watermarks.push(v(n));
  }
  return watermarks;
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

function rowCount(db: Database, watermark?: string): number {
  return watermark === undefined
    ? db
        .prepare(
          /*sql*/ `SELECT count(*) AS "n" FROM "${CHANGE_LOG_STREAM_TABLE}"`,
        )
        .get<{n: number}>().n
    : db
        .prepare(/*sql*/ `
          SELECT count(*) AS "n" FROM "${CHANGE_LOG_STREAM_TABLE}"
          WHERE "watermark" = ?
        `)
        .get<{n: number}>(watermark).n;
}

function bounds(db: Database): {min: string | null; head: string | null} {
  const row = db
    .prepare(/*sql*/ `
      SELECT
        (SELECT min("watermark") FROM "${CHANGE_LOG_STREAM_TABLE}") AS "min",
        (SELECT max("watermark") FROM "${CHANGE_LOG_STREAM_TABLE}") AS "head"
    `)
    .get<{min: string | null; head: string | null}>();
  return row;
}

/**
 * Invariant 5, as amended: every transaction *above* the minimum is whole. The
 * minimum is exempt because it is the only one a tear can touch, and it is
 * checked by {@link expectMinimumIsAValidBoundary}.
 */
function expectCompleteAboveMinimum(db: Database) {
  const incomplete = db
    .prepare(/*sql*/ `
      SELECT "watermark"
      FROM "${CHANGE_LOG_STREAM_TABLE}"
      WHERE "watermark" > (SELECT min("watermark") FROM "${CHANGE_LOG_STREAM_TABLE}")
      GROUP BY "watermark"
      HAVING min("pos") <> 0
        OR max("pos") <> count(*) - 1
        OR json_extract(max(CASE WHEN "pos" = 0 THEN "change" END), '$.tag') <> 'begin'
        OR json_extract(max(CASE WHEN "precommit" IS NOT NULL THEN "change" END), '$.tag') <> 'commit'
        OR sum(CASE WHEN "precommit" IS NOT NULL THEN 1 ELSE 0 END) <> 1
    `)
    .all();
  expect(incomplete).toEqual([]);
}

/**
 * The minimum transaction is either whole or torn, and either way its highest
 * `pos` is still the `commit` row — which is all `plan()` reads from it.
 */
function expectMinimumIsAValidBoundary(db: Database) {
  const {min} = bounds(db);
  if (min === null) {
    return;
  }
  const last = db
    .prepare(/*sql*/ `
      SELECT "precommit", json_extract("change", '$.tag') AS "tag"
      FROM "${CHANGE_LOG_STREAM_TABLE}"
      WHERE "watermark" = ?
      ORDER BY "pos" DESC
      LIMIT 1
    `)
    .get<{precommit: string | null; tag: string}>(min);
  expect(last).toEqual({precommit: min, tag: 'commit'});
}

function expectInvariants(db: Database) {
  expectCompleteAboveMinimum(db);
  expectMinimumIsAValidBoundary(db);
}

const DEFAULTS = {
  externalFloor: NO_FLOOR,
  retentionCutoffMs: NO_CUTOFF,
  maxRows: 1000,
  maxDurationMs: 100,
} satisfies PurgeBatchOptions;

function purge(
  purger: SQLiteChangeLogPurger,
  opts: Partial<PurgeBatchOptions> = {},
): PurgeBatchResult {
  return purger.purgeBatch({...DEFAULTS, ...opts});
}

/** Drains, returning every batch. Bounded so a non-terminating purge fails. */
function drain(
  purger: SQLiteChangeLogPurger,
  opts: Partial<PurgeBatchOptions> = {},
  maxBatches = 10_000,
): PurgeBatchResult[] {
  const batches: PurgeBatchResult[] = [];
  for (let i = 0; i < maxBatches; i++) {
    const result = purge(purger, opts);
    batches.push(result);
    if (!result.moreEligible) {
      return batches;
    }
  }
  throw new Error(`purge did not terminate within ${maxBatches} batches`);
}

/**
 * A clock whose every `start`/`end` pair reports `elapsedMs`, so the duration
 * governor can be driven without depending on how fast a delete happens to run.
 */
function fakeClock() {
  let t = 0;
  let atStart = true;
  const clock = {
    elapsedMs: 0,
    now: () => {
      if (atStart) {
        atStart = false;
        return t;
      }
      atStart = true;
      t += clock.elapsedMs;
      return t;
    },
  };
  return clock;
}

/** The replica path a change-log file hangs off. */
function replicaFileOf(changeLogPath: string): string {
  return changeLogPath.replace(/-change-log$/, '');
}

function queryPlan(db: Database, sql: string, params: unknown): string[] {
  return db
    .prepare(`EXPLAIN QUERY PLAN ${sql}`)
    .all<{detail: string}>(params)
    .map(({detail}) => detail);
}

describe('replicator/sqlite-change-log-purger', () => {
  describe('the three bounds', () => {
    test('nothing is eligible in a freshly seeded log', () => {
      const {db, close} = createLog();
      try {
        const purger = new SQLiteChangeLogPurger(db);

        // The seed is the head, and the head is never a candidate.
        expect(purge(purger)).toEqual({
          deletedRows: 0,
          deletedThrough: undefined,
          moreEligible: false,
        });
        expect(watermarks(db)).toEqual([SEED_WATERMARK]);
      } finally {
        close();
      }
    });

    test('the head cap alone retains the latest transaction', () => {
      const {db, close} = createLog();
      try {
        appendSeries(db, 1, 3);
        const purger = new SQLiteChangeLogPurger(db);

        // No external floor and no time floor: only (b) is doing any work.
        expect(purge(purger)).toEqual({
          deletedRows: 8,
          deletedThrough: v(2),
          moreEligible: false,
        });
        expect(watermarks(db)).toEqual([v(3)]);
        expectInvariants(db);
      } finally {
        close();
      }
    });

    test('the external floor alone binds, strictly', () => {
      const {db, close} = createLog();
      try {
        appendSeries(db, 1, 5);
        const purger = new SQLiteChangeLogPurger(db);

        expect(purge(purger, {externalFloor: v(3)})).toMatchObject({
          deletedThrough: v(2),
          moreEligible: false,
        });
        // v(3) survives: it is the transaction a follower that restored to the
        // backup watermark calls plan() with.
        expect(watermarks(db)).toEqual([v(3), v(4), v(5)]);
        expectInvariants(db);
      } finally {
        close();
      }
    });

    test('the time floor alone binds', () => {
      const {db, close} = createLog();
      try {
        appendSeries(db, 1, 5);
        const purger = new SQLiteChangeLogPurger(db);

        // Transactions carry writeTimeMs = SEED_WRITE_TIME_MS + n.
        expect(
          purge(purger, {retentionCutoffMs: SEED_WRITE_TIME_MS + 3}),
        ).toMatchObject({deletedThrough: v(2), moreEligible: false});
        expect(watermarks(db)).toEqual([v(3), v(4), v(5)]);
        expectInvariants(db);
      } finally {
        close();
      }
    });

    test('the external floor binds when it is lower than the time floor', () => {
      const {db, close} = createLog();
      try {
        appendSeries(db, 1, 5);
        const purger = new SQLiteChangeLogPurger(db);

        expect(
          purge(purger, {
            externalFloor: v(2),
            retentionCutoffMs: SEED_WRITE_TIME_MS + 4,
          }),
        ).toMatchObject({deletedThrough: v(1)});
        expect(watermarks(db)).toEqual([v(2), v(3), v(4), v(5)]);
      } finally {
        close();
      }
    });

    test('the time floor binds when it is lower than the external floor', () => {
      const {db, close} = createLog();
      try {
        appendSeries(db, 1, 5);
        const purger = new SQLiteChangeLogPurger(db);

        expect(
          purge(purger, {
            externalFloor: v(4),
            retentionCutoffMs: SEED_WRITE_TIME_MS + 2,
          }),
        ).toMatchObject({deletedThrough: v(1)});
        expect(watermarks(db)).toEqual([v(2), v(3), v(4), v(5)]);
      } finally {
        close();
      }
    });

    test('the head cap binds when both floors are above the head', () => {
      const {db, close} = createLog();
      try {
        appendSeries(db, 1, 3);
        const purger = new SQLiteChangeLogPurger(db);

        // The stale-log case: the backup watermark has run past a log that
        // stopped advancing, and deleting to it would empty the file.
        expect(purge(purger, {externalFloor: v(99)})).toMatchObject({
          deletedThrough: v(2),
          moreEligible: false,
        });
        expect(watermarks(db)).toEqual([v(3)]);
      } finally {
        close();
      }
    });

    test('a retention cutoff far in the future never purges above the floor', () => {
      const {db, close} = createLog();
      try {
        appendSeries(db, 1, 5);
        const purger = new SQLiteChangeLogPurger(db);

        // The time bound is ANDed on top of the floor, never an alternative to
        // it: no cutoff can license deleting the floor's transaction.
        drain(purger, {
          externalFloor: v(3),
          retentionCutoffMs: Number.MAX_SAFE_INTEGER,
        });
        expect(watermarks(db)).toEqual([v(3), v(4), v(5)]);
      } finally {
        close();
      }
    });

    test('a cutoff below every write time purges nothing', () => {
      const {db, close} = createLog();
      try {
        appendSeries(db, 1, 5);
        const purger = new SQLiteChangeLogPurger(db);

        expect(purge(purger, {retentionCutoffMs: 0})).toEqual({
          deletedRows: 0,
          deletedThrough: undefined,
          moreEligible: false,
        });
        expect(rowCount(db)).toBe(2 + 5 * 3);
      } finally {
        close();
      }
    });
  });

  describe('the S3 safety property (invariant 14)', () => {
    // The floor is where "purged" and "gone" become the same thing: the change
    // log is excluded from the litestream backup, so above the confirmed backup
    // watermark it is the only copy a restoring follower can catch up from.
    // This gets its own case rather than being implied by the per-floor tests.
    test('no transaction at or above a moving floor is ever deleted', () => {
      const {db, path, close} = createLog();
      using reader = new SQLiteChangeLogReader(lc, path);
      try {
        const purger = new SQLiteChangeLogPurger(db);
        let next = 1;
        let floor = 0;
        // A fixed-seed LCG, so a failure is reproducible.
        let seed = 0x5eed;
        const rand = (n: number) => {
          seed = (seed * 1103515245 + 12345) & 0x7fffffff;
          return seed % n;
        };

        for (let round = 0; round < 60; round++) {
          const appended = 1 + rand(4);
          appendSeries(db, next, appended, 1 + rand(3));
          next += appended;

          // The backup watermark only ever advances, and always trails head.
          floor = Math.min(next - 1, floor + rand(3));
          const externalFloor = v(floor);

          purge(purger, {
            externalFloor,
            retentionCutoffMs: NO_CUTOFF,
            maxRows: 4 + rand(20),
          });

          const {min} = bounds(db);
          expect(min).not.toBeNull();
          // Row-count form: nothing at or above the floor was removed.
          expect(min! <= externalFloor).toBe(true);
          // Plan form: the follower that restored to exactly this watermark can
          // still catch up from here. This is the assertion that fails on a
          // non-strict floor comparison, which a row count alone would miss.
          expect(reader.plan(externalFloor)).toMatchObject({kind: 'range'});
          expectInvariants(db);
        }
      } finally {
        close();
      }
    });

    test('a subscriber exactly at the floor keeps its catchup boundary', () => {
      const {db, path, close} = createLog();
      using reader = new SQLiteChangeLogReader(lc, path);
      try {
        appendSeries(db, 1, 6);
        const purger = new SQLiteChangeLogPurger(db);

        drain(purger, {externalFloor: v(4)});

        expect(reader.plan(v(4))).toEqual({
          kind: 'range',
          minWatermark: v(4),
          headWatermark: v(6),
        });
        expect(reader.plan(v(3))).toEqual({
          kind: 'too-old',
          minWatermark: v(4),
          headWatermark: v(6),
        });
      } finally {
        close();
      }
    });
  });

  describe('batching', () => {
    test('deletes fewer than maxRows in one batch', () => {
      const {db, close} = createLog();
      try {
        appendSeries(db, 1, 3);
        const purger = new SQLiteChangeLogPurger(db);

        expect(purge(purger, {maxRows: 100})).toEqual({
          deletedRows: 8,
          deletedThrough: v(2),
          moreEligible: false,
        });
      } finally {
        close();
      }
    });

    test('stops at exactly maxRows and reports more eligible', () => {
      const {db, close} = createLog();
      try {
        // 2 seed rows, then transactions of 3 rows each.
        appendSeries(db, 1, 6);
        const purger = new SQLiteChangeLogPurger(db);

        expect(purge(purger, {maxRows: 5})).toEqual({
          deletedRows: 5,
          deletedThrough: v(1),
          moreEligible: true,
        });
        expect(watermarks(db)).toEqual([v(2), v(3), v(4), v(5), v(6)]);
      } finally {
        close();
      }
    });

    test('many small transactions drain across batches', () => {
      const {db, close} = createLog();
      try {
        appendSeries(db, 1, 200);
        const purger = new SQLiteChangeLogPurger(db);

        const batches = drain(purger, {maxRows: 20});

        expect(batches.length).toBeGreaterThan(1);
        for (const batch of batches.slice(0, -1)) {
          expect(batch.deletedRows).toBeGreaterThan(0);
          expect(batch.deletedRows).toBeLessThanOrEqual(20);
        }
        expect(watermarks(db)).toEqual([v(200)]);
        expectInvariants(db);
      } finally {
        close();
      }
    });

    test('repeated calls make strictly monotonic progress', () => {
      const {db, close} = createLog();
      try {
        appendSeries(db, 1, 40, 2);
        const purger = new SQLiteChangeLogPurger(db);

        let previousMin = bounds(db).min;
        let previousRows = rowCount(db);
        for (let i = 0; ; i++) {
          expect(i).toBeLessThan(1000);
          const batch = purge(purger, {maxRows: 9});
          const {min} = bounds(db);
          const rows = rowCount(db);
          if (batch.deletedRows > 0) {
            expect(rows).toBeLessThan(previousRows);
            if (batch.deletedThrough !== undefined) {
              expect(min! > previousMin!).toBe(true);
            }
          }
          previousMin = min;
          previousRows = rows;
          expectInvariants(db);
          if (!batch.moreEligible) {
            break;
          }
        }
        expect(watermarks(db)).toEqual([v(40)]);
      } finally {
        close();
      }
    });
  });

  describe('the oversized transaction', () => {
    const CHANGES = 400;
    const OVERSIZED_ROWS = CHANGES + 2;
    const MAX_ROWS = 40;

    /** A log whose minimum is one transaction far larger than a batch. */
    function createOversizedLog() {
      const log = createLog();
      append(log.db, v(1), CHANGES, SEED_WRITE_TIME_MS + 1);
      appendSeries(log.db, 2, 3);
      const purger = new SQLiteChangeLogPurger(log.db);
      // The seed goes first and whole; the tear starts on the next batch.
      expect(purge(purger, {maxRows: MAX_ROWS})).toEqual({
        deletedRows: 2,
        deletedThrough: SEED_WATERMARK,
        moreEligible: true,
      });
      expect(bounds(log.db).min).toBe(v(1));
      return {...log, purger};
    }

    test('is torn across chunks that each stay within the row target', () => {
      const {db, close, purger} = createOversizedLog();
      try {
        let chunks = 0;
        let torn = 0;
        for (let i = 0; ; i++) {
          expect(i).toBeLessThan(100);
          const result = purge(purger, {maxRows: MAX_ROWS});
          expect(result.deletedRows).toBeGreaterThan(0);
          // The bound that matters: an arbitrarily large transaction never
          // produces a batch larger than the target.
          expect(result.deletedRows).toBeLessThanOrEqual(MAX_ROWS);
          if (result.deletedThrough !== undefined) {
            break;
          }
          chunks++;
          torn += result.deletedRows;
          // Mid-tear: the transaction is still the minimum, and its commit row
          // is still there.
          expect(bounds(db).min).toBe(v(1));
          expectInvariants(db);
        }
        // 402 rows in 40-row chunks: ten tears, and the two rows that are left
        // ride out on the next ordinary whole-transaction delete.
        expect(chunks).toBe(10);
        expect(torn).toBe(chunks * MAX_ROWS);
        expect(rowCount(db, v(1))).toBe(0);
        expect(watermarks(db)).toEqual([v(4)]);
      } finally {
        close();
      }
    });

    test('keeps plan() answering range at the torn watermark and too-old below', () => {
      const {path, close, purger} = createOversizedLog();
      using reader = new SQLiteChangeLogReader(lc, path);
      try {
        for (;;) {
          const result = purge(purger, {maxRows: MAX_ROWS});
          if (result.deletedThrough !== undefined) {
            break;
          }
          // The boundary check reads the highest `pos` at the watermark, so
          // deleting bottom-up keeps it valid throughout.
          expect(reader.plan(v(1))).toMatchObject({
            kind: 'range',
            minWatermark: v(1),
          });
          expect(reader.plan(SEED_WATERMARK)).toMatchObject({kind: 'too-old'});
        }
        // And below the torn watermark it stays too-old after the tear too.
        expect(reader.plan(SEED_WATERMARK)).toMatchObject({kind: 'too-old'});
        expect(reader.plan(v(1))).toMatchObject({kind: 'too-old'});
      } finally {
        close();
      }
    });

    test('the final chunk removes the commit row and advances min together', () => {
      const {path, close, purger} = createOversizedLog();
      using observer = openChangeLogDB(lc, replicaFileOf(path), {
        readonly: true,
      });
      try {
        for (let i = 0; ; i++) {
          expect(i).toBeLessThan(100);
          // The floor keeps everything above the torn transaction ineligible,
          // so the batch that finishes the tear finishes nothing else.
          const result = purge(purger, {
            maxRows: MAX_ROWS,
            externalFloor: v(2),
          });
          const {min} = bounds(observer);
          const remaining = rowCount(observer, v(1));
          // Seen from another connection, "the boundary is gone" and "the
          // minimum moved on" are never observable apart: they land in the same
          // SQLite transaction.
          expect(remaining === 0).toBe(min !== v(1));
          if (result.deletedThrough !== undefined) {
            expect(result.deletedThrough).toBe(v(1));
            expect(remaining).toBe(0);
            expect(min).toBe(v(2));
            expect(result.moreEligible).toBe(false);
            break;
          }
          expect(remaining).toBeGreaterThan(0);
        }
      } finally {
        close();
      }
    });

    // The minimum is torn even when it is not itself in the eligible set, which
    // a writer whose clock went backwards can produce. Safe because both
    // watermark bounds are prefix-closed — a non-empty eligible set implies
    // `min < externalFloor` and `min < head` — and the prefix delete the batch
    // would otherwise run removes the minimum anyway.
    test('finishes a minimum that the time floor alone would have retained', () => {
      const file = new DbFile('sqlite-change-log-purger');
      files.push(file);
      // The seed's writeTimeMs is the change-streamer's own clock at
      // reconciliation, which can sit ahead of the upstream commit times the
      // transactions that follow carry.
      const {db} = openChangeLogDBForWriting(lc, file.path, {
        ...ANCHOR,
        nowMs: 5_000,
      });
      try {
        append(db, v(1), 400, 2_000);
        appendSeries(db, 2, 1);
        const purger = new SQLiteChangeLogPurger(db);
        const opts = {maxRows: 40, retentionCutoffMs: 3_000};

        // v(1) is eligible and does not fit; the seed is not eligible at all.
        const first = purge(purger, opts);
        expect(first).toEqual({
          deletedRows: 2,
          deletedThrough: SEED_WATERMARK,
          moreEligible: true,
        });
        expect(bounds(db).min).toBe(v(1));

        drain(purger, opts);
        expect(watermarks(db)).toEqual([v(2)]);
        expectInvariants(db);
      } finally {
        db.close();
      }
    });

    test('a reader started at the torn watermark is unaffected', async () => {
      const {db, path, close, purger} = createOversizedLog();
      using reader = new SQLiteChangeLogReader(lc, path);
      try {
        const {head} = bounds(db);
        expect(reader.plan(v(1))).toMatchObject({kind: 'range'});

        const delivered: WatermarkedChange[] = [];
        for await (const batch of reader.read(v(1), head!, 2)) {
          delivered.push(...batch);
          // Interleave the tear with the read. It only ever touches rows the
          // reader started strictly after.
          purge(purger, {maxRows: MAX_ROWS, externalFloor: v(2)});
        }

        expect([...new Set(delivered.map(([watermark]) => watermark))]).toEqual(
          [v(2), v(3), v(4)],
        );
      } finally {
        close();
      }
    });

    test('a crash mid-tear resumes with no recovery state', () => {
      const {db, path, close, purger} = createOversizedLog();
      try {
        expect(purge(purger, {maxRows: MAX_ROWS})).toMatchObject({
          deletedThrough: undefined,
          moreEligible: true,
        });
        const remaining = rowCount(db, v(1));
        expect(remaining).toBeGreaterThan(0);
        expect(remaining).toBeLessThan(OVERSIZED_ROWS);
        close();

        // Reopening is a reconcile: the tear is at the tail, so the head still
        // lands on the resume watermark and nothing is wiped.
        const replicaFile = path.replace(/-change-log$/, '');
        const {db: reopened, result} = openChangeLogDBForWriting(
          lc,
          replicaFile,
          {...ANCHOR, resumeWatermark: v(4)},
        );
        try {
          expect(result).toEqual({
            action: 'none',
            head: v(4),
            cookiesStale: false,
          });
          expect(rowCount(reopened, v(1))).toBe(remaining);

          drain(new SQLiteChangeLogPurger(reopened), {maxRows: MAX_ROWS});
          expect(watermarks(reopened)).toEqual([v(4)]);
          expectInvariants(reopened);
        } finally {
          reopened.close();
        }
      } finally {
        close();
      }
    });

    test('a tear respects the external floor like any other batch', () => {
      const {db, close, purger} = createOversizedLog();
      try {
        // Only the torn transaction is below the floor, so the drain must stop
        // once it completes rather than continuing into v(2).
        drain(purger, {maxRows: MAX_ROWS, externalFloor: v(2)});
        expect(watermarks(db)).toEqual([v(2), v(3), v(4)]);
        expectInvariants(db);
      } finally {
        close();
      }
    });
  });

  describe('the duration bound', () => {
    // `maxRows` cannot bound a statement's duration on its own, because row
    // cost varies with payload size. The purger measures each delete and
    // shrinks its budget, which is what keeps a purge statement from blocking
    // the stream loop — purge shares the writer's connection, so a long delete
    // stalls the appending path and the forward behind it directly.
    test('shrinks the row budget when a delete overruns', () => {
      const {db, close} = createLog();
      const clock = fakeClock();
      try {
        appendSeries(db, 1, 200);
        const purger = new SQLiteChangeLogPurger(db, clock.now);
        clock.elapsedMs = 200; // against a 100 ms target

        purge(purger, {maxRows: 64, maxDurationMs: 100});
        expect(purger.budgetRows).toBe(32);
        purge(purger, {maxRows: 64, maxDurationMs: 100});
        expect(purger.budgetRows).toBe(16);
      } finally {
        close();
      }
    });

    test('grows the budget back once deletes come in under the target', () => {
      const {db, close} = createLog();
      const clock = fakeClock();
      try {
        // Three-row transactions, so a budget that is a multiple of three is
        // consumed exactly.
        appendSeries(db, 1, 100);
        const purger = new SQLiteChangeLogPurger(db, clock.now);
        const opts = {maxRows: 66, maxDurationMs: 100};

        clock.elapsedMs = 200;
        purge(purger, opts);
        expect(purger.budgetRows).toBe(33);

        clock.elapsedMs = 0;
        expect(purge(purger, opts).deletedRows).toBe(33);
        expect(purger.budgetRows).toBe(66);
      } finally {
        close();
      }
    });

    test('does not grow when the eligible set, not the budget, was the limit', () => {
      const {db, close} = createLog();
      const clock = fakeClock();
      try {
        appendSeries(db, 1, 30);
        const purger = new SQLiteChangeLogPurger(db, clock.now);
        const opts = {maxRows: 66, maxDurationMs: 100};

        clock.elapsedMs = 200;
        purge(purger, opts);
        expect(purger.budgetRows).toBe(33);

        // Fast, but only because there was almost nothing left to delete. That
        // is no evidence that a larger statement would stay under the target.
        clock.elapsedMs = 0;
        expect(purge(purger, opts).deletedRows).toBeLessThan(33);
        expect(purger.budgetRows).toBe(33);
      } finally {
        close();
      }
    });

    test('never grows past maxRows and never shrinks below one row', () => {
      const {db, close} = createLog();
      const clock = fakeClock();
      try {
        appendSeries(db, 1, 50);
        const purger = new SQLiteChangeLogPurger(db, clock.now);
        clock.elapsedMs = 1_000;

        for (let i = 0; i < 20; i++) {
          const result = purge(purger, {maxRows: 8, maxDurationMs: 1});
          expect(result.deletedRows).toBeGreaterThan(0);
          expect(result.deletedRows).toBeLessThanOrEqual(8);
          expect(purger.budgetRows!).toBeGreaterThanOrEqual(1);
          expect(purger.budgetRows!).toBeLessThanOrEqual(8);
        }
        // A one-row budget still makes progress, by tearing one row at a time.
        expect(purger.budgetRows).toBe(1);
        expect(rowCount(db)).toBeLessThan(2 + 50 * 3);
        expectInvariants(db);
      } finally {
        close();
      }
    });

    test('an oversized transaction stays within the duration target', () => {
      const {db, close} = createLog();
      try {
        // A real transaction rather than a stubbed clock: the failure mode is
        // statement duration.
        append(db, v(1), 20_000, SEED_WRITE_TIME_MS + 1);
        appendSeries(db, 2, 1);
        const purger = new SQLiteChangeLogPurger(db);

        const maxDurationMs = 100;
        for (const _ of drain(purger, {maxRows: 1000, maxDurationMs})) {
          // The governor only ever narrows the budget it was given.
          expect(purger.budgetRows!).toBeLessThanOrEqual(1000);
        }
        expect(watermarks(db)).toEqual([v(2)]);
      } finally {
        close();
      }
    });

    test('rejects a non-positive batch size or duration', () => {
      const {db, close} = createLog();
      try {
        const purger = new SQLiteChangeLogPurger(db);
        expect(() => purge(purger, {maxRows: 0})).toThrow(/maxRows/);
        expect(() => purge(purger, {maxRows: 1.5})).toThrow(/maxRows/);
        expect(() => purge(purger, {maxDurationMs: 0})).toThrow(
          /maxDurationMs/,
        );
        expect(db.inTransaction).toBe(false);
      } finally {
        close();
      }
    });
  });

  describe('concurrency', () => {
    test('a reader on a second connection stays valid across a purge', async () => {
      const {db, path, close} = createLog();
      using reader = new SQLiteChangeLogReader(lc, path);
      try {
        appendSeries(db, 1, 12, 2);
        const purger = new SQLiteChangeLogPurger(db);

        const plan = reader.plan(v(6));
        expect(plan).toMatchObject({kind: 'range', headWatermark: v(12)});

        const delivered: WatermarkedChange[] = [];
        for await (const batch of reader.read(v(6), v(12), 3)) {
          delivered.push(...batch);
          // Purge up to, but never into, the range the reader pinned.
          purge(purger, {externalFloor: v(6)});
        }

        expect([...new Set(delivered.map(([watermark]) => watermark))]).toEqual(
          [v(7), v(8), v(9), v(10), v(11), v(12)],
        );
        expect(bounds(db).min).toBe(v(6));
      } finally {
        close();
      }
    });

    test('rolls the batch back and leaves no transaction open on failure', () => {
      const {db, close} = createLog();
      try {
        appendSeries(db, 1, 4);
        const purger = new SQLiteChangeLogPurger(db);
        db.exec(/*sql*/ `
          CREATE TRIGGER "no_deletes" BEFORE DELETE ON "${CHANGE_LOG_STREAM_TABLE}"
          BEGIN SELECT RAISE(ABORT, 'boom'); END;
        `);

        expect(() => purge(purger)).toThrow('boom');
        expect(db.inTransaction).toBe(false);
        expect(rowCount(db)).toBe(2 + 4 * 3);
      } finally {
        close();
      }
    });

    test('does not roll back a transaction the failure already closed', () => {
      // SQLite rolls back automatically for some errors (SQLITE_FULL,
      // SQLITE_IOERR); issuing ROLLBACK then throws and masks the real cause.
      const cause = new Error('database or disk is full');
      const executed: string[] = [];
      const fail = () => {
        throw cause;
      };
      const db = {
        inTransaction: false,
        exec: (sql: string) => void executed.push(sql),
        prepare: () => ({all: fail, get: fail, run: fail}),
      } as unknown as Database;

      expect(() => purge(new SQLiteChangeLogPurger(db))).toThrow(cause);
      expect(executed).toEqual(['BEGIN IMMEDIATE']);
    });
  });

  describe('property: invariants hold under arbitrary schedules', () => {
    /**
     * Rows per transaction, by watermark. The unit of the safety argument:
     * a purge may remove whole transactions below the floor and tear the
     * minimum, but must never thin a transaction it retains.
     */
    function txRowCounts(db: Database): Map<string, number> {
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

    const purgeFuzzScenario = fc.record({
      // The initial series. `skew` makes `writeTimeMs` non-monotone, the
      // clock-went-backwards case the accounting comments reason about.
      txs: fc.array(
        fc.record({
          width: fc.integer({min: 0, max: 6}),
          skew: fc.integer({min: -5, max: 5}),
        }),
        {minLength: 1, maxLength: 10},
      ),
      // Purge batches with arbitrary bounds, interleaved with live appends.
      ops: fc.array(
        fc.oneof(
          fc.record({
            kind: fc.constant('append' as const),
            width: fc.integer({min: 0, max: 4}),
            skew: fc.integer({min: -5, max: 5}),
          }),
          fc.record({
            kind: fc.constant('purge' as const),
            // Floors below, among, at, and beyond every written watermark.
            floorNum: fc.integer({min: 0, max: 25}),
            // Cutoffs that make none, some, or all of the history eligible.
            cutoffOffset: fc.integer({min: -5, max: 30}),
            maxRows: fc.integer({min: 1, max: 12}),
          }),
        ),
        {minLength: 1, maxLength: 12},
      ),
    });

    test('any batch schedule preserves the floor, the head, and the boundary', () => {
      fc.assert(
        fc.property(purgeFuzzScenario, ({txs, ops}) => {
          const {db, close} = createLog();
          try {
            let nextWm = 1;
            const appendTx = (width: number, skew: number) => {
              append(db, v(nextWm), width, SEED_WRITE_TIME_MS + nextWm + skew);
              nextWm++;
            };
            txs.forEach(({width, skew}) => appendTx(width, skew));

            const purger = new SQLiteChangeLogPurger(db);
            for (const op of ops) {
              if (op.kind === 'append') {
                appendTx(op.width, op.skew);
                continue;
              }
              const opts = {
                externalFloor: v(op.floorNum),
                retentionCutoffMs: SEED_WRITE_TIME_MS + op.cutoffOffset,
                maxRows: op.maxRows,
              };
              const before = txRowCounts(db);
              const headBefore = bounds(db).head;
              const result = purge(purger, opts);
              const after = txRowCounts(db);

              // Structure: everything above the minimum is whole, and the
              // minimum — whole or torn — still ends at its commit row.
              expectInvariants(db);

              // The head cap: the latest transaction is never a candidate.
              expect(bounds(db).head).toBe(headBefore);

              // The floor is the safety property: every transaction at or
              // above it survives, whole. Only the retention *courtesy* may
              // be crossed by a clock that went backwards — never these.
              let deleted = 0;
              for (const [w, n] of before) {
                const remaining = after.get(w) ?? 0;
                expect(remaining).toBeLessThanOrEqual(n);
                deleted += n - remaining;
                if (remaining < n) {
                  expect(w < opts.externalFloor).toBe(true);
                  expect(w < headBefore!).toBe(true);
                }
              }
              expect(result.deletedRows).toBe(deleted);

              // Progress and honesty: a batch that reports more work did
              // some, and one that reports none has none left.
              if (result.moreEligible) {
                expect(result.deletedRows).toBeGreaterThan(0);
              } else {
                expect(purge(purger, opts).deletedRows).toBe(0);
              }
            }

            // Whatever the schedule left behind drains to quiescence in
            // bounded batches; drain() throws if purge stops terminating.
            drain(purger, {maxRows: 5});
            expectInvariants(db);
          } finally {
            close();
          }
        }),
        {numRuns: 50},
      );
    });
  });

  describe('transaction sizing', () => {
    // Sizing a candidate must not scan more rows than the batch may delete:
    // the count runs per candidate inside the BEGIN IMMEDIATE, and an
    // uncapped count of a huge transaction would rescan every remaining row
    // on every tear batch, holding the write lock for the duration.
    test('the transaction count is capped at its limit', () => {
      const {db, close} = createLog();
      try {
        append(db, v(1), 10, SEED_WRITE_TIME_MS + 1); // 12 rows with begin+commit
        const count = (limit: number) =>
          db
            .prepare(SQLITE_CHANGE_LOG_COUNT_TRANSACTION_SQL)
            .get<{rows: number}>({watermark: v(1), limit}).rows;

        expect(count(20)).toBe(12); // exact when the transaction fits
        expect(count(5)).toBe(5); // capped when it cannot
        expect(count(12)).toBe(12); // an exact fit is not mistaken for overflow
      } finally {
        close();
      }
    });

    test('the capped transaction count seeks the primary key', () => {
      const {db, close} = createLog();
      try {
        appendSeries(db, 1, 5, 5);
        const details = queryPlan(db, SQLITE_CHANGE_LOG_COUNT_TRANSACTION_SQL, {
          watermark: v(1),
          limit: 10,
        });
        const steps = details.filter(detail =>
          detail.includes(CHANGE_LOG_STREAM_TABLE),
        );
        expect(steps).toHaveLength(1);
        expect(steps[0]).toMatch(/^SEARCH/);
        expect(steps[0]).toMatch(/COVERING INDEX/);
      } finally {
        close();
      }
    });
  });

  describe('query plan', () => {
    test('the eligible-commits query searches the partial index', () => {
      const {db, close} = createLog();
      try {
        appendSeries(db, 1, 20, 50);

        const details = queryPlan(db, SQLITE_CHANGE_LOG_ELIGIBLE_COMMITS_SQL, {
          retentionCutoffMs: NO_CUTOFF,
          externalFloor: NO_FLOOR,
          limit: 10,
        });
        const steps = details.filter(detail =>
          detail.includes(CHANGE_LOG_STREAM_TABLE),
        );
        // The retention scan and the head subquery, both seeks. The partial
        // index carries one entry per transaction rather than one per change,
        // and the ORDER BY rides it instead of sorting.
        expect(steps).toHaveLength(2);
        expect(steps.filter(detail => !detail.startsWith('SEARCH'))).toEqual(
          [],
        );
        expect(steps[0]).toMatch(
          new RegExp(
            `USING COVERING INDEX ${CHANGE_LOG_STREAM_WRITE_TIME_INDEX}`,
          ),
        );
        // The ORDER BY rides the index instead of sorting the eligible set.
        expect(details.join('\n')).not.toMatch(/USE TEMP B-TREE/);
      } finally {
        close();
      }
    });
  });
});
