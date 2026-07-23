import {afterEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {DbFile} from '../../test/lite.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {serializeChangeStreamData} from '../change-streamer/change-log-codec.ts';
import type {WatermarkedChange} from '../change-streamer/change-streamer.ts';
import {SQLiteChangeLogReader} from '../change-streamer/sqlite-change-log-reader.ts';
import {ChangeLogStreamWriter} from './change-log-stream-writer.ts';
import {CHANGE_LOG_STREAM_WRITE_TIME_INDEX} from './schema/change-log-stream.ts';
import {
  initReplicationState,
  updateReplicationWatermark,
} from './schema/replication-state.ts';
import {
  SQLITE_CHANGE_LOG_TIME_FLOOR_SQL,
  SQLiteChangeLogPurger,
  type SQLiteChangeLogPurgeResult,
} from './sqlite-change-log-purger.ts';

const lc = createSilentLogContext();
const files: DbFile[] = [];

afterEach(() => {
  for (const file of files.splice(0)) {
    file.delete();
  }
});

function createReplica(seedWriteTimeMs = 100): {db: Database; file: DbFile} {
  const file = new DbFile('sqlite-change-log-purger');
  files.push(file);
  const db = file.connect(lc);
  db.pragma('journal_mode = wal');
  initReplicationState(db, ['zero_data'], '02');
  db.prepare(/*sql*/ `
    UPDATE "_zero.changeLogStream"
    SET "writeTimeMs" = ?
    WHERE "watermark" = '02' AND "writeTimeMs" IS NOT NULL
  `).run(seedWriteTimeMs);
  db.prepare(/*sql*/ `
    UPDATE "_zero.replicationState" SET "writeTimeMs" = ?
  `).run(seedWriteTimeMs);
  return {db, file};
}

function truncate(): ChangeStreamData {
  return ['data', {tag: 'truncate', relations: []}];
}

function appendTransaction(
  db: Database,
  watermark: string,
  totalRows: number,
  writeTimeMs: number,
): readonly WatermarkedChange[] {
  expect(totalRows).toBeGreaterThanOrEqual(2);
  const runner = new StatementRunner(db);
  const writer = new ChangeLogStreamWriter(db);
  const begin: ChangeStreamData = [
    'begin',
    {tag: 'begin'},
    {commitWatermark: watermark},
  ];
  const data = Array.from({length: totalRows - 2}, truncate);
  const commit: ChangeStreamData = ['commit', {tag: 'commit'}, {watermark}];

  runner.beginImmediate();
  try {
    writer.begin(watermark, serializeChangeStreamData(begin));
    for (const message of data) {
      writer.append(serializeChangeStreamData(message), message[1].tag);
    }
    writer.commit(watermark, serializeChangeStreamData(commit), writeTimeMs);
    updateReplicationWatermark(runner, watermark, writeTimeMs);
    runner.commit();
  } catch (e) {
    runner.rollback();
    throw e;
  }

  return [begin, ...data, commit].map(message => [
    watermark,
    message[1].tag,
    serializeChangeStreamData(message),
  ]);
}

function watermarks(db: Database): readonly string[] {
  return db
    .prepare(/*sql*/ `
      SELECT DISTINCT "watermark"
      FROM "_zero.changeLogStream"
      ORDER BY "watermark"
    `)
    .all<{watermark: string}>()
    .map(({watermark}) => watermark);
}

function assertCompleteTransactions(db: Database): void {
  expect(
    db
      .prepare(/*sql*/ `
        SELECT "watermark"
        FROM "_zero.changeLogStream"
        GROUP BY "watermark"
        HAVING min("pos") <> 0
          OR max("pos") <> count(*) - 1
          OR json_extract(
            max(CASE WHEN "pos" = 0 THEN "change" END), '$.tag'
          ) <> 'begin'
          OR json_extract(
            max(CASE WHEN "precommit" IS NOT NULL THEN "change" END), '$.tag'
          ) <> 'commit'
          OR sum(CASE WHEN "precommit" IS NOT NULL THEN 1 ELSE 0 END) <> 1
      `)
      .all(),
  ).toEqual([]);
}

function populateFourTransactions(db: Database): void {
  appendTransaction(db, '04', 2, 200);
  appendTransaction(db, '06', 2, 300);
  appendTransaction(db, '08', 2, 400);
}

describe('replicator/sqlite-change-log-purger', () => {
  test.each([
    {
      name: 'external floor is limiting',
      externalFloor: '04',
      retentionCutoffMs: 250,
      expectedTimeFloor: '06',
      expectedEffectiveFloor: '04',
      expectedWatermarks: ['04', '06', '08'],
    },
    {
      name: 'time floor is limiting',
      externalFloor: '08',
      retentionCutoffMs: 250,
      expectedTimeFloor: '06',
      expectedEffectiveFloor: '06',
      expectedWatermarks: ['06', '08'],
    },
    {
      name: 'head caps an ahead external floor',
      externalFloor: '0a',
      retentionCutoffMs: 500,
      expectedTimeFloor: '08',
      expectedEffectiveFloor: '08',
      expectedWatermarks: ['08'],
    },
    {
      name: 'equal external and time floors',
      externalFloor: '06',
      retentionCutoffMs: 300,
      expectedTimeFloor: '06',
      expectedEffectiveFloor: '06',
      expectedWatermarks: ['06', '08'],
    },
    {
      name: 'time floor and head are equal',
      externalFloor: '0a',
      retentionCutoffMs: 400,
      expectedTimeFloor: '08',
      expectedEffectiveFloor: '08',
      expectedWatermarks: ['08'],
    },
  ])(
    'combines retention floors when $name',
    ({
      externalFloor,
      retentionCutoffMs,
      expectedTimeFloor,
      expectedEffectiveFloor,
      expectedWatermarks,
    }) => {
      const {db} = createReplica();
      using writer = db;
      populateFourTransactions(writer);
      const purger = new SQLiteChangeLogPurger(writer);

      const result = purger.purgeBatch({
        externalFloor,
        retentionCutoffMs,
        maxRows: 100,
      });

      expect(result).toMatchObject({
        headWatermark: '08',
        timeFloor: expectedTimeFloor,
        effectiveFloor: expectedEffectiveFloor,
        moreEligible: false,
      });
      expect(result.deletedRows).toBe((4 - expectedWatermarks.length) * 2);
      expect(watermarks(writer)).toEqual(expectedWatermarks);
      expect(watermarks(writer)).toContain('08');
      assertCompleteTransactions(writer);
    },
  );

  test.each([
    {
      name: 'zero eligible rows',
      externalFloor: '02',
      maxRows: 10,
      expectedDeletedRows: 0,
      expectedDeletedBefore: undefined,
    },
    {
      name: 'fewer rows than the target',
      externalFloor: '08',
      maxRows: 10,
      expectedDeletedRows: 6,
      expectedDeletedBefore: '08',
    },
    {
      name: 'exactly the target rows',
      externalFloor: '08',
      maxRows: 6,
      expectedDeletedRows: 6,
      expectedDeletedBefore: '08',
    },
  ])(
    'handles $name',
    ({externalFloor, maxRows, expectedDeletedRows, expectedDeletedBefore}) => {
      const {db} = createReplica();
      using writer = db;
      populateFourTransactions(writer);
      const purger = new SQLiteChangeLogPurger(writer);

      const result = purger.purgeBatch({
        externalFloor,
        retentionCutoffMs: Number.MAX_SAFE_INTEGER,
        maxRows,
      });

      expect(result).toMatchObject({
        deletedRows: expectedDeletedRows,
        deletedBeforeWatermark: expectedDeletedBefore,
        moreEligible: false,
      });
      assertCompleteTransactions(writer);
    },
  );

  test('bounds many small transactions and makes monotonic progress', () => {
    const {db} = createReplica();
    using writer = db;
    appendTransaction(writer, '04', 2, 200);
    appendTransaction(writer, '06', 2, 300);
    appendTransaction(writer, '08', 2, 400);
    appendTransaction(writer, '0a', 2, 500);
    appendTransaction(writer, '0c', 2, 600);
    const purger = new SQLiteChangeLogPurger(writer);
    const results: SQLiteChangeLogPurgeResult[] = [];

    do {
      const result = purger.purgeBatch({
        externalFloor: '0c',
        retentionCutoffMs: Number.MAX_SAFE_INTEGER,
        maxRows: 5,
      });
      results.push(result);
      expect(result.deletedRows).toBeGreaterThan(0);
      expect(result.deletedRows).toBeLessThanOrEqual(5);
      expect(watermarks(writer)).toContain('0c');
      assertCompleteTransactions(writer);
    } while (results.at(-1)?.moreEligible);

    expect(results.map(({deletedRows}) => deletedRows)).toEqual([4, 4, 2]);
    expect(
      results.map(({deletedBeforeWatermark}) => deletedBeforeWatermark),
    ).toEqual(['06', '0a', '0c']);
    expect(results.map(({moreEligible}) => moreEligible)).toEqual([
      true,
      true,
      false,
    ]);
    expect(watermarks(writer)).toEqual(['0c']);

    expect(
      purger.purgeBatch({
        externalFloor: '0c',
        retentionCutoffMs: Number.MAX_SAFE_INTEGER,
        maxRows: 5,
      }),
    ).toMatchObject({
      deletedRows: 0,
      deletedBeforeWatermark: undefined,
      moreEligible: false,
    });
  });

  test('deletes one oversized oldest transaction as a soft-limit batch', () => {
    const {db} = createReplica();
    using writer = db;
    appendTransaction(writer, '04', 9, 200);
    appendTransaction(writer, '06', 2, 300);
    appendTransaction(writer, '08', 2, 400);
    const purger = new SQLiteChangeLogPurger(writer);

    // Remove the seed so 04 is the oldest transaction for the focused call.
    expect(
      purger.purgeBatch({
        externalFloor: '04',
        retentionCutoffMs: Number.MAX_SAFE_INTEGER,
        maxRows: 2,
      }).deletedRows,
    ).toBe(2);

    const oversized = purger.purgeBatch({
      externalFloor: '08',
      retentionCutoffMs: Number.MAX_SAFE_INTEGER,
      maxRows: 3,
    });
    expect(oversized).toMatchObject({
      deletedRows: 9,
      deletedBeforeWatermark: '06',
      moreEligible: true,
    });
    expect(watermarks(writer)).toEqual(['06', '08']);
    assertCompleteTransactions(writer);

    const final = purger.purgeBatch({
      externalFloor: '08',
      retentionCutoffMs: Number.MAX_SAFE_INTEGER,
      maxRows: 3,
    });
    expect(final).toMatchObject({
      deletedRows: 2,
      deletedBeforeWatermark: '08',
      moreEligible: false,
    });
    expect(watermarks(writer)).toEqual(['08']);
    assertCompleteTransactions(writer);
  });

  test('uses the partial write-time index for the retention floor', () => {
    const {db} = createReplica();
    using writer = db;
    populateFourTransactions(writer);

    const plans = writer
      .prepare(`EXPLAIN QUERY PLAN ${SQLITE_CHANGE_LOG_TIME_FLOOR_SQL}`)
      .all<{detail: string}>(250);
    const details = plans.map(({detail}) => detail).join('\n');
    expect(details).toMatch(
      new RegExp(
        `\\bSEARCH\\b.*${CHANGE_LOG_STREAM_WRITE_TIME_INDEX.replaceAll(
          '.',
          '\\.',
        )}`,
      ),
    );
    expect(details).not.toMatch(/\bSCAN\b|USE TEMP B-TREE/);
  });

  test('preserves a reader pinned on a second connection while purging', async () => {
    const {db, file} = createReplica();
    using writer = db;
    const tx04 = appendTransaction(writer, '04', 2, 200);
    const tx06 = appendTransaction(writer, '06', 5, 300);
    const tx08 = appendTransaction(writer, '08', 2, 400);
    using reader = new SQLiteChangeLogReader(lc, file.path);
    const plan = reader.plan('04');
    expect(plan).toMatchObject({kind: 'range', headWatermark: '08'});
    if (plan.kind !== 'range') {
      throw new Error('expected a catchable SQLite range');
    }

    const iterator = reader
      .read('04', plan.headWatermark, 1)
      [Symbol.asyncIterator]();
    const first = await iterator.next();
    expect(first.done).toBe(false);

    const purger = new SQLiteChangeLogPurger(writer);
    expect(
      purger.purgeBatch({
        externalFloor: '04',
        retentionCutoffMs: Number.MAX_SAFE_INTEGER,
        maxRows: 100,
      }),
    ).toMatchObject({deletedRows: 2, moreEligible: false});

    const batches: (readonly WatermarkedChange[])[] = [];
    if (first.value !== undefined) {
      batches.push(first.value);
    }
    for (;;) {
      const next = await iterator.next();
      if (next.done) {
        break;
      }
      batches.push(next.value);
    }

    expect(batches.flatMap(batch => batch)).toEqual([...tx06, ...tx08]);
    expect(watermarks(writer)).toEqual(['04', '06', '08']);
    expect(tx04).toHaveLength(2);
    assertCompleteTransactions(writer);
  });

  test('validates batch inputs and transaction ownership', () => {
    const {db} = createReplica();
    using writer = db;
    const purger = new SQLiteChangeLogPurger(writer);

    expect(() =>
      purger.purgeBatch({
        externalFloor: '02',
        retentionCutoffMs: Number.NaN,
        maxRows: 1,
      }),
    ).toThrow('retention cutoff must be a safe integer');
    expect(() =>
      purger.purgeBatch({
        externalFloor: '02',
        retentionCutoffMs: 0,
        maxRows: 0,
      }),
    ).toThrow('purge batch size must be a positive safe integer');

    const runner = new StatementRunner(writer);
    runner.beginImmediate();
    expect(() =>
      purger.purgeBatch({
        externalFloor: '02',
        retentionCutoffMs: 0,
        maxRows: 1,
      }),
    ).toThrow('purge must start outside a transaction');
    expect(writer.inTransaction).toBe(true);
    runner.rollback();
  });
});
