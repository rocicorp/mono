import {existsSync} from 'node:fs';
import SQLite3Database from '@rocicorp/zero-sqlite3';
import {afterEach, describe, expect, test} from 'vitest';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Database, Statement} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {DbFile} from '../../test/lite.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {
  CHANGE_LOG_STREAM_TABLE,
  changeLogFileName,
  deleteChangeLogDB,
  openChangeLogDB,
  reconcileChangeLog,
  type ReplicaAnchor,
} from '../replicator/change-log-db.ts';
import {ChangeLogStreamWriter} from '../replicator/change-log-stream-writer.ts';
import {serializeChangeStreamData} from './change-log-codec.ts';
import type {WatermarkedChange} from './change-streamer.ts';
import {
  SQLITE_CHANGE_LOG_BOUNDARY_SQL,
  SQLITE_CHANGE_LOG_PLAN_SQL,
  SQLITE_CHANGE_LOG_READ_BATCH_SQL,
  SQLiteChangeLogReader,
} from './sqlite-change-log-reader.ts';

const lc = createSilentLogContext();
const files: DbFile[] = [];

afterEach(() => {
  for (const file of files.splice(0)) {
    deleteChangeLogDB(file.path);
    file.delete();
  }
});

/**
 * The replica the change log is anchored to. No fixture in this file creates
 * that replica: the reader is served entirely by the change-log database.
 */
const ANCHOR: ReplicaAnchor = {
  replicaVersion: '01',
  stateVersion: '02',
  writeTimeMs: 1234567890,
};

/** The replica path whose `-change-log` sibling the fixtures build. */
function newReplicaPath(): DbFile {
  const file = new DbFile('sqlite-change-log-reader');
  files.push(file);
  return file;
}

/** A change-log database seeded at {@link ANCHOR}, with no replica beside it. */
function createChangeLog(): {db: Database; file: DbFile; path: string} {
  const file = newReplicaPath();
  const db = openChangeLogDB(lc, file.path, {readonly: false});
  db.pragma('journal_mode = wal');
  reconcileChangeLog(lc, db, ANCHOR);
  return {db, file, path: changeLogFileName(file.path)};
}

function truncate(): ChangeStreamData {
  return ['data', {tag: 'truncate', relations: []}];
}

function appendTransaction(
  db: Database,
  watermark: string,
  data: readonly ChangeStreamData[],
): readonly WatermarkedChange[] {
  const writer = new ChangeLogStreamWriter(new StatementRunner(db));
  const begin: ChangeStreamData = [
    'begin',
    {tag: 'begin'},
    {commitWatermark: watermark},
  ];
  const commit: ChangeStreamData = ['commit', {tag: 'commit'}, {watermark}];

  try {
    writer.begin(watermark, serializeChangeStreamData(begin));
    for (const message of data) {
      writer.append(serializeChangeStreamData(message), message[1].tag);
    }
    writer.commit(watermark, serializeChangeStreamData(commit), Date.now());
  } catch (e) {
    writer.rollback();
    throw e;
  }

  return [begin, ...data, commit].map(message => [
    watermark,
    message[1].tag,
    serializeChangeStreamData(message),
  ]);
}

async function collect(
  reader: SQLiteChangeLogReader,
  fromWatermark: string,
  throughWatermark: string,
  batchSize: number,
): Promise<readonly (readonly WatermarkedChange[])[]> {
  const batches: (readonly WatermarkedChange[])[] = [];
  for await (const batch of reader.read(
    fromWatermark,
    throughWatermark,
    batchSize,
  )) {
    batches.push(batch);
  }
  return batches;
}

function flatten(
  batches: readonly (readonly WatermarkedChange[])[],
): readonly WatermarkedChange[] {
  return batches.flatMap(batch => batch);
}

function countVisitedRows(statement: Statement): number {
  let visited = 0;
  for (let i = 0; ; i++) {
    const nvisit = statement.scanStatus(
      i,
      SQLite3Database.SQLITE_SCANSTAT_NVISIT,
      1,
    );
    if (nvisit === undefined) {
      return visited;
    }
    // Steps that are not loops (a subquery boundary, a constant row) report -1
    // rather than a row count.
    visited += Math.max(Number(nvisit), 0);
  }
}

function queryPlan(db: Database, sql: string, ...params: unknown[]): string[] {
  return db
    .prepare(`EXPLAIN QUERY PLAN ${sql}`)
    .all<{detail: string}>(...params)
    .map(({detail}) => detail);
}

describe('sqlite change log reader', () => {
  test('plans seed, minimum, middle, head, too-old, missing, and ahead watermarks', () => {
    const {db, path} = createChangeLog();
    using writer = db;
    appendTransaction(writer, '04', [truncate()]);
    appendTransaction(writer, '06', [truncate()]);
    appendTransaction(writer, '08', [truncate()]);
    using reader = new SQLiteChangeLogReader(lc, path);

    const bounds = {minWatermark: '02', headWatermark: '08'};
    expect(reader.plan('02')).toEqual({kind: 'range', ...bounds});
    expect(reader.plan('04')).toEqual({kind: 'range', ...bounds});
    expect(reader.plan('06')).toEqual({kind: 'range', ...bounds});
    expect(reader.plan('08')).toEqual({kind: 'range', ...bounds});
    expect(reader.plan('01')).toEqual({kind: 'too-old', ...bounds});
    expect(reader.plan('05')).toEqual({kind: 'too-old', ...bounds});
    expect(reader.plan('09')).toEqual({
      kind: 'ahead',
      headWatermark: '08',
    });

    writer
      .prepare(
        `DELETE FROM "${CHANGE_LOG_STREAM_TABLE}" WHERE "watermark" = '02'`,
      )
      .run();
    expect(reader.plan('04')).toEqual({
      kind: 'range',
      minWatermark: '04',
      headWatermark: '08',
    });
    expect(reader.plan('02')).toEqual({
      kind: 'too-old',
      minWatermark: '04',
      headWatermark: '08',
    });
  });

  test('the head follows the log, with no replica in the picture', () => {
    const {db, file, path} = createChangeLog();
    using writer = db;
    using reader = new SQLiteChangeLogReader(lc, path);

    // A seeded log is serviceable only for a subscriber at the seed.
    expect(reader.plan('02')).toEqual({
      kind: 'range',
      minWatermark: '02',
      headWatermark: '02',
    });

    // Each committed transaction moves the head, and nothing else does.
    appendTransaction(writer, '04', [truncate()]);
    expect(reader.plan('02')).toMatchObject({headWatermark: '04'});
    appendTransaction(writer, '06', [truncate()]);
    expect(reader.plan('02')).toMatchObject({headWatermark: '06'});

    // The reader never touched the replica path, which does not even exist.
    for (const suffix of ['', '-wal', '-wal2', '-shm']) {
      expect(existsSync(file.path + suffix)).toBe(false);
    }
  });

  test('not-ready before the log has a stream table, and ready once it does', async () => {
    const file = newReplicaPath();
    using log = openChangeLogDB(lc, file.path, {readonly: false});
    log.pragma('journal_mode = wal');
    // The change-streamer can start before the replicator has reconciled the
    // log. Statements are prepared on demand, so this does not throw.
    using reader = new SQLiteChangeLogReader(lc, changeLogFileName(file.path));

    expect(reader.plan('02')).toEqual({kind: 'not-ready'});
    await expect(collect(reader, '02', '04', 2)).rejects.toThrow(
      'SQLite change log must be ready before it is read',
    );

    reconcileChangeLog(lc, log, ANCHOR);
    appendTransaction(log, '04', [truncate()]);

    // The same reader, without reconstruction, now serves the log.
    expect(reader.plan('02')).toEqual({
      kind: 'range',
      minWatermark: '02',
      headWatermark: '04',
    });
  });

  test('not-ready for an empty log', () => {
    const {db, path} = createChangeLog();
    using writer = db;
    writer.prepare(`DELETE FROM "${CHANGE_LOG_STREAM_TABLE}"`).run();
    using reader = new SQLiteChangeLogReader(lc, path);

    expect(reader.plan('02')).toEqual({kind: 'not-ready'});
  });

  test('reads a transaction across batches and multiple transactions in one batch', async () => {
    const {db, path} = createChangeLog();
    using writer = db;
    const tx04 = appendTransaction(writer, '04', [
      truncate(),
      truncate(),
      truncate(),
      truncate(),
      truncate(),
    ]);
    const tx06 = appendTransaction(writer, '06', [truncate()]);
    using reader = new SQLiteChangeLogReader(lc, path);

    const smallBatches = await collect(reader, '02', '06', 2);
    expect(smallBatches.map(batch => batch.length)).toEqual([2, 2, 2, 2, 2]);
    expect(flatten(smallBatches)).toEqual([...tx04, ...tx06]);

    const oneBatch = await collect(reader, '02', '06', 20);
    expect(oneBatch).toHaveLength(1);
    expect(oneBatch[0]).toEqual([...tx04, ...tx06]);

    await expect(collect(reader, '06', '06', 2)).resolves.toEqual([]);
  });

  test('pins the head while another connection appends and purges', async () => {
    const {db, path} = createChangeLog();
    using writer = db;
    const tx04 = appendTransaction(writer, '04', [
      truncate(),
      truncate(),
      truncate(),
      truncate(),
    ]);
    const tx06 = appendTransaction(writer, '06', [truncate()]);
    const tx08 = appendTransaction(writer, '08', [truncate()]);
    using reader = new SQLiteChangeLogReader(lc, path);

    const plan = reader.plan('02');
    expect(plan.kind).toBe('range');
    if (plan.kind !== 'range') {
      throw new Error('expected a catchable range');
    }
    const iterator = reader
      .read('02', plan.headWatermark, 3)
      [Symbol.asyncIterator]();
    const first = await iterator.next();
    expect(first.done).toBe(false);

    appendTransaction(writer, '0a', [truncate()]);
    writer
      .prepare(
        `DELETE FROM "${CHANGE_LOG_STREAM_TABLE}" WHERE "watermark" = '02'`,
      )
      .run();

    const batches = first.value === undefined ? [] : [first.value];
    for (;;) {
      const next = await iterator.next();
      if (next.done) {
        break;
      }
      batches.push(next.value);
    }
    expect(flatten(batches)).toEqual([...tx04, ...tx06, ...tx08]);
    expect(flatten(batches).at(-1)?.[0]).toBe('08');
  });

  test('rejects a pinned head the log does not reach', async () => {
    // A head pinned by plan() comes from max("watermark") of the same table, so
    // this can no longer happen from skew between the log and the replica's
    // _zero.replicationState. The assertion remains as a guard against a caller
    // pinning a head from somewhere else.
    const {db, path} = createChangeLog();
    using writer = db;
    appendTransaction(writer, '04', [truncate()]);
    using reader = new SQLiteChangeLogReader(lc, path);

    await expect(collect(reader, '02', '06', 10)).rejects.toThrow(
      'SQLite change log did not end at pinned head 06',
    );
  });

  test('cancellation and abort between batches release the read snapshot', async () => {
    const {db, path} = createChangeLog();
    using writer = db;
    appendTransaction(writer, '04', [
      truncate(),
      truncate(),
      truncate(),
      truncate(),
    ]);
    using reader = new SQLiteChangeLogReader(lc, path);

    const canceled = reader.read('02', '04', 2)[Symbol.asyncIterator]();
    await expect(canceled.next()).resolves.toMatchObject({done: false});
    await canceled.return?.();
    expect(
      writer.pragma<{busy: number}>('wal_checkpoint(TRUNCATE)')[0]?.busy,
    ).toBe(0);

    const controller = new AbortController();
    const aborted = reader
      .read('02', '04', 2, controller.signal)
      [Symbol.asyncIterator]();
    await expect(aborted.next()).resolves.toMatchObject({done: false});
    controller.abort();
    await expect(aborted.next()).rejects.toBeInstanceOf(AbortError);
    expect(
      writer.pragma<{busy: number}>('wal_checkpoint(TRUNCATE)')[0]?.busy,
    ).toBe(0);
  });

  test('close aborts reads and releases the dedicated connection', async () => {
    const {db, path} = createChangeLog();
    using writer = db;
    appendTransaction(writer, '04', [truncate(), truncate()]);
    const reader = new SQLiteChangeLogReader(lc, path);
    const iterator = reader.read('02', '04', 1)[Symbol.asyncIterator]();
    await expect(iterator.next()).resolves.toMatchObject({done: false});

    reader.close();
    reader.close();
    expect(() => reader.plan('02')).toThrow(AbortError);
    await expect(iterator.next()).rejects.toBeInstanceOf(AbortError);
  });

  test('reconstructs canonical bigint, NUL, schema, backfill, and truncate messages byte-for-byte', async () => {
    const {db, path} = createChangeLog();
    using writer = db;
    const messages: ChangeStreamData[] = [
      [
        'data',
        {
          tag: 'insert',
          relation: {
            schema: 'public',
            name: 'items',
            rowKey: {columns: ['id'], type: 'default'},
          },
          new: {id: 9007199254740993n, text: 'before\0after'},
        },
      ],
      [
        'data',
        {
          tag: 'rename-table',
          old: {schema: 'public', name: 'items'},
          new: {schema: 'archive', name: 'items'},
        },
      ],
      [
        'data',
        {
          tag: 'backfill',
          relation: {
            schema: 'archive',
            name: 'items',
            rowKey: {columns: ['id'], type: 'default'},
          },
          columns: ['value'],
          watermark: '03',
          rowValues: [[9007199254740995n, {nested: 9007199254740997n}]],
        },
      ],
      [
        'data',
        {
          tag: 'truncate',
          relations: [
            {
              schema: 'archive',
              name: 'items',
              rowKey: {columns: ['id'], type: 'default'},
            },
          ],
        },
      ],
    ];
    const expected = appendTransaction(writer, '04', messages);
    using reader = new SQLiteChangeLogReader(lc, path);

    const actual = flatten(await collect(reader, '02', '04', 2));
    expect(actual).toEqual(expected);
    expect(actual.map(([, , json]) => json)).toEqual(
      expected.map(([, , json]) => json),
    );
  });

  test('continuation and ceiling query uses the primary-key index', () => {
    const {db} = createChangeLog();
    using writer = db;
    appendTransaction(writer, '04', [truncate()]);

    const details = queryPlan(
      writer,
      SQLITE_CHANGE_LOG_READ_BATCH_SQL,
      '02',
      Number.MAX_SAFE_INTEGER,
      '04',
      100,
    ).join('\n');
    expect(details).toMatch(/\bSEARCH\b/);
    expect(details).not.toMatch(/\bSCAN\b/);
  });

  test('plan query seeks for both bounds instead of scanning the log', () => {
    const {db} = createChangeLog();
    using writer = db;
    for (const watermark of ['04', '06', '08', '0a']) {
      appendTransaction(
        writer,
        watermark,
        Array.from({length: 250}, () => truncate()),
      );
    }

    // Both bounds are separate scalar subqueries so that each gets SQLite's
    // single-aggregate index optimization. Folding them into one aggregate
    // (`SELECT min(x), max(x) FROM t`) turns both into a full scan of the log,
    // which is what this test exists to catch.
    const details = queryPlan(writer, SQLITE_CHANGE_LOG_PLAN_SQL, {
      fromWatermark: '08',
    });
    const logSteps = details.filter(detail =>
      detail.includes(CHANGE_LOG_STREAM_TABLE),
    );
    // max, min, and the boundary check.
    expect(logSteps).toHaveLength(3);
    expect(logSteps.filter(detail => !detail.startsWith('SEARCH'))).toEqual([]);

    // Three seeks: one row visited each, regardless of the size of the log.
    const statement = writer.prepare(SQLITE_CHANGE_LOG_PLAN_SQL);
    expect(statement.get({fromWatermark: '08'})).toEqual({
      headWatermark: '0a',
      minWatermark: '02',
      boundaryExists: 1,
    });
    expect(countVisitedRows(statement)).toBe(3);
  });

  test('boundary query seeks directly to the final row of a large transaction', () => {
    const {db} = createChangeLog();
    using writer = db;
    appendTransaction(
      writer,
      '04',
      Array.from({length: 1_000}, () => truncate()),
    );

    const details = queryPlan(writer, SQLITE_CHANGE_LOG_BOUNDARY_SQL, {
      fromWatermark: '04',
    }).join('\n');
    expect(details).toMatch(/\bSEARCH\b.*\(watermark=\?\)/);
    expect(details).not.toMatch(/\bSCAN\b|USE TEMP B-TREE/);

    const statement = writer.prepare(SQLITE_CHANGE_LOG_BOUNDARY_SQL);
    expect(statement.get({fromWatermark: '04'})).toEqual({boundaryExists: 1});
    expect(countVisitedRows(statement)).toBe(1);
  });
});
