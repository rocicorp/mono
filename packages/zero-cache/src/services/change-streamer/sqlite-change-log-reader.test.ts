import {afterEach, describe, expect, test} from 'vitest';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {DbFile} from '../../test/lite.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {ChangeLogStreamWriter} from '../replicator/change-log-stream-writer.ts';
import {
  initReplicationState,
  updateReplicationWatermark,
} from '../replicator/schema/replication-state.ts';
import {serializeChangeStreamData} from './change-log-codec.ts';
import type {WatermarkedChange} from './change-streamer.ts';
import {
  SQLITE_CHANGE_LOG_READ_BATCH_SQL,
  SQLiteChangeLogReader,
} from './sqlite-change-log-reader.ts';

const lc = createSilentLogContext();
const files: DbFile[] = [];

afterEach(() => {
  for (const file of files.splice(0)) {
    file.delete();
  }
});

function createReplica(): {db: Database; file: DbFile} {
  const file = new DbFile('sqlite-change-log-reader');
  files.push(file);
  const db = file.connect(lc);
  db.pragma('journal_mode = wal');
  initReplicationState(db, ['zero_data'], '02');
  return {db, file};
}

function truncate(): ChangeStreamData {
  return ['data', {tag: 'truncate', relations: []}];
}

function appendTransaction(
  db: Database,
  watermark: string,
  data: readonly ChangeStreamData[],
): readonly WatermarkedChange[] {
  const runner = new StatementRunner(db);
  const writer = new ChangeLogStreamWriter(db);
  const begin: ChangeStreamData = [
    'begin',
    {tag: 'begin'},
    {commitWatermark: watermark},
  ];
  const commit: ChangeStreamData = ['commit', {tag: 'commit'}, {watermark}];

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

describe('sqlite change log reader', () => {
  test('plans seed, minimum, middle, head, too-old, missing, and ahead watermarks', () => {
    const {db, file} = createReplica();
    using writer = db;
    appendTransaction(writer, '04', [truncate()]);
    appendTransaction(writer, '06', [truncate()]);
    appendTransaction(writer, '08', [truncate()]);
    using reader = new SQLiteChangeLogReader(lc, file.path);

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
      .prepare(`DELETE FROM "_zero.changeLogStream" WHERE "watermark" = '02'`)
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

  test('reads a transaction across batches and multiple transactions in one batch', async () => {
    const {db, file} = createReplica();
    using writer = db;
    const tx04 = appendTransaction(writer, '04', [
      truncate(),
      truncate(),
      truncate(),
      truncate(),
      truncate(),
    ]);
    const tx06 = appendTransaction(writer, '06', [truncate()]);
    using reader = new SQLiteChangeLogReader(lc, file.path);

    const smallBatches = await collect(reader, '02', '06', 2);
    expect(smallBatches.map(batch => batch.length)).toEqual([2, 2, 2, 2, 2]);
    expect(flatten(smallBatches)).toEqual([...tx04, ...tx06]);

    const oneBatch = await collect(reader, '02', '06', 20);
    expect(oneBatch).toHaveLength(1);
    expect(oneBatch[0]).toEqual([...tx04, ...tx06]);

    await expect(collect(reader, '06', '06', 2)).resolves.toEqual([]);
  });

  test('pins the head while another connection appends and purges', async () => {
    const {db, file} = createReplica();
    using writer = db;
    const tx04 = appendTransaction(writer, '04', [
      truncate(),
      truncate(),
      truncate(),
      truncate(),
    ]);
    const tx06 = appendTransaction(writer, '06', [truncate()]);
    const tx08 = appendTransaction(writer, '08', [truncate()]);
    using reader = new SQLiteChangeLogReader(lc, file.path);

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
      .prepare(`DELETE FROM "_zero.changeLogStream" WHERE "watermark" = '02'`)
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

  test('cancellation and abort between batches release the read snapshot', async () => {
    const {db, file} = createReplica();
    using writer = db;
    appendTransaction(writer, '04', [
      truncate(),
      truncate(),
      truncate(),
      truncate(),
    ]);
    using reader = new SQLiteChangeLogReader(lc, file.path);

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
    const {db, file} = createReplica();
    using writer = db;
    appendTransaction(writer, '04', [truncate(), truncate()]);
    const reader = new SQLiteChangeLogReader(lc, file.path);
    const iterator = reader.read('02', '04', 1)[Symbol.asyncIterator]();
    await expect(iterator.next()).resolves.toMatchObject({done: false});

    reader.close();
    reader.close();
    expect(() => reader.plan('02')).toThrow(AbortError);
    await expect(iterator.next()).rejects.toBeInstanceOf(AbortError);
  });

  test('reconstructs canonical bigint, NUL, schema, backfill, and truncate messages byte-for-byte', async () => {
    const {db, file} = createReplica();
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
    using reader = new SQLiteChangeLogReader(lc, file.path);

    const actual = flatten(await collect(reader, '02', '04', 2));
    expect(actual).toEqual(expected);
    expect(actual.map(([, , json]) => json)).toEqual(
      expected.map(([, , json]) => json),
    );
  });

  test('continuation and ceiling query uses the primary-key index', () => {
    const {db} = createReplica();
    using writer = db;
    appendTransaction(writer, '04', [truncate()]);

    const plans = writer
      .prepare(`EXPLAIN QUERY PLAN ${SQLITE_CHANGE_LOG_READ_BATCH_SQL}`)
      .all<{detail: string}>('02', Number.MAX_SAFE_INTEGER, '04', 100);
    const details = plans.map(({detail}) => detail).join('\n');
    expect(details).toMatch(/\bSEARCH\b/);
    expect(details).not.toMatch(/\bSCAN\b/);
  });
});
