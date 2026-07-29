import {afterEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {DbFile, expectTableExact} from '../../test/lite.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {serializeChangeStreamData} from '../change-streamer/change-log-codec.ts';
import {
  CHANGE_LOG_STREAM_TABLE,
  deleteChangeLogDB,
  openChangeLogDB,
  reconcileChangeLog,
  type ReplicaAnchor,
} from './change-log-db.ts';
import {
  ChangeLogStreamWriter,
  estimateChangeLogStreamRowBytes,
} from './change-log-stream-writer.ts';

const lc = createSilentLogContext();

const ANCHOR: ReplicaAnchor = {
  replicaVersion: '01',
  stateVersion: '02',
  writeTimeMs: 1_000,
};

const files: DbFile[] = [];

afterEach(() => {
  let file: DbFile | undefined;
  while ((file = files.pop())) {
    deleteChangeLogDB(file.path);
    file.delete();
  }
});

/** The seed transaction {@link reconcileChangeLog} writes at the replica head. */
const SEED_ROWS = [
  {
    watermark: ANCHOR.stateVersion,
    pos: 0,
    change: '{"tag":"begin"}',
    precommit: null,
    writeTimeMs: null,
  },
  {
    watermark: ANCHOR.stateVersion,
    pos: 1,
    change: '{"tag":"commit"}',
    precommit: ANCHOR.stateVersion,
    writeTimeMs: ANCHOR.writeTimeMs,
  },
];

function createChangeLog(): Database {
  const db = new Database(lc, ':memory:');
  reconcileChangeLog(lc, db, ANCHOR);
  return db;
}

/** A file-backed change log, so a second connection can observe durability. */
function createChangeLogFile(): {db: Database; observer: Database} {
  const file = new DbFile('change-log-stream-writer-test');
  files.push(file);
  const db = openChangeLogDB(lc, file.path, {readonly: false});
  db.pragma('journal_mode = wal');
  reconcileChangeLog(lc, db, ANCHOR);
  return {db, observer: openChangeLogDB(lc, file.path, {readonly: true})};
}

function json(data: ChangeStreamData): string {
  return serializeChangeStreamData(data);
}

function head(db: Database): string | null {
  return db
    .prepare(
      /*sql*/ `SELECT max("watermark") AS "head" FROM "${CHANGE_LOG_STREAM_TABLE}"`,
    )
    .get<{head: string | null}>().head;
}

function rowCount(db: Database): number {
  return db
    .prepare(
      /*sql*/ `SELECT count(*) AS "count" FROM "${CHANGE_LOG_STREAM_TABLE}"`,
    )
    .get<{count: number}>().count;
}

const BEGIN = json(['begin', {tag: 'begin'}, {commitWatermark: '06'}]);
const COMMIT = json(['commit', {tag: 'commit'}, {watermark: '06'}]);
const TRUNCATE = json(['data', {tag: 'truncate', relations: []}]);

describe('replicator/change-log-stream-writer', () => {
  test('writes contiguous positions and commit-only metadata', () => {
    using db = createChangeLog();
    const writer = new ChangeLogStreamWriter(new StatementRunner(db));
    const writeTimeMs = 1_234_567;

    const insert = json([
      'data',
      {
        tag: 'insert',
        relation: {
          schema: 'public',
          name: 'issues',
          rowKey: {columns: ['id'], type: 'default'},
        },
        new: {id: 9007199254740993n, text: 'before\0after'},
      },
    ]);
    const rename = json([
      'data',
      {
        tag: 'rename-table',
        old: {schema: 'public', name: 'issues'},
        new: {schema: 'public', name: 'renamed'},
      },
    ]);
    writer.begin('06', BEGIN);
    writer.append(insert, 'insert');
    writer.append(rename, 'rename-table');
    const stats = writer.commit('06', COMMIT, writeTimeMs);
    expect(stats).toEqual({
      rows: 4,
      hash: expect.stringMatching(/^[0-9a-f]{64}$/),
      estimatedBytes:
        estimateChangeLogStreamRowBytes('06', '{"tag":"begin"}') +
        estimateChangeLogStreamRowBytes(
          '06',
          '{"tag":"insert","relation":{"schema":"public","name":"issues","rowKey":{"columns":["id"],"type":"default"}},"new":{"id":9007199254740993,"text":"before\\u0000after"}}',
        ) +
        estimateChangeLogStreamRowBytes(
          '06',
          '{"tag":"rename-table","old":{"schema":"public","name":"issues"},"new":{"schema":"public","name":"renamed"}}',
        ) +
        estimateChangeLogStreamRowBytes('06', '{"tag":"commit"}', '06', true),
    });

    expectTableExact(
      db,
      CHANGE_LOG_STREAM_TABLE,
      [
        ...SEED_ROWS,
        {
          watermark: '06',
          pos: 0,
          change: '{"tag":"begin"}',
          precommit: null,
          writeTimeMs: null,
        },
        {
          watermark: '06',
          pos: 1,
          change:
            '{"tag":"insert","relation":{"schema":"public","name":"issues","rowKey":{"columns":["id"],"type":"default"}},"new":{"id":9007199254740993,"text":"before\\u0000after"}}',
          precommit: null,
          writeTimeMs: null,
        },
        {
          watermark: '06',
          pos: 2,
          change:
            '{"tag":"rename-table","old":{"schema":"public","name":"issues"},"new":{"schema":"public","name":"renamed"}}',
          precommit: null,
          writeTimeMs: null,
        },
        {
          watermark: '06',
          pos: 3,
          change: '{"tag":"commit"}',
          precommit: '06',
          writeTimeMs,
        },
      ],
      'number',
      'watermark',
      'pos',
    );
  });

  test('owns its transaction, which is durable at commit', () => {
    const {db, observer} = createChangeLogFile();
    using _db = db;
    using _observer = observer;
    const writer = new ChangeLogStreamWriter(new StatementRunner(db));

    expect(db.inTransaction).toBe(false);
    writer.begin('06', BEGIN);
    expect(db.inTransaction).toBe(true);
    writer.append(TRUNCATE, 'truncate');
    // Nothing is visible to another connection until the writer commits.
    expect(head(observer)).toBe(ANCHOR.stateVersion);

    writer.commit('06', COMMIT, 789);
    expect(db.inTransaction).toBe(false);
    expect(head(observer)).toBe('06');
  });

  test('rollback discards the transaction and is safe outside one', () => {
    using db = createChangeLog();
    const writer = new ChangeLogStreamWriter(new StatementRunner(db));

    writer.begin('06', BEGIN);
    writer.append(TRUNCATE, 'truncate');
    writer.rollback();

    expect(db.inTransaction).toBe(false);
    expect(rowCount(db)).toBe(SEED_ROWS.length);

    // The processor's failure path also rolls back after a commit that
    // succeeded and a replica write that did not.
    expect(() => writer.rollback()).not.toThrow();
    expect(rowCount(db)).toBe(SEED_ROWS.length);

    // The writer is reusable afterwards.
    writer.begin('06', BEGIN);
    writer.commit('06', COMMIT, 789);
    expect(head(db)).toBe('06');
  });

  test('rolls back a transaction that was never committed by the caller', () => {
    const {db, observer} = createChangeLogFile();
    using _db = db;
    using _observer = observer;
    const writer = new ChangeLogStreamWriter(new StatementRunner(db));

    writer.begin('06', BEGIN);
    writer.append(TRUNCATE, 'truncate');
    writer.rollback();

    expect(head(observer)).toBe(ANCHOR.stateVersion);
    expect(rowCount(observer)).toBe(SEED_ROWS.length);
  });

  test('streams a large transaction', () => {
    using db = createChangeLog();
    const writer = new ChangeLogStreamWriter(new StatementRunner(db));

    const rows = 2500;
    writer.begin('06', BEGIN);
    for (let i = 0; i < rows; i++) {
      writer.append(TRUNCATE, 'truncate');
    }
    writer.commit('06', COMMIT, 789);

    expect(
      db
        .prepare(/*sql*/ `
          SELECT count(*) AS "count", min("pos") AS "minPos", max("pos") AS "maxPos"
            FROM "${CHANGE_LOG_STREAM_TABLE}"
            WHERE "watermark" = '06'
        `)
        .get(),
    ).toEqual({count: rows + 2, minPos: 0, maxPos: rows + 1});
  });

  test('a second begin without a commit is an invariant failure', () => {
    using db = createChangeLog();
    const writer = new ChangeLogStreamWriter(new StatementRunner(db));

    writer.begin('06', BEGIN);
    expect(() => writer.begin('07', BEGIN)).toThrow(
      'change-log stream transaction already open at 06',
    );
    // The open transaction is the first one, not a nested second.
    writer.rollback();
    expect(db.inTransaction).toBe(false);
  });
});
