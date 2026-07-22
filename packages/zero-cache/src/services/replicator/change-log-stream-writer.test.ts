import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {expectTableExact} from '../../test/lite.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {serializeChangeStreamData} from '../change-streamer/change-log-codec.ts';
import {
  ChangeLogStreamWriter,
  estimateChangeLogStreamRowBytes,
} from './change-log-stream-writer.ts';
import {CHANGE_LOG_STREAM_TABLE} from './schema/change-log-stream.ts';
import {
  initReplicationState,
  updateReplicationWatermark,
} from './schema/replication-state.ts';

const lc = createSilentLogContext();

function json(data: ChangeStreamData): string {
  return serializeChangeStreamData(data);
}

describe('replicator/change-log-stream-writer', () => {
  test('writes contiguous positions and commit-only metadata', () => {
    using db = new Database(lc, ':memory:');
    initReplicationState(db, ['zero_data'], '02');
    const runner = new StatementRunner(db);
    const writer = new ChangeLogStreamWriter(db);
    const writeTimeMs = 1_234_567;

    runner.beginImmediate();
    const begin = json(['begin', {tag: 'begin'}, {commitWatermark: '06'}]);
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
    const commit = json(['commit', {tag: 'commit'}, {watermark: '06'}]);
    writer.begin('06', begin);
    writer.append(insert, 'insert');
    writer.append(rename, 'rename-table');
    const stats = writer.commit('06', commit, writeTimeMs);
    expect(stats).toEqual({
      rows: 4,
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
    updateReplicationWatermark(runner, '06', writeTimeMs);
    runner.commit();

    expectTableExact(
      db,
      CHANGE_LOG_STREAM_TABLE,
      [
        {
          watermark: '02',
          pos: 0,
          change: '{"tag":"begin"}',
          precommit: null,
          writeTimeMs: null,
        },
        {
          watermark: '02',
          pos: 1,
          change: '{"tag":"commit"}',
          precommit: '02',
          writeTimeMs: expect.any(Number),
        },
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

    expect(
      db
        .prepare(
          `SELECT "stateVersion", "writeTimeMs" FROM "_zero.replicationState"`,
        )
        .get(),
    ).toEqual({stateVersion: '06', writeTimeMs});
  });

  test('rollback discards rows and a large transaction is streamed', () => {
    using db = new Database(lc, ':memory:');
    initReplicationState(db, ['zero_data'], '02');
    const runner = new StatementRunner(db);
    const writer = new ChangeLogStreamWriter(db);
    const begin = json(['begin', {tag: 'begin'}, {commitWatermark: '06'}]);
    const truncate = json(['data', {tag: 'truncate', relations: []}]);

    runner.beginImmediate();
    writer.begin('06', begin);
    writer.append(truncate, 'truncate');
    runner.rollback();
    writer.rollback();

    expect(
      db
        .prepare(`SELECT count(*) AS "count" FROM "${CHANGE_LOG_STREAM_TABLE}"`)
        .get(),
    ).toEqual({count: 2});

    const rowCount = 2500;
    runner.beginImmediate();
    writer.begin('06', begin);
    for (let i = 0; i < rowCount; i++) {
      writer.append(truncate, 'truncate');
    }
    writer.commit(
      '06',
      json(['commit', {tag: 'commit'}, {watermark: '06'}]),
      789,
    );
    updateReplicationWatermark(runner, '06', 789);
    runner.commit();

    expect(
      db
        .prepare(/*sql*/ `
          SELECT count(*) AS "count", min("pos") AS "minPos", max("pos") AS "maxPos"
            FROM "${CHANGE_LOG_STREAM_TABLE}"
            WHERE "watermark" = '06'
        `)
        .get(),
    ).toEqual({count: rowCount + 2, minPos: 0, maxPos: rowCount + 1});
  });
});
