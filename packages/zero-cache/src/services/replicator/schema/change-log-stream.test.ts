import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {expectTableExact} from '../../../test/lite.ts';
import {
  CHANGE_LOG_STREAM_TABLE,
  CHANGE_LOG_STREAM_WRITE_TIME_INDEX,
  CREATE_CHANGE_LOG_STREAM_SCHEMA,
  seedChangeLogStream,
} from './change-log-stream.ts';

function createTestDB(): Database {
  const db = new Database(createSilentLogContext(), ':memory:');
  db.exec(/*sql*/ `
    CREATE TABLE "_zero.replicationState" (
      "stateVersion" TEXT NOT NULL,
      "writeTimeMs" INTEGER,
      "lock" INTEGER PRIMARY KEY DEFAULT 1 CHECK ("lock" = 1)
    );
    ${CREATE_CHANGE_LOG_STREAM_SCHEMA}
  `);
  return db;
}

describe('replicator/schema/change-log-stream', () => {
  test('creates the stream table and partial retention index', () => {
    using db = createTestDB();

    expect(
      db.prepare(`PRAGMA table_info("${CHANGE_LOG_STREAM_TABLE}")`).all(),
    ).toEqual([
      {
        cid: 0,
        name: 'watermark',
        type: 'TEXT',
        notnull: 1,
        dflt_value: null,
        pk: 1,
      },
      {
        cid: 1,
        name: 'pos',
        type: 'INTEGER',
        notnull: 1,
        dflt_value: null,
        pk: 2,
      },
      {
        cid: 2,
        name: 'change',
        type: 'TEXT',
        notnull: 1,
        dflt_value: null,
        pk: 0,
      },
      {
        cid: 3,
        name: 'precommit',
        type: 'TEXT',
        notnull: 0,
        dflt_value: null,
        pk: 0,
      },
      {
        cid: 4,
        name: 'writeTimeMs',
        type: 'INTEGER',
        notnull: 0,
        dflt_value: null,
        pk: 0,
      },
    ]);

    expect(
      db.prepare(`PRAGMA index_list("${CHANGE_LOG_STREAM_TABLE}")`).all(),
    ).toEqual(
      expect.arrayContaining([
        {
          seq: expect.any(Number),
          name: CHANGE_LOG_STREAM_WRITE_TIME_INDEX,
          unique: 0,
          origin: 'c',
          partial: 1,
        },
      ]),
    );
    expect(
      db
        .prepare(`PRAGMA index_info("${CHANGE_LOG_STREAM_WRITE_TIME_INDEX}")`)
        .all(),
    ).toEqual([
      {seqno: 0, cid: 4, name: 'writeTimeMs'},
      {seqno: 1, cid: 0, name: 'watermark'},
    ]);
  });

  test('seeds one complete transaction idempotently', () => {
    using db = createTestDB();
    db.prepare(/*sql*/ `
      INSERT INTO "_zero.replicationState" ("stateVersion", "writeTimeMs")
      VALUES ('01', 12345)
    `).run();

    seedChangeLogStream(db);
    seedChangeLogStream(db);

    expectTableExact(
      db,
      CHANGE_LOG_STREAM_TABLE,
      [
        {
          watermark: '01',
          pos: 0,
          change: '{"tag":"begin"}',
          precommit: null,
          writeTimeMs: null,
        },
        {
          watermark: '01',
          pos: 1,
          change: '{"tag":"commit"}',
          precommit: '01',
          writeTimeMs: 12345,
        },
      ],
      'number',
      'watermark',
      'pos',
    );

    expect(
      db
        .prepare(
          `SELECT * FROM "${CHANGE_LOG_STREAM_TABLE}" WHERE "watermark" > ?`,
        )
        .all('01'),
    ).toEqual([]);
  });
});
