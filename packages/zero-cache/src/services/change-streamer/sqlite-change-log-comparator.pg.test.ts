import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {type PgTest, test} from '../../test/db.ts';
import {DbFile} from '../../test/lite.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {ChangeLogStreamWriter} from '../replicator/change-log-stream-writer.ts';
import {
  initReplicationState,
  updateReplicationWatermark,
} from '../replicator/schema/replication-state.ts';
import {
  extractChangeSubstring,
  serializeChangeStreamData,
} from './change-log-codec.ts';
import {ensureReplicationConfig, setupCDCTables} from './schema/tables.ts';
import {createSQLiteChangeLogComparator} from './sqlite-change-log-comparator.ts';

const lc = createSilentLogContext();
const SHARD = {appID: 'zero', shardNum: 1};
const REPLICA_VERSION = '02';
const WATERMARK = '06';

describe('change-streamer/sqlite-change-log-comparator PG/SQLite integration', () => {
  let pg: PostgresDB;
  let sqlite: Database;
  let file: DbFile;

  beforeEach<PgTest>(async ({testDBs}) => {
    pg = await testDBs.create('sqlite_change_log_comparator', {
      typeOpts: {sendStringAsJson: true},
    });
    await pg.begin(tx => setupCDCTables(lc, tx, SHARD));
    await ensureReplicationConfig(
      lc,
      pg,
      {
        replicaVersion: REPLICA_VERSION,
        publications: [],
        watermark: REPLICA_VERSION,
      },
      SHARD,
      true,
    );

    file = new DbFile('sqlite-change-log-comparator');
    sqlite = file.connect(lc);
    sqlite.exec(/*sql*/ `
      CREATE TABLE "_zero.versionHistory" (
        "dataVersion" INTEGER NOT NULL,
        "schemaVersion" INTEGER NOT NULL,
        "minSafeVersion" INTEGER NOT NULL,
        "lock" INTEGER PRIMARY KEY DEFAULT 1 CHECK ("lock" = 1)
      );
      INSERT INTO "_zero.versionHistory"
        ("dataVersion", "schemaVersion", "minSafeVersion")
        VALUES (14, 14, 0);
    `);
    initReplicationState(sqlite, ['zero_data'], REPLICA_VERSION);

    return async () => {
      sqlite.close();
      file.delete();
      await testDBs.drop(pg);
    };
  });

  test('streams byte-identical canonical rows for all message families', async () => {
    const relation = {
      schema: 'public',
      name: 'items',
      rowKey: {columns: ['id'], type: 'default' as const},
    };
    const messages: ChangeStreamData[] = [
      [
        'begin',
        {tag: 'begin', json: 's', skipAck: true},
        {commitWatermark: WATERMARK},
      ],
      [
        'data',
        {
          tag: 'insert',
          relation,
          new: {
            id: 9_007_199_254_740_993n,
            text: 'before\0after',
            nested: {array: [{}, []]},
          },
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
      ['data', {tag: 'truncate', relations: [relation]}],
      [
        'data',
        {
          tag: 'backfill',
          relation,
          columns: [],
          watermark: '04',
          rowValues: [[9_007_199_254_740_995n, {nested: 'value'}]],
          status: {rows: 1, totalRows: 1, totalBytes: 0},
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: WATERMARK}],
    ];
    await writePostgresTransaction(pg, messages);
    writeSQLiteTransaction(sqlite, messages);

    const comparator = createSQLiteChangeLogComparator(lc, pg, file.path, {
      replicaVersion: REPLICA_VERSION,
      shard: SHARD,
      retentionMs: 1,
      batchSize: 2,
      samplePercent: 100,
      warmupStartedAtMs: 0,
      now: () => 1000,
    });
    try {
      await expect(comparator.compareWatermark(WATERMARK)).resolves.toEqual(
        expect.objectContaining({
          outcome: 'match',
          reason: 'match',
          comparedRows: messages.length,
        }),
      );

      sqlite
        .prepare(/*sql*/ `
          UPDATE "_zero.changeLogStream"
             SET "change" = json_set("change", '$.new.text', 'changed')
           WHERE "watermark" = ? AND "pos" = 1
        `)
        .run(WATERMARK);
      await expect(comparator.compareWatermark(WATERMARK)).resolves.toEqual(
        expect.objectContaining({
          outcome: 'divergence',
          reason: 'byte-mismatch',
          rowIndex: 1,
        }),
      );
    } finally {
      await comparator.close();
    }
  });
});

async function writePostgresTransaction(
  pg: PostgresDB,
  messages: readonly ChangeStreamData[],
): Promise<void> {
  await pg.begin(async tx => {
    for (let pos = 0; pos < messages.length; pos++) {
      const message = messages[pos];
      const tag = message[1].tag;
      const row = {
        watermark: WATERMARK,
        pos,
        change: extractChangeSubstring(serializeChangeStreamData(message), tag),
        precommit: tag === 'commit' ? WATERMARK : null,
      };
      await tx`INSERT INTO "zero_1/cdc"."changeLog" ${tx(row)}`;
    }
    await tx`
      UPDATE "zero_1/cdc"."replicationState"
         SET "lastWatermark" = ${WATERMARK}
    `;
  });
}

function writeSQLiteTransaction(
  sqlite: Database,
  messages: readonly ChangeStreamData[],
): void {
  const runner = new StatementRunner(sqlite);
  const writer = new ChangeLogStreamWriter(sqlite);
  runner.beginImmediate();
  for (const message of messages) {
    const json = serializeChangeStreamData(message);
    switch (message[0]) {
      case 'begin':
        writer.begin(WATERMARK, json);
        break;
      case 'commit':
        writer.commit(WATERMARK, json, 100);
        break;
      case 'data':
        writer.append(json, message[1].tag);
        break;
      case 'rollback':
        throw new Error('test transaction must commit');
    }
  }
  updateReplicationWatermark(runner, WATERMARK, 100);
  runner.commit();
}
