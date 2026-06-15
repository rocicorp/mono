import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {DbFile, initDB} from '../../test/lite.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {LocalWriteWorkerClient} from './local-write-worker-client.ts';
import {initReplicationState} from './schema/replication-state.ts';
import {ReplicationMessages} from './test-utils.ts';

describe('local-write-worker-client', () => {
  let dbFile: DbFile;
  let mainDb: Database;
  let worker: LocalWriteWorkerClient;

  beforeEach(async () => {
    const lc = createSilentLogContext();
    dbFile = new DbFile('local-write-worker-client-test');
    mainDb = dbFile.connect(lc);
    mainDb.pragma('journal_mode = wal');

    initReplicationState(mainDb, ['zero_data'], '02');
    initDB(
      mainDb,
      `
      CREATE TABLE issues(
        issueID INTEGER,
        bool BOOL,
        _0_version TEXT,
        PRIMARY KEY(issueID, bool)
      );
      `,
    );

    worker = new LocalWriteWorkerClient();
    await worker.init(
      dbFile.path,
      'serving',
      {
        busyTimeout: 30000,
        analysisLimit: 1000,
      },
      {level: 'error', format: 'text'},
    );
  });

  afterEach(async () => {
    await worker.stop();
    mainDb.close();
    dbFile.delete();
  });

  test('processMessages returns one result per committed transaction', async () => {
    const issues = new ReplicationMessages({issues: ['issueID', 'bool']});

    const messages: ChangeStreamData[] = [
      ['begin', issues.begin(), {commitWatermark: '06'}],
      ['data', issues.insert('issues', {issueID: 123, bool: true})],
      ['commit', issues.commit(), {watermark: '06'}],
      ['begin', issues.begin(), {commitWatermark: '07'}],
      ['data', issues.insert('issues', {issueID: 456, bool: false})],
      ['commit', issues.commit(), {watermark: '07'}],
    ];

    const results = await worker.processMessages(messages);

    expect(results).toEqual([
      {
        watermark: '06',
        completedBackfill: undefined,
        schemaUpdated: false,
        changeLogUpdated: true,
      },
      {
        watermark: '07',
        completedBackfill: undefined,
        schemaUpdated: false,
        changeLogUpdated: true,
      },
    ]);

    const rows = mainDb
      .prepare('SELECT issueID, bool, _0_version FROM issues ORDER BY issueID')
      .all();
    expect(rows).toEqual([
      {issueID: 123, bool: 1, _0_version: '06'},
      {issueID: 456, bool: 0, _0_version: '07'},
    ]);
  });
});
