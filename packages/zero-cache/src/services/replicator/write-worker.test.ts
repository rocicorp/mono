import {once} from 'node:events';
import {Worker} from 'node:worker_threads';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {DbFile, initDB} from '../../test/lite.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {serializeChangeStreamData} from '../change-streamer/change-log-codec.ts';
import {initReplicationState} from './schema/replication-state.ts';
import {ReplicationMessages} from './test-utils.ts';
import {
  deserializeError,
  serializeError,
  ThreadWriteWorkerClient,
  type Request,
  type SerializedChangeStreamData,
} from './write-worker-client.ts';

function serialized(data: ChangeStreamData): SerializedChangeStreamData {
  return {data, json: serializeChangeStreamData(data)};
}

describe('write-worker', () => {
  let dbFile: DbFile;
  let mainDb: Database;
  let worker: ThreadWriteWorkerClient;

  beforeEach(async () => {
    const lc = createSilentLogContext();
    dbFile = new DbFile('write-worker-test');
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

    worker = new ThreadWriteWorkerClient();
    await worker.init(
      dbFile.path,
      'serving',
      false,
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

  test('getSubscriptionState returns correct state', async () => {
    const state = await worker.getSubscriptionState();
    expect(state).toEqual({
      replicaVersion: '02',
      publications: ['zero_data'],
      watermark: '02',
    });
  });

  test('processMessage handles a full transaction', async () => {
    const issues = new ReplicationMessages({issues: ['issueID', 'bool']});

    const messages: ChangeStreamData[] = [
      ['begin', issues.begin(), {commitWatermark: '06'}],
      ['data', issues.insert('issues', {issueID: 123, bool: true})],
      ['data', issues.insert('issues', {issueID: 456, bool: false})],
      ['commit', issues.commit(), {watermark: '06'}],
    ];

    let commitResult = null;
    for (let i = 0; i < messages.length; i++) {
      const result = await worker.processMessage(serialized(messages[i]));
      if (result) {
        commitResult = result;
      }
    }

    expect(commitResult).toEqual({
      watermark: '06',
      completedBackfill: undefined,
      schemaUpdated: false,
      changeLogUpdated: true,
    });

    // Verify the data is in the database by reading with main thread connection.
    const rows = mainDb
      .prepare('SELECT issueID, bool, _0_version FROM issues ORDER BY issueID')
      .all();
    expect(rows).toEqual([
      {issueID: 123, bool: 1, _0_version: '06'},
      {issueID: 456, bool: 0, _0_version: '06'},
    ]);

    // Verify watermark was updated.
    const state = await worker.getSubscriptionState();
    expect(state.watermark).toBe('06');
  });

  test('enabled writer is atomic across abort and worker restart', async () => {
    await worker.stop();
    worker = new ThreadWriteWorkerClient();
    await worker.init(
      dbFile.path,
      'serving',
      true,
      {
        busyTimeout: 30000,
        analysisLimit: 1000,
      },
      {level: 'error', format: 'text'},
    );

    const issues = new ReplicationMessages({issues: ['issueID', 'bool']});
    await worker.processMessage(
      serialized(['begin', issues.begin(), {commitWatermark: '05'}]),
    );
    await worker.processMessage(
      serialized(['data', issues.insert('issues', {issueID: 1, bool: true})]),
    );
    worker.abort();

    const messages: ChangeStreamData[] = [
      ['begin', issues.begin(), {commitWatermark: '06'}],
      ['data', issues.insert('issues', {issueID: 123, bool: true})],
      ['data', issues.insert('issues', {issueID: 456, bool: false})],
      ['commit', issues.commit(), {watermark: '06'}],
    ];
    let commitResult = null;
    for (const message of messages) {
      commitResult =
        (await worker.processMessage(serialized(message))) ?? commitResult;
    }
    expect(commitResult).toMatchObject({
      watermark: '06',
      changeLogStream: {rows: 4, estimatedBytes: expect.any(Number)},
    });

    await worker.stop();
    worker = new ThreadWriteWorkerClient();
    await worker.init(
      dbFile.path,
      'serving',
      true,
      {
        busyTimeout: 30000,
        analysisLimit: 1000,
      },
      {level: 'error', format: 'text'},
    );

    expect(await worker.getSubscriptionState()).toMatchObject({
      watermark: '06',
    });
    expect(
      mainDb
        .prepare(/*sql*/ `
          SELECT "watermark", "pos", json_extract("change", '$.tag') AS "tag",
                 "precommit", "writeTimeMs"
            FROM "_zero.changeLogStream"
            WHERE "watermark" IN ('05', '06')
            ORDER BY "watermark", "pos"
        `)
        .all(),
    ).toEqual([
      {
        watermark: '06',
        pos: 0,
        tag: 'begin',
        precommit: null,
        writeTimeMs: null,
      },
      {
        watermark: '06',
        pos: 1,
        tag: 'insert',
        precommit: null,
        writeTimeMs: null,
      },
      {
        watermark: '06',
        pos: 2,
        tag: 'insert',
        precommit: null,
        writeTimeMs: null,
      },
      {
        watermark: '06',
        pos: 3,
        tag: 'commit',
        precommit: '06',
        writeTimeMs: expect.any(Number),
      },
    ]);
    const state = mainDb
      .prepare(
        `SELECT "stateVersion", "writeTimeMs" FROM "_zero.replicationState"`,
      )
      .get<{stateVersion: string; writeTimeMs: number}>();
    const commit = mainDb
      .prepare(/*sql*/ `
        SELECT "writeTimeMs" FROM "_zero.changeLogStream"
          WHERE "watermark" = '06' AND "precommit" IS NOT NULL
      `)
      .get<{writeTimeMs: number}>();
    expect(state).toEqual({
      stateVersion: '06',
      writeTimeMs: commit.writeTimeMs,
    });
  });

  test('abort rolls back pending transaction', async () => {
    const issues = new ReplicationMessages({issues: ['issueID', 'bool']});

    // Start a transaction but don't commit
    await worker.processMessage(
      serialized(['begin', issues.begin(), {commitWatermark: '06'}]),
    );
    await worker.processMessage(
      serialized(['data', issues.insert('issues', {issueID: 123, bool: true})]),
    );

    // Abort should roll back
    worker.abort();

    // Verify nothing was written
    const rows = mainDb.prepare('SELECT * FROM issues').all();
    expect(rows).toEqual([]);

    // Should be able to process a new transaction after abort
    const messages: ChangeStreamData[] = [
      ['begin', issues.begin(), {commitWatermark: '07'}],
      ['data', issues.insert('issues', {issueID: 789, bool: false})],
      ['commit', issues.commit(), {watermark: '07'}],
    ];

    for (let i = 0; i < messages.length; i++) {
      await worker.processMessage(serialized(messages[i]));
    }

    const rowsAfter = mainDb.prepare('SELECT issueID FROM issues').all();
    expect(rowsAfter).toEqual([{issueID: 789}]);
  });

  test('stop shuts down cleanly', async () => {
    await worker.stop();
    // Create a new worker for afterEach cleanup
    worker = new ThreadWriteWorkerClient();
    await worker.init(
      dbFile.path,
      'serving',
      false,
      {
        busyTimeout: 30000,
        analysisLimit: 1000,
      },
      {level: 'error', format: 'text'},
    );
  });

  // This test verifies the ChangeProcessor's internal error path:
  // ChangeProcessor catches the error via #fail, which posts a {writeError}
  // message (rejecting the pending promise AND calling the errorHandler),
  // rather than the error propagating through the worker's generic try/catch.
  test('error handling: fail message on worker error', async () => {
    let errorReceived: Error | undefined;
    worker.onError(err => {
      errorReceived = err;
    });

    // Send a processMessage without a begin - should cause a failure
    await expect(
      worker.processMessage(
        serialized([
          'data',
          {
            tag: 'insert',
            relation: {
              schema: 'public',
              name: 'nonexistent',
              rowKey: {columns: ['id'], type: 'default'},
            },
            new: {id: [1, 'int4']},
          },
        ]),
      ),
    ).rejects.toThrow();

    expect(errorReceived).toBeDefined();
  });

  test('worker structured clone preserves the serialized envelope', async () => {
    const data: ChangeStreamData = [
      'data',
      {
        tag: 'insert',
        relation: {
          schema: 'public',
          name: 'issues',
          rowKey: {columns: ['issueID'], type: 'default'},
        },
        new: {
          issueID: 9007199254740993n,
          text: 'before\0after',
        },
      },
    ];
    const request = {
      method: 'processMessage',
      args: [serialized(data)],
    } satisfies Request<'processMessage'>;
    const echoWorker = new Worker(
      /*js*/ `
        const {parentPort} = require('node:worker_threads');
        parentPort.once('message', message => parentPort.postMessage(message));
      `,
      {eval: true},
    );

    try {
      const response = once(echoWorker, 'message');
      echoWorker.postMessage(request);
      const [roundTripped] = (await response) as [typeof request];

      expect(roundTripped).toEqual(request);
      expect(roundTripped.args[0].data[1]).toMatchObject({
        new: {issueID: 9007199254740993n, text: 'before\0after'},
      });
      expect(roundTripped.args[0].json).toContain(
        '"text":"before\\u0000after"',
      );
    } finally {
      await echoWorker.terminate();
    }
  });

  test('error serialization preserves useful fields', () => {
    const sqliteError = new Error('database is locked', {
      cause: new Error('checkpoint in progress'),
    });
    sqliteError.name = 'SqliteError';
    Object.defineProperties(sqliteError, {
      code: {value: 'SQLITE_BUSY'},
      errno: {value: 5},
    });

    const deserialized = deserializeError(serializeError(sqliteError));

    expect(deserialized).toMatchObject({
      name: 'SqliteError',
      message: 'database is locked',
      code: 'SQLITE_BUSY',
      errno: 5,
    });
    expect(deserialized.cause).toMatchObject({
      message: 'checkpoint in progress',
    });
  });
});
