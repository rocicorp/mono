import {resolver} from '@rocicorp/resolver';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {Worker} from 'worker_threads';
import {DbFile} from '../test/lite.js';

describe('db/wal-checkpoint', () => {
  let dbFile: DbFile;

  const lc = createSilentLogContext();

  beforeEach(() => {
    dbFile = new DbFile('wal-checkpoint');
    const conn = dbFile.connect(lc);
    conn.pragma('journal_mode = WAL');
    conn.pragma('synchronous = NORMAL');
    conn.exec(`
      CREATE TABLE foo(id INTEGER PRIMARY KEY);
      INSERT INTO foo(id) VALUES(1);
      INSERT INTO foo(id) VALUES(2);
      INSERT INTO foo(id) VALUES(3);
    `);
    conn.close();
  });

  afterEach(async () => {
    await dbFile.unlink();
  });

  type CheckpointResult = {
    busy: number;
    log: number;
    checkpointed: number;
  }[];

  test('checkpointing', async () => {
    // The read lock logic must be run in a different thread.
    const worker = new Worker(
      `
      const {parentPort} = require('worker_threads');
      const Database = require('better-sqlite3');

      // Acquire a read lock.
      const reader = new Database('${dbFile.path}');
      const count = reader.prepare('select count(*) as count from foo');

      reader.prepare('begin concurrent').run();
      const before = count.get();

      parentPort.postMessage('ready');

      // Release the read lock when a message is posted to this worker.
      parentPort.once('message', () => {
        reader.prepare('rollback').run();
        const after = count.get();

        // Respond with the before and after counts.
        parentPort.postMessage({before, after});
      });
    `,
      {eval: true},
    );

    const {promise: running, resolve: setRunning} = resolver();
    worker.once('message', setRunning);
    await running;

    const writer = dbFile.connect(lc);
    writer.pragma('busy_timeout = 50');

    const insert = writer.prepare('INSERT INTO foo(id) VALUES(?)');
    for (let i = 10; i < 20; i++) {
      insert.run(i);
    }

    // A passive wal_checkpoint does not block and won't succeed in
    // checkpointing, but it returns the state of the wal.
    expect(
      (writer.pragma('wal_checkpoint(PASSIVE)') as CheckpointResult)[0],
    ).toEqual({busy: 0, log: 10, checkpointed: 0});

    // A RESTART should fail with 'busy' while the read lock is held.
    expect(
      (writer.pragma('wal_checkpoint(RESTART)') as CheckpointResult)[0],
    ).toEqual({busy: 1, log: 10, checkpointed: 0});

    // A wal_checkpoint should succeed once the read lock is released.
    worker.postMessage('release');
    expect(
      (writer.pragma('wal_checkpoint(RESTART)') as CheckpointResult)[0],
    ).toEqual({busy: 0, log: 10, checkpointed: 10});

    const {promise: response, resolve: setResponse} = resolver<unknown>();
    worker.once('message', setResponse);
    expect(await response).toEqual({
      before: {count: 3},
      after: {count: 13},
    });

    // New writes should be written from the beginning of the WAL.
    insert.run(20);
    insert.run(21);
    expect(
      (writer.pragma('wal_checkpoint(PASSIVE)') as CheckpointResult)[0],
    ).toEqual({busy: 0, log: 2, checkpointed: 2});

    insert.run(22);
    expect(
      (writer.pragma('wal_checkpoint(PASSIVE)') as CheckpointResult)[0],
    ).toEqual({busy: 0, log: 1, checkpointed: 1});
  });
});
