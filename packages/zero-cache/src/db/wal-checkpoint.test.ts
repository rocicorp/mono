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

  type Checkpoint = {
    busy: number;
    log: number;
    checkpointed: number;
  }[];

  type TransactionMode = 'IMMEDIATE' | 'CONCURRENT';

  test.each([['IMMEDIATE'], ['CONCURRENT']] satisfies [TransactionMode][])(
    'wal_checkpoint with BEGIN %s',
    async transaction => {
      const writer = dbFile.connect(lc);
      writer.pragma('wal_autocheckpoint = 0');
      writer.pragma('busy_timeout = 100');

      const insert = writer.prepare('INSERT INTO foo(id) VALUES(?)');
      for (let i = 10; i < 20; i++) {
        insert.run(i);
      }

      // Simulate a concurrent transaction (e.g. replicator) in a different thread.
      const worker = new Worker(
        `
      const {parentPort} = require('worker_threads');
      const Database = require('better-sqlite3');

      // Acquire a read lock.
      const reader = new Database('${dbFile.path}');
      reader.pragma('busy_timeout = 5000');
      const count = reader.prepare('select count(*) as count from foo');

      reader.prepare('begin ${transaction}').run();
      const before = count.get();
      reader.prepare('insert into foo(id) VALUES(30)').run();

      parentPort.postMessage('inTransaction');

      // Commit the transaction when a message is posted to this worker.
      parentPort.once('message', () => {
        reader.prepare('commit').run();
        const after = count.get();

        // Respond with the before and after counts.
        parentPort.postMessage({before, after});
      });
    `,
        {eval: true},
      );

      const {promise: inTransaction, resolve: setRunning} = resolver();
      worker.once('message', setRunning);
      await inTransaction;

      // Signal the worker to proceed and immediately force a wal_checkpoint.
      worker.postMessage('commit');
      const [result] = writer.pragma('wal_checkpoint(RESTART)') as Checkpoint;

      if (transaction === 'CONCURRENT') {
        // With a BEGIN CONCURRENT, the checkpoint fails. Note that if this were
        // litestream, this would result in a deadlock as litestream does not
        // set a busy_timeout.
        expect(result).toEqual({busy: 1, log: 10, checkpointed: 10});
        return;
      }
      // With normal transactions, the wal_checkpoint waits for a write to finish.
      expect(result).toEqual({busy: 0, log: 11, checkpointed: 11});

      const {promise: response, resolve: setResponse} = resolver<unknown>();
      worker.once('message', setResponse);
      expect(await response).toEqual({
        before: {count: 13},
        after: {count: 14},
      });

      // New writes should be written from the beginning of the WAL.
      insert.run(20);
      insert.run(21);
      expect(
        (writer.pragma('wal_checkpoint(PASSIVE)') as Checkpoint)[0],
      ).toEqual({busy: 0, log: 2, checkpointed: 2});

      insert.run(22);
      expect(
        (writer.pragma('wal_checkpoint(PASSIVE)') as Checkpoint)[0],
      ).toEqual({busy: 0, log: 1, checkpointed: 1});
    },
  );
});
