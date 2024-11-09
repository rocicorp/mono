import {PG_UNIQUE_VIOLATION} from '@drdgvhbh/postgres-error-codes';
import postgres from 'postgres';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.js';
import {Queue} from '../../../shared/src/queue.js';
import {sleep} from '../../../shared/src/sleep.js';
import {expectTables, testDBs} from '../test/db.js';
import type {PostgresDB} from '../types/pg.js';
import {
  Mode,
  TransactionPool,
  importSnapshot,
  sharedSnapshot,
  synchronizedSnapshots,
} from './transaction-pool.js';

describe('db/transaction-pool', () => {
  let db: PostgresDB;
  const lc = createSilentLogContext();

  beforeEach(async () => {
    db = await testDBs.create('transaction_pool_test');
    await db`
    CREATE TABLE foo (
      id int PRIMARY KEY,
      val text
    );
    CREATE TABLE workers (id SERIAL);
    CREATE TABLE keepalive (id SERIAL);
    CREATE TABLE cleaned (id SERIAL);
    `.simple();
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  // Add a sleep in before each task to exercise concurrency. Otherwise
  // it's always just the first worker that churns through all of the tasks.
  const task = (stmt: string) => async (tx: postgres.TransactionSql) => {
    await sleep(5);
    return [tx.unsafe(stmt)];
  };

  const initTask = task(`INSERT INTO workers (id) VALUES (DEFAULT);`);
  const cleanupTask = task(`INSERT INTO cleaned (id) VALUES (DEFAULT);`);
  const keepaliveTask = task(`INSERT INTO keepalive (id) VALUES (DEFAULT);`);

  test('single transaction, serialized processing', async () => {
    const single = new TransactionPool(
      lc,
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      1,
      1,
    );

    expect(single.isRunning()).toBe(false);
    single.run(db);
    expect(single.isRunning()).toBe(true);

    single.process(task(`INSERT INTO foo (id) VALUES (1)`));
    single.process(task(`INSERT INTO foo (id) VALUES (6)`));
    single.process(task(`UPDATE foo SET val = 'foo' WHERE id < 5`));
    single.process(task(`INSERT INTO foo (id) VALUES (3)`));
    single.setDone();
    expect(single.isRunning()).toBe(false);

    await single.done();

    await expectTables(db, {
      ['public.foo']: [
        {id: 1, val: 'foo'},
        {id: 3, val: null},
        {id: 6, val: null},
      ],
      ['public.workers']: [{id: 1}],
      ['public.cleaned']: [{id: 1}],
    });
  });

  test('ref counting', async () => {
    const single = new TransactionPool(
      lc,
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      1,
      1,
    );

    expect(single.isRunning()).toBe(false);
    single.run(db);
    expect(single.isRunning()).toBe(true);

    // 1 -> 2 -> 3
    single.ref();
    expect(single.isRunning()).toBe(true);
    single.ref();
    expect(single.isRunning()).toBe(true);

    // 3 -> 2 -> 1
    single.unref();
    expect(single.isRunning()).toBe(true);
    single.unref();
    expect(single.isRunning()).toBe(true);

    // 1 -> 0
    single.unref();
    expect(single.isRunning()).toBe(false);

    await single.done();

    await expectTables(db, {
      ['public.workers']: [{id: 1}],
      ['public.cleaned']: [{id: 1}],
    });
  });

  test('multiple transactions', async () => {
    const pool = new TransactionPool(
      lc,
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      3,
      3,
    );

    expect(pool.isRunning()).toBe(false);
    pool.run(db);
    expect(pool.isRunning()).toBe(true);

    pool.process(task(`INSERT INTO foo (id) VALUES (1)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (3)`));
    pool.process(task(`INSERT INTO foo (id) VALUES (2)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (8, 'foo')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (5)`));
    pool.setDone();

    expect(pool.isRunning()).toBe(false);
    await pool.done();

    await expectTables(db, {
      ['public.foo']: [
        {id: 1, val: null},
        {id: 2, val: null},
        {id: 3, val: null},
        {id: 5, val: null},
        {id: 6, val: 'foo'},
        {id: 8, val: 'foo'},
      ],
      ['public.workers']: [{id: 1}, {id: 2}, {id: 3}],
      ['public.cleaned']: [{id: 1}, {id: 2}, {id: 3}],
    });
  });

  test('pool resizing before run', async () => {
    const pool = new TransactionPool(
      lc,
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      2,
      5,
    );

    pool.process(task(`INSERT INTO foo (id) VALUES (1)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (3)`));
    pool.process(task(`INSERT INTO foo (id) VALUES (2)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (8, 'foo')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (5)`));
    pool.setDone();

    await pool.run(db).done();

    await expectTables(db, {
      ['public.foo']: [
        {id: 1, val: null},
        {id: 2, val: null},
        {id: 3, val: null},
        {id: 5, val: null},
        {id: 6, val: 'foo'},
        {id: 8, val: 'foo'},
      ],
      ['public.workers']: [{id: 1}, {id: 2}, {id: 3}, {id: 4}, {id: 5}],
      ['public.cleaned']: [{id: 1}, {id: 2}, {id: 3}, {id: 4}, {id: 5}],
    });
  });

  test('pool resizing after run', async () => {
    const pool = new TransactionPool(
      lc,
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      2,
      5,
    );

    const processing = new Queue<boolean>();
    const canProceed = new Queue<boolean>();

    const blockingTask =
      (stmt: string) => async (tx: postgres.TransactionSql) => {
        void processing.enqueue(true);
        await canProceed.dequeue();
        return task(stmt)(tx);
      };

    pool.run(db);

    pool.process(blockingTask(`INSERT INTO foo (id) VALUES (1)`));
    pool.process(blockingTask(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));

    for (let i = 0; i < 2; i++) {
      await processing.dequeue();
    }

    pool.process(blockingTask(`INSERT INTO foo (id) VALUES (3)`));
    pool.process(blockingTask(`INSERT INTO foo (id) VALUES (2)`));
    pool.process(blockingTask(`INSERT INTO foo (id, val) VALUES (8, 'foo')`));
    pool.process(blockingTask(`INSERT INTO foo (id) VALUES (5)`));
    pool.setDone();

    for (let i = 2; i < 5; i++) {
      await processing.dequeue();
    }

    // Let all 6 tasks proceed.
    for (let i = 0; i < 6; i++) {
      void canProceed.enqueue(true);
    }

    await pool.done();

    await expectTables(db, {
      ['public.foo']: [
        {id: 1, val: null},
        {id: 2, val: null},
        {id: 3, val: null},
        {id: 5, val: null},
        {id: 6, val: 'foo'},
        {id: 8, val: 'foo'},
      ],
      ['public.workers']: [{id: 1}, {id: 2}, {id: 3}, {id: 4}, {id: 5}],
      ['public.cleaned']: [{id: 1}, {id: 2}, {id: 3}, {id: 4}, {id: 5}],
    });
  });

  // TODO: Debug test flakiness or delete functionality
  test.skip('pool resizing and idle/keepalive timeouts', async () => {
    const pool = new TransactionPool(
      lc,
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      2,
      5,
      {
        forInitialWorkers: {
          timeoutMs: 100,
          task: keepaliveTask,
        },
        forExtraWorkers: {
          timeoutMs: 50,
          task: 'done',
        },
      },
    );

    const processing = new Queue<boolean>();
    const canProceed = new Queue<boolean>();

    const blockingTask =
      (stmt: string) => async (tx: postgres.TransactionSql) => {
        void processing.enqueue(true);
        await canProceed.dequeue();
        return task(stmt)(tx);
      };

    pool.run(db);

    pool.process(blockingTask(`INSERT INTO foo (id) VALUES (1)`));
    pool.process(blockingTask(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));

    for (let i = 0; i < 2; i++) {
      await processing.dequeue();
    }

    pool.process(blockingTask(`INSERT INTO foo (id) VALUES (3)`));
    pool.process(blockingTask(`INSERT INTO foo (id) VALUES (2)`));
    pool.process(blockingTask(`INSERT INTO foo (id, val) VALUES (8, 'foo')`));

    // Ensure all tasks get a worker.
    for (let i = 2; i < 5; i++) {
      await processing.dequeue();
    }
    // Let all 5 tasks proceed.
    for (let i = 0; i < 5; i++) {
      void canProceed.enqueue(true);
    }

    // Let the extra workers hit their 50ms idle timeout.
    await sleep(75);

    await expectTables(db, {
      ['public.cleaned']: [{id: 1}, {id: 2}, {id: 3}],
      ['public.keepalive']: [],
    });

    // Repeat to spawn more workers.
    pool.process(blockingTask(`INSERT INTO foo (id) VALUES (10)`));
    pool.process(blockingTask(`INSERT INTO foo (id, val) VALUES (60, 'foo')`));

    for (let i = 0; i < 2; i++) {
      await processing.dequeue();
    }

    pool.process(blockingTask(`INSERT INTO foo (id) VALUES (30)`));
    pool.process(blockingTask(`INSERT INTO foo (id) VALUES (20)`));
    pool.process(blockingTask(`INSERT INTO foo (id, val) VALUES (80, 'foo')`));

    for (let i = 2; i < 5; i++) {
      await processing.dequeue();
    }

    // Let all 5 tasks proceed.
    for (let i = 0; i < 5; i++) {
      void canProceed.enqueue(true);
    }

    // Let the new extra workers hit their 50ms idle timeout.
    await sleep(75);

    await expectTables(db, {
      ['public.cleaned']: [
        {id: 1},
        {id: 2},
        {id: 3},
        {id: 4},
        {id: 5},
        {id: 6},
      ],
      ['public.keepalive']: [],
    });

    // Let the initial workers hit their 100ms keepalive timeout.
    await sleep(50);

    pool.setDone();
    await pool.done();

    await expectTables(db, {
      ['public.foo']: [
        {id: 1, val: null},
        {id: 2, val: null},
        {id: 3, val: null},
        {id: 6, val: 'foo'},
        {id: 8, val: 'foo'},
        {id: 10, val: null},
        {id: 20, val: null},
        {id: 30, val: null},
        {id: 60, val: 'foo'},
        {id: 80, val: 'foo'},
      ],
      ['public.workers']: [
        {id: 1},
        {id: 2},
        {id: 3},
        {id: 4},
        {id: 5},
        {id: 6},
        {id: 7},
        {id: 8},
      ],
      ['public.keepalive']: [{id: 1}, {id: 2}],
      ['public.cleaned']: [
        {id: 1},
        {id: 2},
        {id: 3},
        {id: 4},
        {id: 5},
        {id: 6},
        {id: 7},
        {id: 8},
      ],
    });
  });

  // TODO: Debug test flakiness or delete functionality
  test.skip('external failure before running', async () => {
    const pool = new TransactionPool(
      lc,
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      3,
      3,
    );

    pool.process(task(`INSERT INTO foo (id) VALUES (1)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (3)`));
    pool.process(task(`INSERT INTO foo (id) VALUES (2)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (8, 'foo')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (5)`));
    pool.setDone();

    // Set the failure before running.
    pool.fail(new Error('oh nose'));

    const result = await pool
      .run(db)
      .done()
      .catch(e => e);
    expect(result).toBeInstanceOf(Error);

    expect(pool.isRunning()).toBe(false);

    // Nothing should have succeeded.
    await expectTables(db, {
      ['public.foo']: [],
      ['public.workers']: [],
      ['public.cleaned']: [],
    });
  });

  test('pool resizing for sequential read readTasks', async () => {
    await db`
    INSERT INTO foo (id) VALUES (1);
    INSERT INTO foo (id) VALUES (2);
    INSERT INTO foo (id) VALUES (3);
    `.simple();

    const pool = new TransactionPool(
      lc,
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      1,
      3,
    );
    pool.run(db);

    const readTask = () => async (tx: postgres.TransactionSql) =>
      (await tx<{id: number}[]>`SELECT id FROM foo;`.values()).flat();
    expect(await pool.processReadTask(readTask())).toEqual([1, 2, 3]);
    expect(await pool.processReadTask(readTask())).toEqual([1, 2, 3]);
    expect(await pool.processReadTask(readTask())).toEqual([1, 2, 3]);

    pool.setDone();
    await pool.done();
    await expectTables(db, {
      ['public.workers']: [{id: 1}],
      ['public.cleaned']: [{id: 1}],
    });
  });

  // TODO: Debug test flakiness or delete functionality
  test.skip('external failure while running', async () => {
    const pool = new TransactionPool(
      lc,
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      3,
      3,
    );

    pool.process(task(`INSERT INTO foo (id) VALUES (1)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (3)`));
    pool.process(task(`INSERT INTO foo (id) VALUES (2)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (8, 'foo')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (5)`));

    const result = pool
      .run(db)
      .done()
      .catch(e => e);

    // Set the failure after running.
    pool.fail(new Error('oh nose'));
    expect(await result).toBeInstanceOf(Error);

    // Nothing should have succeeded.
    await expectTables(db, {
      ['public.foo']: [],
      ['public.workers']: [],
      ['public.cleaned']: [],
    });
  });

  // TODO: Debug test flakiness or delete functionality
  test.skip('non-statement task error fails pool', async () => {
    const pool = new TransactionPool(
      lc,
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      3,
      3,
    );

    const readError = new Error('doh');

    pool.process(task(`INSERT INTO foo (id) VALUES (1)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (3)`));
    pool.process(task(`INSERT INTO foo (id) VALUES (2)`));
    pool.process(task(`INSERT INTO foo (id) VALUES (5)`));
    pool.process(() => Promise.reject(readError));

    const result = await pool
      .run(db)
      .done()
      .catch(e => e);

    // Ensure that the error is surfaced.
    expect(result).toBe(readError);

    // Nothing should have succeeded.
    await expectTables(db, {
      ['public.foo']: [],
      ['public.workers']: [],
      ['public.cleaned']: [],
    });
  });

  test('postgres error is surfaced', async () => {
    const pool = new TransactionPool(
      lc,
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      3,
      3,
    );

    // With a total of 4 insert statements with id = 1, at least one tx is guaranteed to fail.
    pool.process(task(`INSERT INTO foo (id) VALUES (1)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (1, 'bad')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (3)`));
    pool.process(task(`INSERT INTO foo (id) VALUES (2)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (1, 'double')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (5)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (1, 'oof')`));

    const result = await pool
      .run(db)
      .done()
      .catch(e => e);

    expect(pool.isRunning()).toBe(false);

    // Ensure that the postgres error is surfaced.
    expect(result).toBeInstanceOf(postgres.PostgresError);
    expect((result as postgres.PostgresError).code).toBe(PG_UNIQUE_VIOLATION);

    // Nothing should have succeeded.
    await expectTables(db, {
      ['public.foo']: [],
      ['public.workers']: [],
      ['public.cleaned']: [],
    });
  });

  test.skip('partial success; error from post-resize worker', async () => {
    const pool = new TransactionPool(
      lc,
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      2,
      5,
    );

    const processing = new Queue<boolean>();
    const canProceed = new Queue<boolean>();

    const blockingTask =
      (stmt: string) => async (tx: postgres.TransactionSql) => {
        void processing.enqueue(true);
        await canProceed.dequeue();
        return task(stmt)(tx);
      };

    pool.run(db);

    pool.process(blockingTask(`INSERT INTO foo (id) VALUES (1)`));
    pool.process(blockingTask(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));

    for (let i = 0; i < 2; i++) {
      await processing.dequeue();
    }

    // For the last of the new tasks, induce an error with a unique key violation.
    pool.process(blockingTask(`INSERT INTO foo (id) VALUES (3)`));
    pool.process(blockingTask(`INSERT INTO foo (id) VALUES (2)`));
    pool.process(blockingTask(`INSERT INTO foo (id) VALUES (1)`));

    // Set done so that the workers exit as soon as they've processed their task.
    // This means that the initial two workers will likely exit successfully.
    pool.setDone();

    for (let i = 2; i < 5; i++) {
      await processing.dequeue();
    }

    // Allow the tasks to proceed in order. This maximizes the chance that the
    // first tasks complete (and succeed) before the last task errors, exercising
    // the scenario being tested.
    for (let i = 0; i < 5; i++) {
      await canProceed.enqueue(true);
    }

    // run() should throw the error even though it may not have come from the
    // two initially started workers.
    const result = await pool.done().catch(e => e);

    // Ensure that the postgres error is surfaced.
    expect(result).toBeInstanceOf(postgres.PostgresError);
    expect((result as postgres.PostgresError).code).toBe(PG_UNIQUE_VIOLATION);

    // Note: We don't verify table expectations here because some transactions
    //       may have successfully completed. That's fine, because in practice
    //       it only makes sense to do writes in single-transaction pools.
  });

  test('snapshot synchronization', async () => {
    const processing = new Queue<boolean>();
    const blockingTask = (stmt: string) => (tx: postgres.TransactionSql) => {
      void processing.enqueue(true);
      return task(stmt)(tx);
    };

    const {exportSnapshot, cleanupExport, setSnapshot} =
      synchronizedSnapshots();
    const leader = new TransactionPool(
      lc.withContext('pool', 'leader'),
      Mode.SERIALIZABLE,
      exportSnapshot,
      cleanupExport,
      3,
    );
    const follower = new TransactionPool(
      lc.withContext('pool', 'follower'),
      Mode.SERIALIZABLE,
      setSnapshot,
    );

    // Start off with some existing values in the db.
    await db`
    INSERT INTO foo (id) VALUES (1);
    INSERT INTO foo (id) VALUES (2);
    INSERT INTO foo (id) VALUES (3);
    `.simple();

    // Run both pools.
    leader.run(db);
    follower.run(db);

    // Process some writes on follower.
    follower.process(blockingTask(`INSERT INTO foo (id) VALUES (4);`));
    follower.process(blockingTask(`INSERT INTO foo (id) VALUES (5);`));
    follower.process(blockingTask(`INSERT INTO foo (id) VALUES (6);`));

    // Verify that at least one task is processed, which guarantees that
    // the snapshot was exported.
    await processing.dequeue();

    // Do some writes outside of the transaction.
    await db`
    INSERT INTO foo (id) VALUES (7);
    INSERT INTO foo (id) VALUES (8);
    INSERT INTO foo (id) VALUES (9);
    `.simple();

    // Verify that the leader only sees the initial snapshot.
    const reads: Promise<number[]>[] = [];
    for (let i = 0; i < 3; i++) {
      reads.push(
        leader.processReadTask(async tx =>
          (await tx<{id: number}[]>`SELECT id FROM foo;`.values()).flat(),
        ),
      );
    }
    const results = await Promise.all(reads);
    for (const result of results) {
      // Neither [4, 5, 6] nor [7, 8, 9] should appear.
      expect(result).toEqual([1, 2, 3]);
    }

    follower.setDone();
    leader.setDone();

    await Promise.all([leader.done(), follower.done()]);

    await expectTables(db, {
      ['public.foo']: [
        {id: 1, val: null},
        {id: 2, val: null},
        {id: 3, val: null},
        {id: 4, val: null},
        {id: 5, val: null},
        {id: 6, val: null},
        {id: 7, val: null},
        {id: 8, val: null},
        {id: 9, val: null},
      ],
    });
  });

  // TODO: Debug test flakiness or delete functionality
  test.skip('snapshot synchronization error handling', async () => {
    const {exportSnapshot, cleanupExport, setSnapshot} =
      synchronizedSnapshots();
    const leader = new TransactionPool(
      lc,
      Mode.SERIALIZABLE,
      exportSnapshot,
      cleanupExport,
    );
    const followers = new TransactionPool(
      lc,
      Mode.READONLY,
      setSnapshot,
      undefined,
      3,
    );

    const err = new Error('oh nose');

    leader.fail(err);
    followers.fail(err);

    const result = await Promise.all([
      leader.run(db).done(),
      followers.run(db).done(),
    ]).catch(e => e);

    expect(result).toBe(err);
  });

  test('sharedSnapshot', async () => {
    const processing = new Queue<boolean>();
    const readTask = () => async (tx: postgres.TransactionSql) => {
      void processing.enqueue(true);
      return (await tx<{id: number}[]>`SELECT id FROM foo;`.values()).flat();
    };

    const {init, cleanup} = sharedSnapshot();
    const pool = new TransactionPool(lc, Mode.READONLY, init, cleanup, 2, 5);

    // Start off with some existing values in the db.
    await db`
    INSERT INTO foo (id) VALUES (1);
    INSERT INTO foo (id) VALUES (2);
    INSERT INTO foo (id) VALUES (3);
    `.simple();

    // Run the pool.
    pool.run(db);

    const processed: Promise<number[]>[] = [];

    // Process one read.
    processed.push(pool.processReadTask(readTask()));

    // Verify that at least one task is processed, which guarantees that
    // the snapshot was exported.
    await processing.dequeue();

    // Do some writes outside of the transaction.
    await db`
    INSERT INTO foo (id) VALUES (4);
    INSERT INTO foo (id) VALUES (5);
    INSERT INTO foo (id) VALUES (6);
    `.simple();

    // Process a few more reads to expand the worker pool
    for (let i = 0; i < 5; i++) {
      processed.push(pool.processReadTask(readTask()));
    }

    // Verify that the all workers only see the initial snapshot.
    const results = await Promise.all(processed);
    for (const result of results) {
      // [4, 5, 6] should not appear.
      expect(result).toEqual([1, 2, 3]);
    }

    pool.setDone();
    await pool.done();

    await expectTables(db, {
      ['public.foo']: [
        {id: 1, val: null},
        {id: 2, val: null},
        {id: 3, val: null},
        {id: 4, val: null},
        {id: 5, val: null},
        {id: 6, val: null},
      ],
    });
  });

  test('externally shared snapshot', async () => {
    const processing = new Queue<boolean>();
    const readTask = () => async (tx: postgres.TransactionSql) => {
      void processing.enqueue(true);
      return (await tx<{id: number}[]>`SELECT id FROM foo;`.values()).flat();
    };

    const {init, cleanup, snapshotID} = sharedSnapshot();
    const pool = new TransactionPool(lc, Mode.SERIALIZABLE, init, cleanup, 1);

    // Start off with some existing values in the db.
    await db`
    INSERT INTO foo (id) VALUES (1);
    INSERT INTO foo (id) VALUES (2);
    INSERT INTO foo (id) VALUES (3);
    `.simple();

    // Run the pool.
    pool.run(db);

    // Run the readers.
    const {init: importInit, imported} = importSnapshot(await snapshotID);
    const readers = new TransactionPool(lc, Mode.READONLY, importInit);
    readers.run(db);
    await imported;

    const processed: Promise<number[]>[] = [];

    // Process one read.
    processed.push(pool.processReadTask(readTask()));

    // Do some writes on the pool.
    pool.process(tx => [
      tx`
    INSERT INTO foo (id) VALUES (4);
    INSERT INTO foo (id) VALUES (5);
    INSERT INTO foo (id) VALUES (6);
    `.simple(),
    ]);

    // Process reads from the readers pool.
    for (let i = 0; i < 5; i++) {
      processed.push(readers.processReadTask(readTask()));
    }

    // Verify that the all reads only saw the initial snapshot.
    const results = await Promise.all(processed);
    for (const result of results) {
      // [4, 5, 6] should not appear.
      expect(result).toEqual([1, 2, 3]);
    }

    pool.setDone();
    readers.setDone();
    await Promise.all([pool.done(), readers.done()]);

    await expectTables(db, {
      ['public.foo']: [
        {id: 1, val: null},
        {id: 2, val: null},
        {id: 3, val: null},
        {id: 4, val: null},
        {id: 5, val: null},
        {id: 6, val: null},
      ],
    });
  });

  test('failures reflected in readTasks', async () => {
    await db`
    INSERT INTO foo (id) VALUES (1);
    INSERT INTO foo (id) VALUES (2);
    INSERT INTO foo (id) VALUES (3);
    `.simple();

    const pool = new TransactionPool(lc, Mode.READONLY);
    pool.run(db);

    const readTask = () => async (tx: postgres.TransactionSql) =>
      (await tx<{id: number}[]>`SELECT id FROM foo;`.values()).flat();
    expect(await pool.processReadTask(readTask())).toEqual([1, 2, 3]);

    expect(pool.isRunning()).toBe(true);

    const error = new Error('oh nose');
    pool.fail(error);

    expect(pool.isRunning()).toBe(false);

    const result = await pool.processReadTask(readTask()).catch(e => e);
    expect(result).toBe(error);

    expect(await pool.done().catch(e => e)).toBe(error);
  });
});
