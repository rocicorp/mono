import {PG_UNIQUE_VIOLATION} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import postgres from 'postgres';
import {beforeEach, describe, expect} from 'vitest';
import type {Enum} from '../../../shared/src/enum.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../shared/src/queue.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import {expectTables, test, type PgTest} from '../test/db.ts';
import type {PostgresDB} from '../types/pg.ts';
import * as Mode from './mode-enum.ts';
import {
  importSnapshot,
  sharedSnapshot,
  synchronizedSnapshots,
  TIMEOUT_TASKS,
  TransactionPool,
  type Task,
} from './transaction-pool.ts';

type Mode = Enum<typeof Mode>;

describe('db/transaction-pool', () => {
  let db: PostgresDB;
  let lc: LogContext;
  let pools: TransactionPool[];

  beforeEach<PgTest>(async ({testDBs}) => {
    pools = [];
    lc = createSilentLogContext();
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

    return async () => {
      pools.forEach(pool => pool.abort());
      await testDBs.drop(db);
    };
  });

  function newTransactionPool(
    mode: Mode,
    init?: Task,
    cleanup?: Task,
    initialWorkers = 1,
    maxWorkers = initialWorkers,
    timeoutTasks = TIMEOUT_TASKS, // Overridden for tests.
  ) {
    const pool = new TransactionPool(
      lc,
      mode,
      init,
      cleanup,
      initialWorkers,
      maxWorkers,
      timeoutTasks,
    );
    pools.push(pool);
    return pool;
  }

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
    const single = newTransactionPool(
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      1,
      1,
    );

    expect(single.isRunning()).toBe(false);
    single.run(db);
    expect(single.isRunning()).toBe(true);

    void single.process(task(`INSERT INTO foo (id) VALUES (1)`));
    void single.process(task(`INSERT INTO foo (id) VALUES (6)`));
    void single.process(task(`UPDATE foo SET val = 'foo' WHERE id < 5`));
    void single.process(task(`INSERT INTO foo (id) VALUES (3)`));
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
    const single = newTransactionPool(
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
    const pool = newTransactionPool(
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      3,
      3,
    );

    expect(pool.isRunning()).toBe(false);
    pool.run(db);
    expect(pool.isRunning()).toBe(true);

    void pool.process(task(`INSERT INTO foo (id) VALUES (1)`));
    void pool.process(task(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (3)`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (2)`));
    void pool.process(task(`INSERT INTO foo (id, val) VALUES (8, 'foo')`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (5)`));
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

  // TODO: Remove pool resizing functionality
  test.skip('pool resizing before run', async () => {
    const pool = newTransactionPool(
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      2,
      5,
    );

    void pool.process(task(`INSERT INTO foo (id) VALUES (1)`));
    void pool.process(task(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (3)`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (2)`));
    void pool.process(task(`INSERT INTO foo (id, val) VALUES (8, 'foo')`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (5)`));
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

  // TODO: Remove pool resizing functionality
  test.skip('pool resizing after run', async () => {
    const pool = newTransactionPool(
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
        processing.enqueue(true);
        await canProceed.dequeue();
        return task(stmt)(tx);
      };

    pool.run(db);

    void pool.process(blockingTask(`INSERT INTO foo (id) VALUES (1)`));
    void pool.process(
      blockingTask(`INSERT INTO foo (id, val) VALUES (6, 'foo')`),
    );

    for (let i = 0; i < 2; i++) {
      await processing.dequeue();
    }

    void pool.process(blockingTask(`INSERT INTO foo (id) VALUES (3)`));
    void pool.process(blockingTask(`INSERT INTO foo (id) VALUES (2)`));
    void pool.process(
      blockingTask(`INSERT INTO foo (id, val) VALUES (8, 'foo')`),
    );
    void pool.process(blockingTask(`INSERT INTO foo (id) VALUES (5)`));
    pool.setDone();

    for (let i = 2; i < 5; i++) {
      await processing.dequeue();
    }

    // Let all 6 tasks proceed.
    for (let i = 0; i < 6; i++) {
      canProceed.enqueue(true);
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

  // TODO: Remove pool resizing functionality
  test.skip(
    'pool resizing and idle/keepalive timeouts',
    {retry: 2},
    async () => {
      const pool = newTransactionPool(
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
          processing.enqueue(true);
          await canProceed.dequeue();
          return task(stmt)(tx);
        };

      pool.run(db);

      void pool.process(blockingTask(`INSERT INTO foo (id) VALUES (1)`));
      void pool.process(
        blockingTask(`INSERT INTO foo (id, val) VALUES (6, 'foo')`),
      );

      for (let i = 0; i < 2; i++) {
        await processing.dequeue();
      }

      void pool.process(blockingTask(`INSERT INTO foo (id) VALUES (3)`));
      void pool.process(blockingTask(`INSERT INTO foo (id) VALUES (2)`));
      void pool.process(
        blockingTask(`INSERT INTO foo (id, val) VALUES (8, 'foo')`),
      );

      // Ensure all tasks get a worker.
      for (let i = 2; i < 5; i++) {
        await processing.dequeue();
      }
      // Let all 5 tasks proceed.
      for (let i = 0; i < 5; i++) {
        canProceed.enqueue(true);
      }

      // Let the extra workers hit their 50ms idle timeout.
      await sleep(100);

      await expectTables(db, {
        ['public.cleaned']: [{id: 1}, {id: 2}, {id: 3}],
        ['public.keepalive']: [],
      });

      // Repeat to spawn more workers.
      void pool.process(blockingTask(`INSERT INTO foo (id) VALUES (10)`));
      void pool.process(
        blockingTask(`INSERT INTO foo (id, val) VALUES (60, 'foo')`),
      );

      for (let i = 0; i < 2; i++) {
        await processing.dequeue();
      }

      void pool.process(blockingTask(`INSERT INTO foo (id) VALUES (30)`));
      void pool.process(blockingTask(`INSERT INTO foo (id) VALUES (20)`));
      void pool.process(
        blockingTask(`INSERT INTO foo (id, val) VALUES (80, 'foo')`),
      );

      for (let i = 2; i < 5; i++) {
        await processing.dequeue();
      }

      // Let all 5 tasks proceed.
      for (let i = 0; i < 5; i++) {
        canProceed.enqueue(true);
      }

      // Let the new extra workers hit their 50ms idle timeout.
      await sleep(100);

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
      await sleep(100);

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
    },
  );

  test('external failure before running', async () => {
    const pool = newTransactionPool(
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      3,
      3,
    );

    void pool.process(task(`INSERT INTO foo (id) VALUES (1)`));
    void pool.process(task(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (3)`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (2)`));
    void pool.process(task(`INSERT INTO foo (id, val) VALUES (8, 'foo')`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (5)`));
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

  test('pool not resized for sequential read readTasks', async () => {
    await db`
    INSERT INTO foo (id) VALUES (1);
    INSERT INTO foo (id) VALUES (2);
    INSERT INTO foo (id) VALUES (3);
    `.simple();

    const pool = newTransactionPool(
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

  test('pool not resized for sequential writes', async () => {
    await db`
    INSERT INTO foo (id) VALUES (1);
    INSERT INTO foo (id) VALUES (2);
    INSERT INTO foo (id) VALUES (3);
    `.simple();

    const pool = newTransactionPool(
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      1,
      3,
    );
    pool.run(db);

    await pool.process(task(`INSERT INTO foo (id) VALUES (4)`));
    await pool.process(task(`INSERT INTO foo (id) VALUES (5)`));
    await pool.process(task(`INSERT INTO foo (id) VALUES (6)`));

    pool.setDone();
    await pool.done();
    await expectTables(db, {
      ['public.workers']: [{id: 1}],
      ['public.cleaned']: [{id: 1}],
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

  test('external failure while running', async () => {
    const pool = newTransactionPool(
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      3,
      3,
    );

    void pool.process(task(`INSERT INTO foo (id) VALUES (1)`));
    void pool.process(task(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (3)`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (2)`));
    void pool.process(task(`INSERT INTO foo (id, val) VALUES (8, 'foo')`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (5)`));

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

  test('non-statement task error fails pool', async () => {
    const pool = newTransactionPool(
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      3,
      3,
    );

    const readError = new Error('doh');

    void pool.process(task(`INSERT INTO foo (id) VALUES (1)`));
    void pool.process(task(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (3)`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (2)`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (5)`));
    void pool.process(() => Promise.reject(readError));

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

  test('abort rolls back all transactions', async () => {
    const pool = newTransactionPool(
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      3,
      3,
    );

    void pool.process(task(`INSERT INTO foo (id) VALUES (1)`));
    void pool.process(task(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (3)`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (2)`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (5)`));
    pool.abort();

    await pool.run(db).done();

    // Nothing should have succeeded.
    await expectTables(db, {
      ['public.foo']: [],
      ['public.workers']: [],
      ['public.cleaned']: [],
    });
  });

  test('postgres error is surfaced', async () => {
    const pool = newTransactionPool(
      Mode.SERIALIZABLE,
      initTask,
      cleanupTask,
      3,
      3,
    );

    // With a total of 4 insert statements with id = 1, at least one tx is guaranteed to fail.
    void pool.process(task(`INSERT INTO foo (id) VALUES (1)`));
    void pool.process(task(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));
    void pool.process(task(`INSERT INTO foo (id, val) VALUES (1, 'bad')`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (3)`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (2)`));
    void pool.process(task(`INSERT INTO foo (id, val) VALUES (1, 'double')`));
    void pool.process(task(`INSERT INTO foo (id) VALUES (5)`));
    void pool.process(task(`INSERT INTO foo (id, val) VALUES (1, 'oof')`));
    pool.setDone();

    const result = await pool
      .run(db)
      .done()
      .catch(e => e);

    expect(pool.isRunning()).toBe(false);

    // Ensure that the postgres error is surfaced.
    expect(result).toBeInstanceOf(postgres.PostgresError);
    expect((result as postgres.PostgresError).code).toBe(PG_UNIQUE_VIOLATION);
  });

  test('partial success; error from post-resize worker', async () => {
    const pool = newTransactionPool(
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
        processing.enqueue(true);
        await canProceed.dequeue();
        return task(stmt)(tx);
      };

    pool.run(db);

    void pool.process(blockingTask(`INSERT INTO foo (id) VALUES (1)`));
    void pool.process(
      blockingTask(`INSERT INTO foo (id, val) VALUES (6, 'foo')`),
    );

    for (let i = 0; i < 2; i++) {
      await processing.dequeue();
    }

    // For the last of the new tasks, induce an error with a unique key violation.
    void pool.process(blockingTask(`INSERT INTO foo (id) VALUES (3)`));
    void pool.process(blockingTask(`INSERT INTO foo (id) VALUES (2)`));
    void pool.process(blockingTask(`INSERT INTO foo (id) VALUES (1)`));

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
      canProceed.enqueue(true);
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
      processing.enqueue(true);
      return task(stmt)(tx);
    };

    const {exportSnapshot, cleanupExport, setSnapshot} =
      synchronizedSnapshots();
    const leader = newTransactionPool(
      Mode.SERIALIZABLE,
      exportSnapshot,
      cleanupExport,
      3,
    );
    const follower = newTransactionPool(Mode.SERIALIZABLE, setSnapshot);

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
    void follower.process(blockingTask(`INSERT INTO foo (id) VALUES (4);`));
    void follower.process(blockingTask(`INSERT INTO foo (id) VALUES (5);`));
    void follower.process(blockingTask(`INSERT INTO foo (id) VALUES (6);`));

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

  test('snapshot synchronization error handling', async () => {
    const {exportSnapshot, cleanupExport, setSnapshot} =
      synchronizedSnapshots();
    const leader = newTransactionPool(
      Mode.SERIALIZABLE,
      exportSnapshot,
      cleanupExport,
    );
    const followers = newTransactionPool(Mode.READONLY, setSnapshot, undefined);

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
      processing.enqueue(true);
      return (await tx<{id: number}[]>`SELECT id FROM foo;`.values()).flat();
    };

    const {init, cleanup} = sharedSnapshot();
    const pool = newTransactionPool(Mode.READONLY, init, cleanup, 2, 5);

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
      processing.enqueue(true);
      return (await tx<{id: number}[]>`SELECT id FROM foo;`.values()).flat();
    };

    const {init, cleanup, snapshotID} = sharedSnapshot();
    const pool = newTransactionPool(Mode.SERIALIZABLE, init, cleanup, 1);

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
    const readers = newTransactionPool(Mode.READONLY, importInit);
    readers.run(db);
    await imported;

    const processed: Promise<number[]>[] = [];

    // Process one read.
    processed.push(pool.processReadTask(readTask()));

    // Do some writes on the pool.
    void pool.process(tx => [
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

    const pool = newTransactionPool(Mode.READONLY);
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

  test('errors thrown from readTasks do not fail the pool', async () => {
    await db`
    INSERT INTO foo (id) VALUES (1);
    INSERT INTO foo (id) VALUES (2);
    INSERT INTO foo (id) VALUES (3);
    `.simple();

    const pool = newTransactionPool(Mode.READONLY);
    pool.run(db);

    const error = new Error('oh nose');
    const throwingTask = async () => {
      await sleep(5);
      throw error;
    };
    const result = await pool.processReadTask(throwingTask).catch(e => e);
    expect(result).toBe(error);

    expect(pool.isRunning()).toBe(true);

    const readTask = async (tx: postgres.TransactionSql) =>
      (await tx<{id: number}[]>`SELECT id FROM foo;`.values()).flat();
    expect(await pool.processReadTask(readTask)).toEqual([1, 2, 3]);

    expect(pool.isRunning()).toBe(true);

    pool.setDone();
    await pool.done();
  });
});
