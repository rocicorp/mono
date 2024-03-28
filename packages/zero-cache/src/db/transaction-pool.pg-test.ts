import {PG_UNIQUE_VIOLATION} from '@drdgvhbh/postgres-error-codes';
import {afterEach, beforeEach, describe, expect, test} from '@jest/globals';
import postgres from 'postgres';
import {Queue} from 'shared/src/queue.js';
import {sleep} from 'shared/src/sleep.js';
import {expectTables, testDBs} from '../test/db.js';
import {createSilentLogContext} from '../test/logger.js';
import {TransactionPool} from './transaction-pool.js';

describe('db/transaction-pool', () => {
  let db: postgres.Sql<{bigint: bigint}>;
  const lc = createSilentLogContext();

  beforeEach(async () => {
    db = await testDBs.create('transaction_pool');
    await db`
    CREATE TABLE foo (
      id int PRIMARY KEY,
      val text
    );
    CREATE TABLE workers (
      id SERIAL
    );`.simple();
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

  test('single transaction, serialized processing', async () => {
    const single = new TransactionPool(lc, initTask, 1, 1);

    single.process(task(`INSERT INTO foo (id) VALUES (1)`));
    single.process(task(`INSERT INTO foo (id) VALUES (6)`));
    single.process(task(`UPDATE foo SET val = 'foo' WHERE id < 5`));
    single.process(task(`INSERT INTO foo (id) VALUES (3)`));
    single.setDone();

    await single.run(db);

    await expectTables(db, {
      ['public.foo']: [
        {id: 1, val: 'foo'},
        {id: 3, val: null},
        {id: 6, val: null},
      ],
      ['public.workers']: [{id: 1}],
    });
  });

  test('multiple transactions', async () => {
    const pool = new TransactionPool(lc, initTask, 3, 3);

    pool.process(task(`INSERT INTO foo (id) VALUES (1)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (3)`));
    pool.process(task(`INSERT INTO foo (id) VALUES (2)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (8, 'foo')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (5)`));
    pool.setDone();

    await pool.run(db);

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
    });
  });

  test('pool resizing before run', async () => {
    const pool = new TransactionPool(lc, initTask, 2, 5);

    pool.process(task(`INSERT INTO foo (id) VALUES (1)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (3)`));
    pool.process(task(`INSERT INTO foo (id) VALUES (2)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (8, 'foo')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (5)`));
    pool.setDone();

    await pool.run(db);

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
    });
  });

  test('pool resizing after run', async () => {
    const pool = new TransactionPool(lc, initTask, 2, 5);

    const processing = new Queue<boolean>();
    const canProceed = new Queue<boolean>();

    const blockingTask =
      (stmt: string) => async (tx: postgres.TransactionSql) => {
        void processing.enqueue(true);
        await canProceed.dequeue();
        return task(stmt)(tx);
      };

    const done = pool.run(db);

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

    await done;

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
    });
  });

  test('external failure before running', async () => {
    const pool = new TransactionPool(lc, initTask, 3, 3);

    pool.process(task(`INSERT INTO foo (id) VALUES (1)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (3)`));
    pool.process(task(`INSERT INTO foo (id) VALUES (2)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (8, 'foo')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (5)`));
    pool.setDone();

    // Set the failure before running.
    pool.fail(new Error('oh nose'));

    const result = await pool.run(db).catch(e => e);
    expect(result).toBeInstanceOf(Error);

    // Nothing should have succeeded.
    await expectTables(db, {
      ['public.foo']: [],
      ['public.workers']: [],
    });
  });

  test('external failure while running', async () => {
    const pool = new TransactionPool(lc, initTask, 3, 3);

    pool.process(task(`INSERT INTO foo (id) VALUES (1)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (3)`));
    pool.process(task(`INSERT INTO foo (id) VALUES (2)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (8, 'foo')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (5)`));

    const result = pool.run(db).catch(e => e);

    // Set the failure after running.
    pool.fail(new Error('oh nose'));
    expect(await result).toBeInstanceOf(Error);

    // Nothing should have succeeded.
    await expectTables(db, {
      ['public.foo']: [],
      ['public.workers']: [],
    });
  });

  test('postgres error is surfaced', async () => {
    const pool = new TransactionPool(lc, initTask, 3, 3);

    // With a total of 4 insert statements with id = 1, at least one tx is guaranteed to fail.
    pool.process(task(`INSERT INTO foo (id) VALUES (1)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (6, 'foo')`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (1, 'bad')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (3)`));
    pool.process(task(`INSERT INTO foo (id) VALUES (2)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (1, 'double')`));
    pool.process(task(`INSERT INTO foo (id) VALUES (5)`));
    pool.process(task(`INSERT INTO foo (id, val) VALUES (1, 'oof')`));

    const result = await pool.run(db).catch(e => e);

    // Ensure that the postgres error is surfaced.
    expect(result).toBeInstanceOf(postgres.PostgresError);
    expect((result as postgres.PostgresError).code).toBe(PG_UNIQUE_VIOLATION);

    // Nothing should have succeeded.
    await expectTables(db, {
      ['public.foo']: [],
      ['public.workers']: [],
    });
  });

  test('partial success; error from post-resize worker', async () => {
    const pool = new TransactionPool(lc, initTask, 2, 5);

    const processing = new Queue<boolean>();
    const canProceed = new Queue<boolean>();

    const blockingTask =
      (stmt: string) => async (tx: postgres.TransactionSql) => {
        void processing.enqueue(true);
        await canProceed.dequeue();
        return task(stmt)(tx);
      };

    // Note: run() will `await` its two initial workers.
    const done = pool.run(db);

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
    const result = await done.catch(e => e);

    // Ensure that the postgres error is surfaced.
    expect(result).toBeInstanceOf(postgres.PostgresError);
    expect((result as postgres.PostgresError).code).toBe(PG_UNIQUE_VIOLATION);

    // Note: We don't verify table expectations here because some transactions
    //       may have successfully completed. That's fine, because in practice
    //       it only makes sense to do writes in single-transaction pools.
  });
});
