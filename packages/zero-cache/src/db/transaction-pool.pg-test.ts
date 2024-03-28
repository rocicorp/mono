import {PG_UNIQUE_VIOLATION} from '@drdgvhbh/postgres-error-codes';
import {afterEach, beforeEach, describe, expect, test} from '@jest/globals';
import postgres from 'postgres';
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
    CREATE TABLE tasks (
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

  test('single transaction, serialized processing', async () => {
    const single = new TransactionPool(lc);

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
    });
  });

  test('multiple transactions', async () => {
    const pool = new TransactionPool(lc, 3);

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
    });
  });

  test('multiple transactions with init', async () => {
    const pool = new TransactionPool(
      lc,
      3,
      task(`INSERT INTO tasks (id) VALUES (DEFAULT);`),
    );

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
      ['public.tasks']: [{id: 1}, {id: 2}, {id: 3}],
    });
  });

  test('external failure before running', async () => {
    const pool = new TransactionPool(lc, 3);

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
    });
  });

  test('external failure while running', async () => {
    const pool = new TransactionPool(lc, 3);

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
    });
  });

  test('postgres error is surfaced', async () => {
    const pool = new TransactionPool(lc, 3);

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
    });
  });
});
