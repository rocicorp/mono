import {beforeEach, describe, expect} from 'vitest';
import {test, type PgTest} from '../test/db.ts';
import {type PostgresDB} from '../types/pg.ts';
import {runTx} from './run-transaction.ts';

describe('db/run-transaction', () => {
  let db: PostgresDB;

  beforeEach<PgTest>(async ({testDBs}) => {
    db = await testDBs.create('run_transaction');
    // Ensures deterministic behavior of the tests

    return () => testDBs.drop(db);
  });

  test('statement timeout disabled', async () => {
    await db`SET statement_timeout = 100000`;
    expect(await db`SHOW statement_timeout`).toMatchInlineSnapshot(`
      Result [
        {
          "statement_timeout": "100s",
        },
      ]
    `);

    const txResult = await runTx(db, tx => tx`SHOW statement_timeout`);
    expect(txResult).toMatchInlineSnapshot(`
      Result [
        {
          "statement_timeout": "0",
        },
      ]
    `);
  });

  test('idle in transaction session timeout', async () => {
    expect(await runTx(db, tx => tx`SHOW idle_in_transaction_session_timeout`))
      .toMatchInlineSnapshot(`
      Result [
        {
          "idle_in_transaction_session_timeout": "1min",
        },
      ]
    `);
    expect(
      await runTx(db, tx => tx`SHOW idle_in_transaction_session_timeout`, {
        idleInTransactionTimeoutMs: 125_000,
      }),
    ).toMatchInlineSnapshot(`
      Result [
        {
          "idle_in_transaction_session_timeout": "125s",
        },
      ]
    `);
    expect(
      await runTx(db, tx => tx`SHOW idle_in_transaction_session_timeout`, {
        idleInTransactionTimeoutMs: 30_000,
      }),
    ).toMatchInlineSnapshot(`
      Result [
        {
          "idle_in_transaction_session_timeout": "30s",
        },
      ]
    `);
  });
});
