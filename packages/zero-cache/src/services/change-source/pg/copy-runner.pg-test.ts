import {nanoid} from 'nanoid/non-secure';
import {Writable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {READONLY} from '../../../db/mode-enum.ts';
import {TransactionPool} from '../../../db/transaction-pool.ts';
import {getConnectionURI, testDBs} from '../../../test/db.ts';
import {
  pgClient,
  type PostgresDB,
  type PostgresTransaction,
} from '../../../types/pg.ts';
import {orTimeout} from '../../../types/timeout.ts';
import {CopyRunner} from './copy-runner.ts';

describe('copy-runner', () => {
  const lc = createSilentLogContext();
  let sql: PostgresDB;

  beforeEach(async ctx => {
    sql = await testDBs.create(`copy_runner_${ctx.task.name}`);
    const setup = Array.from({length: NUM_TABLES}, (_, i) =>
      [
        // This is a carefully crafted test that hits a bug in Postgres in which
        // the database stops responding to commands after a certain type/sequence
        // of COPY streams.
        //
        // The following conditions appear to be necessary to trigger the bug:
        // - Sufficient table data streamed from the COPY (hence the 400 rows and `b` column)
        // - A randomly ordered primary key column (the `a` column)
        // - More tables than copy workers, to exercise post-COPY commands (hence the 10 tables).
        //
        // A failure manifests as a "Test timed out" error as Postgres becomes unresponsive.
        `CREATE TABLE t${i} (a TEXT PRIMARY KEY, b TEXT, val INT);`,
        ...Array.from(
          {length: 400},
          (_, r) =>
            `INSERT INTO t${i} (a, b, val) VALUES ('${nanoid()}', '0000000000000000', ${r});`,
        ),
      ].join('\n'),
    ).join('\n');
    await sql.unsafe(setup);

    return async () => {
      await orTimeout(testDBs.drop(sql), 1000);
    };
  });

  async function doCopy(tx: PostgresTransaction, i: number) {
    let bytesReceived = 0;
    const stream = await tx.unsafe(`COPY t${i} TO STDOUT`);
    await pipeline(
      stream,
      new Writable({
        write(chunk, _encoding, callback) {
          bytesReceived += chunk.length;
          callback();
        },
      }),
    );
    return bytesReceived;
  }

  const NUM_TABLES = 10;
  const NUM_COPIERS = 3;

  test('copy_runner', async () => {
    const copyRunner = new CopyRunner(
      lc,
      () => pgClient(lc, getConnectionURI(sql)),
      NUM_COPIERS,
      undefined,
    );

    const results = await Promise.all(
      Array.from({length: NUM_TABLES}, (_, i) => i).map(i =>
        copyRunner.run(tx => doCopy(tx, i)),
      ),
    );
    expect(results).toEqual(Array.from({length: NUM_TABLES}, () => 17090));
    copyRunner.close();
  });

  // This is a meta-test to verify that the database setup in
  // beforeEach() successfully triggers the Postgres hang in the
  // connection-reusing TransactionPool.
  test('transaction_pool', async () => {
    const pool = new TransactionPool(
      lc,
      READONLY,
      undefined,
      undefined,
      NUM_COPIERS,
    );
    pool.run(sql);

    for (let table = 0; table < NUM_TABLES; table++) {
      void pool.processReadTask(tx => doCopy(tx, table));
    }
    pool.setDone();

    // If this succeeds, then the test setup did not successfully
    // reproduce the conditions that make Postgres hang.
    expect(await orTimeout(pool.done(), 2000)).toBe('timed-out');
  });
});
