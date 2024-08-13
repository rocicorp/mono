import {LogContext} from '@rocicorp/logger';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {expectTables, testDBs} from '../../test/db.js';
import type {LexiVersion} from '../../types/lexi-version.js';
import type {PostgresDB} from '../../types/pg.js';
import {CREATE_REPLICATION_TABLES} from './schema/replication.js';
import {TransactionFn, TransactionTrainService} from './transaction-train.js';

const SNAPSHOT_PATTERN = /([0-9A-F]+-){2}[0-9A-F]/;

describe('replicator/transaction-train', () => {
  let db: PostgresDB;
  let lc: LogContext;
  let train: TransactionTrainService;
  let trainDone: Promise<void>;

  beforeEach(async () => {
    db = await testDBs.create('transaction_train_test');
    await db.unsafe(`CREATE SCHEMA _zero;` + CREATE_REPLICATION_TABLES);

    lc = createSilentLogContext();
    train = new TransactionTrainService(lc, db);
    trainDone = train.run();
  });

  afterEach(async () => {
    await train.stop();
    await trainDone;
    await testDBs.drop(db);
  });

  const returnVersionsFn: TransactionFn<{
    stateVersion: LexiVersion;
    snapshotID: string;
  }> = (_writer, _readers, stateVersion, snapshotID) =>
    Promise.resolve({
      stateVersion,
      snapshotID,
    });

  test('initial (null) versions', async () => {
    const versions = await train.runNext(returnVersionsFn);
    expect(versions).toMatchObject({
      stateVersion: '00',
      snapshotID: expect.stringMatching(SNAPSHOT_PATTERN),
    });
  });

  test('version updates', async () => {
    const date = new Date(Date.UTC(2024, 4, 31, 1, 2, 3));
    await train.runNext(writer => {
      writer.process(tx => [
        tx`INSERT INTO _zero."TxLog" ${tx({
          stateVersion: '03',
          lsn: '00/03',
          time: date.toISOString(),
          xid: 123,
        })}`,
      ]);
    });

    const versions = await train.runNext(returnVersionsFn);
    expect(versions).toMatchObject({
      stateVersion: '03',
      snapshotID: expect.stringMatching(SNAPSHOT_PATTERN),
    });

    await expectTables(db, {
      ['_zero.TxLog']: [
        {
          stateVersion: '03',
          lsn: '0/3',
          time: date,
          xid: 123,
        },
      ],
    });
  });
});
