import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {expectTables, testDBs} from '../../test/db.js';
import type {LexiVersion} from '../../types/lexi-version.js';
import type {PostgresDB} from '../../types/pg.js';
import {CREATE_INVALIDATION_TABLES} from './schema/invalidation.js';
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
    await db.unsafe(
      `CREATE SCHEMA _zero;` +
        CREATE_INVALIDATION_TABLES +
        CREATE_REPLICATION_TABLES,
    );

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
    invalidationRegistryVersion: LexiVersion | null;
    snapshotID: string;
  }> = (
    _writer,
    _readers,
    stateVersion,
    invalidationRegistryVersion,
    snapshotID,
  ) =>
    Promise.resolve({
      stateVersion,
      invalidationRegistryVersion,
      snapshotID,
    });

  test('initial (null) versions', async () => {
    const versions = await train.runNext(returnVersionsFn);
    expect(versions).toMatchObject({
      stateVersion: '00',
      invalidationRegistryVersion: null,
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
        tx`UPDATE _zero."InvalidationRegistryVersion" SET ${tx({
          stateVersionAtLastSpecChange: '02',
        })}`,
      ]);
    });

    const versions = await train.runNext(returnVersionsFn);
    expect(versions).toMatchObject({
      stateVersion: '03',
      invalidationRegistryVersion: '02',
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
      ['_zero.InvalidationRegistryVersion']: [
        {stateVersionAtLastSpecChange: '02', lock: 'v'},
      ],
    });
  });

  test('blocks on concurrent updates', async () => {
    const versions1 = await train.runNext(
      async (
        writer,
        readers,
        stateVersion,
        invalidationFiltersRegistryVersion,
        snapshotID,
      ) => {
        const {promise: externalTxStarted, resolve: signalExternalTxWaiting} =
          resolver();

        // Simulate a concurrent lock held by another Replicator.
        void db
          .begin(async tx => {
            void tx`
            SELECT "stateVersionAtLastSpecChange" as version
                FROM _zero."InvalidationRegistryVersion" 
                FOR UPDATE;`
              .simple()
              .execute(); // This statement should block the transaction on lock.

            signalExternalTxWaiting(); // Let the train proceed.

            // Write new versions when the lock has been released.
            await Promise.all([
              tx`INSERT INTO _zero."TxLog" ${tx({
                stateVersion: '05',
                lsn: '00/03',
                time: new Date().toISOString(),
                xid: 123,
              })}`,
              tx`UPDATE _zero."InvalidationRegistryVersion" SET ${tx({
                stateVersionAtLastSpecChange: '03',
              })}`,
            ]);
          })
          .then(() => lc.debug?.('committed concurrent update'));

        await externalTxStarted;

        return returnVersionsFn(
          writer,
          readers,
          stateVersion,
          invalidationFiltersRegistryVersion,
          snapshotID,
        );
      },
    );
    expect(versions1).toMatchObject({
      stateVersion: '00',
      invalidationRegistryVersion: null,
      snapshotID: expect.stringMatching(SNAPSHOT_PATTERN),
    });

    const versions2 = await train.runNext(returnVersionsFn);
    expect(versions2).toMatchObject({
      stateVersion: '05',
      invalidationRegistryVersion: '03',
      snapshotID: expect.stringMatching(SNAPSHOT_PATTERN),
    });
  });
});
