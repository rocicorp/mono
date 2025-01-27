import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../../db/statements.ts';
import {expectTables} from '../../../test/lite.ts';
import {
  getReplicationState,
  getSubscriptionState,
  initReplicationState,
  updateReplicationWatermark,
} from './replication-state.ts';

describe('replicator/schema/replication-state', () => {
  let db: StatementRunner;

  beforeEach(() => {
    db = new StatementRunner(
      new Database(createSilentLogContext(), ':memory:'),
    );
    initReplicationState(db.db, ['zero_data', 'zero_metadata'], '0a');
  });

  test('initial replication state', () => {
    expectTables(db.db, {
      ['_zero.replicationConfig']: [
        {
          lock: 1,
          replicaVersion: '0a',
          publications: '["zero_data","zero_metadata"]',
        },
      ],
      ['_zero.replicationState']: [
        {
          lock: 1,
          stateVersion: '0a',
        },
      ],
    });
  });

  test('subscription state', () => {
    expect(getSubscriptionState(db)).toEqual({
      replicaVersion: '0a',
      publications: ['zero_data', 'zero_metadata'],
      watermark: '0a',
    });
  });

  test('get versions', () => {
    expect(getReplicationState(db)).toEqual({
      stateVersion: '0a',
    });
  });

  test('update watermark state', () => {
    updateReplicationWatermark(db, '0f');
    expectTables(db.db, {
      ['_zero.replicationState']: [
        {
          lock: 1,
          stateVersion: '0f',
        },
      ],
    });
    expect(getReplicationState(db)).toEqual({
      stateVersion: '0f',
    });
    expect(getSubscriptionState(db)).toEqual({
      replicaVersion: '0a',
      publications: ['zero_data', 'zero_metadata'],
      watermark: '0f',
    });

    updateReplicationWatermark(db, '0r');
    expectTables(db.db, {
      ['_zero.replicationState']: [
        {
          lock: 1,
          stateVersion: '0r',
        },
      ],
    });
    expect(getReplicationState(db)).toEqual({
      stateVersion: '0r',
    });
    expect(getSubscriptionState(db)).toEqual({
      replicaVersion: '0a',
      publications: ['zero_data', 'zero_metadata'],
      watermark: '0r',
    });
  });
});
