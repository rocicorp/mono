import {describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {ReplicaState} from '../services/replicator/replicator.ts';
import {inProcChannel} from '../types/processes.ts';
import {Subscription} from '../types/subscription.ts';
import {
  createNotifierFrom,
  createsCanonicalReplicator,
  replicaLogsChangeStream,
  setUpMessageHandlers,
  subscribeTo,
} from './replicator.ts';

const lc = createSilentLogContext();

describe('workers/replicator', () => {
  test('selects exactly one canonical SQLite change-log writer', () => {
    expect(createsCanonicalReplicator(true, 's3://backup', 0)).toBe(true);
    expect(createsCanonicalReplicator(true, undefined, 1)).toBe(true);
    expect(createsCanonicalReplicator(true, undefined, 0)).toBe(false);
    expect(createsCanonicalReplicator(false, undefined, 1)).toBe(false);

    expect(replicaLogsChangeStream('backup', true, true, 's3://backup')).toBe(
      true,
    );
    expect(
      replicaLogsChangeStream('serving-copy', true, true, 's3://backup'),
    ).toBe(false);
    expect(replicaLogsChangeStream('serving', true, true, undefined)).toBe(
      true,
    );
    expect(replicaLogsChangeStream('serving', true, false, undefined)).toBe(
      false,
    );
    expect(replicaLogsChangeStream('serving', false, true, undefined)).toBe(
      false,
    );
  });

  test('replicator subscription', async () => {
    const originalSub = Subscription.create<ReplicaState>();

    const replicator = {
      status: vi.fn(),
      subscribe: () => originalSub,
    };

    const [parent, child] = inProcChannel();

    setUpMessageHandlers(lc, replicator, parent);

    originalSub.push({state: 'version-ready', testSeqNum: 1});
    originalSub.push({state: 'version-ready', testSeqNum: 2});
    const msg3 = originalSub.push({state: 'version-ready', testSeqNum: 3});

    const notifications = [];
    const notifier = createNotifierFrom(lc, child);
    subscribeTo(lc, child);

    for await (const msg of notifier.subscribe()) {
      notifications.push(msg);
      if (notifications.length === 3) {
        break;
      }
    }

    // When the loop has been exited, msg3 should be ACKed.
    expect(await msg3.result).toBe('consumed');

    expect(notifications).toEqual([
      {state: 'version-ready', testSeqNum: 1},
      {state: 'version-ready', testSeqNum: 2},
      {state: 'version-ready', testSeqNum: 3},
    ]);
  });
});
