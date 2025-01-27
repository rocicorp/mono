import {describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {ReplicaState} from '../services/replicator/replicator.ts';
import {inProcChannel} from '../types/processes.ts';
import {Subscription} from '../types/subscription.ts';
import {
  createNotifierFrom,
  setUpMessageHandlers,
  subscribeTo,
} from './replicator.ts';

const lc = createSilentLogContext();

describe('workers/replicator', () => {
  test('replicator subscription', async () => {
    const originalSub = Subscription.create<ReplicaState>();

    const replicator = {
      status: vi.fn(),
      subscribe: () => originalSub,
    };

    const [parent, child] = inProcChannel();

    void setUpMessageHandlers(lc, replicator, parent);

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
