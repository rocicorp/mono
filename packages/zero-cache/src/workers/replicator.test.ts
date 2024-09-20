import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {describe, expect, test, vi} from 'vitest';
import {ReplicaState} from 'zero-cache/src/services/replicator/replicator.js';
import {Subscription} from 'zero-cache/src/types/subscription.js';
import {inProcChannel} from '../types/processes.js';
import {orTimeout} from '../types/timeout.js';
import {
  createNotifierFrom,
  setUpMessageHandlers,
  subscribeTo,
} from './replicator.js';

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

    const msg1 = originalSub.push({state: 'version-ready'});
    const msg2 = originalSub.push({state: 'version-ready', ack: 123});
    const msg3 = originalSub.push({state: 'maintenance', ack: 456});

    const notifications = [];
    const notifier = createNotifierFrom(lc, child);
    subscribeTo(child);

    let i = 0;
    for await (const msg of notifier.subscribe()) {
      notifications.push(msg);
      switch (i++) {
        case 0:
          // Expect msg1 to already be 'consumed' because it is not waiting on an ACK.
          expect(await orTimeout(msg1.result, 5)).toBe('consumed');
          break;
        case 1:
          // msg2 is waiting on an ACK, so it should not be consumed until the next
          // iteration of this loop.
          expect(await orTimeout(msg2.result, 5)).toBe('timed-out');
          break;
        case 2:
          // msg2 should be ACK'ed, and msg3 awaiting the ACK.
          expect(await orTimeout(msg2.result, 5)).toBe('consumed');
          expect(await orTimeout(msg3.result, 5)).toBe('timed-out');
          break;
      }
      if (notifications.length === 3) {
        break;
      }
    }

    // When the loop has been exited, msg3 should be ACKed.
    expect(await msg3.result).toBe('consumed');

    expect(notifications).toEqual([
      {state: 'version-ready'},
      {state: 'version-ready', ack: 123},
      {state: 'maintenance', ack: 456},
    ]);
  });
});
