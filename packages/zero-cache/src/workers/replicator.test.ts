import {describe, expect, test, vi} from 'vitest';
import {ReplicaVersionReady} from 'zero-cache/src/services/replicator/replicator.js';
import {Subscription} from 'zero-cache/src/types/subscription.js';
import {inProcChannel} from '../types/processes.js';
import {
  createNotifierFrom,
  getStatusFromWorker,
  setUpMessageHandlers,
  subscribeTo,
} from './replicator.js';

describe('workers/replicator', () => {
  test('replicator status', async () => {
    const replicator = {
      status: vi.fn().mockResolvedValue({status: 'yo'}),
      subscribe: vi.fn(),
    };

    const [parent, child] = inProcChannel();

    setUpMessageHandlers(replicator, parent);

    // Simulate a status request from the parent.
    const status = await getStatusFromWorker(child);
    expect(status).toEqual({status: 'yo'});
  });

  test('replicator subscription', async () => {
    const originalSub = Subscription.create<ReplicaVersionReady>();

    const replicator = {
      status: vi.fn(),
      subscribe: () => originalSub,
    };

    const [parent, child] = inProcChannel();

    void setUpMessageHandlers(replicator, parent);

    originalSub.push({foo: 'bar'});
    originalSub.push({foo: 'baz'});

    const notifications = [];
    const notifier = createNotifierFrom(child);
    subscribeTo(child);

    for await (const msg of notifier.subscribe()) {
      notifications.push(msg);
      if (notifications.length === 2) {
        break;
      }
    }

    expect(notifications).toEqual([{foo: 'bar'}, {foo: 'baz'}]);
  });
});
