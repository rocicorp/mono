import {beforeEach, describe, expect, test} from 'vitest';
import {CancelableAsyncIterable} from 'zero-cache/src/types/streams.js';
import {Notifier} from './notifier.js';
import {ReplicaVersionReady} from './replicator.js';

describe('replicator/notifier', () => {
  let notifier: Notifier;

  beforeEach(() => {
    notifier = new Notifier();
  });

  async function expectSingleMessage(
    sub: CancelableAsyncIterable<ReplicaVersionReady>,
    payload: ReplicaVersionReady,
  ) {
    for await (const msg of sub) {
      expect(msg).toEqual(payload);
      break;
    }
  }

  test('notify immediately if first notification received', async () => {
    notifier.notifySubscribers();
    const sub = notifier.addSubscription();
    await expectSingleMessage(sub, {});
  });

  test('coalesce', async () => {
    const notifier = new Notifier();
    const sub1 = notifier.addSubscription();
    const sub2 = notifier.addSubscription();

    notifier.notifySubscribers({foo: 1});
    await expectSingleMessage(sub1, {foo: 1});

    notifier.notifySubscribers({foo: 2});
    notifier.notifySubscribers({foo: 3});
    await expectSingleMessage(sub2, {foo: 3});
  });
});
