import {beforeEach, describe, expect, test} from 'vitest';
import {Source} from 'zero-cache/src/types/streams.js';
import {Notifier} from './notifier.js';
import {ReplicaState} from './replicator.js';

describe('replicator/notifier', () => {
  let notifier: Notifier;

  beforeEach(() => {
    notifier = new Notifier();
  });

  async function expectSingleMessage(
    sub: Source<ReplicaState>,
    payload: ReplicaState,
  ) {
    for await (const msg of sub) {
      expect(msg).toEqual(payload);
      break;
    }
  }

  test('notify immediately with last notification received', async () => {
    notifier.notifySubscribers();
    const sub = notifier.subscribe();
    await expectSingleMessage(sub, {state: 'version-ready'});

    notifier.notifySubscribers({state: 'maintenance'});
    await expectSingleMessage(sub, {state: 'maintenance'});

    const sub2 = notifier.subscribe();
    await expectSingleMessage(sub2, {state: 'maintenance'});
  });

  test('watermark', async () => {
    const notifier = new Notifier();
    const sub1 = notifier.subscribe();
    const sub2 = notifier.subscribe();

    notifier.notifySubscribers({state: 'version-ready'});
    await expectSingleMessage(sub1, {state: 'version-ready'});

    notifier.notifySubscribers({state: 'version-ready'});
    notifier.notifySubscribers({state: 'maintenance'});
    await expectSingleMessage(sub2, {state: 'maintenance'});
  });
});
