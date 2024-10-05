import {beforeEach, describe, expect, test} from 'vitest';
import type {Source} from 'zero-cache/src/types/streams.js';
import {Notifier} from './notifier.js';
import type {ReplicaState} from './replicator.js';

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

    notifier.notifySubscribers({state: 'version-ready', testSeqNum: 123});
    await expectSingleMessage(sub, {state: 'version-ready', testSeqNum: 123});

    const sub2 = notifier.subscribe();
    await expectSingleMessage(sub2, {state: 'version-ready', testSeqNum: 123});
  });

  test('watermark', async () => {
    const notifier = new Notifier();
    const sub1 = notifier.subscribe();
    const sub2 = notifier.subscribe();

    const results1 = notifier.notifySubscribers({
      state: 'version-ready',
      testSeqNum: 234,
    });
    await expectSingleMessage(sub1, {state: 'version-ready', testSeqNum: 234});
    expect(await results1[0]).toEqual('consumed');

    notifier.notifySubscribers({state: 'version-ready', testSeqNum: 345});
    expect(await results1[1]).toEqual('coalesced');

    const results2 = notifier.notifySubscribers({
      state: 'version-ready',
      testSeqNum: 456,
    });
    await expectSingleMessage(sub2, {state: 'version-ready', testSeqNum: 456});
    expect(await Promise.all(results2)).toEqual(['consumed']);
  });
});
