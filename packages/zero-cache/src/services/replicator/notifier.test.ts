/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {beforeEach, describe, expect, test} from 'vitest';
import type {Source} from '../../types/streams.ts';
import {Notifier} from './notifier.ts';
import type {ReplicaState} from './replicator.ts';

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
