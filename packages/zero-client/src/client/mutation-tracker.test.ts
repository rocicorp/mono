import {describe, test, expect} from 'vitest';
import {MutationTracker} from './mutation-tracker.ts';
import type {PushResponse} from '../../../zero-protocol/src/push.ts';
import {makeReplicacheMutator} from './custom.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {WriteTransaction} from './replicache-types.ts';
import {zeroData} from '../../../replicache/src/transactions.ts';

describe('MutationTracker', () => {
  const CLIENT_ID = 'test-client-1';

  test('tracks a mutation and resolves on success', async () => {
    const tracker = new MutationTracker();
    tracker.clientID = CLIENT_ID;
    const mutationPromise = tracker.trackMutation(1, []);

    const response: PushResponse = {
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 1},
          result: {},
        },
      ],
    };

    tracker.processPushResponse(response);
    const result = await mutationPromise;
    expect(result).toEqual({});
  });

  test('tracks a mutation and rejects on error', async () => {
    const tracker = new MutationTracker();
    tracker.clientID = CLIENT_ID;
    const mutationPromise = tracker.trackMutation(1, []);

    const response: PushResponse = {
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 1},
          result: {
            error: 'app',
            details: '',
          },
        },
      ],
    };

    tracker.processPushResponse(response);
    await expect(mutationPromise).rejects.toEqual({
      error: 'app',
      details: '',
    });
  });

  test('does not resolve mutators for transient errors', async () => {
    const tracker = new MutationTracker();
    tracker.clientID = CLIENT_ID;
    const mutationPromise = tracker.trackMutation(1, []);

    const response: PushResponse = {
      error: 'unsupported-push-version',
      mutationIDs: [{clientID: CLIENT_ID, id: 1}],
    };

    tracker.processPushResponse(response);
    let called = false;
    void mutationPromise.finally(() => {
      called = true;
    });
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(tracker.size).toBe(1);
    expect(called).toBe(false);
  });

  test('rejects mutations from other clients', () => {
    const tracker = new MutationTracker();
    tracker.clientID = CLIENT_ID;
    void tracker.trackMutation(1, []);

    const response: PushResponse = {
      mutations: [
        {
          id: {clientID: 'other-client', id: 1},
          result: {
            error: 'app',
            details: '',
          },
        },
        {
          id: {clientID: CLIENT_ID, id: 1},
          result: {},
        },
      ],
    };

    expect(() => tracker.processPushResponse(response)).toThrow(
      'received mutation for the wrong client',
    );
  });

  test('handles multiple concurrent mutations', async () => {
    const tracker = new MutationTracker();
    tracker.clientID = CLIENT_ID;
    const mutation1 = tracker.trackMutation(1, []);
    const mutation2 = tracker.trackMutation(2, []);

    const r1 = {};
    const r2 = {};
    const response: PushResponse = {
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 1},
          result: r1,
        },
        {
          id: {clientID: CLIENT_ID, id: 2},
          result: r2,
        },
      ],
    };

    tracker.processPushResponse(response);

    const [result1, result2] = await Promise.all([mutation1, mutation2]);
    expect(result1).toBe(r1);
    expect(result2).toBe(r2);
  });

  test('mutation tracker size goes down each time a mutation is resolved or rejected', () => {
    const tracker = new MutationTracker();
    tracker.clientID = CLIENT_ID;
    void tracker.trackMutation(1, []);
    tracker.trackMutation(2, []).catch(() => {
      // expected
    });

    const response: PushResponse = {
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 1},
          result: {},
        },
        {
          id: {clientID: CLIENT_ID, id: 2},
          result: {
            error: 'app',
          },
        },
      ],
    };

    tracker.processPushResponse(response);
    expect(tracker.size).toBe(0);
  });

  test('mutations are not tracked on rebase', async () => {
    const mt = new MutationTracker();
    mt.clientID = CLIENT_ID;
    const mutator = makeReplicacheMutator(
      createSilentLogContext(),
      mt,
      async () => {},
      createSchema({
        tables: [],
        relationships: [],
      }),
      0,
    );

    const tx = {
      reason: 'rebase',
      mutationID: 1,
      [zeroData]: {},
    };
    await mutator(tx as unknown as WriteTransaction, {});
    expect(mt.size).toBe(0);
  });
});
