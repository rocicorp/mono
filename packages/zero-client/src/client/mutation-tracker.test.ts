import {describe, test, expect, vi} from 'vitest';
import {MutationTracker} from './mutation-tracker.ts';
import type {PushResponse} from '../../../zero-protocol/src/push.ts';
import {makeReplicacheMutator} from './custom.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {WriteTransaction} from './replicache-types.ts';
import {zeroData} from '../../../replicache/src/transactions.ts';
import type {MutationPatch} from '../../../zero-protocol/src/mutations-patch.ts';

const lc = createSilentLogContext();

const ackMutations = () => {};

describe('MutationTracker', () => {
  const CLIENT_ID = 'test-client-1';

  test('tracks a mutation and resolves on success', async () => {
    const tracker = new MutationTracker(lc, ackMutations);
    tracker.clientID = CLIENT_ID;
    const {ephemeralID, serverPromise} = tracker.trackMutation();
    tracker.mutationIDAssigned(ephemeralID, 1);

    const response: PushResponse = {
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 1},
          result: {},
        },
      ],
    };

    tracker.processPushResponse(response);
    const result = await serverPromise;
    expect(result).toEqual({});
  });

  test('tracks a mutation and resolves with error on error', async () => {
    const tracker = new MutationTracker(lc, ackMutations);
    tracker.clientID = CLIENT_ID;
    const {serverPromise, ephemeralID} = tracker.trackMutation();
    tracker.mutationIDAssigned(ephemeralID, 1);

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
    await expect(serverPromise).rejects.toEqual({
      error: 'app',
      details: '',
    });
  });

  test('does not resolve mutators for transient errors', async () => {
    const tracker = new MutationTracker(lc, ackMutations);
    tracker.clientID = CLIENT_ID;
    const {ephemeralID, serverPromise} = tracker.trackMutation();
    tracker.mutationIDAssigned(ephemeralID, 1);

    const response: PushResponse = {
      error: 'unsupportedPushVersion',
      mutationIDs: [{clientID: CLIENT_ID, id: 1}],
    };

    tracker.processPushResponse(response);
    let called = false;
    void serverPromise.finally(() => {
      called = true;
    });
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(tracker.size).toBe(1);
    expect(called).toBe(false);
  });

  test('rejects mutations from other clients', () => {
    const tracker = new MutationTracker(lc, ackMutations);
    tracker.clientID = CLIENT_ID;
    const mutation = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation.ephemeralID, 1);

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
    const tracker = new MutationTracker(lc, ackMutations);
    tracker.clientID = CLIENT_ID;
    const mutation1 = tracker.trackMutation();
    const mutation2 = tracker.trackMutation();

    tracker.mutationIDAssigned(mutation1.ephemeralID, 1);
    tracker.mutationIDAssigned(mutation2.ephemeralID, 2);

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

    const [result1, result2] = await Promise.all([
      mutation1.serverPromise,
      mutation2.serverPromise,
    ]);
    expect(result1).toBe(r1);
    expect(result2).toBe(r2);
  });

  test('mutation tracker size goes down each time a mutation is resolved or rejected', () => {
    const tracker = new MutationTracker(lc, ackMutations);
    tracker.clientID = CLIENT_ID;
    const mutation1 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation1.ephemeralID, 1);

    const mutation2 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation2.ephemeralID, 2);

    mutation2.serverPromise.catch(() => {
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
    const mt = new MutationTracker(lc, ackMutations);
    mt.clientID = CLIENT_ID;
    const mutator = makeReplicacheMutator(
      createSilentLogContext(),
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

  test('mutation responses, received via poke, are processed', async () => {
    const ackMutations = vi.fn();
    const tracker = new MutationTracker(lc, ackMutations);
    tracker.clientID = CLIENT_ID;

    const mutation1 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation1.ephemeralID, 1);
    const mutation2 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation2.ephemeralID, 2);

    const patches: MutationPatch[] = [
      {
        op: 'put',
        mutation: {
          id: {clientID: CLIENT_ID, id: 1},
          result: {},
        },
      },
      {
        op: 'put',
        mutation: {
          id: {clientID: CLIENT_ID, id: 2},
          result: {error: 'app'},
        },
      },
    ];

    tracker.processMutationResponses(patches);
    expect(ackMutations).toHaveBeenCalledOnce();
    expect(ackMutations).toHaveBeenCalledWith({clientID: CLIENT_ID, id: 2});

    await expect(mutation1.serverPromise).resolves.toEqual({});
    await expect(mutation2.serverPromise).rejects.toEqual({
      error: 'app',
    });
  });

  test('tracked mutations are resolved on reconnect', async () => {
    const tracker = new MutationTracker(lc, ackMutations);
    tracker.clientID = CLIENT_ID;

    const mutation1 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation1.ephemeralID, 1);
    const mutation2 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation2.ephemeralID, 2);
    const mutation3 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation3.ephemeralID, 3);
    const mutation4 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation4.ephemeralID, 4);

    expect(tracker.size).toBe(4);

    tracker.onConnected(3);
    await Promise.all([
      mutation1.serverPromise,
      mutation2.serverPromise,
      mutation3.serverPromise,
    ]);

    expect(tracker.size).toBe(1);

    tracker.onConnected(20);

    expect(tracker.size).toBe(0);
    await mutation4.serverPromise;
  });

  test('notified whenever the outstanding mutation count goes to 0', () => {
    const tracker = new MutationTracker(lc, ackMutations);
    tracker.clientID = CLIENT_ID;

    let callCount = 0;
    tracker.onAllMutationsApplied(() => {
      callCount++;
    });

    const mutation1 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation1.ephemeralID, 1);
    tracker.processPushResponse({
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 1},
          result: {},
        },
      ],
    });
    tracker.lmidAdvanced(1);

    expect(callCount).toBe(1);

    try {
      tracker.processPushResponse({
        mutations: [
          {
            id: {clientID: CLIENT_ID, id: 1},
            result: {},
          },
        ],
      });
    } catch (e) {
      // expected
    }

    expect(callCount).toBe(1);

    const mutation2 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation2.ephemeralID, 2);
    const mutation3 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation3.ephemeralID, 3);
    const mutation4 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation4.ephemeralID, 4);

    mutation4.serverPromise.catch(() => {
      // expected
    });

    tracker.processPushResponse({
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 2},
          result: {},
        },
      ],
    });

    expect(callCount).toBe(1);

    tracker.processPushResponse({
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 3},
          result: {},
        },
      ],
    });
    tracker.processPushResponse({
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 4},
          result: {error: 'app'},
        },
      ],
    });
    tracker.lmidAdvanced(4);

    expect(callCount).toBe(2);

    const mutation5 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation5.ephemeralID, 5);
    const mutation6 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation6.ephemeralID, 6);
    const mutation7 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation7.ephemeralID, 7);

    tracker.onConnected(6);

    expect(callCount).toBe(2);

    tracker.onConnected(7);

    expect(callCount).toBe(3);
  });

  test('mutations can be rejected before a mutation id is assigned', async () => {
    const tracker = new MutationTracker(lc, ackMutations);
    tracker.clientID = CLIENT_ID;

    const {ephemeralID, serverPromise} = tracker.trackMutation();
    tracker.rejectMutation(ephemeralID, new Error('test error'));
    let caught: unknown | undefined;

    try {
      await serverPromise;
    } catch (e) {
      caught = e;
    }

    expect(caught).toMatchInlineSnapshot(`[Error: test error]`);
    expect(tracker.size).toBe(0);
  });

  test('trying to resolve a mutation with an a unassigned ephemeral id throws', () => {
    const tracker = new MutationTracker(lc, ackMutations);
    tracker.clientID = CLIENT_ID;

    tracker.trackMutation();
    const response: PushResponse = {
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 1},
          result: {},
        },
      ],
    };
    expect(() => tracker.processPushResponse(response)).toThrow(
      'ephemeral ID is missing. This can happen if a mutation response is received twice but it should be impossible to receive a success response twice for the same mutation.',
    );
  });

  test('resolve a mutation a second time with "already processed" error', () => {
    const tracker = new MutationTracker(lc, ackMutations);
    tracker.clientID = CLIENT_ID;

    const {ephemeralID} = tracker.trackMutation();
    tracker.mutationIDAssigned(ephemeralID, 1);

    const response: PushResponse = {
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 1},
          result: {},
        },
      ],
    };

    tracker.processPushResponse(response);
    expect(tracker.size).toBe(0);

    // alreadyProcessedErrors are ignored if we've already resolved the mutation
    // once.
    expect(() =>
      tracker.processPushResponse({
        mutations: [
          {
            id: {clientID: CLIENT_ID, id: 1},
            result: {
              error: 'alreadyProcessed',
            },
          },
        ],
      }),
    ).not.toThrow();

    // other errors throw since we should not process a mutation more than once
    // unless the error is an alreadyProcessed error.
    expect(() =>
      tracker.processPushResponse({
        mutations: [
          {
            id: {clientID: CLIENT_ID, id: 1},
            result: {
              error: 'app',
            },
          },
        ],
      }),
    ).toThrow('ephemeral ID is missing for mutation error: app.');
  });

  test('advancing lmid past outstanding lmid notifies "all mutations applied" listeners', () => {
    const tracker = new MutationTracker(lc, ackMutations);
    tracker.clientID = CLIENT_ID;

    const listener = vi.fn();
    tracker.onAllMutationsApplied(listener);

    tracker.lmidAdvanced(2);

    expect(listener).toHaveBeenCalled();

    const data = tracker.trackMutation();
    tracker.mutationIDAssigned(data.ephemeralID, 4);

    tracker.lmidAdvanced(3);
    expect(listener).toHaveBeenCalledTimes(1);
    tracker.lmidAdvanced(4);
    expect(listener).toHaveBeenCalledTimes(2);
    tracker.lmidAdvanced(5);
    expect(listener).toHaveBeenCalledTimes(3);
  });

  test('advancing lmid clears limbo mutations up to that lmid', async () => {
    const tracker = new MutationTracker(lc, ackMutations);
    tracker.clientID = CLIENT_ID;

    const mutation1 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation1.ephemeralID, 1);
    const mutation2 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation2.ephemeralID, 2);
    const mutation3 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation3.ephemeralID, 3);

    tracker.processPushResponse({
      error: 'http',
      status: 500,
      details: 'Internal Server Error',
      mutationIDs: [
        {clientID: CLIENT_ID, id: 1},
        {clientID: CLIENT_ID, id: 2},
        {clientID: CLIENT_ID, id: 3},
      ],
    });

    tracker.lmidAdvanced(2);

    let mutation3Resolved = false;
    void mutation3.serverPromise.finally(() => {
      mutation3Resolved = true;
    });

    await Promise.all([mutation1.serverPromise, mutation2.serverPromise]);
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(mutation3Resolved).toBe(false);

    tracker.lmidAdvanced(3);
    await mutation3.serverPromise;
    expect(mutation3Resolved).toBe(true);
  });

  test('failed push causes mutations to resolve that are under the current lmid', async () => {
    const tracker = new MutationTracker(lc, ackMutations);
    tracker.clientID = CLIENT_ID;

    const mutation1 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation1.ephemeralID, 1);
    const mutation2 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation2.ephemeralID, 2);
    const mutation3 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation3.ephemeralID, 3);

    tracker.lmidAdvanced(2);

    tracker.processPushResponse({
      error: 'http',
      status: 500,
      details: 'Internal Server Error',
      mutationIDs: [
        {clientID: CLIENT_ID, id: 1},
        {clientID: CLIENT_ID, id: 2},
        {clientID: CLIENT_ID, id: 3},
      ],
    });

    let mutation3Resolved = false;
    void mutation3.serverPromise.finally(() => {
      mutation3Resolved = true;
    });
    await Promise.all([mutation1.serverPromise, mutation2.serverPromise]);
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(mutation3Resolved).toBe(false);
  });

  test('reconnecting puts outstanding mutations in limbo', async () => {
    const tracker = new MutationTracker(lc, ackMutations);
    tracker.clientID = CLIENT_ID;

    const mutation1 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation1.ephemeralID, 3);
    const mutation2 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation2.ephemeralID, 4);
    const mutation3 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation3.ephemeralID, 5);

    tracker.onConnected(1);

    tracker.lmidAdvanced(5);
    expect(tracker.size).toBe(0);
    await Promise.all([
      mutation1.serverPromise,
      mutation2.serverPromise,
      mutation3.serverPromise,
    ]);
  });

  test('advancing lmid does not resolve mutations that are not in limbo', () => {
    const tracker = new MutationTracker(lc, ackMutations);
    tracker.clientID = CLIENT_ID;

    const mutation1 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation1.ephemeralID, 1);
    const mutation2 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation2.ephemeralID, 2);
    const mutation3 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation3.ephemeralID, 3);

    tracker.lmidAdvanced(5);

    expect(tracker.size).toBe(3);

    tracker.lmidAdvanced(8);

    expect(tracker.size).toBe(3);
  });
});
