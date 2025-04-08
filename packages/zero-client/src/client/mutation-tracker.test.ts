import {describe, test, expect} from 'vitest';
import {MutationTracker} from './mutation-tracker.ts';
import type {PushResponse} from '../../../zero-protocol/src/push.ts';
import {makeReplicacheMutator} from './custom.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {WriteTransaction} from './replicache-types.ts';
import {zeroData} from '../../../replicache/src/transactions.ts';

const lc = createSilentLogContext();
describe('MutationTracker', () => {
  const CLIENT_ID = 'test-client-1';

  test('tracks a mutation and resolves on success', async () => {
    const tracker = new MutationTracker(lc);
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
    const tracker = new MutationTracker(lc);
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
    expect(await serverPromise).toEqual({
      error: 'app',
      details: '',
    });
  });

  test('does not resolve mutators for transient errors', async () => {
    const tracker = new MutationTracker(lc);
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
    const tracker = new MutationTracker(lc);
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
    const tracker = new MutationTracker(lc);
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
    const tracker = new MutationTracker(lc);
    tracker.clientID = CLIENT_ID;
    const mutation1 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation1.ephemeralID, 1);

    const mutation2 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation2.ephemeralID, 2);

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
    const mt = new MutationTracker(lc);
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

  test('tracked mutations are resolved on reconnect', async () => {
    const tracker = new MutationTracker(lc);
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
    const tracker = new MutationTracker(lc);
    tracker.clientID = CLIENT_ID;

    let callCount = 0;
    tracker.onAllMutationsConfirmed(() => {
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
    const tracker = new MutationTracker(lc);
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
    const tracker = new MutationTracker(lc);
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
      'invalid state. An ephemeral id was never assigned to mutation 1',
    );
  });
});
