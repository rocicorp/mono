import {describe, expect, test, vi} from 'vitest';
import type {
  DiffOperation,
  NoIndexDiff,
} from '../../../replicache/src/btree/node.ts';
import {zeroData} from '../../../replicache/src/transactions.ts';
import {assert, unreachable} from '../../../shared/src/asserts.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {ApplicationError} from '../../../zero-protocol/src/application-error.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ProtocolError} from '../../../zero-protocol/src/error.ts';
import type {MutationPatch} from '../../../zero-protocol/src/mutations-patch.ts';
import type {
  PushResponse,
  PushResponseBody,
} from '../../../zero-protocol/src/push.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {ClientErrorKind} from './client-error-kind.ts';
import {makeReplicacheMutator} from './custom.ts';
import {ClientError, isServerError} from './error.ts';
import {toMutationResponseKey} from './keys.ts';
import {MutationTracker} from './mutation-tracker.ts';
import type {WriteTransaction} from './replicache-types.ts';

const lc = createSilentLogContext();

const ackMutations = () => {};
const watch = () => () => {};

// Helper to create a tracker with mocked callbacks
function createTracker() {
  const onFatalError = vi.fn();
  const tracker = new MutationTracker(lc, ackMutations, onFatalError);
  return {tracker, onFatalError};
}

describe('MutationTracker', () => {
  const CLIENT_ID = 'test-client-1';

  test('tracks a mutation and resolves on success', async () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);
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
    expect(result.type).toBe('success');
    expect(onFatalError).not.toHaveBeenCalled();
  });

  test('tracks a mutation and rejects with error on error', async () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);
    const {serverPromise, ephemeralID} = tracker.trackMutation();
    tracker.mutationIDAssigned(ephemeralID, 1);

    const response: PushResponse = {
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 1},
          result: {
            error: 'app',
            message: 'server error',
          },
        },
      ],
    };

    tracker.processPushResponse(response);

    await expect(serverPromise).rejects.toMatchObject({
      name: 'ApplicationError',
      message: 'server error',
      details: undefined,
    });

    expect(onFatalError).not.toHaveBeenCalled();
  });

  test('includes server-provided details when rejecting mutations', async () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);
    const {serverPromise, ephemeralID} = tracker.trackMutation();
    tracker.mutationIDAssigned(ephemeralID, 1);

    const response: PushResponse = {
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 1},
          result: {
            error: 'app',
            message: 'server error',
            details: {source: 'server', code: 42},
          },
        },
      ],
    };

    tracker.processPushResponse(response);

    await expect(serverPromise).rejects.toMatchObject({
      name: 'ApplicationError',
      message: 'server error',
      details: {
        source: 'server',
        code: 42,
      },
    });
    expect(onFatalError).not.toHaveBeenCalled();
  });

  test('calls onFatalError for unsupportedPushVersion and does not resolve mutations', async () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);
    const {ephemeralID, serverPromise} = tracker.trackMutation();
    tracker.mutationIDAssigned(ephemeralID, 1);

    const response: PushResponse = {
      error: 'unsupportedPushVersion',
      mutationIDs: [{clientID: CLIENT_ID, id: 1}],
    };

    tracker.processPushResponse(response);

    // Mutations should not be resolved
    let called = false;
    void serverPromise.finally(() => {
      called = true;
    });
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(tracker.size).toBe(1);
    expect(called).toBe(false);

    // But fatal error callback should have been called
    expect(onFatalError).toHaveBeenCalledOnce();
    expect(onFatalError).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'ProtocolError',
        errorBody: expect.objectContaining({
          kind: ErrorKind.PushFailed,
          message: 'Unsupported push version',
        }),
      }),
    );
  });

  test('emits fatal error for http push failure', () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

    const response: PushResponseBody = {
      error: 'http',
      status: 500,
      details: 'Internal Server Error',
      mutationIDs: [{clientID: CLIENT_ID, id: 1}],
    };

    tracker.processPushResponse(response);

    expect(onFatalError).toHaveBeenCalledTimes(1);
    const fatalError = onFatalError.mock.calls[0][0];
    expect(fatalError).toBeInstanceOf(ProtocolError);
    assert(isServerError(fatalError));
    expect(fatalError.kind).toBe(ErrorKind.PushFailed);
  });

  test('emits fatal error for ooo mutation result', async () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

    const {ephemeralID, serverPromise} = tracker.trackMutation();
    tracker.mutationIDAssigned(ephemeralID, 1);

    const response: PushResponse = {
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 1},
          result: {error: 'oooMutation', details: 'out-of-order'},
        },
      ],
    };

    tracker.processPushResponse(response);

    await expect(serverPromise).rejects.toMatchObject({
      name: 'ProtocolError',
      message: 'Server reported an out-of-order mutation',
      kind: ErrorKind.InvalidPush,
    });

    expect(onFatalError).toHaveBeenCalledTimes(1);
    const fatalError = onFatalError.mock.calls[0][0] as ProtocolError;
    expect(fatalError).toBeInstanceOf(ProtocolError);
    expect(fatalError.kind).toBe(ErrorKind.InvalidPush);
  });

  test('rejects mutations from other clients', () => {
    const {tracker} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);
    const mutation = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation.ephemeralID, 1);

    const response: PushResponse = {
      mutations: [
        {
          id: {clientID: 'other-client', id: 1},
          result: {
            error: 'app',
            message: 'server error',
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
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);
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
    expect(result1.type).toBe('success');
    expect(result2.type).toBe('success');
    expect(onFatalError).not.toHaveBeenCalled();
  });

  test('mutation tracker size goes down each time a mutation is resolved or rejected', async () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);
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
            message: 'server error',
          },
        },
      ],
    };

    tracker.processPushResponse(response);

    expect(tracker.size).toBe(0);

    await expect(mutation2.serverPromise).rejects.toThrow('server error');

    expect(onFatalError).not.toHaveBeenCalled();
  });

  test('mutations are not tracked on rebase', async () => {
    const {tracker: mt} = createTracker();
    mt.setClientIDAndWatch(CLIENT_ID, watch);
    const mutator = makeReplicacheMutator(
      createSilentLogContext(),
      async () => {},
      createSchema({
        tables: [],
        relationships: [],
      }),
      'context',
      'test-mutation',
    );

    const tx = {
      reason: 'rebase',
      mutationID: 1,
      [zeroData]: {},
    };
    await mutator(tx as unknown as WriteTransaction, {});
    expect(mt.size).toBe(0);
  });

  function mutationPatchToDiffOp(p: MutationPatch): DiffOperation<string> {
    switch (p.op) {
      case 'put':
        return {
          op: 'add',
          key: toMutationResponseKey(p.mutation.id),
          newValue: p.mutation.result,
        };
      case 'del':
        return {
          op: 'del',
          key: toMutationResponseKey(p.id),
          oldValue: null, // fine for tests
        };
      default:
        unreachable();
    }
  }

  test('mutation responses, received via poke, are processed', async () => {
    const ackMutations = vi.fn();
    const onFatalError = vi.fn();

    let cb: ((diffs: NoIndexDiff) => void) | undefined;
    const watch = (wcb: (diffs: NoIndexDiff) => void) => {
      cb = wcb;
      return () => {
        cb = undefined;
      };
    };
    const tracker = new MutationTracker(lc, ackMutations, onFatalError);
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

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
          result: {error: 'app', message: 'server error'},
        },
      },
    ];

    // process mutations
    cb!(patches.map(p => mutationPatchToDiffOp(p)));

    await expect(mutation2.serverPromise).rejects.toMatchObject({
      name: 'ApplicationError',
      message: 'server error',
      details: undefined,
    });

    tracker.lmidAdvanced(2);

    expect(ackMutations).toHaveBeenCalledOnce();
    expect(ackMutations).toHaveBeenCalledWith({
      clientID: CLIENT_ID,
      id: 2,
    });
    expect(onFatalError).not.toHaveBeenCalled();
  });

  test('tracked mutations are resolved on reconnect', async () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

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
    expect(onFatalError).not.toHaveBeenCalled();
  });

  test('notified whenever the outstanding mutation count goes to 0', () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

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
    } catch (_e) {
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
          result: {error: 'app', message: 'server error'},
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
    expect(onFatalError).not.toHaveBeenCalled();
  });

  test('mutations can be rejected before a mutation id is assigned', async () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

    const {ephemeralID, serverPromise} = tracker.trackMutation();
    tracker.rejectMutation(ephemeralID, new Error('test error'));

    await expect(serverPromise).rejects.toMatchObject({
      name: 'ApplicationError',
      message: 'test error',
      details: undefined,
    });
    expect(tracker.size).toBe(0);
    expect(onFatalError).not.toHaveBeenCalled();
  });

  test('mutations can be rejected locally with an unwrapped application error', async () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

    const appError = new ApplicationError('test app error', {
      details: {location: 'client'},
    });

    const {ephemeralID, serverPromise} = tracker.trackMutation();
    tracker.rejectMutation(ephemeralID, appError);

    await expect(serverPromise).rejects.toMatchObject({
      name: 'ApplicationError',
      message: 'test app error',
      details: {location: 'client'},
    });
    expect(tracker.size).toBe(0);
    expect(onFatalError).not.toHaveBeenCalled();
  });

  test('trying to resolve a mutation with an unassigned ephemeral id does not throw', () => {
    const {tracker} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

    tracker.trackMutation();
    const response: PushResponse = {
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 1},
          result: {},
        },
      ],
    };
    expect(() => tracker.processPushResponse(response)).not.toThrow();
  });

  test('trying to reject a mutation with an a unassigned ephemeral id does not throw', () => {
    const {tracker} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

    tracker.trackMutation();
    const response: PushResponse = {
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 1},
          result: {error: 'app', message: 'server error'},
        },
      ],
    };
    expect(() => tracker.processPushResponse(response)).not.toThrow();
  });

  test('resolves pending mutation when alreadyProcessed error received', async () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

    const {ephemeralID, serverPromise} = tracker.trackMutation();
    tracker.mutationIDAssigned(ephemeralID, 1);

    tracker.processPushResponse({
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 1},
          result: {
            error: 'alreadyProcessed',
          },
        },
      ],
    });

    const result = await serverPromise;
    expect(result.type).toBe('success');
    expect(tracker.size).toBe(0);
    expect(onFatalError).not.toHaveBeenCalled();
  });

  test('rejectAllOutstandingMutations rejects pending mutations and notifies listeners', async () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

    let notifications = 0;
    tracker.onAllMutationsApplied(() => {
      notifications++;
    });

    const {ephemeralID: id1, serverPromise: promise1} = tracker.trackMutation();
    tracker.mutationIDAssigned(id1, 1);

    const {ephemeralID: id2, serverPromise: promise2} = tracker.trackMutation();
    tracker.mutationIDAssigned(id2, 2);

    const rejection = new ClientError({
      kind: ClientErrorKind.Offline,
      message: 'offline',
    });

    tracker.rejectAllOutstandingMutations(rejection);

    await expect(promise1).rejects.toMatchObject({
      name: 'ClientError',
      message: 'offline',
      errorBody: {
        kind: ClientErrorKind.Offline,
        message: 'offline',
        origin: ErrorOrigin.Client,
      },
    });
    await expect(promise2).rejects.toMatchObject({
      name: 'ClientError',
      message: 'offline',
      errorBody: {
        kind: ClientErrorKind.Offline,
        message: 'offline',
        origin: ErrorOrigin.Client,
      },
    });
    expect(tracker.size).toBe(0);
    expect(notifications).toBe(1);
    expect(onFatalError).not.toHaveBeenCalled();
  });

  test('handles MutationOk delivered after outstanding mutations rejected', async () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

    const {ephemeralID, serverPromise} = tracker.trackMutation();
    tracker.mutationIDAssigned(ephemeralID, 1);

    const rejection = new ClientError({
      kind: ClientErrorKind.Offline,
      message: 'offline',
    });
    tracker.rejectAllOutstandingMutations(rejection);

    await expect(serverPromise).rejects.toMatchObject({
      name: 'ClientError',
      message: 'offline',
      errorBody: {
        kind: ClientErrorKind.Offline,
        message: 'offline',
        origin: ErrorOrigin.Client,
      },
    });

    expect(() =>
      tracker.processPushResponse({
        mutations: [
          {
            id: {clientID: CLIENT_ID, id: 1},
            result: {},
          },
        ],
      }),
    ).not.toThrow();

    expect(onFatalError).not.toHaveBeenCalled();
  });

  test('ignores alreadyProcessed duplicate results after resolving once', () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

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

    // other errors should not throw - we should print a log and ignore them
    expect(() =>
      tracker.processPushResponse({
        mutations: [
          {
            id: {clientID: CLIENT_ID, id: 1},
            result: {
              error: 'app',
              message: 'server error',
            },
          },
        ],
      }),
    ).not.toThrow();
    expect(onFatalError).not.toHaveBeenCalled();
  });

  test('advancing lmid past outstanding lmid notifies "all mutations applied" listeners', () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

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
    expect(onFatalError).not.toHaveBeenCalled();
  });

  test('advancing lmid clears limbo mutations up to that lmid', async () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

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

    // Fatal error callback should be called for http errors
    expect(onFatalError).toHaveBeenCalledOnce();
    expect(onFatalError).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'ProtocolError',
        errorBody: expect.objectContaining({
          kind: ErrorKind.PushFailed,
          message:
            'Fetch from API server returned non-OK status 500: Internal Server Error',
        }),
      }),
    );

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
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

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

    // Fatal error callback should be called for http errors
    expect(onFatalError).toHaveBeenCalledOnce();
    expect(onFatalError).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'ProtocolError',
        errorBody: expect.objectContaining({
          kind: ErrorKind.PushFailed,
          message:
            'Fetch from API server returned non-OK status 500: Internal Server Error',
        }),
      }),
    );

    let mutation3Resolved = false;
    void mutation3.serverPromise.finally(() => {
      mutation3Resolved = true;
    });
    await Promise.all([mutation1.serverPromise, mutation2.serverPromise]);
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(mutation3Resolved).toBe(false);
    // No application errors - mutations resolved via lmid advance after fatal error
  });

  test('reconnecting puts outstanding mutations in limbo', async () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

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
    expect(onFatalError).not.toHaveBeenCalled();
  });

  test('advancing lmid does resolve all mutations before that lmid', () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

    const mutation1 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation1.ephemeralID, 1);
    const mutation2 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation2.ephemeralID, 2);
    const mutation3 = tracker.trackMutation();
    tracker.mutationIDAssigned(mutation3.ephemeralID, 3);

    tracker.lmidAdvanced(5);

    expect(tracker.size).toBe(0);
    expect(onFatalError).not.toHaveBeenCalled();
  });

  describe('fatal error handling', () => {
    test('calls onFatalError for unsupportedSchemaVersion', () => {
      const {tracker, onFatalError} = createTracker();
      tracker.setClientIDAndWatch(CLIENT_ID, watch);

      tracker.processPushResponse({
        error: 'unsupportedSchemaVersion',
        mutationIDs: [],
      });

      expect(onFatalError).toHaveBeenCalledOnce();
      expect(onFatalError).toHaveBeenCalledWith(
        expect.objectContaining({
          name: 'ProtocolError',
          errorBody: expect.objectContaining({
            kind: ErrorKind.PushFailed,
            message: 'Unsupported schema version',
          }),
        }),
      );
    });

    test('calls onFatalError for http error', () => {
      const {tracker, onFatalError} = createTracker();
      tracker.setClientIDAndWatch(CLIENT_ID, watch);

      tracker.processPushResponse({
        error: 'http',
        status: 404,
        details: 'Not Found',
        mutationIDs: [],
      });

      expect(onFatalError).toHaveBeenCalledOnce();
      expect(onFatalError).toHaveBeenCalledWith(
        expect.objectContaining({
          name: 'ProtocolError',
          errorBody: expect.objectContaining({
            kind: ErrorKind.PushFailed,
            message:
              'Fetch from API server returned non-OK status 404: Not Found',
          }),
        }),
      );
    });

    test('calls onFatalError for zeroPusher error', () => {
      const {tracker, onFatalError} = createTracker();
      tracker.setClientIDAndWatch(CLIENT_ID, watch);

      tracker.processPushResponse({
        error: 'zeroPusher',
        details: 'Custom error message',
        mutationIDs: [],
      });

      expect(onFatalError).toHaveBeenCalledOnce();
      expect(onFatalError).toHaveBeenCalledWith(
        expect.objectContaining({
          name: 'ProtocolError',
          errorBody: expect.objectContaining({
            kind: ErrorKind.PushFailed,
            message: 'ZeroPusher error: Custom error message',
          }),
        }),
      );
    });
  });

  test('emits fatal error for unsupportedPushVersion', () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

    const response: PushResponseBody = {
      error: 'unsupportedPushVersion',
      mutationIDs: [{clientID: CLIENT_ID, id: 1}],
    };

    tracker.processPushResponse(response);

    expect(onFatalError).toHaveBeenCalledTimes(1);
    const fatalError = onFatalError.mock.calls[0][0];
    expect(fatalError).toBeInstanceOf(ProtocolError);
    assert(isServerError(fatalError));
    expect(fatalError.kind).toBe(ErrorKind.PushFailed);
    expect(fatalError.message).toContain('Unsupported push version');
  });

  test('emits fatal error for unsupportedSchemaVersion', () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

    const response: PushResponseBody = {
      error: 'unsupportedSchemaVersion',
      mutationIDs: [{clientID: CLIENT_ID, id: 1}],
    };

    tracker.processPushResponse(response);

    expect(onFatalError).toHaveBeenCalledTimes(1);
    const fatalError = onFatalError.mock.calls[0][0];
    expect(fatalError).toBeInstanceOf(ProtocolError);
    assert(isServerError(fatalError));
    expect(fatalError.kind).toBe(ErrorKind.PushFailed);
    expect(fatalError.message).toContain('Unsupported schema version');
  });

  test('emits fatal error for zeroPusher error', () => {
    const {tracker, onFatalError} = createTracker();
    tracker.setClientIDAndWatch(CLIENT_ID, watch);

    const response: PushResponseBody = {
      error: 'zeroPusher',
      details: 'pusher failed',
      mutationIDs: [{clientID: CLIENT_ID, id: 1}],
    };

    tracker.processPushResponse(response);

    expect(onFatalError).toHaveBeenCalledTimes(1);
    const fatalError = onFatalError.mock.calls[0][0];
    expect(fatalError).toBeInstanceOf(ProtocolError);
    assert(isServerError(fatalError));
    expect(fatalError.kind).toBe(ErrorKind.PushFailed);
    expect(fatalError.message).toContain('ZeroPusher error');
    expect(fatalError.message).toContain('pusher failed');
  });
});
