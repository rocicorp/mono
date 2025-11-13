import {assert, describe, expect, test, vi} from 'vitest';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {
  ApplicationError,
  isApplicationError,
} from '../../../zero-protocol/src/application-error.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ProtocolError} from '../../../zero-protocol/src/error.ts';
import {ClientErrorKind} from './client-error-kind.ts';
import type {ConnectionManager, ConnectionState} from './connection-manager.ts';
import {ConnectionStatus} from './connection-status.ts';
import {ClientError} from './error.ts';
import type {MutationTracker} from './mutation-tracker.ts';
import {MutatorProxy} from './mutator-proxy.ts';

function createMockConnectionManager(): {
  manager: ConnectionManager;
  mutationTracker: MutationTracker;
  rejectAllOutstandingMutations: ReturnType<typeof vi.fn>;
  stateCallback: (state: ConnectionState) => void;
} {
  let stateCallback: ((state: ConnectionState) => void) | undefined;

  const manager = {
    subscribe: vi.fn((cb: (state: ConnectionState) => void) => {
      stateCallback = cb;
      return () => {};
    }),
    state: {
      name: ConnectionStatus.Connected,
    } as ConnectionState,
  } as unknown as ConnectionManager;

  const rejectAllOutstandingMutations = vi.fn();
  const mutationTracker = {
    rejectAllOutstandingMutations,
  } as unknown as MutationTracker;

  return {
    manager,
    mutationTracker,
    rejectAllOutstandingMutations,
    stateCallback: (state: ConnectionState) => {
      if (stateCallback) {
        stateCallback(state);
      }
    },
  };
}

describe('MutatorProxy', () => {
  test('subscribes to connection manager on construction', () => {
    const {manager, mutationTracker} = createMockConnectionManager();
    const onApplicationError = vi.fn();

    new MutatorProxy(manager, mutationTracker, onApplicationError);

    expect(manager.subscribe).toHaveBeenCalledWith(expect.any(Function));
  });

  test('mutationRejectionError is initially undefined', () => {
    const {manager, mutationTracker} = createMockConnectionManager();
    const onApplicationError = vi.fn();

    const proxy = new MutatorProxy(
      manager,
      mutationTracker,
      onApplicationError,
    );

    expect(proxy.mutationRejectionError).toBeUndefined();
  });

  describe('connection state changes', () => {
    test('sets rejection error and rejects mutations on Disconnected', () => {
      const {
        manager,
        mutationTracker,
        rejectAllOutstandingMutations,
        stateCallback,
      } = createMockConnectionManager();
      const onApplicationError = vi.fn();
      const proxy = new MutatorProxy(
        manager,
        mutationTracker,
        onApplicationError,
      );

      const error = new ClientError({
        kind: ClientErrorKind.Offline,
        message: 'offline',
      });
      const state: ConnectionState = {
        name: ConnectionStatus.Disconnected,
        reason: error,
      };

      stateCallback(state);

      expect(proxy.mutationRejectionError).toBe(error);
      expect(rejectAllOutstandingMutations).toHaveBeenCalledWith(error);
      expect(rejectAllOutstandingMutations).toHaveBeenCalledTimes(1);
    });

    test('sets rejection error and rejects mutations on Error', () => {
      const {
        manager,
        mutationTracker,
        rejectAllOutstandingMutations,
        stateCallback,
      } = createMockConnectionManager();
      const onApplicationError = vi.fn();
      const proxy = new MutatorProxy(
        manager,
        mutationTracker,
        onApplicationError,
      );

      const error = new ClientError({
        kind: ClientErrorKind.Internal,
        message: 'internal error',
      });
      const state: ConnectionState = {
        name: ConnectionStatus.Error,
        reason: error,
      };

      stateCallback(state);

      expect(proxy.mutationRejectionError).toBe(error);
      expect(rejectAllOutstandingMutations).toHaveBeenCalledWith(error);
      expect(rejectAllOutstandingMutations).toHaveBeenCalledTimes(1);
    });

    test('sets rejection error and rejects mutations on Closed', () => {
      const {
        manager,
        mutationTracker,
        rejectAllOutstandingMutations,
        stateCallback,
      } = createMockConnectionManager();
      const onApplicationError = vi.fn();
      const proxy = new MutatorProxy(
        manager,
        mutationTracker,
        onApplicationError,
      );

      const error = new ClientError({
        kind: ClientErrorKind.ClientClosed,
        message: 'client closed',
      });
      const state: ConnectionState = {
        name: ConnectionStatus.Closed,
        reason: error,
      };

      stateCallback(state);

      expect(proxy.mutationRejectionError).toBe(error);
      expect(rejectAllOutstandingMutations).toHaveBeenCalledWith(error);
      expect(rejectAllOutstandingMutations).toHaveBeenCalledTimes(1);
    });

    test('clears rejection error on Connected', () => {
      const {
        manager,
        mutationTracker,
        rejectAllOutstandingMutations,
        stateCallback,
      } = createMockConnectionManager();
      const onApplicationError = vi.fn();
      const proxy = new MutatorProxy(
        manager,
        mutationTracker,
        onApplicationError,
      );

      // First set an error
      const error = new ClientError({
        kind: ClientErrorKind.Offline,
        message: 'offline',
      });
      stateCallback({
        name: ConnectionStatus.Disconnected,
        reason: error,
      });

      expect(proxy.mutationRejectionError).toBe(error);

      // Then transition to connected
      stateCallback({name: ConnectionStatus.Connected});

      expect(proxy.mutationRejectionError).toBeUndefined();
      expect(rejectAllOutstandingMutations).toHaveBeenCalledTimes(1); // Only called once, not on connected
    });

    test('clears rejection error on Connecting', () => {
      const {
        manager,
        mutationTracker,
        rejectAllOutstandingMutations,
        stateCallback,
      } = createMockConnectionManager();
      const onApplicationError = vi.fn();
      const proxy = new MutatorProxy(
        manager,
        mutationTracker,
        onApplicationError,
      );

      // First set an error
      const error = new ClientError({
        kind: ClientErrorKind.Offline,
        message: 'offline',
      });
      stateCallback({
        name: ConnectionStatus.Disconnected,
        reason: error,
      });

      expect(proxy.mutationRejectionError).toBe(error);

      // Then transition to connecting
      stateCallback({
        name: ConnectionStatus.Connecting,
        attempt: 1,
        disconnectAt: Date.now() + 5000,
      });

      expect(proxy.mutationRejectionError).toBeUndefined();
      expect(rejectAllOutstandingMutations).toHaveBeenCalledTimes(1); // Only called once, not on connecting
    });

    test('clears rejection error on NeedsAuth', () => {
      const {
        manager,
        mutationTracker,
        rejectAllOutstandingMutations,
        stateCallback,
      } = createMockConnectionManager();
      const onApplicationError = vi.fn();
      const proxy = new MutatorProxy(
        manager,
        mutationTracker,
        onApplicationError,
      );

      // First set an error
      const error = new ClientError({
        kind: ClientErrorKind.Offline,
        message: 'offline',
      });
      stateCallback({
        name: ConnectionStatus.Disconnected,
        reason: error,
      });

      expect(proxy.mutationRejectionError).toBe(error);

      // Then transition to needs auth
      const authError = new ProtocolError({
        kind: ErrorKind.Unauthorized,
        origin: ErrorOrigin.Server,
        message: 'unauthorized',
      });
      stateCallback({
        name: ConnectionStatus.NeedsAuth,
        reason: authError,
      });

      expect(proxy.mutationRejectionError).toBeUndefined();
      expect(rejectAllOutstandingMutations).toHaveBeenCalledTimes(1); // Only called once, not on needs auth
    });
  });

  describe('wrapCustomMutator', () => {
    test('returns zero error when mutation rejection error is set', async () => {
      const {manager, mutationTracker, stateCallback} =
        createMockConnectionManager();
      const onApplicationError = vi.fn();
      const proxy = new MutatorProxy(
        manager,
        mutationTracker,
        onApplicationError,
      );

      // Set a rejection error
      const error = new ClientError({
        kind: ClientErrorKind.Offline,
        message: 'offline',
      });
      stateCallback({
        name: ConnectionStatus.Disconnected,
        reason: error,
      });

      // Create a wrapped mutator
      const mutator = vi.fn(() => ({
        client: Promise.resolve(),
        server: Promise.resolve(),
      }));
      const wrapped = proxy.wrapCustomMutator(mutator);

      // Call the wrapped mutator
      const result = wrapped();

      // Mutator should not have been called
      expect(mutator).not.toHaveBeenCalled();

      // Both promises should resolve with zero error
      const clientResult = await result.client;
      const serverResult = await result.server;

      expect(clientResult).toEqual({
        type: 'error',
        error: {
          type: 'zero',
          message: 'offline',
          details: {
            kind: ClientErrorKind.Offline,
            origin: ErrorOrigin.Client,
          },
        },
      });

      expect(serverResult).toEqual({
        type: 'error',
        error: {
          type: 'zero',
          message: 'offline',
          details: {
            kind: ClientErrorKind.Offline,
            origin: ErrorOrigin.Client,
          },
        },
      });
      // the errors were zero errors, not app errors
      expect(onApplicationError).not.toHaveBeenCalled();
    });

    test('returns app error when mutator throws synchronously', async () => {
      const {manager, mutationTracker} = createMockConnectionManager();
      const onApplicationError = vi.fn();
      const proxy = new MutatorProxy(
        manager,
        mutationTracker,
        onApplicationError,
      );

      const thrownError = new Error('mutator failed');
      const mutator = vi.fn(() => {
        throw thrownError;
      });
      const wrapped = proxy.wrapCustomMutator(mutator);

      const result = wrapped();

      const clientResult = await result.client;
      const serverResult = await result.server;

      expect(clientResult.type).toBe('error');
      expect(clientResult).toEqual({
        type: 'error',
        error: {
          type: 'app',
          message: 'mutator failed',
          details: undefined,
        },
      });

      expect(serverResult).toEqual(clientResult);
      expect(mutator).toHaveBeenCalledTimes(1);

      // onApplicationError should be called once
      expect(onApplicationError).toHaveBeenCalledTimes(1);
      expect(onApplicationError).toHaveBeenCalledWith(
        expect.objectContaining({
          message: 'mutator failed',
        }),
      );
      expect(isApplicationError(onApplicationError.mock.calls[0][0])).toBe(
        true,
      );
    });

    test('returns success when mutator succeeds', async () => {
      const {manager, mutationTracker} = createMockConnectionManager();
      const onApplicationError = vi.fn();
      const proxy = new MutatorProxy(
        manager,
        mutationTracker,
        onApplicationError,
      );

      const mutator = vi.fn(() => ({
        client: Promise.resolve(),
        server: Promise.resolve(),
      }));
      const wrapped = proxy.wrapCustomMutator(mutator);

      const result = wrapped();

      const clientResult = await result.client;
      const serverResult = await result.server;

      expect(clientResult).toEqual({type: 'success'});
      expect(serverResult).toEqual({type: 'success'});
      expect(mutator).toHaveBeenCalledTimes(1);

      // onApplicationError should not be called on success
      expect(onApplicationError).not.toHaveBeenCalled();
    });

    test('wraps client promise rejection as app error', async () => {
      const {manager, mutationTracker} = createMockConnectionManager();
      const onApplicationError = vi.fn();
      const proxy = new MutatorProxy(
        manager,
        mutationTracker,
        onApplicationError,
      );

      const clientError = new Error('client failed');
      const mutator = vi.fn(() => ({
        client: Promise.reject(clientError),
        server: Promise.resolve(),
      }));
      const wrapped = proxy.wrapCustomMutator(mutator);

      const result = wrapped();

      const clientResult = await result.client;
      const serverResult = await result.server;

      expect(clientResult.type).toBe('error');
      expect(clientResult).toEqual({
        type: 'error',
        error: {
          type: 'app',
          message: 'client failed',
          details: undefined,
        },
      });

      expect(serverResult).toEqual({type: 'success'});
      expect(mutator).toHaveBeenCalledTimes(1);

      // onApplicationError should be called once
      expect(onApplicationError).toHaveBeenCalledTimes(1);
      expect(onApplicationError).toHaveBeenCalledWith(
        expect.objectContaining({
          message: 'client failed',
        }),
      );
      expect(isApplicationError(onApplicationError.mock.calls[0][0])).toBe(
        true,
      );
    });

    test('wraps server promise rejection as app error', async () => {
      const {manager, mutationTracker} = createMockConnectionManager();
      const onApplicationError = vi.fn();
      const proxy = new MutatorProxy(
        manager,
        mutationTracker,
        onApplicationError,
      );

      const serverError = new Error('server failed');
      const mutator = vi.fn(() => ({
        client: Promise.resolve(),
        server: Promise.reject(serverError),
      }));
      const wrapped = proxy.wrapCustomMutator(mutator);

      const result = wrapped();

      const clientResult = await result.client;
      const serverResult = await result.server;

      expect(clientResult).toEqual({type: 'success'});

      expect(serverResult.type).toBe('error');
      expect(serverResult).toEqual({
        type: 'error',
        error: {
          type: 'app',
          message: 'server failed',
          details: undefined,
        },
      });

      expect(mutator).toHaveBeenCalledTimes(1);

      // onApplicationError should be called once
      expect(onApplicationError).toHaveBeenCalledTimes(1);
      expect(onApplicationError).toHaveBeenCalledWith(
        expect.objectContaining({
          message: 'server failed',
        }),
      );
      expect(isApplicationError(onApplicationError.mock.calls[0][0])).toBe(
        true,
      );
    });

    test('wraps ApplicationError from server as app error', async () => {
      const {manager, mutationTracker} = createMockConnectionManager();
      const onApplicationError = vi.fn();
      const proxy = new MutatorProxy(
        manager,
        mutationTracker,
        onApplicationError,
      );

      const appError = new ApplicationError('validation error', {
        details: {code: 'INVALID_INPUT'},
      });
      const mutator = vi.fn(() => ({
        client: Promise.resolve(),
        server: Promise.reject(appError),
      }));
      const wrapped = proxy.wrapCustomMutator(mutator);

      const result = wrapped();
      const serverResult = await result.server;

      expect(serverResult.type).toBe('error');

      assert(serverResult.type === 'error');
      assert(serverResult.error.type === 'app');

      expect(serverResult.error.message).toBe('validation error');
      expect(serverResult.error.details).toEqual({
        code: 'INVALID_INPUT',
      });

      // onApplicationError should be called once
      expect(onApplicationError).toHaveBeenCalledTimes(1);
      expect(onApplicationError).toHaveBeenCalledWith(appError);
      expect(isApplicationError(onApplicationError.mock.calls[0][0])).toBe(
        true,
      );
    });

    test('forwards args to wrapped mutator', () => {
      const {manager, mutationTracker} = createMockConnectionManager();
      const onApplicationError = vi.fn();
      const proxy = new MutatorProxy(
        manager,
        mutationTracker,
        onApplicationError,
      );

      const mutator = vi.fn((..._args: [] | [ReadonlyJSONValue]) => ({
        client: Promise.resolve(),
        server: Promise.resolve(),
      }));
      const wrapped = proxy.wrapCustomMutator(mutator);

      const args = {userId: '123', action: 'update'};
      wrapped(args);

      expect(mutator).toHaveBeenCalledWith(args);
    });

    test('notifies once when both client and server reject', async () => {
      const {manager, mutationTracker} = createMockConnectionManager();
      const onApplicationError = vi.fn();
      const proxy = new MutatorProxy(
        manager,
        mutationTracker,
        onApplicationError,
      );

      const mutator = vi.fn(() => ({
        client: Promise.reject(new Error('client error')),
        server: new Promise<unknown>((_, reject) => {
          setTimeout(() => reject(new Error('client error')), 0);
        }),
      }));
      const wrapped = proxy.wrapCustomMutator(mutator);

      const result = wrapped();

      const clientResult = await result.client;
      const serverResult = await result.server;

      expect(clientResult).toEqual({
        type: 'error',
        error: {
          type: 'app',
          message: 'client error',
          details: undefined,
        },
      });

      expect(serverResult).toEqual({
        type: 'error',
        error: {
          type: 'app',
          message: 'client error',
          details: undefined,
        },
      });

      // onApplicationError should be called once per failing mutation
      expect(onApplicationError).toHaveBeenCalledTimes(1);
      expect(onApplicationError).toHaveBeenCalledWith(
        expect.objectContaining({
          message: 'client error',
        }),
      );
      expect(isApplicationError(onApplicationError.mock.calls[0][0])).toBe(
        true,
      );
    });

    test('returns already-wrapped success result from client promise', async () => {
      const {manager, mutationTracker} = createMockConnectionManager();
      const onApplicationError = vi.fn();
      const proxy = new MutatorProxy(
        manager,
        mutationTracker,
        onApplicationError,
      );

      const successResult = {type: 'success' as const};
      const mutator = vi.fn(() => ({
        client: Promise.resolve(successResult),
        server: Promise.resolve(),
      }));
      const wrapped = proxy.wrapCustomMutator(mutator);

      const result = wrapped();
      const clientResult = await result.client;

      expect(clientResult).toEqual({type: 'success'});
      expect(mutator).toHaveBeenCalledTimes(1);

      // onApplicationError should not be called on success
      expect(onApplicationError).not.toHaveBeenCalled();
    });

    test('wraps rejection from server promise as app error', async () => {
      const {manager, mutationTracker} = createMockConnectionManager();
      const onApplicationError = vi.fn();
      const proxy = new MutatorProxy(
        manager,
        mutationTracker,
        onApplicationError,
      );

      const clientError = new ApplicationError('client error');
      const serverError = new ApplicationError('server error');

      const mutator = vi.fn(() => ({
        client: Promise.reject(clientError),
        server: new Promise((_, reject) => {
          setTimeout(() => reject(serverError), 1);
        }),
      }));
      const wrapped = proxy.wrapCustomMutator(mutator);

      const result = wrapped();
      const serverResult = await result.server;

      expect(serverResult).toEqual({
        type: 'error',
        error: {
          type: 'app',
          message: serverError.message,
          details: serverError.details,
        },
      });
      expect(mutator).toHaveBeenCalledTimes(1);
      // onApplicationError should be called with the client error
      // because the client error was thrown first
      expect(onApplicationError).toHaveBeenCalledExactlyOnceWith(clientError);
    });

    test('handles non-object result as success', async () => {
      const {manager, mutationTracker} = createMockConnectionManager();
      const onApplicationError = vi.fn();
      const proxy = new MutatorProxy(
        manager,
        mutationTracker,
        onApplicationError,
      );

      const mutator = vi.fn(() => ({
        client: Promise.resolve(null),
        server: Promise.resolve(undefined),
      }));
      const wrapped = proxy.wrapCustomMutator(mutator);

      const result = wrapped();
      const clientResult = await result.client;
      const serverResult = await result.server;

      expect(clientResult).toEqual({type: 'success'});
      expect(serverResult).toEqual({type: 'success'});
      expect(mutator).toHaveBeenCalledTimes(1);

      expect(onApplicationError).not.toHaveBeenCalled();
    });

    test('handles object without type key as success', async () => {
      const {manager, mutationTracker} = createMockConnectionManager();
      const onApplicationError = vi.fn();
      const proxy = new MutatorProxy(
        manager,
        mutationTracker,
        onApplicationError,
      );

      const mutator = vi.fn(() => ({
        client: Promise.resolve({foo: 'bar'}),
        server: Promise.resolve({baz: 123}),
      }));
      const wrapped = proxy.wrapCustomMutator(mutator);

      const result = wrapped();
      const clientResult = await result.client;
      const serverResult = await result.server;

      expect(clientResult).toEqual({type: 'success'});
      expect(serverResult).toEqual({type: 'success'});
      expect(mutator).toHaveBeenCalledTimes(1);

      expect(onApplicationError).not.toHaveBeenCalled();
    });
  });
});
