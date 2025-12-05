import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ProtocolError} from '../../../zero-protocol/src/error.ts';
import {ClientErrorKind} from './client-error-kind.ts';
import {
  ConnectionManager,
  throwIfConnectionError,
  type ConnectionManagerState,
} from './connection-manager.ts';
import {ConnectionStatus} from './connection-status.ts';
import {ClientError, type AuthError} from './error.ts';

const DEFAULT_TIMEOUT_MS = 60 * 1000;

const sharedDisconnectError = new ClientError({
  kind: ClientErrorKind.Offline,
  message: 'Disconnect timed out',
});

describe('ConnectionManager', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  const subscribe = (manager: ConnectionManager) => {
    const listener = vi.fn<(state: ConnectionManagerState) => void>();
    manager.subscribe(listener);
    return listener;
  };

  describe('constructor', () => {
    test('starts in connecting state with default timeout', () => {
      vi.setSystemTime(1_000);
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });

      expect(manager.state).toEqual({
        name: ConnectionStatus.Connecting,
        attempt: 0,
        disconnectAt: 1_000 + DEFAULT_TIMEOUT_MS,
      });
      expect(manager.shouldContinueRunLoop()).toBe(true);
    });

    test('respects custom disconnect timeout', () => {
      vi.setSystemTime(5_000);
      const manager = new ConnectionManager({disconnectTimeout: 10_000});

      expect(
        manager.state.name === ConnectionStatus.Connecting
          ? manager.state.disconnectAt
          : -1,
      ).toEqual(5_000 + 10_000);
    });
  });

  describe('connecting', () => {
    test('increments attempt and keeps disconnect deadline while already connecting', () => {
      vi.setSystemTime(2_500);
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      const listener = subscribe(manager);

      const reason = new ClientError({
        kind: ClientErrorKind.ConnectTimeout,
        message: 'Connect timed out',
      });

      vi.setSystemTime(6_000);
      manager.connecting(reason);

      expect(manager.state).toEqual({
        name: ConnectionStatus.Connecting,
        attempt: 1,
        disconnectAt: 2_500 + DEFAULT_TIMEOUT_MS,
        reason,
      });
      expect(listener).toHaveBeenCalledTimes(1);
    });

    test('starts a new session after the timer resets', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      manager.connected();

      vi.setSystemTime(42_000);
      manager.connecting();

      expect(manager.state).toEqual({
        name: ConnectionStatus.Connecting,
        attempt: 1,
        disconnectAt: 42_000 + DEFAULT_TIMEOUT_MS,
      });
    });

    test('does nothing while disconnected before the timeout expires', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });

      vi.setSystemTime(DEFAULT_TIMEOUT_MS / 2);
      manager.disconnected(sharedDisconnectError);
      expect(manager.is(ConnectionStatus.Disconnected)).toBe(true);

      const listener = subscribe(manager);
      vi.setSystemTime(DEFAULT_TIMEOUT_MS / 2 + 1_000);
      manager.connecting();

      expect(manager.is(ConnectionStatus.Disconnected)).toBe(true);
      expect(listener).not.toHaveBeenCalled();
    });

    test('does nothing when disconnected after timing out', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });

      vi.advanceTimersByTime(DEFAULT_TIMEOUT_MS + 100);
      expect(manager.is(ConnectionStatus.Disconnected)).toBe(true);

      const listener = subscribe(manager);
      vi.advanceTimersByTime(500);
      manager.connecting();

      expect(manager.is(ConnectionStatus.Disconnected)).toBe(true);
      expect(listener).not.toHaveBeenCalled();
    });

    test('does nothing once closed', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      manager.closed();

      const listener = subscribe(manager);
      manager.connecting();

      expect(manager.is(ConnectionStatus.Closed)).toBe(true);
      expect(listener).not.toHaveBeenCalled();
    });

    test('does not create multiple timeout intervals when called rapidly', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: 1_000,
        timeoutCheckIntervalMs: 100,
      });
      const listener = subscribe(manager);

      // Call connecting multiple times rapidly
      manager.connecting(
        new ClientError({
          kind: ClientErrorKind.ConnectTimeout,
          message: 'Connect timed out',
        }),
      );
      manager.connecting(
        new ClientError({
          kind: ClientErrorKind.ConnectTimeout,
          message: 'Connect timed out',
        }),
      );
      manager.connecting(
        new ClientError({
          kind: ClientErrorKind.ConnectTimeout,
          message: 'Connect timed out',
        }),
      );

      // Verify we're still in connecting state
      expect(manager.is(ConnectionStatus.Connecting)).toBe(true);

      // Advance time to trigger timeout - should only fire once
      vi.advanceTimersByTime(1_100);

      // Should have transitioned to disconnected exactly once
      expect(manager.is(ConnectionStatus.Disconnected)).toBe(true);
      expect(
        listener.mock.calls.filter(
          call => call[0].name === ConnectionStatus.Disconnected,
        ).length,
      ).toBe(1);
    });
  });

  describe('connected', () => {
    test('transitions to connected and resets retry window', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      const listener = subscribe(manager);

      vi.setSystemTime(10_000);
      manager.connected();

      expect(manager.state).toEqual({name: ConnectionStatus.Connected});
      expect(listener).toHaveBeenCalledWith({name: ConnectionStatus.Connected});

      listener.mockClear();
      vi.setSystemTime(60_000);
      manager.connecting();

      expect(manager.state).toEqual({
        name: ConnectionStatus.Connecting,
        attempt: 1,
        disconnectAt: 60_000 + DEFAULT_TIMEOUT_MS,
      });
      expect(listener).toHaveBeenCalledTimes(1);
    });

    test('is no-op when already connected', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      manager.connected();

      const listener = subscribe(manager);
      manager.connected();

      expect(listener).not.toHaveBeenCalled();
    });

    test('is no-op when closed', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      manager.closed();

      const listener = subscribe(manager);
      manager.connected();

      expect(listener).not.toHaveBeenCalled();
      expect(manager.is(ConnectionStatus.Closed)).toBe(true);
    });

    test('safely handles stopping timeout interval multiple times', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
        timeoutCheckIntervalMs: 100,
      });

      // First connected() stops the interval
      manager.connected();
      expect(manager.is(ConnectionStatus.Connected)).toBe(true);

      // Second connected() should safely handle already-stopped interval
      const listener = subscribe(manager);
      manager.connected();

      expect(listener).not.toHaveBeenCalled();
      expect(manager.is(ConnectionStatus.Connected)).toBe(true);
    });
  });

  describe('disconnected', () => {
    test('transitions to disconnected from connecting', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      const listener = subscribe(manager);

      manager.disconnected(sharedDisconnectError);

      expect(manager.state).toEqual({
        name: ConnectionStatus.Disconnected,
        reason: sharedDisconnectError,
      });
      expect(listener).toHaveBeenCalledWith({
        name: ConnectionStatus.Disconnected,
        reason: sharedDisconnectError,
      });
    });

    test('stays disconnected after leaving connected', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });

      vi.setSystemTime(2 * 60 * 1000);
      manager.connected();

      vi.setSystemTime(8 * 60 * 1000);
      manager.disconnected(sharedDisconnectError);
      expect(manager.is(ConnectionStatus.Disconnected)).toBe(true);

      vi.setSystemTime(8 * 60 * 1000);
      manager.connecting();

      expect(manager.is(ConnectionStatus.Disconnected)).toBe(true);
    });

    test('is no-op when already disconnected', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      manager.disconnected(sharedDisconnectError);

      const listener = subscribe(manager);
      manager.disconnected(sharedDisconnectError);

      expect(listener).not.toHaveBeenCalled();
    });

    test('is no-op when closed', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      manager.closed();

      const listener = subscribe(manager);
      manager.disconnected(sharedDisconnectError);

      expect(listener).not.toHaveBeenCalled();
      expect(manager.is(ConnectionStatus.Closed)).toBe(true);
    });
  });

  describe('closed', () => {
    test('transitions to closed and blocks further updates', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      const listener = subscribe(manager);

      manager.closed();

      expect(manager.state.name).toEqual(ConnectionStatus.Closed);
      assert(manager.state.name === ConnectionStatus.Closed);
      expect(manager.state.reason.kind).toEqual(ClientErrorKind.ClientClosed);
      expect(manager.shouldContinueRunLoop()).toBe(false);
      expect(listener).toHaveBeenCalledWith({
        name: ConnectionStatus.Closed,
        reason: new ClientError({
          kind: ClientErrorKind.ClientClosed,
          message: 'Zero was explicitly closed by calling zero.close()',
        }),
      });

      listener.mockClear();
      manager.connecting();
      manager.connected();
      manager.disconnected(sharedDisconnectError);

      expect(manager.state.name).toEqual(ConnectionStatus.Closed);
      assert(manager.state.name === ConnectionStatus.Closed);
      expect(manager.state.reason.kind).toEqual(ClientErrorKind.ClientClosed);
      expect(listener).not.toHaveBeenCalled();
    });

    test('is no-op when already closed', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      manager.closed();

      const listener = subscribe(manager);
      manager.closed();

      expect(listener).not.toHaveBeenCalled();
      expect(manager.is(ConnectionStatus.Closed)).toBe(true);
    });
  });

  describe('checkTimeout', () => {
    test('automatically disconnects once the interval detects timeout', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: 1_000,
        timeoutCheckIntervalMs: 100,
      });
      const listener = subscribe(manager);

      vi.advanceTimersByTime(900);
      expect(manager.is(ConnectionStatus.Connecting)).toBe(true);
      expect(listener).not.toHaveBeenCalledWith({
        name: ConnectionStatus.Disconnected,
      });

      vi.advanceTimersByTime(200);
      expect(manager.is(ConnectionStatus.Disconnected)).toBe(true);
      expect(listener).toHaveBeenCalledWith(
        expect.objectContaining({
          name: ConnectionStatus.Disconnected,
          reason: expect.any(ClientError),
        }),
      );
    });

    test('uses the configured timeout check interval', () => {
      const setIntervalSpy = vi.spyOn(globalThis, 'setInterval');

      const manager = new ConnectionManager({
        disconnectTimeout: 5_000,
        timeoutCheckIntervalMs: 2_500,
      });

      try {
        expect(setIntervalSpy).toHaveBeenCalledTimes(1);
        expect(setIntervalSpy).toHaveBeenCalledWith(
          expect.any(Function),
          2_500,
        );
      } finally {
        manager.closed();
        setIntervalSpy.mockRestore();
      }
    });

    test('stops checking timeouts after disconnecting', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: 1_000,
        timeoutCheckIntervalMs: 100,
      });
      const listener = subscribe(manager);

      manager.disconnected(sharedDisconnectError);
      listener.mockClear();

      vi.advanceTimersByTime(1_000);

      expect(listener).not.toHaveBeenCalled();
    });

    test('timeout check handles being in non-connecting state gracefully', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: 1_000,
        timeoutCheckIntervalMs: 100,
      });
      const listener = subscribe(manager);

      // Let the timeout happen naturally (transitions to Disconnected)
      vi.advanceTimersByTime(1_100);
      expect(manager.is(ConnectionStatus.Disconnected)).toBe(true);

      listener.mockClear();

      // Advance more time to let the interval fire again
      // The checkTimeout should see we're not in Connecting state and do nothing
      vi.advanceTimersByTime(1_000);

      // Should still be disconnected, no additional state changes
      expect(manager.is(ConnectionStatus.Disconnected)).toBe(true);
      expect(listener).not.toHaveBeenCalled();
    });
  });

  describe('shouldContinueRunLoop', () => {
    test('is true until the manager is closed', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      expect(manager.shouldContinueRunLoop()).toBe(true);

      manager.connecting();
      expect(manager.shouldContinueRunLoop()).toBe(true);

      manager.connected();
      expect(manager.shouldContinueRunLoop()).toBe(true);

      manager.disconnected(sharedDisconnectError);
      expect(manager.shouldContinueRunLoop()).toBe(true);

      manager.closed();
      expect(manager.shouldContinueRunLoop()).toBe(false);
    });
  });

  describe('state transition cycle', () => {
    test('handles cycle through disconnected: Connecting → Connected → Disconnected → Connected', () => {
      vi.setSystemTime(1_000);
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      const listener = subscribe(manager);

      expect(manager.state).toEqual({
        name: ConnectionStatus.Connecting,
        attempt: 0,
        disconnectAt: 1_000 + DEFAULT_TIMEOUT_MS,
      });

      // First connection succeeds
      vi.setSystemTime(5_000);
      manager.connected();
      expect(manager.state).toEqual({name: ConnectionStatus.Connected});

      // Connection lost, transition to disconnected
      vi.setSystemTime(60_000);
      manager.disconnected(sharedDisconnectError);
      expect(manager.state).toEqual({
        name: ConnectionStatus.Disconnected,
        reason: sharedDisconnectError,
      });

      // Reconnection succeeds (can transition directly from disconnected to connected)
      vi.setSystemTime(65_000);
      manager.connected();
      expect(manager.state).toEqual({name: ConnectionStatus.Connected});

      // Verify all transitions were notified
      expect(listener).toHaveBeenCalledTimes(3);
      expect(listener).toHaveBeenNthCalledWith(1, {
        name: ConnectionStatus.Connected,
      });
      expect(listener).toHaveBeenNthCalledWith(2, {
        name: ConnectionStatus.Disconnected,
        reason: sharedDisconnectError,
      });
      expect(listener).toHaveBeenNthCalledWith(3, {
        name: ConnectionStatus.Connected,
      });
    });
  });

  describe('state change promises', () => {
    test('waitForStateChange resolves on next transition', async () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });

      const waitForChange = manager.waitForStateChange();
      manager.connected();

      await expect(waitForChange).resolves.toEqual({
        name: ConnectionStatus.Connected,
      });
    });

    test('transition nextStatePromise resolves after subsequent state change', async () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });

      // Move to connected first so connecting() starts a fresh session
      manager.connected();
      const {nextStatePromise} = manager.connecting();
      manager.connected();

      await expect(nextStatePromise).resolves.toEqual({
        name: ConnectionStatus.Connected,
      });
    });

    test('transition nextStatePromise resolves on closed', async () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });

      const {nextStatePromise} = manager.connected();
      manager.closed();

      const nextState = await nextStatePromise;
      expect(nextState.name).toEqual(ConnectionStatus.Closed);
      assert(nextState.name === ConnectionStatus.Closed);
      expect(nextState.reason.kind).toEqual(ClientErrorKind.ClientClosed);
    });

    test('waitForStateChange reuses promise until resolved, then creates a new one', async () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });

      const first = manager.waitForStateChange();
      const second = manager.waitForStateChange();
      expect(second).toBe(first);

      let firstResolved = false;
      void first.then(() => {
        firstResolved = true;
      });
      await Promise.resolve();
      expect(firstResolved).toBe(false);

      manager.connected();
      await expect(first).resolves.toEqual({
        name: ConnectionStatus.Connected,
      });

      const third = manager.waitForStateChange();
      expect(third).not.toBe(first);

      manager.disconnected(sharedDisconnectError);
      await expect(third).resolves.toEqual({
        name: ConnectionStatus.Disconnected,
        reason: sharedDisconnectError,
      });
    });

    test('error nextStatePromise resolves when transitioning from connected to connecting', async () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });

      const {nextStatePromise} = manager.connected();
      manager.connecting();

      await expect(nextStatePromise).resolves.toEqual(
        expect.objectContaining({
          name: ConnectionStatus.Connecting,
          attempt: 1,
        }),
      );
    });

    test('error nextStatePromise resolves when transitioning out of error', async () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      const errorDetail = new ClientError({
        kind: ClientErrorKind.ConnectTimeout,
        message: 'connect timeout',
      });

      const {nextStatePromise} = manager.error(errorDetail);

      manager.connecting();

      await expect(nextStatePromise).resolves.toEqual(
        expect.objectContaining({
          name: ConnectionStatus.Connecting,
          attempt: 1,
        }),
      );
    });

    test('cleanup resolves pending waiters', async () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });

      const waitPromise = manager.waitForStateChange();
      manager.cleanup();

      await expect(waitPromise).resolves.toEqual(
        expect.objectContaining({
          name: ConnectionStatus.Connecting,
        }),
      );
    });
  });

  describe('isInTerminalState', () => {
    test('returns true only for error state', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      const errorDetail = new ClientError({
        kind: ClientErrorKind.ConnectTimeout,
        message: 'connect timeout',
      });

      expect(manager.isInTerminalState()).toBe(false);

      manager.connected();
      expect(manager.isInTerminalState()).toBe(false);

      manager.error(errorDetail);
      expect(manager.isInTerminalState()).toBe(true);
    });
  });

  describe('error', () => {
    test('transitions to error state and pauses run loop', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      const listener = subscribe(manager);
      const errorDetail = new ClientError({
        kind: ClientErrorKind.Internal,
        message: 'internal error',
      });

      manager.error(errorDetail);

      expect(manager.state).toEqual({
        name: ConnectionStatus.Error,
        reason: errorDetail,
      });
      expect(manager.isInTerminalState()).toBe(true);
      expect(listener).toHaveBeenCalledWith({
        name: ConnectionStatus.Error,
        reason: errorDetail,
      });
    });

    test('is no-op when already in error state', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      const firstError = new ClientError({
        kind: ClientErrorKind.Internal,
        message: 'first error',
      });
      const secondError = new ClientError({
        kind: ClientErrorKind.NoSocketOrigin,
        message: 'second error',
      });

      manager.error(firstError);
      const listener = subscribe(manager);
      manager.error(secondError);

      expect(manager.state).toEqual({
        name: ConnectionStatus.Error,
        reason: firstError,
      });
      expect(listener).not.toHaveBeenCalled();
    });

    test('is no-op when closed', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      manager.closed();

      const listener = subscribe(manager);
      const errorDetail = new ClientError({
        kind: ClientErrorKind.Internal,
        message: 'error after closed',
      });
      manager.error(errorDetail);

      expect(listener).not.toHaveBeenCalled();
      expect(manager.is(ConnectionStatus.Closed)).toBe(true);
    });

    test('stops timeout checking when transitioning to error', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: 5_000,
        timeoutCheckIntervalMs: 100,
      });
      const listener = subscribe(manager);
      const errorDetail = new ClientError({
        kind: ClientErrorKind.Internal,
        message: 'error',
      });

      manager.error(errorDetail);
      listener.mockClear();

      // Advance time - timeout interval should not fire
      vi.advanceTimersByTime(10_000);

      expect(listener).not.toHaveBeenCalled();
      expect(manager.is(ConnectionStatus.Error)).toBe(true);
    });
  });

  describe('needsAuth', () => {
    test('transitions to needs-auth state and pauses run loop', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      const listener = subscribe(manager);
      const authError = new ProtocolError({
        kind: ErrorKind.Unauthorized,
        message: 'unauthorized',
        origin: ErrorOrigin.ZeroCache,
      }) as AuthError;

      manager.needsAuth(authError);

      expect(manager.state).toEqual({
        name: ConnectionStatus.NeedsAuth,
        reason: authError,
      });
      expect(manager.isInTerminalState()).toBe(true);
      expect(listener).toHaveBeenCalledWith({
        name: ConnectionStatus.NeedsAuth,
        reason: authError,
      });
    });

    test('is no-op when already in needs-auth state', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      const firstError = new ProtocolError({
        kind: ErrorKind.Unauthorized,
        message: 'first unauthorized',
        origin: ErrorOrigin.ZeroCache,
      }) as AuthError;
      const secondError = new ProtocolError({
        kind: ErrorKind.AuthInvalidated,
        message: 'second auth invalidated',
        origin: ErrorOrigin.ZeroCache,
      }) as AuthError;

      manager.needsAuth(firstError);
      const listener = subscribe(manager);
      manager.needsAuth(secondError);

      expect(manager.state).toEqual({
        name: ConnectionStatus.NeedsAuth,
        reason: firstError,
      });
      expect(listener).not.toHaveBeenCalled();
    });

    test('is no-op when closed', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      manager.closed();

      const listener = subscribe(manager);
      const authError = new ProtocolError({
        kind: ErrorKind.Unauthorized,
        message: 'auth error after closed',
        origin: ErrorOrigin.ZeroCache,
      }) as AuthError;
      manager.needsAuth(authError);

      expect(listener).not.toHaveBeenCalled();
      expect(manager.is(ConnectionStatus.Closed)).toBe(true);
    });

    test('stops timeout checking when transitioning to needs-auth', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: 5_000,
        timeoutCheckIntervalMs: 100,
      });
      const listener = subscribe(manager);
      const authError = new ProtocolError({
        kind: ErrorKind.Unauthorized,
        message: 'unauthorized',
        origin: ErrorOrigin.ZeroCache,
      }) as AuthError;

      manager.needsAuth(authError);
      listener.mockClear();

      // Advance time - timeout interval should not fire
      vi.advanceTimersByTime(10_000);

      expect(listener).not.toHaveBeenCalled();
      expect(manager.is(ConnectionStatus.NeedsAuth)).toBe(true);
    });

    test('can transition from connected to needs-auth', () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      manager.connected();
      const listener = subscribe(manager);

      const authError = new ProtocolError({
        kind: ErrorKind.AuthInvalidated,
        message: 'auth invalidated',
        origin: ErrorOrigin.ZeroCache,
      }) as AuthError;

      manager.needsAuth(authError);

      expect(manager.state).toEqual({
        name: ConnectionStatus.NeedsAuth,
        reason: authError,
      });
      expect(listener).toHaveBeenCalledWith({
        name: ConnectionStatus.NeedsAuth,
        reason: authError,
      });
    });

    test('needsAuth nextStatePromise resolves when transitioning out of needs-auth', async () => {
      const manager = new ConnectionManager({
        disconnectTimeout: DEFAULT_TIMEOUT_MS,
      });
      const authError = new ProtocolError({
        kind: ErrorKind.Unauthorized,
        message: 'unauthorized',
        origin: ErrorOrigin.ZeroCache,
      }) as AuthError;

      const {nextStatePromise} = manager.needsAuth(authError);

      manager.connecting();

      await expect(nextStatePromise).resolves.toEqual(
        expect.objectContaining({
          name: ConnectionStatus.Connecting,
          attempt: 1,
        }),
      );
    });
  });

  describe('throwIfConnectionError', () => {
    test('does nothing when state is connecting without reason', () => {
      const state: ConnectionManagerState = {
        name: ConnectionStatus.Connecting,
        attempt: 1,
        disconnectAt: 1_000,
      };

      expect(() => throwIfConnectionError(state)).not.toThrow();
    });

    test('throws when state is closed with non-tolerated client error reason', () => {
      const reason = new ClientError({
        kind: ClientErrorKind.ClientClosed,
        message: 'internal failure',
      });
      const state: ConnectionManagerState = {
        name: ConnectionStatus.Closed,
        reason,
      };

      expect(() => throwIfConnectionError(state)).toThrow(reason);
    });

    test('does nothing when connecting due to tolerated timeout reason', () => {
      const reason = new ClientError({
        kind: ClientErrorKind.ConnectTimeout,
        message: 'connect timeout',
      });
      const state: ConnectionManagerState = {
        name: ConnectionStatus.Connecting,
        attempt: 2,
        disconnectAt: 2_000,
        reason,
      };

      expect(() => throwIfConnectionError(state)).not.toThrow();
    });

    test('does nothing when disconnected due to clean close', () => {
      const reason = new ClientError({
        kind: ClientErrorKind.CleanClose,
        message: 'clean close',
      });
      const state: ConnectionManagerState = {
        name: ConnectionStatus.Error,
        reason,
      };

      expect(() => throwIfConnectionError(state)).not.toThrow();
    });

    test('throws when disconnected due to non-tolerated client error', () => {
      const reason = new ClientError({
        kind: ClientErrorKind.Internal,
        message: 'disconnect internal',
      });
      const state: ConnectionManagerState = {
        name: ConnectionStatus.Error,
        reason,
      };

      expect(() => throwIfConnectionError(state)).toThrow(reason);
    });

    test('throws when in needs-auth state', () => {
      const reason = new ProtocolError({
        kind: ErrorKind.Unauthorized,
        message: 'unauthorized',
        origin: ErrorOrigin.ZeroCache,
      }) as AuthError;
      const state: ConnectionManagerState = {
        name: ConnectionStatus.NeedsAuth,
        reason,
      };

      expect(() => throwIfConnectionError(state)).toThrow(reason);
    });
  });
});
