import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import type {ConnectionState} from './connection-manager.ts';
import {ConnectionManager} from './connection-manager.ts';
import {ConnectionStatus} from './connection-status.ts';

const DEFAULT_TIMEOUT_MS = 5 * 60 * 1000;

describe('ConnectionManager', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  const subscribe = (manager: ConnectionManager) => {
    const listener = vi.fn<(state: ConnectionState) => void>();
    manager.subscribe(listener);
    return listener;
  };

  describe('constructor', () => {
    test('starts in connecting state with default timeout', () => {
      vi.setSystemTime(1_000);
      const manager = new ConnectionManager({
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
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
      const manager = new ConnectionManager({disconnectTimeoutMs: 10_000});

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
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
      });
      const listener = subscribe(manager);

      vi.setSystemTime(6_000);
      manager.connecting('retry');

      expect(manager.state).toEqual({
        name: ConnectionStatus.Connecting,
        attempt: 1,
        disconnectAt: 2_500 + DEFAULT_TIMEOUT_MS,
        reason: 'retry',
      });
      expect(listener).toHaveBeenCalledTimes(1);
    });

    test('starts a new session after the timer resets', () => {
      const manager = new ConnectionManager({
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
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
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
      });

      vi.setSystemTime(DEFAULT_TIMEOUT_MS / 2);
      manager.disconnected();
      expect(manager.is(ConnectionStatus.Disconnected)).toBe(true);

      const listener = subscribe(manager);
      vi.setSystemTime(DEFAULT_TIMEOUT_MS / 2 + 1_000);
      manager.connecting();

      expect(manager.is(ConnectionStatus.Disconnected)).toBe(true);
      expect(listener).not.toHaveBeenCalled();
    });

    test('does nothing when disconnected after timing out', () => {
      const manager = new ConnectionManager({
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
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
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
      });
      manager.closed();

      const listener = subscribe(manager);
      manager.connecting();

      expect(manager.is(ConnectionStatus.Closed)).toBe(true);
      expect(listener).not.toHaveBeenCalled();
    });

    test('does not create multiple timeout intervals when called rapidly', () => {
      const manager = new ConnectionManager({
        disconnectTimeoutMs: 1_000,
        timeoutCheckIntervalMs: 100,
      });
      const listener = subscribe(manager);

      // Call connecting multiple times rapidly
      manager.connecting('attempt1');
      manager.connecting('attempt2');
      manager.connecting('attempt3');

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
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
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
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
      });
      manager.connected();

      const listener = subscribe(manager);
      manager.connected();

      expect(listener).not.toHaveBeenCalled();
    });

    test('is no-op when closed', () => {
      const manager = new ConnectionManager({
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
      });
      manager.closed();

      const listener = subscribe(manager);
      manager.connected();

      expect(listener).not.toHaveBeenCalled();
      expect(manager.is(ConnectionStatus.Closed)).toBe(true);
    });

    test('safely handles stopping timeout interval multiple times', () => {
      const manager = new ConnectionManager({
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
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
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
      });
      const listener = subscribe(manager);

      manager.disconnected();

      expect(manager.state).toEqual({name: ConnectionStatus.Disconnected});
      expect(listener).toHaveBeenCalledWith({
        name: ConnectionStatus.Disconnected,
      });
    });

    test('stays disconnected after leaving connected', () => {
      const manager = new ConnectionManager({
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
      });

      vi.setSystemTime(2 * 60 * 1000);
      manager.connected();

      vi.setSystemTime(8 * 60 * 1000);
      manager.disconnected();
      expect(manager.is(ConnectionStatus.Disconnected)).toBe(true);

      vi.setSystemTime(8 * 60 * 1000);
      manager.connecting();

      expect(manager.is(ConnectionStatus.Disconnected)).toBe(true);
    });

    test('is no-op when already disconnected', () => {
      const manager = new ConnectionManager({
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
      });
      manager.disconnected();

      const listener = subscribe(manager);
      manager.disconnected();

      expect(listener).not.toHaveBeenCalled();
    });

    test('is no-op when closed', () => {
      const manager = new ConnectionManager({
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
      });
      manager.closed();

      const listener = subscribe(manager);
      manager.disconnected();

      expect(listener).not.toHaveBeenCalled();
      expect(manager.is(ConnectionStatus.Closed)).toBe(true);
    });
  });

  describe('closed', () => {
    test('transitions to closed and blocks further updates', () => {
      const manager = new ConnectionManager({
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
      });
      const listener = subscribe(manager);

      manager.closed();

      expect(manager.state).toEqual({name: ConnectionStatus.Closed});
      expect(manager.shouldContinueRunLoop()).toBe(false);
      expect(listener).toHaveBeenCalledWith({name: ConnectionStatus.Closed});

      listener.mockClear();
      manager.connecting();
      manager.connected();
      manager.disconnected();

      expect(manager.state).toEqual({name: ConnectionStatus.Closed});
      expect(listener).not.toHaveBeenCalled();
    });

    test('is no-op when already closed', () => {
      const manager = new ConnectionManager({
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
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
        disconnectTimeoutMs: 1_000,
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
      expect(listener).toHaveBeenCalledWith({
        name: ConnectionStatus.Disconnected,
      });
    });

    test('uses the configured timeout check interval', () => {
      const setIntervalSpy = vi.spyOn(globalThis, 'setInterval');

      const manager = new ConnectionManager({
        disconnectTimeoutMs: 5_000,
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
        disconnectTimeoutMs: 1_000,
        timeoutCheckIntervalMs: 100,
      });
      const listener = subscribe(manager);

      manager.disconnected();
      listener.mockClear();

      vi.advanceTimersByTime(1_000);

      expect(listener).not.toHaveBeenCalled();
    });
  });

  describe('shouldContinueRunLoop', () => {
    test('is true until the manager is closed', () => {
      const manager = new ConnectionManager({
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
      });
      expect(manager.shouldContinueRunLoop()).toBe(true);

      manager.connecting();
      expect(manager.shouldContinueRunLoop()).toBe(true);

      manager.connected();
      expect(manager.shouldContinueRunLoop()).toBe(true);

      manager.disconnected();
      expect(manager.shouldContinueRunLoop()).toBe(true);

      manager.closed();
      expect(manager.shouldContinueRunLoop()).toBe(false);
    });
  });

  describe('state transition cycle', () => {
    test('handles cycle through disconnected: Connecting → Connected → Disconnected → Connected', () => {
      vi.setSystemTime(1_000);
      const manager = new ConnectionManager({
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
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
      manager.disconnected();
      expect(manager.state).toEqual({name: ConnectionStatus.Disconnected});

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
      });
      expect(listener).toHaveBeenNthCalledWith(3, {
        name: ConnectionStatus.Connected,
      });
    });
  });

  describe('state change promises', () => {
    test('waitForStateChange resolves on next transition', async () => {
      const manager = new ConnectionManager({
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
      });

      const waitForChange = manager.waitForStateChange();
      manager.connected();

      await expect(waitForChange).resolves.toBeUndefined();
    });

    test('transition nextStatePromise resolves after subsequent state change', async () => {
      const manager = new ConnectionManager({
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
      });

      // Move to connected first so connecting() starts a fresh session
      manager.connected();
      const {nextStatePromise} = manager.connecting();
      manager.connected();

      await expect(nextStatePromise).resolves.toBeUndefined();
    });

    test('transition nextStatePromise resolves on closed', async () => {
      const manager = new ConnectionManager({
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
      });

      const {nextStatePromise} = manager.connected();
      manager.closed();

      await expect(nextStatePromise).resolves.toBeUndefined();
    });

    test('waitForStateChange reuses promise until resolved, then creates a new one', async () => {
      const manager = new ConnectionManager({
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
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
      await expect(first).resolves.toBeUndefined();

      const third = manager.waitForStateChange();
      expect(third).not.toBe(first);

      manager.disconnected();
      await expect(third).resolves.toBeUndefined();
    });

    test('error nextStatePromise resolves when transitioning from connected to connecting', async () => {
      const manager = new ConnectionManager({
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
      });

      const {nextStatePromise} = manager.connected();
      manager.connecting();

      await expect(nextStatePromise).resolves.toBeUndefined();
    });

    test('cleanup resolves pending waiters', async () => {
      const manager = new ConnectionManager({
        disconnectTimeoutMs: DEFAULT_TIMEOUT_MS,
      });

      const waitPromise = manager.waitForStateChange();
      manager.cleanup();

      await expect(waitPromise).resolves.toBeUndefined();
    });
  });
});
