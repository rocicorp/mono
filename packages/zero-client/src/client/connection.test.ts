import {beforeEach, describe, expect, test, vi} from 'vitest';
import type {ConnectionManager} from './connection-manager.ts';
import {ConnectionStatus} from './connection-status.ts';
import {ConnectionImpl} from './connection.ts';
import {ZeroLogContext} from './zero-log-context.ts';

describe('ConnectionImpl', () => {
  let manager: ConnectionManager;
  let lc: ZeroLogContext;
  let setAuthSpy: ReturnType<typeof vi.fn>;
  let isInTerminalStateMock: ReturnType<typeof vi.fn>;
  let connectingMock: ReturnType<typeof vi.fn>;
  let subscribeMock: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    lc = new ZeroLogContext('debug', {});
    setAuthSpy = vi.fn();
    isInTerminalStateMock = vi.fn().mockReturnValue(false);
    connectingMock = vi
      .fn()
      .mockReturnValue({nextStatePromise: Promise.resolve()});
    const unsubscribe = vi.fn();
    subscribeMock = vi.fn().mockReturnValue(unsubscribe);

    // Mock connection manager with minimal required behavior
    manager = {
      state: {name: ConnectionStatus.Connecting},
      isInTerminalState: isInTerminalStateMock,
      connecting: connectingMock,
      subscribe: subscribeMock,
    } as unknown as ConnectionManager;
  });

  describe('state', () => {
    test('returns current manager state', () => {
      const connection = new ConnectionImpl(manager, lc, setAuthSpy);

      expect(connection.state.current).toBe(manager.state);
    });

    test('subscribe delegates to manager', () => {
      const connection = new ConnectionImpl(manager, lc, setAuthSpy);
      const listener = vi.fn();

      connection.state.subscribe(listener);

      expect(subscribeMock).toHaveBeenCalledWith(listener);
    });
  });

  describe('connect', () => {
    test('returns early when not in terminal state', async () => {
      isInTerminalStateMock.mockReturnValue(false);
      const connection = new ConnectionImpl(manager, lc, setAuthSpy);

      await connection.connect();

      expect(connectingMock).not.toHaveBeenCalled();
      expect(setAuthSpy).not.toHaveBeenCalled();
    });

    test('calls manager.connecting() and waits for state change', async () => {
      isInTerminalStateMock.mockReturnValue(true);
      const nextStatePromise = Promise.resolve(manager.state);
      connectingMock.mockReturnValue({
        nextStatePromise,
      } as ReturnType<ConnectionManager['connecting']>);
      const connection = new ConnectionImpl(manager, lc, setAuthSpy);

      await connection.connect();

      expect(connectingMock).toHaveBeenCalledTimes(1);
      expect(setAuthSpy).not.toHaveBeenCalled();
    });

    test('updates auth when string token is provided', async () => {
      isInTerminalStateMock.mockReturnValue(true);
      const nextStatePromise = Promise.resolve(manager.state);
      connectingMock.mockReturnValue({
        nextStatePromise,
      } as ReturnType<ConnectionManager['connecting']>);
      const connection = new ConnectionImpl(manager, lc, setAuthSpy);

      await connection.connect({auth: 'test-token-123'});

      expect(setAuthSpy).toHaveBeenCalledWith('test-token-123');
      expect(setAuthSpy).toHaveBeenCalledTimes(1);
      expect(connectingMock).toHaveBeenCalledTimes(1);
    });

    test('clears auth when null is provided', async () => {
      isInTerminalStateMock.mockReturnValue(true);
      const nextStatePromise = Promise.resolve(manager.state);
      connectingMock.mockReturnValue({
        nextStatePromise,
      } as ReturnType<ConnectionManager['connecting']>);
      const connection = new ConnectionImpl(manager, lc, setAuthSpy);

      await connection.connect({auth: null});

      expect(setAuthSpy).toHaveBeenCalledWith(null);
      expect(setAuthSpy).toHaveBeenCalledTimes(1);
      expect(connectingMock).toHaveBeenCalledTimes(1);
    });

    test('clears auth when undefined is provided', async () => {
      isInTerminalStateMock.mockReturnValue(true);
      const nextStatePromise = Promise.resolve(manager.state);
      connectingMock.mockReturnValue({
        nextStatePromise,
      } as ReturnType<ConnectionManager['connecting']>);
      const connection = new ConnectionImpl(manager, lc, setAuthSpy);

      await connection.connect({auth: undefined});

      expect(setAuthSpy).toHaveBeenCalledWith(undefined);
      expect(setAuthSpy).toHaveBeenCalledTimes(1);
      expect(connectingMock).toHaveBeenCalledTimes(1);
    });

    test('updates auth when called outside terminal state', async () => {
      isInTerminalStateMock.mockReturnValue(false);
      const connection = new ConnectionImpl(manager, lc, setAuthSpy);

      await connection.connect({auth: 'new-token'});

      expect(setAuthSpy).toHaveBeenCalledWith('new-token');
      expect(setAuthSpy).toHaveBeenCalledTimes(1);
      expect(connectingMock).not.toHaveBeenCalled();
    });
  });
});
