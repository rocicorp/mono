import {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import type {
  ConnectionManager,
  ConnectionManagerState,
} from './connection-manager.ts';
import {ConnectionStatus} from './connection-status.ts';
import {ClientErrorKind} from './client-error-kind.ts';
import {
  type ConnectionState,
  ConnectionImpl,
  ConnectionSource,
} from './connection.ts';
import {ClientError} from './error.ts';

describe('ConnectionImpl', () => {
  let manager: ConnectionManager;
  let lc: LogContext;
  let setAuthSpy: ReturnType<
    typeof vi.fn<(auth: string | null | undefined) => void>
  >;
  let isInTerminalStateMock: ReturnType<typeof vi.fn>;
  let requestConnectMock: ReturnType<typeof vi.fn>;
  let waitForStateChangeMock: ReturnType<typeof vi.fn>;
  let subscribeMock: ReturnType<typeof vi.fn>;
  let state: ConnectionManagerState;

  beforeEach(() => {
    lc = new LogContext('debug', {});
    setAuthSpy = vi.fn();
    isInTerminalStateMock = vi.fn().mockReturnValue(false);
    requestConnectMock = vi.fn();
    waitForStateChangeMock = vi.fn();
    const unsubscribe = vi.fn();
    subscribeMock = vi.fn().mockReturnValue(unsubscribe);
    state = {
      name: ConnectionStatus.Connecting,
      attempt: 0,
      disconnectAt: 0,
    };
    waitForStateChangeMock.mockResolvedValue(state);

    // Mock connection manager with minimal required behavior
    manager = {
      get state() {
        return state;
      },
      isInTerminalState: isInTerminalStateMock,
      requestConnect: requestConnectMock,
      waitForStateChange: waitForStateChangeMock,
      subscribe: subscribeMock,
    } as unknown as ConnectionManager;
  });

  describe('connect', () => {
    test('returns early when not in terminal state', async () => {
      isInTerminalStateMock.mockReturnValue(false);
      const connection = new ConnectionImpl(manager, lc, setAuthSpy);

      await connection.connect();

      expect(requestConnectMock).not.toHaveBeenCalled();
      expect(waitForStateChangeMock).not.toHaveBeenCalled();
      expect(setAuthSpy).not.toHaveBeenCalled();
    });

    test('requests connect and waits for state change', async () => {
      isInTerminalStateMock.mockReturnValue(true);
      state = {
        name: ConnectionStatus.Error,
        reason: new ClientError({
          kind: ClientErrorKind.Internal,
          message: 'err',
        }),
      };
      const nextStatePromise = Promise.resolve(state);
      waitForStateChangeMock.mockReturnValue(nextStatePromise);
      const connection = new ConnectionImpl(manager, lc, setAuthSpy);

      await connection.connect();

      expect(requestConnectMock).toHaveBeenCalledTimes(1);
      expect(waitForStateChangeMock).toHaveBeenCalledTimes(1);
      expect(setAuthSpy).not.toHaveBeenCalled();
    });

    test('updates auth when string token is provided', async () => {
      isInTerminalStateMock.mockReturnValue(true);
      state = {
        name: ConnectionStatus.Error,
        reason: new ClientError({
          kind: ClientErrorKind.Internal,
          message: 'err',
        }),
      };
      const nextStatePromise = Promise.resolve(state);
      waitForStateChangeMock.mockReturnValue(nextStatePromise);
      const connection = new ConnectionImpl(manager, lc, setAuthSpy);

      await connection.connect({auth: 'test-token-123'});

      expect(setAuthSpy).toHaveBeenCalledWith('test-token-123');
      expect(setAuthSpy).toHaveBeenCalledTimes(1);
      expect(requestConnectMock).toHaveBeenCalledTimes(1);
      expect(waitForStateChangeMock).toHaveBeenCalledTimes(1);
    });

    test('clears auth when null is provided', async () => {
      isInTerminalStateMock.mockReturnValue(true);
      state = {
        name: ConnectionStatus.Error,
        reason: new ClientError({
          kind: ClientErrorKind.Internal,
          message: 'err',
        }),
      };
      const nextStatePromise = Promise.resolve(state);
      waitForStateChangeMock.mockReturnValue(nextStatePromise);
      const connection = new ConnectionImpl(manager, lc, setAuthSpy);

      await connection.connect({auth: null});

      expect(setAuthSpy).toHaveBeenCalledWith(null);
      expect(setAuthSpy).toHaveBeenCalledTimes(1);
      expect(requestConnectMock).toHaveBeenCalledTimes(1);
      expect(waitForStateChangeMock).toHaveBeenCalledTimes(1);
    });

    test('clears auth when undefined is provided', async () => {
      isInTerminalStateMock.mockReturnValue(true);
      state = {
        name: ConnectionStatus.Error,
        reason: new ClientError({
          kind: ClientErrorKind.Internal,
          message: 'err',
        }),
      };
      const nextStatePromise = Promise.resolve(state);
      waitForStateChangeMock.mockReturnValue(nextStatePromise);
      const connection = new ConnectionImpl(manager, lc, setAuthSpy);

      await connection.connect({auth: undefined});

      expect(setAuthSpy).toHaveBeenCalledWith(undefined);
      expect(setAuthSpy).toHaveBeenCalledTimes(1);
      expect(requestConnectMock).toHaveBeenCalledTimes(1);
      expect(waitForStateChangeMock).toHaveBeenCalledTimes(1);
    });

    test('updates auth when called outside terminal state', async () => {
      isInTerminalStateMock.mockReturnValue(false);
      const connection = new ConnectionImpl(manager, lc, setAuthSpy);

      await connection.connect({auth: 'new-token'});

      expect(setAuthSpy).toHaveBeenCalledWith('new-token');
      expect(setAuthSpy).toHaveBeenCalledTimes(1);
      expect(requestConnectMock).not.toHaveBeenCalled();
      expect(waitForStateChangeMock).not.toHaveBeenCalled();
    });
  });
});

describe('ConnectionSource', () => {
  let manager: ConnectionManager;
  let subscribeMock: ReturnType<typeof vi.fn>;
  let managerListeners: Array<(state: unknown) => void>;

  beforeEach(() => {
    managerListeners = [];
    const unsubscribe = vi.fn();
    subscribeMock = vi.fn((listener: (state: unknown) => void) => {
      managerListeners.push(listener);
      return unsubscribe;
    });

    manager = {
      state: {name: ConnectionStatus.Connecting},
      subscribe: subscribeMock,
    } as unknown as ConnectionManager;
  });

  test('returns cached state initialized from manager state', () => {
    const source = new ConnectionSource(manager);

    const state1 = source.current;
    const state2 = source.current;

    expect(state1).toStrictEqual({name: 'connecting'});

    // returns the same (cached) object
    expect(state1).toBe(state2);
  });

  test('listener receives same state object as cached state', () => {
    const source = new ConnectionSource(manager);

    let receivedState;
    source.subscribe(state => {
      receivedState = state;
    });

    const newState = {
      name: ConnectionStatus.Connected,
    };
    for (const l of managerListeners) {
      l(newState);
    }

    // this must be the exact same object
    expect(receivedState).toBe(source.current);
  });

  test('current reflects state changes even before external subscribe', () => {
    // This test verifies the fix for the race condition where connection
    // completes before React subscribes, causing `current` to return stale state.
    const source = new ConnectionSource(manager);

    expect(source.current).toStrictEqual({name: 'connecting'});

    // Simulate connection completing BEFORE any external subscription
    const connectedState = {name: ConnectionStatus.Connected};
    for (const l of managerListeners) {
      l(connectedState);
    }

    // current should reflect the new state even though we never subscribed
    expect(source.current).toStrictEqual({name: 'connected'});
  });

  test('subscribes to manager in constructor', () => {
    new ConnectionSource(manager);

    // ConnectionSource should subscribe to manager immediately in constructor
    expect(subscribeMock).toHaveBeenCalledTimes(1);
  });

  test('multiple external subscribers all receive notifications', () => {
    const source = new ConnectionSource(manager);

    const received1: ConnectionState[] = [];
    const received2: ConnectionState[] = [];

    source.subscribe(state => received1.push(state));
    source.subscribe(state => received2.push(state));

    const connectedState = {name: ConnectionStatus.Connected};
    for (const l of managerListeners) {
      l(connectedState);
    }

    expect(received1).toHaveLength(1);
    expect(received2).toHaveLength(1);
    expect(received1[0]).toStrictEqual({name: 'connected'});
    expect(received2[0]).toStrictEqual({name: 'connected'});
  });
});
