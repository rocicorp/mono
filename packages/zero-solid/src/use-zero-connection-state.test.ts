import {renderHook} from '@solidjs/testing-library';
import {afterEach, describe, expect, test, vi, type Mock} from 'vitest';
import {ConnectionStatus} from '../../zero-client/src/client/connection-status.ts';
import type {ConnectionState} from '../../zero-client/src/client/connection.ts';

vi.mock('./use-zero.ts', () => ({
  useZero: vi.fn(),
}));

import {useZeroConnectionState} from './use-zero-connection-state.ts';
import {useZero} from './use-zero.ts';

type ZeroLike = {
  connection: {
    state: {
      current: ConnectionState;
      subscribe: (listener: (state: ConnectionState) => void) => () => void;
    };
  };
};

const useZeroMock = useZero as unknown as Mock<() => () => ZeroLike>;

afterEach(() => {
  vi.clearAllMocks();
});

function mockZero(initialState: ConnectionState) {
  const listeners = new Set<(state: ConnectionState) => void>();
  const unsubscribeMock = vi.fn();

  const stateStore: ZeroLike['connection']['state'] = {
    current: initialState,
    subscribe: vi.fn(listener => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
        unsubscribeMock();
      };
    }),
  };

  const zero: ZeroLike = {
    connection: {
      state: stateStore,
    },
  };

  useZeroMock.mockReturnValue(() => zero);

  return {
    stateStore,
    listeners,
    unsubscribeMock,
    subscribeMock: stateStore.subscribe,
  };
}

describe('useZeroConnectionState (Solid)', () => {
  test('returns the current connection state and updates on changes', () => {
    const initialState: ConnectionState = {
      name: 'connecting',
    };
    const {stateStore, listeners, subscribeMock} = mockZero(initialState);

    const {result, cleanup} = renderHook(useZeroConnectionState);

    expect(subscribeMock).toHaveBeenCalledTimes(1);
    expect(result()).toEqual(initialState);

    const nextState: ConnectionState = {
      name: ConnectionStatus.Disconnected,
      reason: 'disconnected error',
    };

    stateStore.current = nextState;
    for (const listener of listeners) {
      listener(nextState);
    }

    expect(result()).toEqual(nextState);

    cleanup();
  });

  test('unsubscribes from connection state updates on cleanup', () => {
    const initialState: ConnectionState = {
      name: ConnectionStatus.Connected,
    };
    const {unsubscribeMock, subscribeMock} = mockZero(initialState);

    const {cleanup} = renderHook(useZeroConnectionState);

    expect(subscribeMock).toHaveBeenCalledTimes(1);

    cleanup();

    expect(unsubscribeMock).toHaveBeenCalledTimes(1);
  });
});
