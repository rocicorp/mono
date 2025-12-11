import {renderHook} from '@solidjs/testing-library';
import {createSignal} from 'solid-js';
import {afterEach, describe, expect, test, vi, type Mock} from 'vitest';
import {ConnectionStatus, type ConnectionState} from './zero.ts';

vi.mock('./use-zero.ts', () => ({
  useZero: vi.fn(),
}));

import {useConnectionState} from './use-connection-state.ts';
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

function mockZero(
  initialState: ConnectionState,
  options?: {register?: boolean},
) {
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

  if (options?.register !== false) {
    useZeroMock.mockReturnValue(() => zero);
  }

  return {
    zero,
    stateStore,
    listeners,
    unsubscribeMock,
    subscribeMock: stateStore.subscribe,
  };
}

describe('useConnectionState (Solid)', () => {
  test('returns the current connection state and updates on changes', () => {
    const initialState: ConnectionState = {
      name: 'connecting',
    };
    const {stateStore, listeners, subscribeMock} = mockZero(initialState);

    const {result, cleanup} = renderHook(useConnectionState);

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

    const {cleanup} = renderHook(useConnectionState);

    expect(subscribeMock).toHaveBeenCalledTimes(1);

    cleanup();

    expect(unsubscribeMock).toHaveBeenCalledTimes(1);
  });

  test('re-subscribes when zero instance changes', () => {
    const initialState: ConnectionState = {
      name: 'connecting',
    };
    const nextInitialState: ConnectionState = {
      name: ConnectionStatus.Connected,
    };

    const zeroA = mockZero(initialState, {register: false});
    const zeroB = mockZero(nextInitialState, {register: false});

    const [currentZero, setCurrentZero] = createSignal<ZeroLike>(zeroA.zero);
    useZeroMock.mockReturnValue(currentZero);

    const {result, cleanup} = renderHook(useConnectionState);

    expect(result()).toEqual(initialState);
    expect(zeroA.subscribeMock).toHaveBeenCalledTimes(1);

    setCurrentZero(zeroB.zero);

    expect(zeroA.unsubscribeMock).toHaveBeenCalledTimes(1);
    expect(zeroB.subscribeMock).toHaveBeenCalledTimes(1);

    const updatedState: ConnectionState = {
      name: ConnectionStatus.Disconnected,
      reason: 'network',
    };
    zeroB.stateStore.current = updatedState;
    for (const listener of zeroB.listeners) {
      listener(updatedState);
    }

    expect(result()).toEqual(updatedState);

    cleanup();
  });
});
