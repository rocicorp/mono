import {act, useEffect, type ReactNode} from 'react';
import {createRoot, type Root} from 'react-dom/client';
import {afterEach, describe, expect, test, vi, type Mock} from 'vitest';
import {ConnectionStatus, type ConnectionState} from './zero.ts';

vi.mock('./zero-provider.tsx', () => ({
  useZero: vi.fn(),
}));

import {useConnectionState} from './use-connection-state.tsx';
import {useZero} from './zero-provider.tsx';

type ZeroLike = {
  connection: {
    state: {
      current: ConnectionState;
      subscribe: (listener: (state: ConnectionState) => void) => () => void;
    };
  };
};

const useZeroMock = useZero as unknown as Mock<() => ZeroLike>;

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

  useZeroMock.mockReturnValue(zero);

  return {
    stateStore,
    listeners,
    unsubscribeMock,
    subscribeMock: stateStore.subscribe,
  };
}

function renderWithRoot(children: ReactNode): Root {
  const container = document.createElement('div');
  const root = createRoot(container);
  act(() => {
    root.render(children);
  });
  return root;
}

describe('useConnectionState (React)', () => {
  test('returns the current connection state and updates on changes', () => {
    const initialState: ConnectionState = {
      name: 'connecting',
    };
    const {stateStore, listeners, subscribeMock} = mockZero(initialState);
    const observedStates: ConnectionState[] = [];

    function TestComponent({
      onState,
    }: {
      onState: (state: ConnectionState) => void;
    }) {
      const state = useConnectionState();
      useEffect(() => {
        onState(state);
      }, [state, onState]);
      return null;
    }

    const onState = (state: ConnectionState) => {
      observedStates.push(state);
    };

    const root = renderWithRoot(<TestComponent onState={onState} />);

    expect(subscribeMock).toHaveBeenCalledTimes(1);
    expect(observedStates).toEqual([initialState]);

    const nextState: ConnectionState = {
      name: ConnectionStatus.Disconnected,
      reason: 'disconnected error',
    };

    act(() => {
      stateStore.current = nextState;
      for (const listener of listeners) {
        listener(nextState);
      }
    });

    expect(observedStates).toEqual([initialState, nextState]);

    act(() => {
      root.unmount();
    });
  });

  test('unsubscribes from connection state updates on unmount', () => {
    const initialState: ConnectionState = {
      name: ConnectionStatus.Connected,
    };
    const {unsubscribeMock, subscribeMock} = mockZero(initialState);

    function TestComponent() {
      useConnectionState();
      return null;
    }

    const root = renderWithRoot(<TestComponent />);

    expect(subscribeMock).toHaveBeenCalledTimes(1);

    act(() => {
      root.unmount();
    });

    expect(unsubscribeMock).toHaveBeenCalledTimes(1);
  });
});
