import {createEffect, createSignal, onCleanup, type Accessor} from 'solid-js';
import type {ConnectionState} from '../../zero/src/zero.ts';
import {useZero} from './use-zero.ts';

/**
 * Tracks the connection status of the current Zero instance.
 *
 * @returns The connection status of the Zero instance.
 * @see {@link ConnectionState} for more details on the connection state.
 */
export function useZeroConnectionState(): Accessor<ConnectionState> {
  const zero = useZero();

  const [connectionState, setConnectionState] = createSignal<ConnectionState>(
    zero().connection.state.current,
  );

  createEffect(() => {
    const unsubscribe = zero().connection.state.subscribe(setConnectionState);

    onCleanup(unsubscribe);
  });

  return connectionState;
}
