import {useSyncExternalStore} from 'react';
import {useZero} from './zero-provider.tsx';
import type {ConnectionState} from './zero.ts';

/**
 * Hook to subscribe to the connection status of the Zero instance.
 *
 * @returns The connection status of the Zero instance.
 * @see {@link ConnectionState} for more details on the connection state.
 */
export function useZeroConnectionState(): ConnectionState {
  const zero = useZero();
  return useSyncExternalStore(
    zero.connection.state.subscribe,
    () => zero.connection.state.current,
    () => zero.connection.state.current,
  );
}
