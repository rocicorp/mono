import {useSyncExternalStore} from 'react';
import {useZero} from './zero-provider.tsx';

/**
 * Hook to subscribe to the online status of the Zero instance.
 *
 * This is useful when you want to update state based on the online status.
 *
 * @returns The online status of the Zero instance.
 */
export function useZeroOnline(): boolean {
  const zero = useZero();
  return useSyncExternalStore(
    zero.onOnline,
    () => zero.online,
    () => zero.online,
  );
}
