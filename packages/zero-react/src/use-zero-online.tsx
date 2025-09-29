import {useSyncExternalStore} from 'react';
import {useZero} from './zero-provider.tsx';
import type {OnlineStatus} from '../../zero-client/src/client/online-manager.ts';

/**
 * Hook to subscribe to the online status of the Zero instance.
 *
 * This is useful when you want to update state based on the online status.
 *
 * @returns The online status of the Zero instance.
 *
 * The status can be one of:
 * - `online` - the client is online and accepting writes.
 * - `offline` - the client is offline and will reject writes until the client becomes online again.
 * - `offline-pending` - the client cannot reach the server and will enter offline mode after `offlineDelayMs` milliseconds,
 *   but will accept writes until then.
 *
 * @example
 * const online = useZeroOnline();
 *
 * <span>
 *   {online === 'online' ? 'Online' : online === 'offline' ? 'Offline' : 'Offline Pending'}
 * </span>
 */
export function useZeroOnline(): OnlineStatus {
  const zero = useZero();
  return useSyncExternalStore(
    zero.onOnline,
    () => zero.online,
    () => zero.online,
  );
}
