import {createSignal, onCleanup, type Accessor} from 'solid-js';
import {useZero} from './use-zero.ts';
import type {OnlineStatus} from '../../zero-client/src/client/online-manager.ts';

/**
 * Tracks the online status of the current Zero instance.
 *
 * @returns An accessor — call `online()` to get a reactive `OnlineStatus`.
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
 *   {online() === 'online' ? 'Online' : online() === 'offline' ? 'Offline' : 'Offline Pending'}
 * </span>
 *
 * @see {@link https://zero.rocicorp.dev/docs/offline Offline mode docs}
 */
export function useZeroOnline(): Accessor<OnlineStatus> {
  const zero = useZero()();

  const [online, setOnline] = createSignal<OnlineStatus>(zero.online);

  const unsubscribe = zero.onOnline(setOnline);

  onCleanup(unsubscribe);

  return online;
}
