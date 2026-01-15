import {createEffect, createSignal, onCleanup, type Accessor} from 'solid-js';
import {useZero} from './use-zero.ts';

/**
 * Tracks the online status of the current Zero instance.
 *
 * @returns An accessor — call `online()` to get a reactive `boolean`.
 * @deprecated Use {@linkcode useConnectionState} instead, which provides more detailed connection state.
 */
export function useZeroOnline(): Accessor<boolean> {
  const zero = useZero();

  const [online, setOnline] = createSignal<boolean>(zero().online);

  createEffect(() => {
    const unsubscribe = zero().onOnline(setOnline);

    onCleanup(unsubscribe);
  });

  return online;
}
