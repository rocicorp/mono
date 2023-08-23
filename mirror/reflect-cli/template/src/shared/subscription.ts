import type {Reflect} from '@rocicorp/reflect/client';
import {useSubscribe} from 'replicache-react';
import {ClientState, clientStatePrefix} from './client-state';
import type {M} from './mutators';

export function useCount(reflect: Reflect<M>, key: string) {
  return useSubscribe(
    reflect,
    async tx => ((await tx.get(key)) ?? 0) as number,
    0,
  );
}

export function useClientStates(reflect: Reflect<M>) {
  return useSubscribe(
    reflect,
    async tx =>
      (await tx
        .scan({prefix: clientStatePrefix})
        .entries()
        .toArray()) as (readonly [string, ClientState])[],
    [],
  );
}
