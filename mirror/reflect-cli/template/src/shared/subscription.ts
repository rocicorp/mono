import type {Reflect} from '@rocicorp/reflect/client';
import {useSubscribe} from 'replicache-react';
import {clientStatePrefix} from './client-state';
import type {ClientState} from './client-state';
import type {M} from './mutators';

export function useClientStates(reflect: Reflect<M>) {
  return useSubscribe(
    reflect,
    async tx => {
      const clientStateEntries = (await tx
        .scan({prefix: clientStatePrefix})
        .entries()
        .toArray()) as [string, ClientState][];
      const clientStates = clientStateEntries
        .map<[string, ClientState]>(([key, clientState]) => {
          const id = key.substring(clientStatePrefix.length);
          return [id, clientState];
        })
        .filter(([id, _]) => id !== tx.clientID);
      return Object.fromEntries(clientStates);
    },
    {},
  );
}
