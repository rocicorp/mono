import {useReducer} from 'react';

import {M, getServerLogs} from '@/demo/shared/mutators';
import type {ReadTransaction, Reflect} from '@rocicorp/reflect';
import {useSubscribe} from 'replicache-react';

export function useCount(
  reflect: Reflect<M> | undefined,
  key: string,
  clog: (key: string, val: number) => void,
) {
  return useSubscribe(
    reflect,
    async (tx: ReadTransaction) => {
      const count = (await tx.get(key)) as number | undefined;
      // dont want to log if key doesn't exist yet or we are getting  default value from subscribe
      if (count === undefined) {
        return 0;
      }
      clog(key, count);
      return count;
    },
    undefined,
  );
}

export function useServerLogs(reflect: Reflect<M> | undefined) {
  return useSubscribe(reflect, tx => getServerLogs(tx), []);
}

export type ConsoleAction = {type: 'APPEND'; payload: string} | {type: 'CLEAR'};

export function useClientConsoleReducer(initialState: string[] = []) {
  return useReducer((state: string[], action: ConsoleAction) => {
    switch (action.type) {
      case 'APPEND': {
        // We only display 5 lines of console output.
        const newState = state.slice(-4);
        newState.push(action.payload);
        return newState;
      }
      case 'CLEAR':
        return [];
      default:
        return state;
    }
  }, initialState);
}
