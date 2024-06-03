import type {Context} from '@rocicorp/zql/src/zql/context/context.js';
import type {Materialite} from '@rocicorp/zql/src/zql/ivm/materialite.js';

const emptyFunction = () => {};
export function createContext(materialite: Materialite): Context {
  return {
    materialite,
    getSource: (_name, _ordering) => {
      throw new Error('Not implemented');
    },
    subscriptionAdded: () => emptyFunction,
  };
}
