import type {Store} from '../dag/store.js';
import type {Hash} from '../hash.js';
import {withRead} from '../with-transactions.js';

export function getRoot(store: Store, headName: string): Promise<Hash> {
  return withRead(store, async read => {
    const head = await read.getHead(headName);
    if (head === undefined) {
      throw new Error(`No head found for ${headName}`);
    }
    return head;
  });
}
