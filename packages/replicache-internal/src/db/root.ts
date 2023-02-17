import type * as dag from '../dag/mod.js';
import type {Hash} from '../hash.js';
import {withRead} from '../with-transactions.js';

export function getRoot(store: dag.Store, headName: string): Promise<Hash> {
  return withRead(store, async read => {
    const head = await read.getHead(headName);
    if (head === undefined) {
      throw new Error(`No head found for ${headName}`);
    }
    return head;
  });
}
