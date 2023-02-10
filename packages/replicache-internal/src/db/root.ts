import type * as dag from '../dag/mod';
import type {Hash} from '../hash';

export function getRoot(store: dag.Store, headName: string): Promise<Hash> {
  return store.withRead(async read => {
    const head = await read.getHead(headName);
    if (head === undefined) {
      throw new Error(`No head found for ${headName}`);
    }
    return head;
  });
}
