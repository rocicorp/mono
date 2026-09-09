import {compareUTF8} from 'compare-utf8';
import {BTreeSet} from '../../../shared/src/btree-set.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import type {Storage} from './operator.ts';

type Entry = [key: string, value: JSONValue];

function comparator(a: Entry, b: Entry): number {
  return compareUTF8(a[0], b[0]);
}

/**
 * MemoryStorage is a simple in-memory implementation of `Storage` for use
 * on the client and in tests.
 */
export class MemoryStorage implements Storage {
  #data: BTreeSet<Entry> = new BTreeSet(comparator);

  set(key: string, value: JSONValue) {
    this.#data.add([key, value]);
  }

  get(key: string, def?: JSONValue): JSONValue | undefined {
    const r = this.#data.get([key, null]);
    if (r !== undefined) {
      return r[1];
    }
    return def;
  }

  del(key: string) {
    this.#data.delete([key, null]);
  }

  cloneData(): Record<string, JSONValue> {
    return structuredClone(Object.fromEntries(this.#data.toArray()));
  }
}
