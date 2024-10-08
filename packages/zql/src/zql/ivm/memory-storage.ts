import {BTree} from '../../../../btree/src/mod.js';
import type {JSONValue} from '../../../../shared/src/json.js';
import type {Storage} from './operator.js';
import type {Stream} from './stream.js';

/**
 * MemoryStorage is a simple in-memory implementation of `Storage` for use
 * on the client and in tests.
 */
export class MemoryStorage implements Storage {
  #data: BTree<string, JSONValue> = new BTree();

  set(key: string, value: JSONValue) {
    this.#data.add(key, value);
  }

  get(key: string, def?: JSONValue): JSONValue | undefined {
    const r = this.#data.get(key);
    if (r !== undefined) {
      return r;
    }
    return def;
  }

  del(key: string) {
    this.#data.delete(key);
  }

  *scan(options?: {prefix: string}): Stream<[string, JSONValue]> {
    for (const [key, value] of this.#data.entries(options?.prefix)) {
      if (options && !key.startsWith(options.prefix)) {
        return;
      }
      yield [key, value];
    }
  }

  cloneData(): Record<string, JSONValue> {
    return structuredClone(Object.fromEntries(this.#data.entries()));
  }
}
