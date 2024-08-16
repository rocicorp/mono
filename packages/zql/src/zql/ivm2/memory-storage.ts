import type {JSONValue} from 'replicache';
import type {Storage} from './operator.js';
import type {Value} from './data.js';

/**
 * MemoryStorage is a simple in-memory implementation of `Storage` for use
 * on the client and in tests.
 */
export class MemoryStorage implements Storage {
  #data: Record<string, JSONValue> = {};

  set(key: Value[], value: JSONValue) {
    // Could use a fancier encoding of the key in the future if scan is ever
    // needed.
    this.#data[JSON.stringify(key)] = value;
  }

  get(key: Value[], def?: JSONValue): JSONValue | undefined {
    const r = this.#data[JSON.stringify(key)];
    if (r !== undefined) {
      return r;
    }
    return def;
  }

  del(key: Value[]) {
    delete this.#data[JSON.stringify(key)];
  }

  cloneData(): Record<string, JSONValue> {
    return structuredClone(this.#data);
  }
}
