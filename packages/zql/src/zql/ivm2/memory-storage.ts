import type {JSONValue} from 'replicache';
import type {Storage} from './operator.js';
import type {NormalizedValue, Value} from './data.js';
import BTree from 'btree';
import type {Stream} from './stream.js';

/**
 * MemoryStorage is a simple in-memory implementation of `Storage` for use
 * on the client and in tests.
 */
export class MemoryStorage implements Storage {
  #data: BTree<NormalizedValue[], JSONValue> = new BTree(
    undefined,
    normalizedValueArrayCompare,
  );

  set(key: NormalizedValue[], value: JSONValue) {
    // Could use a fancier encoding of the key in the future if scan is ever
    // needed.
    this.#data.add(key, value);
  }

  get(key: NormalizedValue[], def?: JSONValue): JSONValue | undefined {
    const r = this.#data.get(key);
    if (r !== undefined) {
      return r;
    }
    return def;
  }

  del(key: NormalizedValue[]) {
    this.#data.delete(key);
  }

  *scan(
    options: {
      prefix: NormalizedValue[];
    } = {prefix: []},
  ): Stream<[NormalizedValue[], JSONValue]> {
    for (const [key, value] of this.#data.entries(options.prefix)) {
      if (!isPrefix(options.prefix, key)) {
        return;
      }
      yield [key, value];
    }
  }

  cloneData(): Record<string, JSONValue> {
    const data: Record<string, JSONValue> = {};
    for (const [key, value] of this.#data.entries()) {
      data[JSON.stringify(key)] = value;
    }
    return structuredClone(data);
  }
}

function isPrefix(a: Value[], b: Value[]) {
  if (a.length > b.length) {
    return false;
  }
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) {
      return false;
    }
  }
  return true;
}

function normalizedValueArrayCompare(
  a: NormalizedValue[],
  b: NormalizedValue[],
): -1 | 0 | 1 {
  const minLength = Math.min(a.length, b.length);
  for (let i = 0; i < minLength; i++) {
    const ai = a[i];
    const bi = b[i];
    if (ai === null || bi === null) {
      if (ai !== null) {
        return 1;
      }
      if (bi !== null) {
        return -1;
      }
    } else {
      if (ai < bi) {
        return -1;
      }
      if (ai > bi) {
        return 1;
      }
    }
  }
  if (a.length < b.length) {
    return -1;
  }
  if (a.length > b.length) {
    return 1;
  }
  return 0;
}
