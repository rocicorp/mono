import {compareUTF8} from 'compare-utf8';
import * as valita from 'shared/src/valita.js';
import type {JSONValue} from '../types/bigint-json.js';
import {batchScan, scan} from './scan-storage.js';
import type {ListOptions, Storage} from './storage.js';

/**
 * Implements a write cache for key/value pairs on top of some lower-level
 * storage. Writes are buffered in the cache, with their effects readable from
 * the cache, but not applied to the underlying {@link Storage} until
 * {@link WriteCache.flush flush()} is called. This provides the mechanism for
 * atomic flushing of multiple writes.
 *
 * WriteCaches can be stacked: WriteCache itself implements Storage so that
 * you can create multiple layers of caches and control when they flush.
 */
export class WriteCache implements Storage {
  #storage: Storage;
  #cache: Map<string, {value: JSONValue | undefined}> = new Map();

  constructor(storage: Storage) {
    this.#storage = storage;
  }

  #put<T extends JSONValue>(key: string, value: T) {
    this.#cache.set(key, {value});
  }

  // eslint-disable-next-line require-await
  async put<T extends JSONValue>(key: string, value: T): Promise<void> {
    this.#put(key, value);
  }

  // eslint-disable-next-line require-await
  async putEntries<T extends JSONValue>(
    entries: Record<string, T>,
  ): Promise<void> {
    for (const [key, value] of Object.entries(entries)) {
      this.#put(key, value);
    }
  }

  #del(key: string) {
    this.#cache.set(key, {value: undefined});
  }

  // eslint-disable-next-line require-await
  async del(key: string): Promise<void> {
    this.#del(key);
  }

  // eslint-disable-next-line require-await
  async delEntries(keys: string[]): Promise<void> {
    for (const key of keys) {
      this.#del(key);
    }
  }

  // eslint-disable-next-line require-await
  async get<T extends JSONValue>(
    key: string,
    schema: valita.Type<T>,
  ): Promise<T | undefined> {
    const cached = this.#cache.get(key);
    return cached
      ? // We don't validate on cache hits partly for perf reasons and also
        // because we should have already validated with same schema during
        // initial read.
        (cached.value as T | undefined)
      : this.#storage.get(key, schema);
  }

  async getEntries<T extends JSONValue>(
    keys: string[],
    schema: valita.Type<T>,
  ): Promise<Map<string, T>> {
    const result = new Map<string, T>();
    const keysToGetFromStorage = [];
    for (const key of keys) {
      const cached = this.#cache.get(key);
      if (cached) {
        // We don't validate on cache hits partly for perf reasons and also
        // because we should have already validated with same schema during
        // initial read.
        if (cached.value !== undefined) {
          result.set(key, cached.value as T);
        }
      } else {
        keysToGetFromStorage.push(key);
      }
    }
    const fromStorage = await this.#storage.getEntries(
      keysToGetFromStorage,
      schema,
    );
    for (const [key, value] of fromStorage.entries()) {
      result.set(key, value);
    }
    return result;
  }

  /**
   * @returns Whether there are any pending writes in the cache. Note that
   * redundant writes (e.g. deleting a non-existing key) are still considered writes.
   */
  isDirty(): boolean {
    return this.#cache.size > 0;
  }

  pending(): (PutOp | DelOp)[] {
    const res: (PutOp | DelOp)[] = [];
    for (const [key, {value}] of this.#cache.entries()) {
      if (value === undefined) {
        res.push({op: 'del', key});
      } else {
        res.push({op: 'put', key, value});
      }
    }
    return res;
  }

  pendingCounts(): {
    delCount: number;
    putCount: number;
  } {
    const counts = {
      delCount: 0,
      putCount: 0,
    };
    for (const {value} of this.#cache.values()) {
      if (value === undefined) {
        counts.delCount++;
      } else {
        counts.putCount++;
      }
    }
    return counts;
  }

  async flush(): Promise<void> {
    // Note the order of operations: all del()` and put() calls are
    // invoked before await. This ensures atomicity of the flushed
    // writes, as described in:
    //
    // https://developers.cloudflare.com/durable-objects/api/transactional-storage-api/#supported-options-1
    const promises = [];
    for (const [key, {value}] of this.#cache.entries()) {
      if (value === undefined) {
        promises.push(this.#storage.del(key));
      } else {
        promises.push(this.#storage.put(key, value));
      }
    }
    await Promise.all(promises);
    this.#cache.clear();
  }

  scan<T extends JSONValue>(
    options: ListOptions,
    schema: valita.Type<T>,
  ): AsyncIterable<[key: string, value: T]> {
    return scan(this, options, schema);
  }

  batchScan<T extends JSONValue>(
    options: ListOptions,
    schema: valita.Type<T>,
    batchSize: number,
  ): AsyncIterable<Map<string, T>> {
    return batchScan(this, options, schema, batchSize);
  }

  async list<T extends JSONValue>(
    options: ListOptions,
    schema: valita.Type<T>,
  ): Promise<Map<string, T>> {
    const {prefix, start, end, limit} = options;
    const startKey = start?.key;
    const exclusive = start?.exclusive;

    // if the caller specified a limit, and we have local deletes, adjust
    // how many we fetch from the underlying storage.
    let adjustedLimit = limit;
    if (adjustedLimit !== undefined) {
      let deleted = 0;
      for (const [, {value}] of this.#cache.entries()) {
        if (value === undefined) {
          deleted++;
        }
      }
      adjustedLimit += deleted;
    }

    const base = new Map(
      await this.#storage.list({...options, limit: adjustedLimit}, schema),
    );

    // build a list of pending changes to overlay atop stored values
    const pending: [string, T | undefined][] = [];
    for (const entry of this.#cache.entries()) {
      const [k, v] = entry;

      if (
        (!prefix || k.startsWith(prefix)) &&
        (!startKey ||
          (exclusive
            ? compareUTF8(k, startKey) > 0
            : compareUTF8(k, startKey) >= 0)) &&
        (!end || compareUTF8(k, end) < 0)
      ) {
        if (v.value === undefined) {
          pending.push([k, undefined]);
        } else {
          pending.push([k, valita.parse(v.value, schema)]);
        }
      }
    }

    // The map of entries coming back from DurableStorage is utf8 sorted.
    // Maintain this by merging the pending changes in-order
    pending.sort(([a], [b]) => compareUTF8(a, b));

    const out = new Map<string, T>();
    const a = base.entries();
    const b = pending.values();

    let iterResultA = a.next();
    let iterResultB = b.next();
    let count = 0;

    function add(k: string, v: T | undefined) {
      if (v !== undefined) {
        out.set(k, v);
        count++;
      }
    }

    while (
      !(iterResultB.done && iterResultA.done) &&
      (!limit || count < limit)
    ) {
      if (!iterResultB.done) {
        const [bKey, bValue] = iterResultB.value;

        if (!iterResultA.done) {
          const [aKey, aValue] = iterResultA.value;

          const cmp = compareUTF8(aKey, bKey);
          if (cmp === 0) {
            add(bKey, bValue);
            iterResultA = a.next();
            iterResultB = b.next();
          } else if (cmp < 0) {
            add(aKey, aValue);
            iterResultA = a.next();
          } else {
            add(bKey, bValue);
            iterResultB = b.next();
          }
        } else {
          add(bKey, bValue);
          iterResultB = b.next();
        }
      } else {
        add(iterResultA.value[0], iterResultA.value[1]);
        iterResultA = a.next();
      }
    }

    return out;
  }
}

// Carried over from `reflect-protocol`. No longer used by still useful for testing.
export type PutOp = {
  op: 'put';
  key: string;
  value: JSONValue;
};

export type DelOp = {
  op: 'del';
  key: string;
};

export type Patch = (PutOp | DelOp)[];
