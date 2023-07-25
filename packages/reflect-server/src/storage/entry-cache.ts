import {compareUTF8} from 'compare-utf8';
import type {Patch} from 'reflect-protocol';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import * as valita from 'shared/src/valita.js';
import {batchScan, scan} from './scan-storage.js';
import type {ListOptions, Storage} from './storage.js';

/**
 * Implements a read/write cache for key/value pairs on top of some lower-level
 * storage.
 *
 * This is designed to be stacked: EntryCache itself implements Storage so that
 * you can create multiple layers of caches and control when they flush.
 *
 * TODO: We can remove the read side of this since DO does caching itself internally!
 */
export class EntryCache implements Storage {
  #storage: Storage;
  #cache: Map<string, {value?: ReadonlyJSONValue | undefined; dirty: boolean}> =
    new Map();

  constructor(storage: Storage) {
    this.#storage = storage;
  }

  #put<T extends ReadonlyJSONValue>(key: string, value: T) {
    this.#cache.set(key, {value, dirty: true});
  }

  // eslint-disable-next-line require-await
  async put<T extends ReadonlyJSONValue>(key: string, value: T): Promise<void> {
    this.#put(key, value);
  }

  // eslint-disable-next-line require-await
  async putEntries<T extends ReadonlyJSONValue>(
    entries: Record<string, T>,
  ): Promise<void> {
    for (const [key, value] of Object.entries(entries)) {
      this.#put(key, value);
    }
  }

  #del(key: string) {
    this.#cache.set(key, {value: undefined, dirty: true});
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

  async get<T extends ReadonlyJSONValue>(
    key: string,
    schema: valita.Type<T>,
  ): Promise<T | undefined> {
    const cached = this.#cache.get(key);
    if (cached) {
      // We don't validate on cache hits partly for perf reasons and also
      // because we should have already validated with same schema during
      // initial read.
      return cached.value as T | undefined;
    }
    const value = await this.#storage.get(key, schema);
    this.#cache.set(key, {value, dirty: false});
    return value;
  }

  /**
   * @returns Whether there are any pending writes in the cache. Note that
   * redundant writes (e.g. deleting a non-existing key) are still considered writes.
   */
  isDirty(): boolean {
    for (const value of this.#cache.values()) {
      if (value.dirty) {
        return true;
      }
    }
    return false;
  }

  pending(): Patch {
    const res: Patch = [];
    for (const [key, {value, dirty}] of this.#cache.entries()) {
      if (dirty) {
        if (value === undefined) {
          res.push({op: 'del', key});
        } else {
          res.push({op: 'put', key, value});
        }
      }
    }
    return res;
  }

  async flush(): Promise<void> {
    // Note the order of operations: all del()` and put() calls are
    // invoked before await. This ensures atomicity of the flushed
    // writes, as described in:
    //
    // https://developers.cloudflare.com/workers/learning/using-durable-objects/#accessing-persistent-storage-from-a-durable-object
    await Promise.all(
      [...this.#cache.entries()]
        // Destructure ALL the things
        .filter(([, {dirty}]) => dirty)
        .map(([k, {value}]) => {
          if (value === undefined) {
            return this.#storage.del(k);
          }
          return this.#storage.put(k, value);
        }),
    );

    this.#cache.clear();
  }

  scan<T extends ReadonlyJSONValue>(
    options: ListOptions,
    schema: valita.Type<T>,
  ): AsyncIterable<[key: string, value: T]> {
    return scan(this, options, schema);
  }

  batchScan<T extends ReadonlyJSONValue>(
    options: ListOptions,
    schema: valita.Type<T>,
    batchSize: number,
  ): AsyncIterable<Map<string, T>> {
    return batchScan(this, options, schema, batchSize);
  }

  async list<T extends ReadonlyJSONValue>(
    options: ListOptions,
    schema: valita.Type<T>,
  ): Promise<Map<string, T>> {
    const {prefix, start, limit} = options;
    const startKey = start?.key;
    const exclusive = start?.exclusive;

    // if the caller specified a limit, and we have local deletes, adjust
    // how many we fetch from the underlying storage.
    let adjustedLimit = limit;
    if (adjustedLimit !== undefined) {
      let deleted = 0;
      for (const [, {value, dirty}] of this.#cache.entries()) {
        if (dirty && value === undefined) {
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
        v.dirty &&
        (!prefix || k.startsWith(prefix)) &&
        (!startKey ||
          (exclusive
            ? compareUTF8(k, startKey) > 0
            : compareUTF8(k, startKey) >= 0))
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
