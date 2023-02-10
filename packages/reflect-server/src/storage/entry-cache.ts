import { compareUTF8 } from "compare-utf8";
import type { JSONValue } from "replicache";
import type * as z from "superstruct";
import type { JSONType } from "../protocol/json.js";
import type { Patch } from "../protocol/poke.js";
import { superstructAssert } from "../util/superstruct.js";
import type { ListOptions, Storage } from "./storage.js";

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
  private _storage: Storage;
  private _cache: Map<string, { value?: JSONValue; dirty: boolean }> =
    new Map();

  constructor(storage: Storage) {
    this._storage = storage;
  }

  async put<T extends JSONValue>(key: string, value: T): Promise<void> {
    this._cache.set(key, { value, dirty: true });
  }
  async del(key: string): Promise<void> {
    this._cache.set(key, { value: undefined, dirty: true });
  }
  async get<T extends JSONValue>(
    key: string,
    schema: z.Struct<T>
  ): Promise<T | undefined> {
    const cached = this._cache.get(key);
    if (cached) {
      // We don't validate on cache hits partly for perf reasons and also
      // because we should have already validated with same schema during
      // initial read.
      return cached.value as T | undefined;
    }
    const value = await this._storage.get(key, schema);
    this._cache.set(key, { value, dirty: false });
    return value;
  }

  pending(): Patch {
    const res: Patch = [];
    for (const [key, { value, dirty }] of this._cache.entries()) {
      if (dirty) {
        if (value === undefined) {
          res.push({ op: "del", key });
        } else {
          res.push({ op: "put", key, value: value as JSONType });
        }
      }
    }
    return res;
  }

  async flush(): Promise<void> {
    await Promise.all(
      [...this._cache.entries()]
        // Destructure ALL the things
        .filter(([, { dirty }]) => dirty)
        .map(([k, { value }]) => {
          if (value === undefined) {
            return this._storage.del(k);
          } else {
            return this._storage.put(k, value);
          }
        })
    );
  }

  async list<T extends JSONValue>(
    options: ListOptions,
    schema: z.Struct<T>
  ): Promise<Map<string, T>> {
    const { prefix, start, limit } = options;
    const startKey = start?.key;
    const exclusive = start?.exclusive;

    // if the caller specified a limit, and we have local deletes, adjust
    // how many we fetch from the underlying storage.
    let adjustedLimit = limit;
    if (adjustedLimit !== undefined) {
      let deleted = 0;
      for (const [, { value, dirty }] of this._cache.entries()) {
        if (dirty && value === undefined) {
          deleted++;
        }
      }
      adjustedLimit += deleted;
    }

    const base = new Map(
      await this._storage.list({ ...options, limit: adjustedLimit }, schema)
    );

    // build a list of pending changes to overlay atop stored values
    const pending: [string, T | undefined][] = [];
    for (const entry of this._cache.entries()) {
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
          superstructAssert(v.value, schema);
          pending.push([k, v.value]);
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
