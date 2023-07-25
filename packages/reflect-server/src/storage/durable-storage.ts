import {compareUTF8} from 'compare-utf8';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import type * as valita from 'shared/src/valita.js';
import {
  MAX_ENTRIES_TO_GET,
  delEntry,
  getEntries,
  getEntry,
  listEntries,
  putEntry,
} from '../db/data.js';
import {batchScan, scan} from './scan-storage.js';
import type {ListOptions, Storage} from './storage.js';

const baseAllowConcurrency = true;

// DurableObjects has a lot of clever optimizations we can take advantage of,
// but they require some thought as to whether they fit with what we are doing.
// These settings make DO behave more like a basic kv store and thus work
// better with our existing code.
// TODO: Evaluate these options and perhaps simplify our code by taking advantage.
const baseOptions = {
  // We already control currency with locks at a higher level in the game loop.
  allowConcurrency: baseAllowConcurrency,
} as const;

/**
 * Implements the Storage interface in terms of the database.
 */
export class DurableStorage implements Storage {
  #durable: DurableObjectStorage;
  readonly #baseOptions: Readonly<DurableObjectPutOptions>;

  constructor(durable: DurableObjectStorage, allowUnconfirmed = true) {
    this.#durable = durable;
    this.#baseOptions = {
      allowConcurrency: baseAllowConcurrency,
      allowUnconfirmed,
    };
  }

  put<T extends ReadonlyJSONValue>(key: string, value: T): Promise<void> {
    return putEntry(this.#durable, key, value, this.#baseOptions);
  }

  putEntries<T extends ReadonlyJSONValue>(
    entries: Record<string, T>,
  ): Promise<void> {
    return this.#durable.put(entries, this.#baseOptions);
  }

  del(key: string): Promise<void> {
    return delEntry(this.#durable, key, this.#baseOptions);
  }

  delEntries(keys: string[]): Promise<void> {
    return this.#durable.delete(keys, this.#baseOptions).then(() => undefined);
  }

  get<T extends ReadonlyJSONValue>(
    key: string,
    schema: valita.Type<T>,
  ): Promise<T | undefined> {
    return getEntry(this.#durable, key, schema, baseOptions);
  }

  /**
   * Fetches multiple entries from storage with as few reads as possible.
   * Reads of up to {@link MAX_ENTRIES_TO_GET} keys are done with a single
   * fetch, for which values are cross-consistent. Larger reads are split
   * into parallel fetches which may not necessarily be consistent with each
   * other. If consistency is required, the application must guarantee this
   * with its own locking scheme.
   */
  async getEntries<T extends ReadonlyJSONValue>(
    keys: string[],
    schema: valita.Type<T>,
  ): Promise<Map<string, T>> {
    // Simple case that does not require partitioning.
    if (keys.length <= MAX_ENTRIES_TO_GET) {
      return getEntries(this.#durable, keys, schema, baseOptions);
    }
    // Partition the keys in groups no larger than MAX_ENTRIES_TO_GET.
    const partitionedKeys = [];
    for (let start = 0; start < keys.length; ) {
      const end = Math.min(start + MAX_ENTRIES_TO_GET, keys.length + 1);
      partitionedKeys.push(keys.slice(start, end));
      start = end;
    }
    // Perform parallel getEntries()
    const partitionedEntries = await Promise.all(
      partitionedKeys.map(partition =>
        getEntries(this.#durable, partition, schema, baseOptions),
      ),
    );
    // Merge and sort to adhere to the sorted-key guarantee of Durable Object APIs.
    const entries = [];
    for (const partition of partitionedEntries) {
      entries.push(...[...partition]);
    }
    entries.sort(([keyA], [keyB]) => compareUTF8(keyA, keyB));
    return new Map(entries);
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

  list<T extends ReadonlyJSONValue>(
    options: ListOptions,
    schema: valita.Type<T>,
  ): Promise<Map<string, T>> {
    const doOptions = doListOptions(options);
    return listEntries(this.#durable, schema, doOptions);
  }

  deleteAll(): Promise<void> {
    return this.#durable.deleteAll();
  }

  flush(): Promise<void> {
    return this.#durable.sync();
  }
}

function doListOptions(opts: ListOptions): DurableObjectListOptions {
  const doOpts: DurableObjectListOptions = {
    allowConcurrency: baseAllowConcurrency,
  };

  if (opts.prefix !== undefined) {
    doOpts.prefix = opts.prefix;
  }
  if (opts.limit !== undefined) {
    doOpts.limit = opts.limit;
  }

  if (opts.start) {
    const {key, exclusive} = opts.start;
    if (exclusive) {
      doOpts.startAfter = key;
    } else {
      doOpts.start = key;
    }
  }
  return doOpts;
}
