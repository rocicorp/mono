import type {
  DurableObjectListOptions,
  DurableObjectStorage,
} from '@cloudflare/workers-types';
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
} from './data.js';
import {batchScan, scan} from './scan-storage.js';
import type {ListOptions, Storage} from './storage.js';

// DurableObjects has a lot of clever optimizations for simplifying the
// concurrency semantics of a single application writing to shared state:
// https://blog.cloudflare.com/durable-objects-easy-fast-correct-choose-three/
//
// However, in zero-cache a Durable Object is used to run multiple independent
// services, along the lines of a VM running containers, each of which
// may use Durable Object storage within its own namespace/prefix.
//
// In this setup, the input / output gate logic of Durable Objects unnecessarily
// introduce delays from activity of unrelated services. They are thus disabled for
// zero, which instead relies on standard per-service locking for concurrency within
// a service, and the WriteCache for isolated atomic transactions.
const ioOptions = {
  // Disables input gates, which would otherwise cause the reads from one service to
  // block the reads of another service.
  allowConcurrency: true,
  // Disables output gates, which would otherwise cause the writes from one service
  // to block outgoing I/O of other services.
  allowUnconfirmed: true,
} as const;

/**
 * Implements the Storage interface in terms of the database.
 */
export class DurableStorage implements Storage {
  readonly #durable: DurableObjectStorage;

  constructor(durable: DurableObjectStorage) {
    this.#durable = durable;
  }

  put<T extends ReadonlyJSONValue>(key: string, value: T): Promise<void> {
    return putEntry(this.#durable, key, value, ioOptions);
  }

  putEntries<T extends ReadonlyJSONValue>(
    entries: Record<string, T>,
  ): Promise<void> {
    return this.#durable.put(entries, ioOptions);
  }

  del(key: string): Promise<void> {
    return delEntry(this.#durable, key, ioOptions);
  }

  delEntries(keys: string[]): Promise<void> {
    return this.#durable.delete(keys, ioOptions).then(() => undefined);
  }

  get<T extends ReadonlyJSONValue>(
    key: string,
    schema: valita.Type<T>,
  ): Promise<T | undefined> {
    return getEntry(this.#durable, key, schema, ioOptions);
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
      return getEntries(this.#durable, keys, schema, ioOptions);
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
        getEntries(this.#durable, partition, schema, ioOptions),
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
    allowConcurrency: ioOptions.allowConcurrency,
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
