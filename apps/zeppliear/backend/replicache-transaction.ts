import type {
  DeepReadonly,
  IndexKey,
  JSONValue,
  ReadTransaction,
  ReadonlyJSONValue,
  ScanIndexOptions,
  ScanNoIndexOptions,
  ScanOptions,
  ScanResult,
  TransactionEnvironment,
  TransactionReason,
  WriteTransaction,
} from 'replicache';
import {delEntries, getEntry, putEntries} from './data';
import type {Executor} from './pg';

export type SyncOrderFn = (
  tx: ReadTransaction,
  entry: [key: string, value: JSONValue],
) => Promise<string>;

/**
 * Implements Replicache's WriteTransaction interface in terms of a Postgres
 * transaction.
 */
export class ReplicacheTransaction implements WriteTransaction {
  readonly #spaceID: string;
  readonly #clientID: string;
  readonly #version: number;
  readonly #mutationID: number;
  readonly #executor: Executor;
  readonly #getSyncOrder: SyncOrderFn;
  readonly #cache: Map<string, {value: JSONValue | undefined; dirty: boolean}> =
    new Map();

  constructor(
    executor: Executor,
    spaceID: string,
    clientID: string,
    version: number,
    mutationId: number,
    getSyncOrder: SyncOrderFn,
  ) {
    this.#spaceID = spaceID;
    this.#clientID = clientID;
    this.#version = version;
    this.#mutationID = mutationId;
    this.#executor = executor;
    this.#getSyncOrder = getSyncOrder;
  }

  get reason(): TransactionReason {
    return 'authoritative';
  }

  get environment(): TransactionEnvironment {
    return 'server';
  }

  get location() {
    return this.environment;
  }

  get mutationID(): number {
    return this.#mutationID;
  }

  get clientID(): string {
    return this.#clientID;
  }

  async put(key: string, value: JSONValue): Promise<void> {
    await this.set(key, value);
  }
  set(key: string, value: JSONValue): Promise<void> {
    this.#cache.set(key, {value, dirty: true});
    return Promise.resolve();
  }
  async del(key: string): Promise<boolean> {
    const had = await this.has(key);
    this.#cache.set(key, {value: undefined, dirty: true});
    return had;
  }
  async get(key: string): Promise<JSONValue | undefined> {
    const entry = this.#cache.get(key);
    if (entry) {
      return entry.value;
    }
    const value = await getEntry(this.#executor, this.#spaceID, key);
    this.#cache.set(key, {value, dirty: false});
    return value;
  }
  async has(key: string): Promise<boolean> {
    const val = await this.get(key);
    return val !== undefined;
  }

  // TODO!
  isEmpty(): Promise<boolean> {
    throw new Error('Method isEmpty not implemented');
  }

  scan(options: ScanIndexOptions): ScanResult<IndexKey, ReadonlyJSONValue>;
  scan(options?: ScanNoIndexOptions): ScanResult<string, ReadonlyJSONValue>;
  scan(options?: ScanOptions): ScanResult<IndexKey | string, ReadonlyJSONValue>;
  scan<V extends ReadonlyJSONValue>(
    options: ScanIndexOptions,
  ): ScanResult<IndexKey, DeepReadonly<V>>;
  scan<V extends ReadonlyJSONValue>(
    options?: ScanNoIndexOptions,
  ): ScanResult<string, DeepReadonly<V>>;
  scan<V extends ReadonlyJSONValue>(
    options?: ScanOptions,
  ): ScanResult<IndexKey | string, DeepReadonly<V>>;
  scan(): ScanResult<IndexKey | string, ReadonlyJSONValue> {
    throw new Error('Method scan not implemented.');
  }

  async flush(): Promise<void> {
    const dirtyEntries = [...this.#cache.entries()].filter(
      ([, {dirty}]) => dirty,
    );
    const entriesToPut: [string, JSONValue, string][] = [];
    for (const dirtyEntry of dirtyEntries) {
      if (dirtyEntry[1].value !== undefined) {
        entriesToPut.push([
          dirtyEntry[0],
          dirtyEntry[1].value,
          await this.#getSyncOrder(this, [dirtyEntry[0], dirtyEntry[1].value]),
        ]);
      }
    }
    const keysToDel = dirtyEntries
      .filter(([, {value}]) => value === undefined)
      .map(([key]) => key);
    await Promise.all([
      delEntries(this.#executor, this.#spaceID, keysToDel, this.#version),
      putEntries(this.#executor, this.#spaceID, entriesToPut, this.#version),
    ]);
  }
}
