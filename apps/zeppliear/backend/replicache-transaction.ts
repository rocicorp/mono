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
} from "replicache";
import { delEntries, getEntry, putEntries } from "./data";
import type { Executor } from "./pg";

export type SyncOrderFn = (
  tx: ReadTransaction,
  entry: [key: string, value: JSONValue]
) => Promise<string>;

/**
 * Implements Replicache's WriteTransaction interface in terms of a Postgres
 * transaction.
 */
export class ReplicacheTransaction implements WriteTransaction {
  private readonly _spaceID: string;
  private readonly _clientID: string;
  private readonly _version: number;
  private readonly _mutationID: number;
  private readonly _executor: Executor;
  private readonly _getSyncOrder: SyncOrderFn;
  private readonly _cache: Map<
    string,
    { value: JSONValue | undefined; dirty: boolean }
  > = new Map();

  constructor(
    executor: Executor,
    spaceID: string,
    clientID: string,
    version: number,
    mutationId: number,
    getSyncOrder: SyncOrderFn
  ) {
    this._spaceID = spaceID;
    this._clientID = clientID;
    this._version = version;
    this._mutationID = mutationId;
    this._executor = executor;
    this._getSyncOrder = getSyncOrder;
  }

  get reason(): TransactionReason {
    return "authoritative";
  }

  get environment(): TransactionEnvironment {
    return "server";
  }

  get location() {
    return this.environment;
  }

  get mutationID(): number {
    return this._mutationID;
  }

  get clientID(): string {
    return this._clientID;
  }

  async put(key: string, value: JSONValue): Promise<void> {
    await this.set(key, value);
  }
  async set(key: string, value: JSONValue): Promise<void> {
    this._cache.set(key, { value, dirty: true });
  }
  async del(key: string): Promise<boolean> {
    const had = await this.has(key);
    this._cache.set(key, { value: undefined, dirty: true });
    return had;
  }
  async get(key: string): Promise<JSONValue | undefined> {
    const entry = this._cache.get(key);
    if (entry) {
      return entry.value;
    }
    const value = await getEntry(this._executor, this._spaceID, key);
    this._cache.set(key, { value, dirty: false });
    return value;
  }
  async has(key: string): Promise<boolean> {
    const val = await this.get(key);
    return val !== undefined;
  }

  // TODO!
  async isEmpty(): Promise<boolean> {
    throw new Error("Method isEmpty not implemented");
  }

  scan(options: ScanIndexOptions): ScanResult<IndexKey, ReadonlyJSONValue>;
  scan(options?: ScanNoIndexOptions): ScanResult<string, ReadonlyJSONValue>;
  scan(options?: ScanOptions): ScanResult<IndexKey | string, ReadonlyJSONValue>;
  scan<V extends ReadonlyJSONValue>(
    options: ScanIndexOptions
  ): ScanResult<IndexKey, DeepReadonly<V>>;
  scan<V extends ReadonlyJSONValue>(
    options?: ScanNoIndexOptions
  ): ScanResult<string, DeepReadonly<V>>;
  scan<V extends ReadonlyJSONValue>(
    options?: ScanOptions
  ): ScanResult<IndexKey | string, DeepReadonly<V>>;
  scan(): ScanResult<IndexKey | string, ReadonlyJSONValue> {
    throw new Error("Method scan not implemented.");
  }

  async flush(): Promise<void> {
    const dirtyEntries = [...this._cache.entries()].filter(
      ([, { dirty }]) => dirty
    );
    const entriesToPut: [string, JSONValue, string][] = [];
    for (const dirtyEntry of dirtyEntries) {
      if (dirtyEntry[1].value !== undefined) {
        entriesToPut.push([
          dirtyEntry[0],
          dirtyEntry[1].value,
          await this._getSyncOrder(this, [dirtyEntry[0], dirtyEntry[1].value]),
        ]);
      }
    }
    const keysToDel = dirtyEntries
      .filter(([, { value }]) => value === undefined)
      .map(([key]) => key);
    await Promise.all([
      delEntries(this._executor, this._spaceID, keysToDel, this._version),
      putEntries(this._executor, this._spaceID, entriesToPut, this._version),
    ]);
  }
}
