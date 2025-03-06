import type {LogContext} from '@rocicorp/logger';
import {greaterThan} from 'compare-utf8';
import type {JSONValue, ReadonlyJSONValue} from '../../shared/src/json.ts';
import {type IndexKey, decodeIndexKey} from './db/index.ts';
import type {Read} from './db/read.ts';
import type {Write} from './db/write.ts';
import {deepFreeze} from './frozen-json.ts';
import type {IndexDefinition} from './index-defs.ts';
import type {ScanResult} from './scan-iterator.ts';
import {ScanResultImpl, fromKeyForIndexScanInternal} from './scan-iterator.ts';
import {
  type KeyTypeForScanOptions,
  type ScanIndexOptions,
  type ScanNoIndexOptions,
  type ScanOptions,
  isScanIndexOptions,
  toDbScanOptions,
} from './scan-options.ts';
import type {ScanSubscriptionInfo} from './subscriptions.ts';
import type {ClientID} from './sync/ids.ts';
import {rejectIfClosed, throwIfClosed} from './transaction-closed-error.ts';

export type TransactionEnvironment = 'client' | 'server';
export type TransactionLocation = TransactionEnvironment;
export type TransactionReason = 'initial' | 'rebase' | 'authoritative';

/**
 * Basic deep readonly type. It works for {@link JSONValue}.
 */
export type DeepReadonly<T> = T extends
  | null
  | boolean
  | string
  | number
  | undefined
  ? T
  : DeepReadonlyObject<T>;

export type DeepReadonlyObject<T> = {
  readonly [K in keyof T]: DeepReadonly<T[K]>;
};

/**
 * ReadTransactions are used with {@link Replicache.query} and
 * {@link Replicache.subscribe} and allows read operations on the
 * database.
 */
export interface ReadTransaction {
  readonly clientID: ClientID;
  /** @deprecated Use {@link ReadTransaction.location} instead. */
  readonly environment: TransactionLocation;
  readonly location: TransactionLocation;

  /**
   * Get a single value from the database. If the `key` is not present this
   * returns `undefined`.
   *
   * Important: The returned JSON is readonly and should not be modified. This
   * is only enforced statically by TypeScript and there are no runtime checks
   * for performance reasons. If you mutate the return value you will get
   * undefined behavior.
   */

  get(key: string): Promise<ReadonlyJSONValue | undefined>;
  get<T extends JSONValue>(key: string): Promise<DeepReadonly<T> | undefined>;

  /** Determines if a single `key` is present in the database. */
  has(key: string): Promise<boolean>;

  /** Whether the database is empty. */
  isEmpty(): Promise<boolean>;

  /**
   * Gets many values from the database. This returns a {@link ScanResult} which
   * implements `AsyncIterable`. It also has methods to iterate over the
   * {@link ScanResult.keys | keys} and {@link ScanResult.entries | entries}.
   *
   * If `options` has an `indexName`, then this does a scan over an index with
   * that name. A scan over an index uses a tuple for the key consisting of
   * `[secondary: string, primary: string]`.
   *
   * If the {@link ScanResult} is used after the `ReadTransaction` has been closed
   * it will throw a {@link TransactionClosedError}.
   *
   * Important: The returned JSON is readonly and should not be modified. This
   * is only enforced statically by TypeScript and there are no runtime checks
   * for performance reasons. If you mutate the return value you will get
   * undefined behavior.
   */
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
}

let transactionIDCounter = 0;

export class ReadTransactionImpl implements ReadTransaction {
  readonly clientID: ClientID;
  readonly dbtx: Read;
  protected readonly _lc: LogContext;

  /**
   * The location in which this transaction is being used. This is either `client` or `server`.
   */
  readonly location: TransactionLocation;
  /** @deprecated Use {@link ReadTransaction.location} instead. */
  readonly environment: TransactionLocation;

  constructor(
    clientID: ClientID,
    dbRead: Read,
    lc: LogContext,
    rpcName = 'openReadTransaction',
  ) {
    this.clientID = clientID;
    this.dbtx = dbRead;
    this._lc = lc
      .withContext(rpcName)
      .withContext('txid', transactionIDCounter++);
    this.environment = 'client';
    this.location = 'client';
  }

  get(key: string): Promise<ReadonlyJSONValue | undefined>;
  get<V extends JSONValue>(key: string): Promise<DeepReadonly<V> | undefined> {
    return (
      rejectIfClosed(this.dbtx) ||
      (this.dbtx.get(key) as Promise<DeepReadonly<V> | undefined>)
    );
  }

  // eslint-disable-next-line require-await
  async has(key: string): Promise<boolean> {
    throwIfClosed(this.dbtx);
    return this.dbtx.has(key);
  }

  // eslint-disable-next-line require-await
  async isEmpty(): Promise<boolean> {
    throwIfClosed(this.dbtx);
    return this.dbtx.isEmpty();
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

  scan(
    options?: ScanOptions,
  ): ScanResult<IndexKey | string, ReadonlyJSONValue> {
    return scan(options, this.dbtx, noop);
  }
}

function noop(_: unknown): void {
  // empty
}

function scan<Options extends ScanOptions, V extends JSONValue>(
  options: Options | undefined,
  dbRead: Read,
  onLimitKey: (inclusiveLimitKey: string) => void,
): ScanResult<KeyTypeForScanOptions<Options>, V> {
  const iter = getScanIterator<Options, V>(dbRead, options);
  return makeScanResultFromScanIteratorInternal(
    iter,
    options ?? ({} as Options),
    dbRead,
    onLimitKey,
  );
}

// An implementation of ReadTransaction that keeps track of `keys` and `scans`
// for use with Subscriptions.
export class SubscriptionTransactionWrapper implements ReadTransaction {
  readonly #keys: Set<string> = new Set();
  readonly #scans: ScanSubscriptionInfo[] = [];
  readonly #tx: ReadTransactionImpl;

  constructor(tx: ReadTransactionImpl) {
    this.#tx = tx;
  }

  get environment(): TransactionLocation {
    return this.#tx.location;
  }

  get location(): TransactionLocation {
    return this.#tx.location;
  }

  get clientID(): string {
    return this.#tx.clientID;
  }

  isEmpty(): Promise<boolean> {
    // Any change to the subscription requires rerunning it.
    this.#scans.push({options: {}});
    return this.#tx.isEmpty();
  }

  get(key: string): Promise<ReadonlyJSONValue | undefined>;
  get<T extends JSONValue>(key: string): Promise<DeepReadonly<T> | undefined> {
    this.#keys.add(key);
    return this.#tx.get(key) as Promise<DeepReadonly<T> | undefined>;
  }

  has(key: string): Promise<boolean> {
    this.#keys.add(key);
    return this.#tx.has(key);
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

  scan(
    options?: ScanOptions,
  ): ScanResult<IndexKey | string, ReadonlyJSONValue> {
    const scanInfo: ScanSubscriptionInfo = {
      options: toDbScanOptions(options),
      inclusiveLimitKey: undefined,
    };
    this.#scans.push(scanInfo);
    return scan(options, this.#tx.dbtx, inclusiveLimitKey => {
      scanInfo.inclusiveLimitKey = inclusiveLimitKey;
    });
  }

  get keys(): ReadonlySet<string> {
    return this.#keys;
  }

  get scans(): ScanSubscriptionInfo[] {
    return this.#scans;
  }
}

/**
 * WriteTransactions are used with *mutators* which are registered using
 * {@link ReplicacheOptions.mutators} and allows read and write operations on the
 * database.
 */
export interface WriteTransaction<TZeroData = unknown> extends ReadTransaction {
  /**
   * The ID of the mutation that is being applied.
   */
  readonly mutationID: number;

  /**
   * The reason for the transaction. This can be `initial`, `rebase` or `authoriative`.
   */
  readonly reason: TransactionReason;

  readonly zeroData: TZeroData;

  /**
   * Sets a single `value` in the database. The value will be frozen (using
   * `Object.freeze`) in debug mode.
   */
  set(key: string, value: ReadonlyJSONValue): Promise<void>;

  /**
   * @deprecated Use {@link WriteTransaction.set} instead.
   */
  put(key: string, value: ReadonlyJSONValue): Promise<void>;

  /**
   * Removes a `key` and its value from the database. Returns `true` if there was a
   * `key` to remove.
   */
  del(key: string): Promise<boolean>;
}

export class WriteTransactionImpl<TZeroData = unknown>
  extends ReadTransactionImpl
  implements WriteTransaction<TZeroData>
{
  // use `declare` to specialize the type.
  declare readonly dbtx: Write;
  readonly reason: TransactionReason;
  readonly mutationID: number;
  readonly zeroData: TZeroData;

  constructor(
    clientID: ClientID,
    mutationID: number,
    reason: TransactionReason,
    zeroData: TZeroData,
    dbWrite: Write,
    lc: LogContext,
    rpcName = 'openWriteTransaction',
  ) {
    super(clientID, dbWrite, lc, rpcName);
    this.mutationID = mutationID;
    this.reason = reason;
    this.zeroData = zeroData;
  }

  put(key: string, value: ReadonlyJSONValue): Promise<void> {
    return this.set(key, value);
  }

  async set(key: string, value: ReadonlyJSONValue): Promise<void> {
    throwIfClosed(this.dbtx);
    await this.dbtx.put(this._lc, key, deepFreeze(value));
  }

  del(key: string): Promise<boolean> {
    return rejectIfClosed(this.dbtx) ?? this.dbtx.del(this._lc, key);
  }
}

export type CreateIndexDefinition = IndexDefinition & {name: string};

type Entry<Key, Value> = readonly [key: Key, value: Value];

type IndexKeyEntry<Value> = Entry<IndexKey, Value>;

type StringKeyEntry<Value> = Entry<string, Value>;

export type EntryForOptions<
  Options extends ScanOptions,
  V,
> = Options extends ScanIndexOptions ? IndexKeyEntry<V> : StringKeyEntry<V>;

function getScanIterator<Options extends ScanOptions, V>(
  dbRead: Read,
  options: Options | undefined,
): AsyncIterable<EntryForOptions<Options, V>> {
  if (options && isScanIndexOptions(options)) {
    return getScanIteratorForIndexMap(dbRead, options) as AsyncIterable<
      EntryForOptions<Options, V>
    >;
  }

  return dbRead.map.scan(fromKeyForNonIndexScan(options)) as AsyncIterable<
    EntryForOptions<Options, V>
  >;
}

export function fromKeyForNonIndexScan(
  options: ScanNoIndexOptions | undefined,
): string {
  if (!options) {
    return '';
  }

  const {prefix = '', start} = options;
  if (start && greaterThan(start.key, prefix)) {
    return start.key;
  }
  return prefix;
}

function makeScanResultFromScanIteratorInternal<
  Options extends ScanOptions,
  V extends JSONValue,
>(
  iter: AsyncIterable<EntryForOptions<Options, V>>,
  options: Options,
  dbRead: Read,
  onLimitKey: (inclusiveLimitKey: string) => void,
): ScanResult<KeyTypeForScanOptions<Options>, V> {
  return new ScanResultImpl(iter, options, dbRead, onLimitKey);
}

async function* getScanIteratorForIndexMap(
  dbRead: Read,
  options: ScanIndexOptions,
): AsyncIterable<IndexKeyEntry<ReadonlyJSONValue>> {
  const map = dbRead.getMapForIndex(options.indexName);
  for await (const entry of map.scan(fromKeyForIndexScanInternal(options))) {
    yield [decodeIndexKey(entry[0]), entry[1]];
  }
}
