import type {LogContext} from '@rocicorp/logger';
import {greaterThan} from 'compare-utf8';
import {ReadonlyJSONValue, deepFreeze} from './json.js';
import {
  isScanIndexOptions,
  KeyTypeForScanOptions,
  ScanIndexOptions,
  ScanOptions,
  toDbScanOptions,
} from './scan-options.js';
import {fromKeyForIndexScanInternal, ScanResultImpl} from './scan-iterator.js';
import type {ScanResult} from './scan-iterator.js';
import {throwIfClosed} from './transaction-closed-error.js';
import type * as db from './db/mod.js';
import type {ScanSubscriptionInfo} from './subscriptions.js';
import type {ClientID, ScanNoIndexOptions} from './mod.js';
import {decodeIndexKey, IndexKey} from './db/index.js';
import type {IndexDefinition} from './index-defs.js';

/**
 * ReadTransactions are used with {@link Replicache.query} and
 * {@link Replicache.subscribe} and allows read operations on the
 * database.
 */
export interface ReadTransaction {
  readonly clientID: ClientID;

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
  scan(): ScanResult<string>;

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
  scan<Options extends ScanOptions>(
    options?: Options,
  ): ScanResult<KeyTypeForScanOptions<Options>>;
}

let transactionIDCounter = 0;

export class ReadTransactionImpl implements ReadTransaction {
  readonly clientID: ClientID;
  readonly dbtx: db.Read;
  protected readonly _lc: LogContext;

  constructor(
    clientID: ClientID,
    dbRead: db.Read,
    lc: LogContext,
    rpcName = 'openReadTransaction',
  ) {
    this.clientID = clientID;
    this.dbtx = dbRead;
    this._lc = lc
      .addContext(rpcName)
      .addContext('txid', transactionIDCounter++);
  }

  // eslint-disable-next-line require-await
  async get(key: string): Promise<ReadonlyJSONValue | undefined> {
    throwIfClosed(this.dbtx);
    return this.dbtx.get(key);
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

  scan(): ScanResult<string>;
  scan<Options extends ScanOptions>(
    options?: Options,
  ): ScanResult<KeyTypeForScanOptions<Options>>;
  scan<Options extends ScanOptions>(
    options?: Options,
  ): ScanResult<KeyTypeForScanOptions<Options>> {
    return scan(options, this.dbtx, noop);
  }
}

function noop(_: unknown): void {
  // empty
}

function scan<Options extends ScanOptions>(
  options: Options | undefined,
  dbRead: db.Read,
  onLimitKey: (inclusiveLimitKey: string) => void,
): ScanResult<KeyTypeForScanOptions<Options>> {
  const iter = getScanIterator<Options>(dbRead, options);
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
  private readonly _keys: Set<string> = new Set();
  private readonly _scans: ScanSubscriptionInfo[] = [];
  private readonly _tx: ReadTransactionImpl;

  constructor(tx: ReadTransactionImpl) {
    this._tx = tx;
  }

  get clientID(): string {
    return this._tx.clientID;
  }

  isEmpty(): Promise<boolean> {
    // Any change to the subscription requires rerunning it.
    this._scans.push({options: {}});
    return this._tx.isEmpty();
  }

  get(key: string): Promise<ReadonlyJSONValue | undefined> {
    this._keys.add(key);
    return this._tx.get(key);
  }

  has(key: string): Promise<boolean> {
    this._keys.add(key);
    return this._tx.has(key);
  }

  scan(): ScanResult<string>;
  scan<Options extends ScanOptions>(
    options?: Options,
  ): ScanResult<KeyTypeForScanOptions<Options>>;
  scan<Options extends ScanOptions>(
    options?: Options,
  ): ScanResult<KeyTypeForScanOptions<Options>> {
    const scanInfo: ScanSubscriptionInfo = {
      options: toDbScanOptions(options),
      inclusiveLimitKey: undefined,
    };
    this._scans.push(scanInfo);
    return scan(options, this._tx.dbtx, inclusiveLimitKey => {
      scanInfo.inclusiveLimitKey = inclusiveLimitKey;
    });
  }

  get keys(): ReadonlySet<string> {
    return this._keys;
  }

  get scans(): ScanSubscriptionInfo[] {
    return this._scans;
  }
}

/**
 * WriteTransactions are used with *mutators* which are registered using
 * {@link ReplicacheOptions.mutators} and allows read and write operations on the
 * database.
 */
export interface WriteTransaction extends ReadTransaction {
  /**
   * Sets a single `value` in the database. The value will be frozen (using
   * `Object.freeze`) in debug mode.
   */
  put(key: string, value: ReadonlyJSONValue): Promise<void>;

  /**
   * Removes a `key` and its value from the database. Returns `true` if there was a
   * `key` to remove.
   */
  del(key: string): Promise<boolean>;
}

export class WriteTransactionImpl
  extends ReadTransactionImpl
  implements WriteTransaction
{
  // use `declare` to specialize the type.
  declare readonly dbtx: db.Write;

  constructor(
    clientID: ClientID,
    dbWrite: db.Write,
    lc: LogContext,
    rpcName = 'openWriteTransaction',
  ) {
    super(clientID, dbWrite, lc, rpcName);
  }

  async put(key: string, value: ReadonlyJSONValue): Promise<void> {
    throwIfClosed(this.dbtx);
    await this.dbtx.put(this._lc, key, deepFreeze(value));
  }

  async del(key: string): Promise<boolean> {
    throwIfClosed(this.dbtx);
    return await this.dbtx.del(this._lc, key);
  }
}

export type CreateIndexDefinition = IndexDefinition & {name: string};

type Entry<Key, Value> = readonly [key: Key, value: Value];

type IndexKeyEntry<Value> = Entry<IndexKey, Value>;

type StringKeyEntry<Value> = Entry<string, Value>;

export type EntryForOptions<Options extends ScanOptions> =
  Options extends ScanIndexOptions
    ? IndexKeyEntry<ReadonlyJSONValue>
    : StringKeyEntry<ReadonlyJSONValue>;

function getScanIterator<Options extends ScanOptions>(
  dbRead: db.Read,
  options: Options | undefined,
): AsyncIterable<EntryForOptions<Options>> {
  if (options && isScanIndexOptions(options)) {
    return getScanIteratorForIndexMap(dbRead, options) as AsyncIterable<
      EntryForOptions<Options>
    >;
  }

  return dbRead.map.scan(fromKeyForNonIndexScan(options)) as AsyncIterable<
    EntryForOptions<Options>
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

function makeScanResultFromScanIteratorInternal<Options extends ScanOptions>(
  iter: AsyncIterable<EntryForOptions<Options>>,
  options: Options,
  dbRead: db.Read,
  onLimitKey: (inclusiveLimitKey: string) => void,
): ScanResult<KeyTypeForScanOptions<Options>> {
  return new ScanResultImpl(iter, options, dbRead, onLimitKey);
}

async function* getScanIteratorForIndexMap(
  dbRead: db.Read,
  options: ScanIndexOptions,
): AsyncIterable<IndexKeyEntry<ReadonlyJSONValue>> {
  const map = dbRead.getMapForIndex(options.indexName);
  for await (const entry of map.scan(fromKeyForIndexScanInternal(options))) {
    yield [decodeIndexKey(entry[0]), entry[1]];
  }
}
