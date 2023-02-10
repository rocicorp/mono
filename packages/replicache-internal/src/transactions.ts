import type {LogContext} from '@rocicorp/logger';
import {greaterThan} from 'compare-utf8';
import type {JSONValue, ReadonlyJSONValue} from './json';
import {
  isScanIndexOptions,
  KeyTypeForScanOptions,
  ScanIndexOptions,
  ScanOptions,
  toDbScanOptions,
} from './scan-options';
import {fromKeyForIndexScanInternal, ScanResultImpl} from './scan-iterator';
import type {ScanResult} from './scan-iterator';
import {throwIfClosed} from './transaction-closed-error';
import * as db from './db/mod';
import * as sync from './sync/mod';
import type {Hash} from './hash';
import type {ScanSubscriptionInfo} from './subscriptions';
import type {ScanNoIndexOptions} from './mod.js';
import {decodeIndexKey, IndexKey} from './db/index.js';
import {
  toInternalValue,
  InternalValue,
  ToInternalValueReason,
  fromInternalValue,
  FromInternalValueReason,
} from './internal-value.js';

/**
 * ReadTransactions are used with [[Replicache.query]] and
 * [[Replicache.subscribe]] and allows read operations on the
 * database.
 */
export interface ReadTransaction {
  readonly clientID: string;

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
   * Gets many values from the database. This returns a [[ScanResult]] which
   * implements `AsyncIterable`. It also has methods to iterate over the
   * [[ScanResult.keys|keys]] and [[ScanResult.entries|entries]].
   *
   * If `options` has an `indexName`, then this does a scan over an index with
   * that name. A scan over an index uses a tuple for the key consisting of
   * `[secondary: string, primary: string]`.
   *
   * If the [[ScanResult]] is used after the `ReadTransaction` has been closed
   * it will throw a [[TransactionClosedError]].
   *
   * Important: The returned JSON is readonly and should not be modified. This
   * is only enforced statically by TypeScript and there are no runtime checks
   * for performance reasons. If you mutate the return value you will get
   * undefined behavior.
   */
  scan(): ScanResult<string, ReadonlyJSONValue>;

  /**
   * Gets many values from the database. This returns a [[ScanResult]] which
   * implements `AsyncIterable`. It also has methods to iterate over the
   * [[ScanResult.keys|keys]] and [[ScanResult.entries|entries]].
   *
   * If `options` has an `indexName`, then this does a scan over an index with
   * that name. A scan over an index uses a tuple for the key consisting of
   * `[secondary: string, primary: string]`.
   *
   * If the [[ScanResult]] is used after the `ReadTransaction` has been closed
   * it will throw a [[TransactionClosedError]].
   *
   * Important: The returned JSON is readonly and should not be modified. This
   * is only enforced statically by TypeScript and there are no runtime checks
   * for performance reasons. If you mutate the return value you will get
   * undefined behavior.
   */
  scan<Options extends ScanOptions>(
    options?: Options,
  ): ScanResult<KeyTypeForScanOptions<Options>, ReadonlyJSONValue>;
}

let transactionIDCounter = 0;

export class ReadTransactionImpl<
  Value extends ReadonlyJSONValue = ReadonlyJSONValue,
> implements ReadTransaction
{
  readonly clientID: string;
  readonly dbtx: db.Read;
  protected readonly _lc: LogContext;

  constructor(
    clientID: string,
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

  async get(key: string): Promise<Value | undefined> {
    throwIfClosed(this.dbtx);
    const v = await this.dbtx.get(key);
    return v !== undefined
      ? (fromInternalValue(
          v,
          FromInternalValueReason.ReadTransactionGet,
        ) as Value)
      : undefined;
  }

  async has(key: string): Promise<boolean> {
    throwIfClosed(this.dbtx);
    return this.dbtx.has(key);
  }

  async isEmpty(): Promise<boolean> {
    throwIfClosed(this.dbtx);
    return this.dbtx.isEmpty();
  }

  scan(): ScanResult<string, Value>;
  scan<Options extends ScanOptions>(
    options?: Options,
  ): ScanResult<KeyTypeForScanOptions<Options>, Value>;
  scan<Options extends ScanOptions>(
    options?: Options,
  ): ScanResult<KeyTypeForScanOptions<Options>, Value> {
    return scan(options, this.dbtx, noop);
  }
}

function noop(_: unknown): void {
  // empty
}

function scan<Options extends ScanOptions, Value>(
  options: Options | undefined,
  dbRead: db.Read,
  onLimitKey: (inclusiveLimitKey: string) => void,
): ScanResult<KeyTypeForScanOptions<Options>, Value> {
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

  scan(): ScanResult<string, ReadonlyJSONValue>;
  scan<Options extends ScanOptions>(
    options?: Options,
  ): ScanResult<KeyTypeForScanOptions<Options>, ReadonlyJSONValue>;
  scan<Options extends ScanOptions>(
    options?: Options,
  ): ScanResult<KeyTypeForScanOptions<Options>, ReadonlyJSONValue> {
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
 * [[ReplicacheOptions.mutators]] and allows read and write operations on the
 * database.
 */
export interface WriteTransaction extends ReadTransaction {
  /**
   * Sets a single `value` in the database. The `value` will be encoded using
   * `JSON.stringify`.
   */
  put(key: string, value: JSONValue): Promise<void>;

  /**
   * Removes a `key` and its value from the database. Returns `true` if there was a
   * `key` to remove.
   */
  del(key: string): Promise<boolean>;

  /**
   * Overrides [[ReadTransaction.get]] to return a mutable [[JSONValue]].
   */
  get(key: string): Promise<JSONValue | undefined>;

  /**
   * Overrides [[ReadTransaction.scan]] to return a mutable [[JSONValue]].
   */
  scan(): ScanResult<string, JSONValue>;
  scan<Options extends ScanOptions>(
    options?: Options,
  ): ScanResult<KeyTypeForScanOptions<Options>, JSONValue>;
}

export class WriteTransactionImpl
  extends ReadTransactionImpl<JSONValue>
  implements WriteTransaction
{
  // use `declare` to specialize the type.
  declare readonly dbtx: db.Write;

  constructor(
    clientID: string,
    dbWrite: db.Write,
    lc: LogContext,
    rpcName = 'openWriteTransaction',
  ) {
    super(clientID, dbWrite, lc, rpcName);
  }

  async get(key: string): Promise<JSONValue | undefined> {
    throwIfClosed(this.dbtx);
    const v = await this.dbtx.get(key);
    return v === undefined
      ? undefined
      : (fromInternalValue(
          v,
          FromInternalValueReason.WriteTransactionGet,
        ) as JSONValue);
  }

  async put(key: string, value: JSONValue): Promise<void> {
    throwIfClosed(this.dbtx);
    const internalValue = toInternalValue(
      value,
      ToInternalValueReason.WriteTransactionPut,
    );
    await this.dbtx.put(this._lc, key, internalValue);
  }

  async del(key: string): Promise<boolean> {
    throwIfClosed(this.dbtx);
    return await this.dbtx.del(this._lc, key);
  }

  async commit(
    generateDiffs: boolean,
    commitDagWrite = true,
  ): Promise<[Hash, sync.DiffsMap]> {
    const txn = this.dbtx;
    throwIfClosed(txn);

    // this auto updating of heads on commit needs to be changed
    const headName = txn.isRebase()
      ? sync.SYNC_HEAD_NAME
      : db.DEFAULT_HEAD_NAME;
    return await txn.commitWithDiffs(headName, generateDiffs, commitDagWrite);
  }
}

interface IndexTransaction extends ReadTransaction {
  /**
   * Creates a persistent secondary index in Replicache which can be used with
   * scan.
   *
   * If the named index already exists with the same definition this returns
   * success immediately. If the named index already exists, but with a
   * different definition an error is thrown.
   */
  createIndex(def: CreateIndexDefinition): Promise<void>;

  /**
   * Drops an index previously created with [[createIndex]].
   */
  dropIndex(name: string): Promise<void>;
}

/**
 * The definition of an index. This is used with
 * [[Replicache.createIndex|createIndex]] when creating indexes.
 */
export interface CreateIndexDefinition {
  /** The name of the index. This is used when you [[ReadTransaction.scan|scan]] over an index. */
  name: string;

  /**
   * The prefix, if any, to limit the index over. If not provided the values of
   * all keys are indexed.
   */
  prefix?: string;

  /**
   * A [JSON Pointer](https://tools.ietf.org/html/rfc6901) pointing at the sub
   * value inside each value to index over.
   *
   * For example, one might index over users' ages like so:
   * `createIndex({name: 'usersByAge', keyPrefix: '/user/', jsonPointer: '/age'})`
   */
  jsonPointer: string;

  /**
   * If `true`, indexing empty values will not emit a warning.  Defaults to `false`.
   */
  allowEmpty?: boolean;
}

export class IndexTransactionImpl
  extends WriteTransactionImpl
  implements IndexTransaction
{
  constructor(clientID: string, dbWrite: db.Write, lc: LogContext) {
    super(clientID, dbWrite, lc, 'openIndexTransaction');
  }

  async createIndex(options: CreateIndexDefinition): Promise<void> {
    throwIfClosed(this.dbtx);
    await this.dbtx.createIndex(
      this._lc,
      options.name,
      options.prefix ?? '',
      options.jsonPointer,
      options.allowEmpty ?? false,
    );
  }

  async dropIndex(name: string): Promise<void> {
    throwIfClosed(this.dbtx);
    await this.dbtx.dropIndex(name);
  }
}

type Entry<Key, Value> = readonly [key: Key, value: Value];

type IndexKeyEntry<Value> = Entry<IndexKey, Value>;

type StringKeyEntry<Value> = Entry<string, Value>;

export type EntryForOptions<Options extends ScanOptions> =
  Options extends ScanIndexOptions
    ? IndexKeyEntry<InternalValue>
    : StringKeyEntry<InternalValue>;

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

function makeScanResultFromScanIteratorInternal<
  Options extends ScanOptions,
  Value,
>(
  iter: AsyncIterable<EntryForOptions<Options>>,
  options: Options,
  dbRead: db.Read,
  onLimitKey: (inclusiveLimitKey: string) => void,
): ScanResult<KeyTypeForScanOptions<Options>, Value> {
  return new ScanResultImpl(iter, options, dbRead, onLimitKey);
}

async function* getScanIteratorForIndexMap(
  dbRead: db.Read,
  options: ScanIndexOptions,
): AsyncIterable<IndexKeyEntry<InternalValue>> {
  const map = dbRead.getMapForIndex(options.indexName);
  for await (const entry of map.scan(fromKeyForIndexScanInternal(options))) {
    // No need to clone the value since it will be cloned as needed by
    // ScanResultImpl.
    yield [decodeIndexKey(entry[0]), entry[1]];
  }
}
