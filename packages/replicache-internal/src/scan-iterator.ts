import {deepClone, ReadonlyJSONValue} from './json';
import {Closed, throwIfClosed} from './transaction-closed-error';
import {
  isScanIndexOptions,
  KeyTypeForScanOptions,
  normalizeScanOptionIndexedStartKey,
  ScanIndexOptions,
  ScanOptionIndexedStartKey,
  ScanOptions,
} from './scan-options';
import {asyncIterableToArray} from './async-iterable-to-array';
import type {ReadonlyEntry} from './btree/node';
import {encodeIndexScanKey, IndexKey} from './db/index.js';
import {EntryForOptions, fromKeyForNonIndexScan} from './transactions.js';

type ScanKey = string | IndexKey;

type ToValue<Options extends ScanOptions, V> = (
  entry: EntryForOptions<Options>,
) => V;

type ShouldDeepClone = {shouldDeepClone: boolean};

/**
 * This class is used for the results of [[ReadTransaction.scan|scan]]. It
 * implements `AsyncIterable<JSONValue>` which allows you to use it in a `for
 * await` loop. There are also methods to iterate over the [[keys]],
 * [[entries]] or [[values]].
 */
export class ScanResultImpl<
  Options extends ScanOptions,
  V extends ReadonlyJSONValue,
> implements ScanResult<KeyTypeForScanOptions<Options>, V>
{
  private readonly _iter: AsyncIterable<EntryForOptions<Options>>;
  private readonly _options: Options;
  private readonly _dbDelegateOptions: Closed & ShouldDeepClone;
  private readonly _onLimitKey: (inclusiveLimitKey: string) => void;

  constructor(
    iter: AsyncIterable<EntryForOptions<Options>>,
    options: Options,
    dbDelegateOptions: Closed & ShouldDeepClone,
    onLimitKey: (inclusiveLimitKey: string) => void,
  ) {
    this._iter = iter;
    this._options = options;
    this._dbDelegateOptions = dbDelegateOptions;
    this._onLimitKey = onLimitKey;
  }

  /** The default AsyncIterable. This is the same as [[values]]. */
  [Symbol.asyncIterator](): AsyncIterableIteratorToArrayWrapper<V> {
    return this.values();
  }

  /** Async iterator over the values of the [[ReadTransaction.scan|scan]] call. */
  values(): AsyncIterableIteratorToArrayWrapper<V> {
    const clone = this._dbDelegateOptions.shouldDeepClone
      ? deepClone
      : (x: ReadonlyJSONValue) => x;
    return new AsyncIterableIteratorToArrayWrapper(
      this._newIterator(e => clone(e[1])) as AsyncIterableIterator<V>,
    );
  }

  /**
   * Async iterator over the keys of the [[ReadTransaction.scan|scan]]
   * call. If the [[ReadTransaction.scan|scan]] is over an index the key
   * is a tuple of `[secondaryKey: string, primaryKey]`
   */
  keys(): AsyncIterableIteratorToArrayWrapper<KeyTypeForScanOptions<Options>> {
    type K = KeyTypeForScanOptions<Options>;
    const toValue = (e: EntryForOptions<Options>) => e[0];
    return new AsyncIterableIteratorToArrayWrapper(
      this._newIterator(toValue as ToValue<Options, K>),
    );
  }

  /**
   * Async iterator over the entries of the [[ReadTransaction.scan|scan]]
   * call. An entry is a tuple of key values. If the
   * [[ReadTransaction.scan|scan]] is over an index the key is a tuple of
   * `[secondaryKey: string, primaryKey]`
   */
  entries(): AsyncIterableIteratorToArrayWrapper<
    readonly [KeyTypeForScanOptions<Options>, V]
  > {
    type K = KeyTypeForScanOptions<Options>;
    const clone = this._dbDelegateOptions.shouldDeepClone
      ? deepClone
      : (x: ReadonlyJSONValue) => x;
    const toValue = (e: EntryForOptions<Options>) => clone(e);
    return new AsyncIterableIteratorToArrayWrapper(
      this._newIterator(toValue as ToValue<Options, readonly [K, V]>),
    );
  }

  /** Returns all the values as an array. Same as `values().toArray()` */
  toArray(): Promise<V[]> {
    return this.values().toArray();
  }

  private _newIterator<T>(
    toValue: ToValue<Options, T>,
  ): AsyncIterableIterator<T> {
    return scanIterator(
      toValue,
      this._iter,
      this._options,
      this._dbDelegateOptions,
      this._onLimitKey,
    );
  }
}

export interface ScanResult<K extends ScanKey, V extends ReadonlyJSONValue>
  extends AsyncIterable<V> {
  /** The default AsyncIterable. This is the same as [[values]]. */
  [Symbol.asyncIterator](): AsyncIterableIteratorToArrayWrapper<V>;

  /** Async iterator over the values of the [[ReadTransaction.scan|scan]] call. */
  values(): AsyncIterableIteratorToArrayWrapper<V>;

  /**
   * Async iterator over the keys of the [[ReadTransaction.scan|scan]]
   * call. If the [[ReadTransaction.scan|scan]] is over an index the key
   * is a tuple of `[secondaryKey: string, primaryKey]`
   */
  keys(): AsyncIterableIteratorToArrayWrapper<K>;

  /**
   * Async iterator over the entries of the [[ReadTransaction.scan|scan]]
   * call. An entry is a tuple of key values. If the
   * [[ReadTransaction.scan|scan]] is over an index the key is a tuple of
   * `[secondaryKey: string, primaryKey]`
   */
  entries(): AsyncIterableIteratorToArrayWrapper<readonly [K, V]>;

  /** Returns all the values as an array. Same as `values().toArray()` */
  toArray(): Promise<V[]>;
}

/**
 * A class that wraps an async iterable iterator to add a [[toArray]] method.
 *
 * Usage:
 *
 * ```ts
 * const keys: string[] = await rep.scan().keys().toArray();
 * ```
 */
export class AsyncIterableIteratorToArrayWrapper<V>
  implements AsyncIterableIterator<V>
{
  private readonly _it: AsyncIterableIterator<V>;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  readonly next: (v?: any) => Promise<IteratorResult<V>>;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  readonly return?: (value?: any) => Promise<IteratorResult<V>>;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  readonly throw?: (e?: any) => Promise<IteratorResult<V>>;

  constructor(it: AsyncIterableIterator<V>) {
    this._it = it;

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    this.next = (v: any) => it.next(v);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-non-null-assertion
    this.return = it.return ? (v: any) => it.return!(v) : undefined;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-non-null-assertion
    this.throw = it.throw ? (v: any) => it.throw!(v) : undefined;
  }

  toArray(): Promise<V[]> {
    return asyncIterableToArray(this._it);
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<V> {
    return this._it[Symbol.asyncIterator]();
  }
}

async function* scanIterator<Options extends ScanOptions, V>(
  toValue: ToValue<Options, V>,
  iter: AsyncIterable<EntryForOptions<Options>>,
  options: Options,
  closed: Closed,
  onLimitKey: (inclusiveLimitKey: string) => void,
): AsyncIterableIterator<V> {
  throwIfClosed(closed);

  let {limit = Infinity} = options;
  const {prefix = ''} = options;
  let exclusive = options.start?.exclusive;

  const isIndexScan = isScanIndexOptions(options);

  // iter has already been moved to the first entry
  for await (const entry of iter) {
    const key = entry[0];
    const keyToMatch: string = isIndexScan ? key[0] : (key as string);
    if (!keyToMatch.startsWith(prefix)) {
      return;
    }

    if (exclusive) {
      exclusive = true;
      if (isIndexScan) {
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        if (shouldSkipIndexScan(key as IndexKey, options.start!.key)) {
          continue;
        }
      } else {
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        if (shouldSkipNonIndexScan(key as string, options.start!.key)) {
          continue;
        }
      }
    }

    yield toValue(entry);

    if (--limit === 0 && !isIndexScan) {
      onLimitKey(key as string);
      return;
    }
  }
}

function shouldSkipIndexScan(
  key: IndexKey,
  startKey: ScanOptionIndexedStartKey,
): boolean {
  const [secondaryStartKey, primaryStartKey] =
    normalizeScanOptionIndexedStartKey(startKey);
  const [secondaryKey, primaryKey] = normalizeScanOptionIndexedStartKey(key);
  if (secondaryKey !== secondaryStartKey) {
    return false;
  }
  if (primaryStartKey === undefined) {
    return true;
  }
  return primaryKey === primaryStartKey;
}

function shouldSkipNonIndexScan(key: string, startKey: string): boolean {
  return key === startKey;
}

/**
 * This is called when doing a [[ReadTransaction.scan|scan]] without an
 * `indexName`.
 *
 * @param fromKey The `fromKey` is computed by `scan` and is the key of the
 * first entry to return in the iterator. It is based on `prefix` and
 * `start.key` of the [[ScanNoIndexOptions]].
 */
export type GetScanIterator = (
  fromKey: string,
) => AsyncIterable<ReadonlyEntry<ReadonlyJSONValue>>;

/**
 * This is called when doing a [[ReadTransaction.scan|scan]] with an
 * `indexName`.
 *
 * @param indexName The name of the index we are scanning over.
 * @param fromSecondaryKey The `fromSecondaryKey` is computed by `scan` and is
 * the secondary key of the first entry to return in the iterator. It is based
 * on `prefix` and `start.key` of the [[ScanIndexOptions]].
 * @param fromPrimaryKey The `fromPrimaryKey` is computed by `scan` and is the
 * primary key of the first entry to return in the iterator. It is based on
 * `prefix` and `start.key` of the [[ScanIndexOptions]].
 */
export type GetIndexScanIterator = (
  indexName: string,
  fromSecondaryKey: string,
  fromPrimaryKey: string | undefined,
) => AsyncIterable<readonly [key: IndexKey, value: ReadonlyJSONValue]>;

/**
 * A helper function that makes it easier to implement [[ReadTransaction.scan]]
 * with a custom backend
 */
export function makeScanResult<Options extends ScanOptions>(
  options: Options,
  getScanIterator: Options extends ScanIndexOptions
    ? GetIndexScanIterator
    : GetScanIterator,
): ScanResult<KeyTypeForScanOptions<Options>, ReadonlyJSONValue> {
  type AsyncIter = AsyncIterable<EntryForOptions<Options>>;

  if (isScanIndexOptions(options)) {
    const [fromSecondaryKey, fromPrimaryKey] = fromKeyForIndexScan(options);
    const iter = (getScanIterator as GetIndexScanIterator)(
      options.indexName,
      fromSecondaryKey,
      fromPrimaryKey,
    ) as AsyncIter;
    return new ScanResultImpl(
      iter,
      options,
      {closed: false, shouldDeepClone: false},
      _ => {
        // noop
      },
    );
  }
  const fromKey = fromKeyForNonIndexScan(options);
  const iter = (getScanIterator as GetScanIterator)(fromKey) as AsyncIter;

  return new ScanResultImpl(
    iter,
    options,
    {closed: false, shouldDeepClone: false},
    _ => {
      // noop
    },
  );
}
export function fromKeyForIndexScan(
  options: ScanIndexOptions,
): readonly [secondary: string, primary?: string] {
  const {prefix, start} = options;
  const prefixNormalized: [secondary: string, primary?: string] = [
    prefix ?? '',
    undefined,
  ];

  if (!start) {
    return prefixNormalized;
  }

  const startKeyNormalized = normalizeScanOptionIndexedStartKey(start.key);
  if (startKeyNormalized[0] > prefixNormalized[0]) {
    return startKeyNormalized;
  }
  if (
    startKeyNormalized[0] === prefixNormalized[0] &&
    startKeyNormalized[1] !== undefined
  ) {
    return startKeyNormalized;
  }

  return prefixNormalized;
}

export function fromKeyForIndexScanInternal(options: ScanIndexOptions): string {
  const {prefix, start} = options;
  let prefix2 = '';
  if (prefix !== undefined) {
    prefix2 = encodeIndexScanKey(prefix, undefined);
  }
  if (!start) {
    return prefix2;
  }

  const {key} = start;
  const [secondary, primary] = normalizeScanOptionIndexedStartKey(key);
  const startKey = encodeIndexScanKey(secondary, primary);

  if (startKey > prefix2) {
    return startKey;
  }

  return prefix2;
}
