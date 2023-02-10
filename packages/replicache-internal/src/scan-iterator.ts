import {greaterThan} from 'compare-utf8';
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
import type {IterableUnion} from './iterable-union.js';

type ScanKey = string | IndexKey;

type ToValue<Options extends ScanOptions, Value> = (
  entry: EntryForOptions<Options, Value>,
) => Value;

type ShouldDeepClone = {shouldDeepClone: boolean};

/**
 * This class is used for the results of [[ReadTransaction.scan|scan]]. It
 * implements `AsyncIterable<JSONValue>` which allows you to use it in a `for
 * await` loop. There are also methods to iterate over the [[keys]],
 * [[entries]] or [[values]].
 */
export class ScanResultImpl<Options extends ScanOptions, Value>
  implements ScanResult<KeyTypeForScanOptions<Options>, Value>
{
  private readonly _iter: AsyncIterable<EntryForOptions<Options, Value>>;
  private readonly _options: Options;
  private readonly _dbDelegateOptions: Closed & ShouldDeepClone;
  private readonly _onLimitKey: (inclusiveLimitKey: string) => void;

  constructor(
    iter: AsyncIterable<EntryForOptions<Options, Value>>,
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
  [Symbol.asyncIterator](): AsyncIterableIteratorToArray<Value> {
    return this.values();
  }

  /** Async iterator over the values of the [[ReadTransaction.scan|scan]] call. */
  values(): AsyncIterableIteratorToArray<Value> {
    const clone = this._dbDelegateOptions.shouldDeepClone
      ? deepClone
      : (x: ReadonlyJSONValue) => x;
    const toValue: ToValue<Options, Value> = e => clone(e[1]) as Value;
    return new AsyncIterableIteratorToArrayWrapperImpl(
      this._newIterator(toValue),
    );
  }

  /**
   * Async iterator over the keys of the [[ReadTransaction.scan|scan]]
   * call. If the [[ReadTransaction.scan|scan]] is over an index the key
   * is a tuple of `[secondaryKey: string, primaryKey]`
   */
  keys(): AsyncIterableIteratorToArray<KeyTypeForScanOptions<Options>> {
    type K = KeyTypeForScanOptions<Options>;
    const toValue: ToValue<Options, K> = e => e[0] as K;
    return new AsyncIterableIteratorToArrayWrapperImpl<K>(
      this._newIterator<K>(toValue),
    );
  }

  /**
   * Async iterator over the entries of the [[ReadTransaction.scan|scan]]
   * call. An entry is a tuple of key values. If the
   * [[ReadTransaction.scan|scan]] is over an index the key is a tuple of
   * `[secondaryKey: string, primaryKey]`
   */
  entries(): AsyncIterableIteratorToArray<
    readonly [KeyTypeForScanOptions<Options>, Value]
  > {
    type Entry = readonly [KeyTypeForScanOptions<Options>, Value];
    const clone = this._dbDelegateOptions.shouldDeepClone
      ? deepClone
      : (x: ReadonlyJSONValue) => x;
    const toValue: ToValue<Options, Entry> = e => clone(e) as Entry;
    return new AsyncIterableIteratorToArrayWrapperImpl(
      this._newIterator<Entry>(toValue),
    );
  }

  /** Returns all the values as an array. Same as `values().toArray()` */
  toArray(): Promise<Value[]> {
    return this.values().toArray();
  }

  private _newIterator<T>(
    toValue: ToValue<Options, T>,
  ): AsyncIterableIterator<T> {
    return scanIterator(
      toValue,
      this._iter as unknown as AsyncIterable<EntryForOptions<Options, T>>,
      this._options,
      this._dbDelegateOptions,
      this._onLimitKey,
    );
  }
}

export interface ScanResult<K extends ScanKey, V extends ReadonlyJSONValue>
  extends AsyncIterable<V> {
  /** The default AsyncIterable. This is the same as [[values]]. */
  [Symbol.asyncIterator](): AsyncIterableIteratorToArray<V>;

  /** Async iterator over the values of the [[ReadTransaction.scan|scan]] call. */
  values(): AsyncIterableIteratorToArray<V>;

  /**
   * Async iterator over the keys of the [[ReadTransaction.scan|scan]]
   * call. If the [[ReadTransaction.scan|scan]] is over an index the key
   * is a tuple of `[secondaryKey: string, primaryKey]`
   */
  keys(): AsyncIterableIteratorToArray<K>;

  /**
   * Async iterator over the entries of the [[ReadTransaction.scan|scan]]
   * call. An entry is a tuple of key values. If the
   * [[ReadTransaction.scan|scan]] is over an index the key is a tuple of
   * `[secondaryKey: string, primaryKey]`
   */
  entries(): AsyncIterableIteratorToArray<readonly [K, V]>;

  /** Returns all the values as an array. Same as `values().toArray()` */
  toArray(): Promise<V[]>;
}

/**
 * An interface that adds a [[toArray]] method to `AsyncIterableIterator`.
 *
 * Usage:
 *
 * ```ts
 * const keys: string[] = await rep.scan().keys().toArray();
 * ```
 */
export interface AsyncIterableIteratorToArray<V>
  extends AsyncIterableIterator<V> {
  toArray(): Promise<V[]>;
}

class AsyncIterableIteratorToArrayWrapperImpl<V>
  implements AsyncIterableIterator<V>
{
  private readonly _it: AsyncIterableIterator<V>;

  constructor(it: AsyncIterableIterator<V>) {
    this._it = it;
  }

  next() {
    return this._it.next();
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<V> {
    return this._it[Symbol.asyncIterator]();
  }

  toArray(): Promise<V[]> {
    return asyncIterableToArray(this._it);
  }
}

async function* scanIterator<Options extends ScanOptions, Value>(
  toValue: ToValue<Options, Value>,
  iter: AsyncIterable<EntryForOptions<Options, Value>>,
  options: Options,
  closed: Closed,
  onLimitKey: (inclusiveLimitKey: string) => void,
): AsyncIterableIterator<Value> {
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
export type GetScanIterator<V> = (
  fromKey: string,
) => IterableUnion<ReadonlyEntry<V>>;

/**
 * When using [[makeScanResult]] this is the type used for the function called when doing a [[ReadTransaction.scan|scan]] with an
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
export type GetIndexScanIterator<V> = (
  indexName: string,
  fromSecondaryKey: string,
  fromPrimaryKey: string | undefined,
) => IterableUnion<readonly [key: IndexKey, value: V]>;

/**
 * A helper function that makes it easier to implement [[ReadTransaction.scan]]
 * with a custom backend.
 *
 * If you are implementing a custom backend and have an in memory pending async
 * iterable we provide two helper functions to make it easier to merge these
 * together. [[mergeAsyncIterables]] and [[filterAsyncIterable]].
 *
 * For example:
 *
 * ```ts
 * const scanResult = makeScanResult(
 *   options,
 *   options.indexName
 *     ? () => {
 *         throw Error('not implemented');
 *       }
 *     : fromKey => {
 *         const persisted: AsyncIterable<Entry<ReadonlyJSONValue>> = ...;
 *         const pending: AsyncIterable<Entry<ReadonlyJSONValue | undefined>> = ...;
 *         const iter = await mergeAsyncIterables(persisted, pending);
 *         const filteredIter = await filterAsyncIterable(
 *           iter,
 *           entry => entry[1] !== undefined,
 *         );
 *         return filteredIter;
 *       },
 * );
 * ```
 */
export function makeScanResult<Options extends ScanOptions, Value>(
  options: Options,
  getScanIterator: Options extends ScanIndexOptions
    ? GetIndexScanIterator<Value>
    : GetScanIterator<Value>,
): ScanResult<KeyTypeForScanOptions<Options>, Value> {
  type AsyncIter = AsyncIterable<EntryForOptions<Options, Value>>;

  if (isScanIndexOptions(options)) {
    const [fromSecondaryKey, fromPrimaryKey] = fromKeyForIndexScan(options);
    return new ScanResultImpl(
      (getScanIterator as GetIndexScanIterator<Value>)(
        options.indexName,
        fromSecondaryKey,
        fromPrimaryKey,
      ) as AsyncIter,
      options,
      {closed: false, shouldDeepClone: false},
      _ => {
        // noop
      },
    );
  }
  const fromKey = fromKeyForNonIndexScan(options);
  return new ScanResultImpl(
    (getScanIterator as GetScanIterator<Value>)(fromKey) as AsyncIter,
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
  if (greaterThan(startKeyNormalized[0], prefixNormalized[0])) {
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

  if (greaterThan(startKey, prefix2)) {
    return startKey;
  }

  return prefix2;
}
