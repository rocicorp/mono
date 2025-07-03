import type {LogContext} from '@rocicorp/logger';
import {compareUTF8, greaterThan, lessThan, lessThanEq} from 'compare-utf8';
import {assert} from '../../shared/src/asserts.ts';
import {binarySearch} from '../../shared/src/binary-search.ts';
import type {Enum} from '../../shared/src/enum.ts';
import {deepEqual} from '../../shared/src/json.ts';
import type {
  Diff,
  DiffOperation,
  IndexDiff,
  InternalDiff,
  InternalDiffOperation,
  NoIndexDiff,
} from './btree/node.ts';
import type {IndexKey} from './db/index.ts';
import {decodeIndexKey} from './db/index.ts';
import type {ScanOptions} from './db/scan.ts';
import * as InvokeKind from './invoke-kind-enum.ts';
import type {DiffComputationConfig, DiffsMap} from './sync/diff.ts';
import {
  type ReadTransaction,
  SubscriptionTransactionWrapper,
} from './transactions.ts';
import type {QueryInternal} from './types.ts';

type InvokeKind = Enum<typeof InvokeKind>;

export interface Subscription<R> {
  hasIndexSubscription(indexName: string): boolean;

  invoke(
    tx: ReadTransaction,
    kind: InvokeKind,
    diffs: DiffsMap | undefined,
  ): Promise<R>;

  matches(diffs: DiffsMap): boolean;

  updateDeps(
    keys: ReadonlySet<string>,
    scans: ReadonlyArray<Readonly<ScanSubscriptionInfo>>,
  ): void;

  readonly onData: (result: R) => void;
  readonly onError: ((error: unknown) => void) | undefined;
  readonly onDone: (() => void) | undefined;
}

const emptySet: ReadonlySet<string> = new Set();

const unitializedLastValue = Symbol();
type UnitializedLastValue = typeof unitializedLastValue;

export class SubscriptionImpl<R> implements Subscription<R> {
  readonly #body: (tx: ReadTransaction) => Promise<R>;
  readonly #onData: (result: R) => void;
  #lastValue: R | UnitializedLastValue = unitializedLastValue;
  #keys = emptySet;
  #scans: readonly Readonly<ScanSubscriptionInfo>[] = [];

  readonly onError: ((error: unknown) => void) | undefined;
  readonly onDone: (() => void) | undefined;
  readonly #isEqual: (a: R, b: R) => boolean;

  constructor(
    body: (tx: ReadTransaction) => Promise<R>,
    onData: (result: R) => void,
    onError: ((error: unknown) => void) | undefined,
    onDone: (() => void) | undefined,
    // deepEqual operates on any JSON value but argument might be more specific.
    isEqual: (a: R, b: R) => boolean = deepEqual as (a: R, b: R) => boolean,
  ) {
    this.#body = body;
    this.#onData = onData;
    this.onError = onError;
    this.onDone = onDone;
    this.#isEqual = isEqual;
  }

  hasIndexSubscription(indexName: string): boolean {
    for (const scan of this.#scans) {
      if (scan.options.indexName === indexName) {
        return true;
      }
    }
    return false;
  }

  invoke(
    tx: ReadTransaction,
    _kind: InvokeKind,
    _diffs: DiffsMap | undefined,
  ): Promise<R> {
    return this.#body(tx);
  }

  matches(diffs: DiffsMap): boolean {
    for (const [indexName, diff] of diffs) {
      if (diffMatchesSubscription(this.#keys, this.#scans, indexName, diff)) {
        return true;
      }
    }

    return false;
  }

  updateDeps(
    keys: ReadonlySet<string>,
    scans: readonly Readonly<ScanSubscriptionInfo>[],
  ): void {
    this.#keys = keys;
    this.#scans = scans;
  }

  onData(result: R): void {
    if (
      this.#lastValue === unitializedLastValue ||
      !this.#isEqual(this.#lastValue, result)
    ) {
      this.#lastValue = result;
      this.#onData(result);
    }
  }
}

export {SubscriptionImpl as SubscriptionImplForTesting};

/**
 * Function that gets passed into {@link Replicache.experimentalWatch} and gets
 * called when the data in Replicache changes.
 *
 * @experimental This type is experimental and may change in the future.
 */
export type WatchNoIndexCallback = (diff: NoIndexDiff) => void;

export type WatchCallbackForOptions<Options extends WatchOptions> =
  Options extends WatchIndexOptions ? WatchIndexCallback : WatchNoIndexCallback;

/**
 * Function that gets passed into {@link Replicache.experimentalWatch} when doing a
 * watch on a secondary index map and gets called when the data in Replicache
 * changes.
 *
 * @experimental This type is experimental and may change in the future.
 */
export type WatchIndexCallback = (diff: IndexDiff) => void;

/**
 * Options for {@link Replicache.experimentalWatch}.
 *
 * @experimental This interface is experimental and may change in the future.
 */
export type WatchOptions = WatchIndexOptions | WatchNoIndexOptions;

/**
 * Options object passed to {@link Replicache.experimentalWatch}. This is for an
 * index watch.
 */
export type WatchIndexOptions = WatchNoIndexOptions & {
  /**
   * When provided, the `watch` is limited to the changes that apply to the index map.
   */
  indexName: string;
};

/**
 * Options object passed to {@link Replicache.experimentalWatch}. This is for a non
 * index watch.
 */
export type WatchNoIndexOptions = {
  /**
   * When provided, the `watch` is limited to changes where the `key` starts
   * with `prefix`.
   */
  prefix?: string | undefined;

  /**
   * When this is set to `true` (default is `false`), the `watch` callback will
   * be called once asynchronously when watch is called. The arguments in that
   * case is a diff where we consider all the existing values in Replicache as
   * being added.
   */
  initialValuesInFirstDiff?: boolean | undefined;
};

export type WatchCallback = (diff: Diff) => void;

export class WatchSubscription implements Subscription<Diff | undefined> {
  readonly #callback: WatchCallback;
  readonly #prefix: string;
  readonly #indexName: string | undefined;
  readonly #initialValuesInFirstDiff: boolean;

  readonly onError: ((error: unknown) => void) | undefined = undefined;
  readonly onDone: (() => void) | undefined = undefined;

  constructor(callback: WatchCallback, options?: WatchOptions) {
    this.#callback = callback;
    this.#prefix = options?.prefix ?? '';
    this.#indexName = (options as WatchIndexOptions)?.indexName;
    this.#initialValuesInFirstDiff = options?.initialValuesInFirstDiff ?? false;
  }

  hasIndexSubscription(indexName: string): boolean {
    return this.#indexName === indexName;
  }

  onData(result: Diff | undefined): void {
    if (result !== undefined) {
      this.#callback(result);
    }
  }

  invoke(
    tx: ReadTransaction,
    kind: InvokeKind,
    diffs: DiffsMap | undefined,
  ): Promise<Diff | undefined> {
    const invoke = async <Key extends IndexKey | string>(
      indexName: string | undefined,
      prefix: string,
      compareKey: (diff: DiffOperation<Key>) => string,
      convertInternalDiff: (
        diff: InternalDiff,
      ) => readonly DiffOperation<Key>[],
    ): Promise<readonly DiffOperation<Key>[] | undefined> => {
      let diff: readonly DiffOperation<Key>[];
      if (kind === InvokeKind.InitialRun) {
        if (!this.#initialValuesInFirstDiff) {
          // We are using `undefined` here as a sentinel value to indicate that we
          // should not call the callback in `onDone`.
          return undefined;
        }

        // For the initial run, we need to get the "diffs" for the whole tree.
        assert(diffs === undefined);

        const newDiff: DiffOperation<Key>[] = [];
        for await (const entry of tx.scan({prefix, indexName}).entries()) {
          newDiff.push({
            op: 'add',
            key: entry[0] as Key,
            newValue: entry[1],
          });
        }
        diff = newDiff;
      } else {
        assert(diffs);
        const maybeDiff = diffs.get(indexName ?? '') ?? [];
        diff = convertInternalDiff(maybeDiff);
      }
      const newDiff: DiffOperation<Key>[] = [];
      const {length} = diff;
      for (
        let i = diffBinarySearch(diff, prefix, compareKey);
        i < length;
        i++
      ) {
        if (compareKey(diff[i]).startsWith(prefix)) {
          newDiff.push(diff[i]);
        } else {
          break;
        }
      }

      // For initial run we should always return something.
      return kind === InvokeKind.InitialRun || newDiff.length > 0
        ? newDiff
        : undefined;
    };

    if (this.#indexName) {
      return invoke<IndexKey>(
        this.#indexName,
        this.#prefix,
        diff => diff.key[0],
        internalDiff => convertDiffValues(internalDiff, decodeIndexKey),
      );
    }

    return invoke<string>(
      undefined,
      this.#prefix,
      diff => diff.key,
      internalDiff => convertDiffValues(internalDiff, k => k),
    );
  }

  matches(diffs: DiffsMap): boolean {
    const diff = diffs.get(this.#indexName ?? '');
    if (diff === undefined) {
      return false;
    }

    return watcherMatchesDiff(diff, this.#prefix, this.#indexName);
  }

  updateDeps(
    _keys: ReadonlySet<string>,
    _scans: readonly Readonly<ScanSubscriptionInfo>[],
  ): void {
    // not used
  }
}

function convertDiffValues<Key>(
  diff: InternalDiff,
  convertKey: (k: string) => Key,
): DiffOperation<Key>[] {
  return diff.map(op => {
    const key = convertKey(op.key);
    switch (op.op) {
      case 'add':
        return {
          op: 'add',
          key,
          newValue: op.newValue,
        };
      case 'change':
        return {
          op: 'change',
          key,
          oldValue: op.oldValue,
          newValue: op.newValue,
        };
      case 'del':
        return {
          op: 'del',
          key,
          oldValue: op.oldValue,
        };
    }
  });
}

/**
 * The options passed to {@link Replicache.subscribe}.
 */
export interface SubscribeOptions<R> {
  /**
   * Called when the return value of the body function changes.
   */
  onData: (result: R) => void;

  /**
   * If present, called when an error occurs.
   */
  onError?: ((error: unknown) => void) | undefined;

  /**
   * If present, called when the subscription is removed/done.
   */
  onDone?: (() => void) | undefined;

  /**
   * If present this function is used to determine if the value returned by the
   * body function has changed. If not provided a JSON deep equality check is
   * used.
   */
  isEqual?: ((a: R, b: R) => boolean) | undefined;
}

export type UnknownSubscription = Subscription<unknown>;

type SubscriptionSet = Set<UnknownSubscription>;

export interface SubscriptionsManager extends DiffComputationConfig {
  clear(): void;
  fire(diffs: DiffsMap): Promise<void>;
  hasPendingSubscriptionRuns: boolean;
  add<R>(subscription: Subscription<R>): () => void;
}

export class SubscriptionsManagerImpl implements SubscriptionsManager {
  readonly #subscriptions: SubscriptionSet = new Set();
  readonly #pendingSubscriptions: SubscriptionSet = new Set();
  readonly #queryInternal: QueryInternal;
  readonly #lc: LogContext;
  hasPendingSubscriptionRuns = false;
  readonly #signal: AbortSignal;

  constructor(
    queryInternal: QueryInternal,
    lc: LogContext,
    signal: AbortSignal,
  ) {
    this.#queryInternal = queryInternal;
    this.#lc = lc;
    this.#signal = signal;
  }

  add<R>(subscription: Subscription<R>): () => void {
    this.#subscriptions.add(subscription as UnknownSubscription);
    void this.#scheduleInitialSubscriptionRun(
      subscription as UnknownSubscription,
    );
    return () =>
      this.#subscriptions.delete(subscription as UnknownSubscription);
  }

  clear(): void {
    for (const subscription of this.#subscriptions) {
      subscription.onDone?.();
    }
    this.#subscriptions.clear();
  }

  fire(diffs: DiffsMap): Promise<void> {
    const subscriptions = subscriptionsForDiffs(this.#subscriptions, diffs);
    return this.#fireSubscriptions(subscriptions, InvokeKind.Regular, diffs);
  }

  async #fireSubscriptions(
    subscriptions: Iterable<UnknownSubscription>,
    kind: InvokeKind,
    diffs: DiffsMap | undefined,
  ) {
    if (this.#signal.aborted) {
      return;
    }

    const subs = [...subscriptions] as readonly Subscription<unknown>[];
    if (subs.length === 0) {
      return;
    }

    // Use allSettled to gather fulfilled and rejected promises.
    const results = await this.#queryInternal(tx =>
      Promise.allSettled(
        subs.map(async s => {
          const stx = new SubscriptionTransactionWrapper(tx);
          try {
            return await s.invoke(stx, kind, diffs);
          } finally {
            // We need to keep track of the subscription keys even if there was an
            // exception because changes to the keys can make the subscription
            // body succeed.
            s.updateDeps(stx.keys, stx.scans);
          }
        }),
      ),
    );

    this.callCallbacks(subs, results);
  }

  // Public method so that ZQL can wrap it in a transaction.
  callCallbacks(
    subs: readonly Subscription<unknown>[],
    results: PromiseSettledResult<unknown>[],
  ) {
    for (let i = 0; i < subs.length; i++) {
      const s = subs[i];
      const result = results[i];
      if (result.status === 'fulfilled') {
        s.onData(result.value);
      } else {
        if (s.onError) {
          s.onError(result.reason);
        } else {
          this.#lc.error?.('Error in subscription body:', result.reason);
        }
      }
    }
  }

  async #scheduleInitialSubscriptionRun(s: UnknownSubscription) {
    this.#pendingSubscriptions.add(s);

    if (!this.hasPendingSubscriptionRuns) {
      this.hasPendingSubscriptionRuns = true;
      await Promise.resolve();
      this.hasPendingSubscriptionRuns = false;
      const subscriptions = [...this.#pendingSubscriptions];
      this.#pendingSubscriptions.clear();
      await this.#fireSubscriptions(
        subscriptions,
        InvokeKind.InitialRun,
        undefined,
      );
    }
  }

  shouldComputeDiffs(): boolean {
    return this.#subscriptions.size > 0;
  }

  shouldComputeDiffsForIndex(indexName: string): boolean {
    for (const s of this.#subscriptions) {
      if (s.hasIndexSubscription(indexName)) {
        return true;
      }
    }
    return false;
  }
}

export type ScanSubscriptionInfo = {
  options: ScanOptions;
  inclusiveLimitKey?: string | undefined;
};

function diffMatchesSubscription(
  keys: ReadonlySet<string>,
  scans: Iterable<Readonly<ScanSubscriptionInfo>>,
  indexName: string,
  diff: InternalDiff,
): boolean {
  // Keys can only match for non index scans.
  if (indexName === '') {
    for (const diffEntry of diff) {
      if (keys.has(diffEntry.key)) {
        return true;
      }
    }
  }

  for (const scanInfo of scans) {
    if (scanInfoMatchesDiff(scanInfo, indexName, diff)) {
      return true;
    }
  }
  return false;
}

function scanInfoMatchesDiff(
  scanInfo: ScanSubscriptionInfo,
  changeIndexName: string,
  diff: InternalDiff,
): boolean {
  // TODO(arv): Use binary search
  for (const diffEntry of diff) {
    if (scanInfoMatchesKey(scanInfo, changeIndexName, diffEntry.key)) {
      return true;
    }
  }

  return false;
}

export function scanInfoMatchesKey(
  scanInfo: ScanSubscriptionInfo,
  changeIndexName: string,
  changedKey: string,
): boolean {
  const {
    indexName = '',
    limit,
    prefix,
    startKey,
    startExclusive,
    startSecondaryKey,
  } = scanInfo.options;

  if (changeIndexName !== indexName) {
    return false;
  }

  if (!indexName) {
    // A scan with limit <= 0 can have no matches
    if (limit !== undefined && limit <= 0) {
      return false;
    }

    // No prefix and no start. Must recompute the subscription because all keys
    // will have an effect on the subscription.
    if (!prefix && !startKey) {
      return true;
    }

    if (
      prefix &&
      (!changedKey.startsWith(prefix) ||
        isKeyPastInclusiveLimit(scanInfo, changedKey))
    ) {
      return false;
    }

    if (
      startKey &&
      ((startExclusive && lessThanEq(changedKey, startKey)) ||
        lessThan(changedKey, startKey) ||
        isKeyPastInclusiveLimit(scanInfo, changedKey))
    ) {
      return false;
    }

    return true;
  }

  // No prefix and no start. Must recompute the subscription because all keys
  // will have an effect on the subscription.
  if (!prefix && !startKey && !startSecondaryKey) {
    return true;
  }

  const [changedKeySecondary, changedKeyPrimary] = decodeIndexKey(changedKey);

  if (prefix) {
    if (!changedKeySecondary.startsWith(prefix)) {
      return false;
    }
  }

  if (
    startSecondaryKey &&
    ((startExclusive && lessThanEq(changedKeySecondary, startSecondaryKey)) ||
      lessThan(changedKeySecondary, startSecondaryKey))
  ) {
    return false;
  }

  if (
    startKey &&
    ((startExclusive && lessThanEq(changedKeyPrimary, startKey)) ||
      lessThan(changedKeyPrimary, startKey))
  ) {
    return false;
  }

  return true;
}

function isKeyPastInclusiveLimit(
  scanInfo: ScanSubscriptionInfo,
  changedKey: string,
): boolean {
  const {inclusiveLimitKey} = scanInfo;
  return (
    scanInfo.options.limit !== undefined &&
    inclusiveLimitKey !== undefined &&
    greaterThan(changedKey, inclusiveLimitKey)
  );
}

function* subscriptionsForDiffs<V>(
  subscriptions: Set<Subscription<V>>,
  diffs: DiffsMap,
): Generator<Subscription<V>> {
  for (const subscription of subscriptions) {
    if (subscription.matches(diffs)) {
      yield subscription;
    }
  }
}

function watcherMatchesDiff(
  diff: InternalDiff,
  prefix: string,
  indexName: string | undefined,
): boolean {
  if (prefix === '') {
    return true;
  }

  const compareKey = indexName
    ? (diffOp: InternalDiffOperation) => decodeIndexKey(diffOp.key)[0]
    : (diffOp: InternalDiffOperation) => diffOp.key;
  const i = diffBinarySearch(diff, prefix, compareKey);
  return i < diff.length && compareKey(diff[i]).startsWith(prefix);
}

export function diffBinarySearch<Key, Value>(
  diff: readonly InternalDiffOperation<Key, Value>[],
  prefix: string,
  compareKey: (diff: InternalDiffOperation<Key, Value>) => string,
): number {
  return binarySearch(diff.length, i =>
    compareUTF8(prefix, compareKey(diff[i])),
  );
}
