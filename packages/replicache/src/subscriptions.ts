import {compareUTF8, greaterThan, lessThan, lessThanEq} from 'compare-utf8';
import * as db from './db/mod.js';
import type * as sync from './sync/mod.js';
import {assert} from './asserts.js';
import type {
  Diff,
  InternalDiffOperation,
  DiffOperation,
  IndexDiff,
  InternalDiff,
  NoIndexDiff,
} from './btree/node.js';
import {deepEqual, ReadonlyJSONValue} from './json.js';
import {
  ReadTransaction,
  SubscriptionTransactionWrapper,
} from './transactions.js';
import type {QueryInternal} from './replicache.js';
import type {LogContext} from '@rocicorp/logger';
import {binarySearch} from './binary-search.js';
import type {DiffComputationConfig} from './sync/diff.js';

const enum InvokeKind {
  InitialRun,
  Regular,
}

interface Subscription<R> {
  hasIndexSubscription(indexName: string): boolean;

  invoke(
    tx: ReadTransaction,
    kind: InvokeKind,
    diffs: sync.DiffsMap | undefined,
  ): Promise<R>;

  matches(diffs: sync.DiffsMap): boolean;

  updateDeps(
    keys: ReadonlySet<string>,
    scans: ReadonlyArray<Readonly<ScanSubscriptionInfo>>,
  ): void;

  readonly onData: (result: R) => void;
  readonly onError: ((error: unknown) => void) | undefined;
  readonly onDone: (() => void) | undefined;
}

const emptySet: ReadonlySet<string> = new Set();

class SubscriptionImpl<R extends ReadonlyJSONValue | undefined>
  implements Subscription<R>
{
  private readonly _body: (tx: ReadTransaction) => Promise<R>;
  private readonly _onData: (result: R) => void;
  private _skipEqualsCheck = true;
  private _lastValue: R | undefined = undefined;
  private _keys = emptySet;
  private _scans: readonly Readonly<ScanSubscriptionInfo>[] = [];

  readonly onError: ((error: unknown) => void) | undefined;
  readonly onDone: (() => void) | undefined;

  constructor(
    body: (tx: ReadTransaction) => Promise<R>,
    onData: (result: R) => void,
    onError: ((error: unknown) => void) | undefined,
    onDone: (() => void) | undefined,
  ) {
    this._body = body;
    this._onData = onData;
    this.onError = onError;
    this.onDone = onDone;
  }

  hasIndexSubscription(indexName: string): boolean {
    for (const scan of this._scans) {
      if (scan.options.indexName === indexName) {
        return true;
      }
    }
    return false;
  }

  invoke(
    tx: ReadTransaction,
    _kind: InvokeKind,
    _diffs: sync.DiffsMap | undefined,
  ): Promise<R> {
    return this._body(tx);
  }

  matches(diffs: sync.DiffsMap): boolean {
    for (const [indexName, diff] of diffs) {
      if (diffMatchesSubscription(this._keys, this._scans, indexName, diff)) {
        return true;
      }
    }

    return false;
  }

  updateDeps(
    keys: ReadonlySet<string>,
    scans: readonly Readonly<ScanSubscriptionInfo>[],
  ): void {
    this._keys = keys;
    this._scans = scans;
  }

  onData(result: R): void {
    if (this._skipEqualsCheck || !deepEqual(result, this._lastValue)) {
      this._lastValue = result;
      this._skipEqualsCheck = false;
      this._onData(result);
    }
  }
}

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
  prefix?: string;

  /**
   * When this is set to `true` (default is `false`), the `watch` callback will
   * be called once asynchronously when watch is called. The arguments in that
   * case is a diff where we consider all the existing values in Replicache as
   * being added.
   */
  initialValuesInFirstDiff?: boolean;
};

export type WatchCallback = (diff: Diff) => void;

class WatchImpl implements Subscription<Diff | undefined> {
  private readonly _callback: WatchCallback;
  private readonly _prefix: string;
  private readonly _indexName: string | undefined;
  private readonly _initialValuesInFirstDiff: boolean;

  readonly onError: ((error: unknown) => void) | undefined = undefined;
  readonly onDone: (() => void) | undefined = undefined;

  constructor(callback: WatchCallback, options: WatchOptions) {
    this._callback = callback;
    this._prefix = options.prefix ?? '';
    this._indexName = (options as WatchIndexOptions).indexName;
    this._initialValuesInFirstDiff = options.initialValuesInFirstDiff ?? false;
  }

  hasIndexSubscription(indexName: string): boolean {
    return this._indexName === indexName;
  }

  onData(result: Diff | undefined): void {
    if (result !== undefined) {
      this._callback(result);
    }
  }

  invoke(
    tx: ReadTransaction,
    kind: InvokeKind,
    diffs: sync.DiffsMap | undefined,
  ): Promise<Diff | undefined> {
    const invoke = async <Key extends db.IndexKey | string>(
      indexName: string | undefined,
      prefix: string,
      compareKey: (diff: DiffOperation<Key>) => string,
      convertInternalDiff: (
        diff: InternalDiff,
      ) => readonly DiffOperation<Key>[],
    ): Promise<readonly DiffOperation<Key>[] | undefined> => {
      let diff: readonly DiffOperation<Key>[];
      if (kind === InvokeKind.InitialRun) {
        if (!this._initialValuesInFirstDiff) {
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

    if (this._indexName) {
      return invoke<db.IndexKey>(
        this._indexName,
        this._prefix,
        diff => diff.key[0],
        internalDiff => convertDiffValues(internalDiff, db.decodeIndexKey),
      );
    }

    return invoke<string>(
      undefined,
      this._prefix,
      diff => diff.key,
      internalDiff => convertDiffValues(internalDiff, k => k),
    );
  }

  matches(diffs: sync.DiffsMap): boolean {
    const diff = diffs.get(this._indexName ?? '');
    if (diff === undefined) {
      return false;
    }

    return watcherMatchesDiff(diff, this._prefix, this._indexName);
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
export interface SubscribeOptions<R extends ReadonlyJSONValue | undefined> {
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
}

type UnknownSubscription = Subscription<unknown>;

type SubscriptionSet = Set<UnknownSubscription>;

export class SubscriptionsManager implements DiffComputationConfig {
  private readonly _subscriptions: SubscriptionSet = new Set();
  private readonly _pendingSubscriptions: SubscriptionSet = new Set();
  private readonly _queryInternal: QueryInternal;
  private readonly _lc: LogContext;
  hasPendingSubscriptionRuns = false;

  constructor(queryInternal: QueryInternal, lc: LogContext) {
    this._queryInternal = queryInternal;
    this._lc = lc;
  }

  private _add(subscription: UnknownSubscription): () => void {
    this._subscriptions.add(subscription);
    void this._scheduleInitialSubscriptionRun(subscription);
    return () => this._subscriptions.delete(subscription);
  }

  addSubscription<R extends ReadonlyJSONValue | undefined>(
    body: (tx: ReadTransaction) => Promise<R>,
    {onData, onError, onDone}: SubscribeOptions<R>,
  ): () => void {
    const s = new SubscriptionImpl(
      body,
      onData,
      onError,
      onDone,
    ) as unknown as UnknownSubscription;

    return this._add(s);
  }

  addWatch(
    callback: WatchCallback,
    options: WatchOptions | undefined,
  ): () => void {
    const w = new WatchImpl(callback, options ?? {}) as UnknownSubscription;
    return this._add(w);
  }

  clear(): void {
    for (const subscription of this._subscriptions) {
      subscription.onDone?.();
    }
    this._subscriptions.clear();
  }

  async fire(diffs: sync.DiffsMap): Promise<void> {
    const subscriptions = subscriptionsForDiffs(this._subscriptions, diffs);
    await this._fireSubscriptions(subscriptions, InvokeKind.Regular, diffs);
  }

  private async _fireSubscriptions(
    subscriptions: Iterable<UnknownSubscription>,
    kind: InvokeKind,
    diffs: sync.DiffsMap | undefined,
  ) {
    const subs = [...subscriptions] as readonly Subscription<unknown>[];
    if (subs.length === 0) {
      return;
    }

    // Use allSettled to gather fulfilled and rejected promises.
    const results = await this._queryInternal(tx =>
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
    for (let i = 0; i < subs.length; i++) {
      const s = subs[i];
      const result = results[i];
      if (result.status === 'fulfilled') {
        s.onData(result.value);
      } else {
        if (s.onError) {
          s.onError(result.reason);
        } else {
          this._lc.error?.(result.reason);
        }
      }
    }
  }

  private async _scheduleInitialSubscriptionRun(s: UnknownSubscription) {
    this._pendingSubscriptions.add(s);

    if (!this.hasPendingSubscriptionRuns) {
      this.hasPendingSubscriptionRuns = true;
      await Promise.resolve();
      this.hasPendingSubscriptionRuns = false;
      const subscriptions = [...this._pendingSubscriptions];
      this._pendingSubscriptions.clear();
      await this._fireSubscriptions(
        subscriptions,
        InvokeKind.InitialRun,
        undefined,
      );
    }
  }

  shouldComputeDiffs(): boolean {
    return this._subscriptions.size > 0;
  }

  shouldComputeDiffsForIndex(indexName: string): boolean {
    for (const s of this._subscriptions) {
      if (s.hasIndexSubscription(indexName)) {
        return true;
      }
    }
    return false;
  }
}

export type ScanSubscriptionInfo = {
  options: db.ScanOptions;
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

  const [changedKeySecondary, changedKeyPrimary] =
    db.decodeIndexKey(changedKey);

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
  diffs: sync.DiffsMap,
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
    ? (diffOp: InternalDiffOperation) => db.decodeIndexKey(diffOp.key)[0]
    : (diffOp: InternalDiffOperation) => diffOp.key;
  const i = diffBinarySearch(diff, prefix, compareKey);
  return i < diff.length && compareKey(diff[i]).startsWith(prefix);
}

function diffBinarySearch<Key, Value>(
  diff: readonly InternalDiffOperation<Key, Value>[],
  prefix: string,
  compareKey: (diff: InternalDiffOperation<Key, Value>) => string,
): number {
  return binarySearch(diff.length, i =>
    compareUTF8(prefix, compareKey(diff[i])),
  );
}
