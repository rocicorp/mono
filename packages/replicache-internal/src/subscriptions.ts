import * as db from './db/mod';
import type * as sync from './sync/mod';
import {assert} from './asserts';
import type {Diff, DiffOperation} from './btree/node';
import {deepEqual, JSONValue, ReadonlyJSONValue} from './json';
import type {DiffsMap} from './sync/pull';
import {ReadTransaction, SubscriptionTransactionWrapper} from './transactions';
import type {QueryInternal} from './replicache';
import type {LogContext} from '@rocicorp/logger';
import {binarySearch} from './binary-search';

const enum InvokeKind {
  IndexChange,
  InitialRun,
  Regular,
}

interface Subscription<R, E> {
  invoke(
    tx: ReadTransaction,
    kind: InvokeKind,
    diffs: DiffsMap | undefined,
  ): Promise<R>;

  matches(diffs: DiffsMap): boolean;

  matchesIndexDefinitionChange(name: string): boolean;

  updateDeps(
    keys: ReadonlySet<string>,
    scans: ReadonlyArray<Readonly<ScanSubscriptionInfo>>,
  ): void;

  readonly onData: (result: R) => void;
  readonly onError: ((error: E) => void) | undefined;
  readonly onDone: (() => void) | undefined;
}

const emptySet: ReadonlySet<string> = new Set();

class SubscriptionImpl<R, E> implements Subscription<R, E> {
  private readonly _body: (tx: ReadTransaction) => Promise<R>;
  private readonly _onData: (result: R) => void;
  private _skipEqualsCheck = true;
  private _lastValue: R | undefined = undefined;
  private _keys = emptySet;
  private _scans: readonly Readonly<ScanSubscriptionInfo>[] = [];

  readonly onError: ((error: E) => void) | undefined;
  readonly onDone: (() => void) | undefined;

  constructor(
    body: (tx: ReadTransaction) => Promise<R>,
    onData: (result: R) => void,
    onError: ((error: E) => void) | undefined,
    onDone: (() => void) | undefined,
  ) {
    this._body = body;
    this._onData = onData;
    this.onError = onError;
    this.onDone = onDone;
  }

  invoke(
    tx: ReadTransaction,
    _kind: InvokeKind,
    _diffs: DiffsMap | undefined,
  ): Promise<R> {
    return this._body(tx);
  }

  matches(diffs: DiffsMap): boolean {
    for (const [indexName, diff] of diffs) {
      if (diffMatchesSubscription(this._keys, this._scans, indexName, diff)) {
        return true;
      }
    }

    return false;
  }

  matchesIndexDefinitionChange(name: string): boolean {
    return this._scans.some(scanInfo => scanInfo.options.indexName === name);
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
 * Function that gets passed into [[Replicache.experimentalWatch]] and gets
 * called when the data in Replicache changes.
 *
 * @experimental This type is experimental and may change in the future.
 */
export type WatchCallback = (diff: Diff) => void;

export type CallbackEntry = {
  cb: WatchCallback;
  prefix: string;
};

/**
 * Options for [[Replicache.experimentalWatch]].
 *
 * @experimental This interface is experimental and may change in the future.
 */
export interface WatchOptions {
  /**
   * When provided, the `watch` is limited to changes where the `key` starts with
   * `prefix`.
   */
  prefix?: string;

  /**
   * When this is set to `true` (default is `false`), the `watch` callback will
   * be called once asynchronously when watch is called. The arguments in that
   * case is a diff where we consider all the existing values in Replicache as
   * being added.
   */
  initialValuesInFirstDiff?: boolean;
}

class WatchImpl implements Subscription<Diff | undefined, unknown> {
  private readonly _callback: WatchCallback;
  private readonly _prefix: string;
  private readonly _initialValuesInFirstDiff: boolean;

  readonly onError: ((error: unknown) => void) | undefined = undefined;
  readonly onDone: (() => void) | undefined = undefined;

  constructor(callback: WatchCallback, options: WatchOptions) {
    this._callback = callback;
    this._prefix = options.prefix ?? '';
    this._initialValuesInFirstDiff = options.initialValuesInFirstDiff ?? false;
  }

  onData(result: Diff | undefined): void {
    if (result !== undefined) {
      this._callback(result);
    }
  }

  async invoke(
    tx: ReadTransaction,
    kind: InvokeKind,
    diffs: DiffsMap | undefined,
  ): Promise<Diff | undefined> {
    let diff: Diff;
    if (kind === InvokeKind.InitialRun) {
      if (!this._initialValuesInFirstDiff) {
        // We are using `undefined` here as a sentinel value to indicate that we
        // should not call the callback in `onDone`.
        return undefined;
      }

      // For the initial run, we need to get the "diffs" for the whole tree.

      assert(diffs === undefined);
      const newDiff: DiffOperation[] = [];
      for await (const entry of tx.scan({prefix: this._prefix}).entries()) {
        newDiff.push({
          op: 'add',
          key: entry[0],
          newValue: entry[1],
        });
      }
      diff = newDiff;
    } else {
      assert(diffs);
      const maybeDiff = diffs.get('');
      if (maybeDiff === undefined) {
        return [];
      }
      diff = maybeDiff;
    }

    if (this._prefix === '') {
      return diff;
    }

    const newDiff: DiffOperation[] = [];
    const {length} = diff;
    const prefix = this._prefix;
    const compare = (i: number) => prefix <= diff[i].key;

    for (let i = binarySearch(length, compare); i < length; i++) {
      if (diff[i].key.startsWith(this._prefix)) {
        newDiff.push(diff[i]);
      } else {
        break;
      }
    }
    return newDiff.length > 0 ? newDiff : undefined;
  }

  matches(diffs: DiffsMap): boolean {
    const diff = diffs.get('');
    if (diff === undefined) {
      return false;
    }

    return watcherMatchesDiff(diff, this._prefix);
  }

  matchesIndexDefinitionChange(_name: string): boolean {
    return false;
  }

  updateDeps(
    _keys: ReadonlySet<string>,
    _scans: readonly Readonly<ScanSubscriptionInfo>[],
  ): void {
    // not used
  }
}

/**
 * The options passed to [[Replicache.subscribe]].
 */
export interface SubscribeOptions<R extends ReadonlyJSONValue | undefined, E> {
  /**
   * Called when the return value of the body function changes.
   */
  onData: (result: R) => void;

  /**
   * If present, called when an error occurs.
   */
  onError?: (error: E) => void;

  /**
   * If present, called when the subscription is removed/done.
   */
  onDone?: () => void;
}

type UnknownSubscription = Subscription<unknown, unknown>;

type SubscriptionSet = Set<UnknownSubscription>;

export class SubscriptionsManager {
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

  addSubscription<R extends ReadonlyJSONValue | undefined, E>(
    body: (tx: ReadTransaction) => Promise<R>,
    {onData, onError, onDone}: SubscribeOptions<R, E>,
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
    const subs = [...subscriptions] as readonly Subscription<
      unknown,
      unknown
    >[];
    if (subs.length === 0) {
      return;
    }

    type R =
      | {ok: true; value: JSONValue | undefined}
      | {ok: false; error: unknown};
    const results = await this._queryInternal(async tx => {
      // TODO(arv): Use Promise.allSettled instead of manually doing this.
      const promises = subs.map(async s => {
        // Tag the result so we can deal with success vs error below.
        const stx = new SubscriptionTransactionWrapper(tx);
        try {
          const value = await s.invoke(stx, kind, diffs);
          return {ok: true, value} as R;
        } catch (error) {
          return {ok: false, error} as R;
        } finally {
          // We need to keep track of the subscription keys even if there was an
          // exception because changes to the keys can make the subscription
          // body succeed.
          s.updateDeps(stx.keys, stx.scans);
        }
      });
      return await Promise.all(promises);
    });
    for (let i = 0; i < subs.length; i++) {
      const s = subs[i];
      const result = results[i];
      if (result.ok) {
        s.onData(result.value);
        // }
      } else {
        if (s.onError) {
          s.onError(result.error);
        } else {
          this._lc.error?.(result.error);
        }
      }
    }
  }

  async indexDefinitionChanged(name: string): Promise<void> {
    // When an index definition changes we fire all subscriptions that uses
    // index scans with that index.
    const subscriptions = subscriptionsForIndexDefinitionChanged(
      this._subscriptions,
      name,
    );

    await this._fireSubscriptions(
      subscriptions,
      InvokeKind.IndexChange,
      undefined,
    );
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
}

export type ScanSubscriptionInfo = {
  options: db.ScanOptions;
  inclusiveLimitKey?: string;
};

function diffMatchesSubscription(
  keys: ReadonlySet<string>,
  scans: Iterable<Readonly<ScanSubscriptionInfo>>,
  indexName: string,
  diff: Diff,
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

export function scanInfoMatchesDiff(
  scanInfo: ScanSubscriptionInfo,
  changeIndexName: string,
  diff: Diff,
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
      ((startExclusive && changedKey <= startKey) ||
        changedKey < startKey ||
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
    ((startExclusive && changedKeySecondary <= startSecondaryKey) ||
      changedKeySecondary < startSecondaryKey)
  ) {
    return false;
  }

  if (
    startKey &&
    ((startExclusive && changedKeyPrimary <= startKey) ||
      changedKeyPrimary < startKey)
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
    changedKey > inclusiveLimitKey
  );
}

function* subscriptionsForDiffs<V, E>(
  subscriptions: Set<Subscription<V, E>>,
  diffs: sync.DiffsMap,
): Generator<Subscription<V, E>> {
  for (const subscription of subscriptions) {
    if (subscription.matches(diffs)) {
      yield subscription;
    }
  }
}

function* subscriptionsForIndexDefinitionChanged<V, E>(
  subscriptions: Set<Subscription<V, E>>,
  name: string,
): Generator<Subscription<V, E>> {
  for (const subscription of subscriptions) {
    if (subscription.matchesIndexDefinitionChange(name)) {
      yield subscription;
    }
  }
}

function watcherMatchesDiff(diff: Diff, prefix: string): boolean {
  if (prefix === '') {
    return true;
  }

  const i = binarySearch(diff.length, i => prefix <= diff[i].key);

  return i < diff.length && diff[i].key.startsWith(prefix);
}
