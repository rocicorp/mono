import {assert} from '../../../shared/src/asserts.ts';
import type {Immutable} from '../../../shared/src/immutable.ts';
import {emptyArray} from '../../../shared/src/sentinels.ts';
import type {ErroredQuery} from '../../../zero-protocol/src/custom-queries.ts';
import type {TTL} from '../query/ttl.ts';
import type {Listener, ResultType, TypedView} from '../query/typed-view.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import type {Change} from './change.ts';
import {skipYields, type Input, type Output} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {applyChange, type ViewChange} from './view-apply-change.ts';
import type {Entry, Format} from './view.ts';

function changeToViewChange(change: Change): ViewChange {
  switch (change[ChangeIndex.TYPE]) {
    case ChangeType.ADD:
      return {type: 'add', node: change[ChangeIndex.NODE]};
    case ChangeType.REMOVE:
      return {type: 'remove', node: change[ChangeIndex.NODE]};
    case ChangeType.CHILD:
      return {
        type: 'child',
        node: change[ChangeIndex.NODE],
        child: {
          relationshipName: change[ChangeIndex.CHILD_DATA].relationshipName,
          change: changeToViewChange(change[ChangeIndex.CHILD_DATA].change),
        },
      };
    case ChangeType.EDIT:
      return {
        type: 'edit',
        node: change[ChangeIndex.NODE],
        oldNode: change[ChangeIndex.OLD_NODE],
      };
  }
}

/**
 * Implements a materialized view of the output of an operator.
 *
 * It might seem more efficient to use an immutable b-tree for the
 * materialization, but it's not so clear. Inserts in the middle are
 * asymptotically slower in an array, but can often be done with zero
 * allocations, where changes to the b-tree will often require several allocs.
 *
 * Also the plain array view is more convenient for consumers since you can dump
 * it into console to see what it is, rather than having to iterate it.
 */
// `V` is unconstrained: besides a `View` it may be a scalar `Value` for a
// top-level aggregate query, whose materialized result is the bare value.
export class ArrayView<V> implements Output, TypedView<V> {
  readonly #input: Input;
  readonly #listeners = new Set<Listener<V>>();
  // Read lazily: a DeferredInput has no schema until its pipeline is attached,
  // and the schema is only needed once there is a change to apply.
  #schema: SourceSchema | undefined;
  readonly #format: Format;

  // Synthetic "root" entry that has a single "" relationship, so that we can
  // treat all changes, including the root change, generically.
  //
  // applyChange is immutable: it returns a new root, preserving the references
  // of unchanged subtrees so consumers (React.memo / Solid) can skip them. We
  // therefore reassign #root on every change rather than mutating in place.
  #root: Entry;

  onDestroy: (() => void) | undefined;

  #dirty = false;
  // Set by holdData(): the root that `data` and newly added listeners keep
  // seeing until releaseData() or the next flush, while pushes build up #root
  // behind it.
  #committedRoot: Entry | undefined;
  // From holdData() until the next flush. Outlasts #committedRoot, which
  // releaseData() drops first.
  #held = false;
  // The result type and error as of holdData(): what a listener added while
  // held is called with, so it never sees old rows paired with a new type.
  #heldResultType: ResultType = 'unknown';
  #heldError: ErroredQuery | undefined;
  #resultType: ResultType = 'unknown';
  #error: ErroredQuery | undefined;
  readonly #updateTTL: (ttl: TTL) => void;

  // Objects/arrays created or cloned during the current (un-flushed)
  // transaction. applyChange may mutate these in place rather than copying
  // again, since they are not yet observed by any listener. Replaced at
  // flush(), after which the committed snapshot must be copied-on-write again.
  // A WeakSet (not a Set) so it never extends the lifetime of transient clones.
  #txnDirty: WeakSet<object> = new WeakSet();

  constructor(
    input: Input,
    format: Format,
    queryComplete: true | ErroredQuery | Promise<true>,
    updateTTL: (ttl: TTL) => void,
  ) {
    this.#input = input;
    this.#format = format;
    this.#updateTTL = updateTTL;
    this.#root = {'': format.singular ? undefined : []};
    input.setOutput(this);

    if (queryComplete === true) {
      this.#resultType = 'complete';
    } else if ('error' in queryComplete) {
      this.#resultType = 'error';
      this.#error = queryComplete;
    } else {
      void queryComplete
        .then(() => this.#setResultType('complete'))
        .catch(e => this.#setResultType('error', e));
    }
    this.#hydrate();
  }

  get data() {
    return (this.#committedRoot ?? this.#root)[''] as V;
  }

  #getSchema(): SourceSchema {
    return (this.#schema ??= this.#input.getSchema());
  }

  addListener(listener: Listener<V>) {
    assert(!this.#listeners.has(listener), 'Listener already registered');
    this.#listeners.add(listener);

    this.#fireListener(listener);

    return () => {
      this.#listeners.delete(listener);
    };
  }

  #fireListeners() {
    for (const listener of this.#listeners) {
      this.#fireListener(listener);
    }
  }

  #fireListener(listener: Listener<V>) {
    if (this.#committedRoot !== undefined) {
      // Held rows go with the held type; after releaseData() both are current.
      listener(
        this.data as Immutable<V>,
        this.#heldResultType,
        this.#heldError,
      );
      return;
    }
    listener(this.data as Immutable<V>, this.#resultType, this.#error);
  }

  destroy() {
    this.onDestroy?.();
  }

  #hydrate() {
    this.#dirty = true;
    for (const node of skipYields(this.#input.fetch({}))) {
      this.#root = applyChange(
        this.#root,
        {type: 'add', node},
        this.#getSchema(),
        '',
        this.#format,
        false /* withIDs */,
        true /* mutate: #root is freshly created and not yet observed by any
                 consumer, so build it in place to avoid O(N^2) array copies.
                 Every later push() is immutable, preserving reference
                 stability for unchanged subtrees. */,
      );
    }
    this.flush();
  }

  push(change: Change) {
    this.#dirty = true;
    this.#root = applyChange(
      this.#root,
      changeToViewChange(change),
      this.#getSchema(),
      '',
      this.#format,
      false /* withIDs */,
      this.#txnDirty /* mutate: copy-on-write within this transaction */,
    );
    return emptyArray;
  }

  /**
   * Keep exposing the current snapshot, whatever is pushed in the meantime,
   * until {@link releaseData} or the next {@link flush}. Result type
   * notifications are held back until that flush.
   *
   * A transaction is normally one synchronous task, so nobody can look at a
   * view between its pushes and its flush. Deferred pipelines are different:
   * they are hydrated over several tasks and flushed together (see
   * `QueryDelegate.onPipelinesReady`), and a view that already has its rows
   * must not show them early.
   */
  holdData(): void {
    this.#committedRoot = this.#root;
    this.#held = true;
    this.#heldResultType = this.#resultType;
    this.#heldError = this.#error;
  }

  /**
   * Make `data` current again without notifying anyone yet; listeners are
   * still only called by the next {@link flush}. Views released together get
   * this called on all of them before any is flushed, so a listener of one
   * that reads another sees its rows too.
   */
  releaseData(): void {
    this.#committedRoot = undefined;
  }

  flush() {
    this.#committedRoot = undefined;
    this.#held = false;
    if (!this.#dirty) {
      return;
    }
    this.#dirty = false;
    this.#fireListeners();
    // The snapshot just handed to listeners is now observed; the next
    // transaction must copy-on-write rather than mutate these objects. A fresh
    // WeakSet drops all "owned" marks (WeakSet has no clear()).
    this.#txnDirty = new WeakSet();
  }

  updateTTL(ttl: TTL) {
    this.#updateTTL(ttl);
  }

  /**
   * The store holds the server-confirmed complete result of this query from a
   * previous sync (the persisted got-queries key exists, pre-authoritative).
   * Never downgrades 'complete'/'error', and never resolves anything that
   * waits on 'complete' — freshness stays a promise only the connection can
   * keep.
   */
  markCached(): void {
    if (this.#resultType === 'unknown') {
      this.#setResultType('cached');
    }
  }

  /** The got key was deleted (eviction) while still pre-authoritative. */
  unmarkCached(): void {
    if (this.#resultType === 'cached') {
      this.#setResultType('unknown');
    }
  }

  #setResultType(resultType: ResultType, error?: ErroredQuery) {
    this.#resultType = resultType;
    this.#error = error;
    // A dirty view is mid-transaction: rows were pushed and not yet flushed,
    // and the objects in #txnDirty are still mutable. Firing now would hand
    // those to listeners and then fire again at flush() with the same data, so
    // let the pending flush deliver the new result type instead.
    // A held view is waiting to be exposed together with others at the next
    // flush; that goes for its result type as much as for its rows.
    if (this.#held) {
      // Dirty so that the flush notifies even when no rows were pushed.
      this.#dirty = true;
    } else if (!this.#dirty) {
      this.#fireListeners();
    }
  }
}
