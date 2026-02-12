import {assert} from '../../../shared/src/asserts.ts';
import type {Immutable} from '../../../shared/src/immutable.ts';
import {mapValues} from '../../../shared/src/objects.ts';
import {emptyArray} from '../../../shared/src/sentinels.ts';
import type {ErroredQuery} from '../../../zero-protocol/src/custom-queries.ts';
import type {TTL} from '../query/ttl.ts';
import type {Listener, ResultType, TypedView} from '../query/typed-view.ts';
import type {Change} from './change.ts';
import type {Node} from './data.ts';
import {skipYields, type Input, type Output} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {
  applyChanges,
  type ExpandedNode,
  type ViewChange,
} from './view-apply-change.ts';
import type {Entry, Format, View} from './view.ts';

/**
 * Eagerly expand a Node's lazy relationship generators into arrays.
 * This captures the current state of the source at the moment of expansion.
 */
function expandNode(node: Node): ExpandedNode {
  return {
    row: node.row,
    relationships: mapValues(node.relationships, v =>
      Array.from(skipYields(v()), expandNode),
    ),
  };
}

/**
 * Expand a Change by eagerly evaluating all lazy relationship generators.
 */
function expandChange(change: Change): ViewChange {
  switch (change.type) {
    case 'add':
    case 'remove':
      return {type: change.type, node: expandNode(change.node)};
    case 'edit':
      return {
        type: 'edit',
        node: expandNode(change.node),
        oldNode: expandNode(change.oldNode),
      };
    case 'child':
      return {
        type: 'child',
        node: expandNode(change.node),
        child: {
          relationshipName: change.child.relationshipName,
          change: expandChange(change.child.change),
        },
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
export class ArrayView<V extends View> implements Output, TypedView<V> {
  readonly #input: Input;
  readonly #listeners = new Set<Listener<V>>();
  readonly #schema: SourceSchema;
  readonly #format: Format;

  // Synthetic "root" entry that has a single "" relationship, so that we can
  // treat all changes, including the root change, generically.
  #root: Entry;

  onDestroy: (() => void) | undefined;

  #dirty = false;
  #resultType: ResultType = 'unknown';
  #error: ErroredQuery | undefined;
  readonly #updateTTL: (ttl: TTL) => void;

  // Pending changes buffered for batch application (O(N + K) optimization)
  #pendingChanges: ViewChange[] = [];

  constructor(
    input: Input,
    format: Format,
    queryComplete: true | ErroredQuery | Promise<true>,
    updateTTL: (ttl: TTL) => void,
  ) {
    this.#input = input;
    this.#schema = input.getSchema();
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
      const flushAndFire = () =>
        this.#dirty ? this.flush() : this.#fireListeners();
      void queryComplete
        .then(() => {
          this.#resultType = 'complete';
          flushAndFire();
        })
        .catch((e: ErroredQuery) => {
          this.#resultType = 'error';
          this.#error = e;
          flushAndFire();
        });
    }
    this.#hydrate();
  }

  get data() {
    // Auto-flush for backwards compatibility. Recommended: push() then flush().
    //
    //   push(A) ──► buffer ──► push(B) ──► buffer ──► flush() ──► apply all
    //                                                    │
    //   Legacy code may read .data here ─────────────────┘ (before flush)
    //                          │
    //                          ▼
    //   Without auto-flush: stale data (missing A, B)
    //   With auto-flush:    current data (has A, B)
    this.#applyPendingChanges();
    return this.#root[''] as V;
  }

  #applyPendingChanges() {
    if (this.#pendingChanges.length > 0) {
      this.#root = applyChanges(
        this.#root,
        this.#pendingChanges,
        this.#schema,
        '',
        this.#format,
      );
      this.#pendingChanges = [];
    }
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
    listener(this.data as Immutable<V>, this.#resultType, this.#error);
  }

  destroy() {
    this.onDestroy?.();
  }

  #hydrate() {
    this.#dirty = true;
    // During hydration, expand and apply nodes immediately
    for (const node of skipYields(this.#input.fetch({}))) {
      const expanded = expandNode(node);
      this.#root = applyChanges(
        this.#root,
        [{type: 'add', node: expanded}],
        this.#schema,
        '',
        this.#format,
      );
    }
    this.flush();
  }

  push(change: Change) {
    this.#dirty = true;
    // Eagerly expand the change to capture current source state.
    // This is critical: lazy generators would see stale data if deferred.
    // Buffer the change for batch application (O(N + K) optimization).
    const expanded = expandChange(change);
    this.#pendingChanges.push(expanded);
    return emptyArray;
  }

  flush() {
    if (!this.#dirty) {
      return;
    }
    this.#dirty = false;
    // Apply all pending changes in one batch (O(N + K) optimization)
    this.#applyPendingChanges();
    this.#fireListeners();
  }

  updateTTL(ttl: TTL) {
    this.#updateTTL(ttl);
  }
}
