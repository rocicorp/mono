import {assert} from '../../../shared/src/asserts.ts';
import type {Immutable} from '../../../shared/src/immutable.ts';
import type {Writable} from '../../../shared/src/writable.ts';
import type {ErroredQuery} from '../../../zero-protocol/src/custom-queries.ts';
import type {TTL} from '../query/ttl.ts';
import type {Listener, ResultType, TypedView} from '../query/typed-view.ts';
import type {Change} from './change.ts';
import type {Input, Output} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {applyChange} from './view-apply-change.ts';
import type {Entry, Format, View} from './view.ts';

/**
 * Shallow copy a View tree for React immutability.
 * Creates new object and array references but reuses primitive values.
 * This is much more efficient than deep cloning for large result sets.
 */
function shallowCopyView(value: View): View {
  if (value === undefined) {
    return undefined;
  }
  if (Array.isArray(value)) {
    // Map creates a new array, and we shallow copy each entry
    return value.map(shallowCopyEntry);
  }
  // Singular entry
  return shallowCopyEntry(value as Entry);
}

/**
 * Shallow copy an Entry, recursively copying nested Views.
 * Preserves symbol properties (refCount, id) and creates new object
 * references for React's reference equality checks.
 */
function shallowCopyEntry(entry: Entry): Entry {
  // Use object spread to shallow copy enumerable properties
  const newEntry: Record<string, unknown> = {...entry};

  // Copy symbol properties (refCountSymbol, idSymbol)
  for (const sym of Object.getOwnPropertySymbols(entry)) {
    newEntry[sym as unknown as string] = (entry as Record<symbol, unknown>)[
      sym
    ];
  }

  // Recursively shallow copy nested views (relationships)
  for (const key of Object.keys(newEntry)) {
    const value = newEntry[key];
    if (Array.isArray(value)) {
      // Check if this is a View (array of Entry objects with symbols)
      // or just a plain array (e.g., JSON column data)
      if (
        value.length > 0 &&
        typeof value[0] === 'object' &&
        value[0] !== null &&
        Object.getOwnPropertySymbols(value[0]).length > 0
      ) {
        // Array of Entry objects (one-to-many relationship)
        newEntry[key] = value.map(shallowCopyEntry);
      } else {
        // Plain array (JSON data) - shallow copy the array itself
        newEntry[key] = [...value];
      }
    } else if (
      value !== null &&
      value !== undefined &&
      typeof value === 'object' &&
      Object.getOwnPropertySymbols(value).length > 0
    ) {
      // Nested Entry object (one-to-one relationship)
      // Only shallow copy if it has symbols (indicating it's an Entry)
      newEntry[key] = shallowCopyEntry(value as Entry);
    } else if (
      value !== null &&
      value !== undefined &&
      typeof value === 'object'
    ) {
      // Plain object (JSON data) - shallow copy it
      newEntry[key] = Array.isArray(value) ? [...value] : {...value};
    }
    // Primitives are already copied by spread operator
  }

  return newEntry as Entry;
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
  readonly #root: Entry;

  onDestroy: (() => void) | undefined;

  #dirty = false;
  #resultType: ResultType = 'unknown';
  #error: ErroredQuery | undefined;
  readonly #updateTTL: (ttl: TTL) => void;

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
      void queryComplete
        .then(() => {
          this.#resultType = 'complete';
          this.#fireListeners();
        })
        .catch(e => {
          this.#resultType = 'error';
          this.#error = e;
          this.#fireListeners();
        });
    }
    this.#hydrate();
  }

  get data() {
    return this.#root[''] as V;
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
    for (const node of this.#input.fetch({})) {
      applyChange(
        this.#root,
        {type: 'add', node},
        this.#schema,
        '',
        this.#format,
      );
    }
    this.flush();
  }

  push(change: Change): void {
    this.#dirty = true;
    applyChange(this.#root, change, this.#schema, '', this.#format);
  }

  flush() {
    if (!this.#dirty) {
      return;
    }
    this.#dirty = false;

    // Create new object/array references for React's immutability.
    // This shallow copy is much cheaper than deep cloning while still
    // ensuring React detects changes via reference equality.
    (this.#root as Writable<Entry>)[''] = shallowCopyView(
      this.#root[''] as View,
    );

    this.#fireListeners();
  }

  updateTTL(ttl: TTL) {
    this.#updateTTL(ttl);
  }
}
