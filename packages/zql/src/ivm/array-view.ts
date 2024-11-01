import {assert} from '../../../shared/src/asserts.js';
import type {Immutable} from '../../../shared/src/immutable.js';
import type {Listener, TypedView} from '../query/typed-view.js';
import type {Change} from './change.js';
import type {Input, Output} from './operator.js';
import type {TableSchema} from './schema.js';
import {applyChange} from './view-apply-change.js';
import type {Entry, Format, View} from './view.js';

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
  readonly #schema: TableSchema;
  readonly #format: Format;

  // Synthetic "root" entry that has a single "" relationship, so that we can
  // treat all changes, including the root change, generically.
  readonly #root: Entry;

  onDestroy: (() => void) | undefined;

  #dirty = false;

  constructor(
    input: Input,
    format: Format = {singular: false, relationships: {}},
  ) {
    this.#input = input;
    this.#schema = input.getSchema();
    this.#format = format;
    this.#input.setOutput(this);
    this.#root = {'': format.singular ? undefined : []};

    this.#hydrate();
  }

  get data() {
    return this.#root[''] as V;
  }

  addListener(listener: Listener<V>) {
    assert(!this.#listeners.has(listener), 'Listener already registered');
    this.#listeners.add(listener);

    listener(this.data as Immutable<V>);

    return () => {
      this.#listeners.delete(listener);
    };
  }

  #fireListeners() {
    for (const listener of this.#listeners) {
      listener(this.data as Immutable<V>);
    }
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
    this.#fireListeners();
  }
}
