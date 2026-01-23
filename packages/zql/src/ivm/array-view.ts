import {assert} from '../../../shared/src/asserts.ts';
import type {Immutable} from '../../../shared/src/immutable.ts';
import {emptyArray} from '../../../shared/src/sentinels.ts';
import type {ErroredQuery} from '../../../zero-protocol/src/custom-queries.ts';
import type {TTL} from '../query/ttl.ts';
import type {Listener, ResultType, TypedView} from '../query/typed-view.ts';
import type {Change} from './change.ts';
import {skipYields, type Input, type Output} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {applyChange} from './view-apply-change.ts';
import type {Entry, Format, View} from './view.ts';

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
    for (const node of skipYields(this.#input.fetch({}))) {
      this.#root = applyChange(
        this.#root,
        {type: 'add', node},
        this.#schema,
        '',
        this.#format,
      );
    }
    this.flush();
  }

  push(change: Change) {
    this.#dirty = true;
    this.#root = applyChange(this.#root, change, this.#schema, '', this.#format);
    return emptyArray;
  }

  flush() {
    if (!this.#dirty) {
      return;
    }
    this.#dirty = false;
    this.#fireListeners();
  }

  updateTTL(ttl: TTL) {
    this.#updateTTL(ttl);
  }
}
