import {assert} from '../../../shared/src/asserts.ts';
import type {Immutable} from '../../../shared/src/immutable.ts';
import type {Listener, TypedView} from './typed-view.ts';
import type {Change} from './change.ts';
import type {Input, Output} from './operator.ts';
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
export class ArrayView<V extends View, TOptions = unknown>
  implements Output, TypedView<V>
{
  readonly #input: Input;
  readonly #listeners = new Set<Listener<V>>();
  readonly #schema: SourceSchema;
  readonly #format: Format;

  // Synthetic "root" entry that has a single "" relationship, so that we can
  // treat all changes, including the root change, generically.
  readonly #root: Entry;

  onDestroy: (() => void) | undefined;

  #dirty = false;
  #complete = false;
  readonly #updateOptions: (options: TOptions) => void;

  constructor(
    input: Input,
    format: Format,
    queryComplete: true | Promise<true>,
    updateOptions: (optons: TOptions) => void,
  ) {
    this.#input = input;
    this.#schema = input.getSchema();
    this.#format = format;
    this.#updateOptions = updateOptions;
    this.#root = {'': format.singular ? undefined : []};
    input.setOutput(this);

    if (queryComplete === true) {
      this.#complete = true;
    } else {
      void queryComplete.then(() => {
        this.#complete = true;
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
    listener(
      this.data as Immutable<V>,
      this.#complete ? 'complete' : 'unknown',
    );
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

  updateOptions(options: TOptions) {
    this.#updateOptions(options);
  }
}
