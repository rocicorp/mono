import {assert} from 'shared/src/asserts.js';
import {Immutable} from 'shared/src/immutable.js';
import {must} from 'shared/src/must.js';
import {Change} from './change.js';
import {Comparator, Row, Value} from './data.js';
import {Input, Output} from './operator.js';
import {Schema} from './schema.js';

/**
 * Called when the view changes. The received data should be considered
 * immutable. Caller must not modify it. Passed data is valid until next
 * time listener is called.
 */
export type Listener = (entries: Immutable<EntryList>) => void;

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
export class ArrayView implements Output {
  readonly #input: Input;
  readonly #view: EntryList;
  readonly #listeners = new Set<Listener>();
  readonly #schema: Schema;

  onDestroy: (() => void) | undefined;

  #hydrated = false;
  #dirty = false;

  constructor(input: Input) {
    this.#input = input;
    this.#schema = input.getSchema();
    this.#input.setOutput(this);
    this.#view = [];
  }

  get data() {
    return this.#view;
  }

  addListener(listener: Listener) {
    assert(!this.#listeners.has(listener), 'Listener already registered');
    this.#listeners.add(listener);
    if (this.#hydrated) {
      listener(this.#view);
    }

    return () => {
      this.#listeners.delete(listener);
    };
  }

  #fireListeners() {
    for (const listener of this.#listeners) {
      listener(this.#view);
    }
  }

  destroy() {
    this.#input.destroy();
    this.onDestroy?.();
  }

  hydrate() {
    if (this.#hydrated) {
      throw new Error("Can't hydrate twice");
    }
    this.#hydrated = true;
    for (const node of this.#input.fetch({})) {
      this.#dirty = true;
      applyChange(this.#view, {type: 'add', node}, this.#schema);
    }
    this.flush();
  }

  push(change: Change): void {
    this.#dirty = true;
    applyChange(this.#view, change, this.#schema);
  }

  flush() {
    if (!this.#dirty) {
      return;
    }
    this.#dirty = false;
    this.#fireListeners();
  }
}

export type EntryList = Entry[];
export type Entry = Record<string, Value | EntryList>;

function applyChange(view: EntryList, change: Change, schema: Schema) {
  if (schema.isHidden) {
    switch (change.type) {
      case 'add':
      case 'remove': {
        for (const [relationship, children] of Object.entries(
          change.node.relationships,
        )) {
          const childSchema = must(schema.relationships?.[relationship]);
          for (const node of children) {
            applyChange(view, {type: change.type, node}, childSchema);
          }
        }
        return;
      }
      case 'child': {
        const childSchema = must(
          schema.relationships?.[change.child.relationshipName],
        );
        applyChange(view, change.child.change, childSchema);
        return;
      }
      default:
        change satisfies never;
    }
  }

  if (change.type === 'add') {
    const newEntry: Entry = {
      ...change.node.row,
    };
    const {pos, found} = binarySearch(view, newEntry, schema.compareRows);
    assert(!found, 'node already exists');
    view.splice(pos, 0, newEntry);

    for (const [relationship, children] of Object.entries(
      change.node.relationships,
    )) {
      // TODO: Is there a flag to make TypeScript complain that dictionary access might be undefined?
      const childSchema = must(schema.relationships?.[relationship]);
      const newView: EntryList = [];
      newEntry[relationship] = newView;
      for (const node of children) {
        applyChange(newView, {type: 'add', node}, childSchema);
      }
    }
  } else if (change.type === 'remove') {
    const {pos, found} = binarySearch(
      view,
      change.node.row,
      schema.compareRows,
    );
    assert(found, 'node does not exist');
    view.splice(pos, 1);
  } else {
    change.type satisfies 'child';
    const {pos, found} = binarySearch(view, change.row, schema.compareRows);
    assert(found, 'node does not exist');

    const existing = view[pos];
    const childSchema = must(
      schema.relationships?.[change.child.relationshipName],
    );
    const existingList = existing[change.child.relationshipName];
    assert(Array.isArray(existingList));
    applyChange(existingList, change.child.change, childSchema);
  }
}

function binarySearch(view: EntryList, target: Entry, comparator: Comparator) {
  let low = 0;
  let high = view.length - 1;
  while (low <= high) {
    const mid = (low + high) >>> 1;
    const comparison = comparator(view[mid] as Row, target as Row);
    if (comparison < 0) {
      low = mid + 1;
    } else if (comparison > 0) {
      high = mid - 1;
    } else {
      return {pos: mid, found: true};
    }
  }
  return {pos: low, found: false};
}
