import {Change} from './change.js';
import {Row, Comparator} from './data.js';
import {Input, Output} from './operator.js';
import {assert} from 'shared/src/asserts.js';
import {Schema} from './schema.js';
import {must} from 'shared/src/must.js';
import {DeepReadonly} from 'replicache';

export type Listener = (view: DeepReadonly<View>) => void;

/**
 * Implements a materialized view of the output of an operator.
 *
 * The materialization we choose is JS Array. There's an interesting dx/perf
 * tradeoff here.
 *
 * It would be more efficient from this class's pov to store a BTree so that
 * inserts/removes can be fast. But if the user is going to immediately slurp
 * that thing into an array because it's more ergonomic to use in React, then
 * they'll copy all the data on each change.
 *
 * On the other hand, once we have first-class edit support, adds/removes will
 * be much less common so maybe the array isn't that bad after all.
 *
 * Also, React is constantly copying and diffing objects anyway so maybe none of
 * this matters 😂.
 *
 * Net net, DX wins here. Arrays are easier to work with and more common in
 * UI development. We'll start there.
 */
export class MemoryView implements Output {
  readonly #input: Input;
  readonly #view: View;
  readonly #listeners = new Set<Listener>();
  readonly #schema: Schema;

  #hydrated = false;

  constructor(input: Input) {
    this.#input = input;
    this.#schema = input.getSchema();

    this.#input.setOutput(this);
    this.#view = [];
  }

  addListener(listener: Listener) {
    assert(!this.#listeners.has(listener), 'Listener already registered');
    this.#listeners.add(listener);
  }

  removeListener(listener: Listener) {
    assert(this.#listeners.has(listener), 'Listener not registered');
    this.#listeners.delete(listener);
  }

  #fireListeners() {
    for (const listener of this.#listeners) {
      listener(this.#view);
    }
  }

  hydrate() {
    if (this.#hydrated) {
      throw new Error("Can't hydrate twice");
    }
    for (const node of this.#input.fetch({})) {
      applyChange(this.#view, {type: 'add', node}, this.#schema);
    }
    this.#fireListeners();
  }

  push(change: Change): void {
    applyChange(this.#view, change, this.#schema);
    this.#fireListeners();
  }
}

type View = Entry[];

type Entry = {
  row: Row;
  related: Record<string, View>;
};

function applyChange(view: View, change: Change, schema: Schema) {
  const v: Readonly<View> = [];
  v[0].related = {};

  if (change.type === 'add') {
    const newEntry: Entry = {
      row: change.node.row,
      related: {},
    };
    const {pos, found} = binarySearch(view, newEntry.row, schema.compareRows);
    assert(!found, 'node already exists');
    view.splice(pos, 0, newEntry);

    for (const [relationship, children] of Object.entries(
      change.node.relationships,
    )) {
      // TODO: Is there a flag to make TypeScript complain that dictionary access might be undefined?
      const childSchema = must(schema.relationships[relationship]);
      const newView: View = [];
      newEntry.related[relationship] = newView;
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
      schema.relationships[change.child.relationshipName],
    );
    applyChange(
      existing.related[change.child.relationshipName],
      change.child.change,
      childSchema,
    );
  }
}

function binarySearch(view: View, target: Row, comparator: Comparator) {
  let low = 0;
  let high = view.length - 1;
  while (low <= high) {
    const mid = (low + high) >>> 1;
    const comparison = comparator(view[mid].row, target);
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
