import BTree from 'btree';
import {Change} from './change.js';
import {Row, Value} from './data.js';
import {Input, Output} from './operator.js';
import {assert} from 'shared/src/asserts.js';
import {Schema} from './schema.js';
import {must} from 'shared/src/must.js';
import {StoppableIterator} from './stopable-iterator.js';

/**
 * A listener that is called when the view changes. The iterable passed is only
 * valid for the lifetime of the listener call. If the listener needs to keep
 * the data around, it should copy it.
 */
export type Listener = (data: Iterable<Entry>) => void;

/**
 * A row with its relationships.
 */
export type Entry = Record<string, Value | Iterable<Entry>>;

/**
 * Implements a materialized view of the output of a Pipeline.
 */
export class View implements Output {
  readonly #input: Input;
  readonly #entries: EntryList;
  readonly #listeners = new Set<Listener>();

  #hydrated = false;

  constructor(input: Input) {
    this.#input = input;

    this.#input.setOutput(this);
    this.#entries = new EntryList(this.#input.getSchema());
  }

  addListener(listener: Listener) {
    assert(!this.#listeners.has(listener), 'Listener already registered');
    this.#listeners.add(listener);
  }

  removeListener(listener: Listener) {
    assert(this.#listeners.has(listener), 'Listener not registered');
    this.#listeners.delete(listener);
  }

  destroy(): void {
    this.#input.destroy();
  }

  #fireListeners() {
    for (const listener of this.#listeners) {
      listener(this.#entries);
    }
    this.#entries.stopIterators();
  }

  hydrate() {
    if (this.#hydrated) {
      throw new Error("Can't hydrate twice");
    }
    this.#hydrated = true;

    for (const node of this.#input.fetch({})) {
      this.#entries.applyChange({type: 'add', node});
    }
    this.#fireListeners();
  }

  push(change: Change): void {
    this.#entries.applyChange(change);
    this.#fireListeners();
  }
}

class EntryList {
  #data: BTree<Entry, undefined>;
  #schema: Schema;
  #iterators: StoppableIterator<Entry>[] = [];

  constructor(schema: Schema) {
    this.#data = new BTree<Entry, undefined>([], (e1, e2) =>
      schema.compareRows(e1 as Row, e2 as Row),
    );
    this.#schema = schema;
  }

  applyChange(change: Change) {
    if (change.type === 'add') {
      const newEntry: Entry = {...change.node.row};
      const added = this.#data.add(newEntry, undefined);
      assert(added, 'row already exists');
      for (const [relationship, children] of Object.entries(
        change.node.relationships,
      )) {
        // TODO: Is there a flag to make TypeScript complain that dictionary access might be undefined?
        const childSchema = must(this.#schema.relationships[relationship]);
        const newView = new EntryList(childSchema);
        newEntry[relationship] = newView;
        for (const node of children) {
          newView.applyChange({type: 'add', node});
        }
      }
    } else if (change.type === 'remove') {
      const deleted = this.#data.delete(change.node.row);
      assert(deleted, 'row does not exist');
    } else {
      change.type satisfies 'child';
      const [[existing]] = this.#data.entries(change.row);
      assert(existing, 'parent row does not exist');
      const child = existing[change.child.relationshipName];
      assert(child instanceof EntryList);
      child.applyChange(change.child.change);
    }
  }

  [Symbol.iterator]() {
    const it = new StoppableIterator(this.#data.keys());
    this.#iterators.push(it);
    return it;
  }

  stopIterators() {
    for (const it of this.#iterators) {
      it.stop();
    }
    this.#iterators = [];
    for (const entry of this.#data.keys()) {
      for (const [, child] of Object.entries(entry)) {
        if (child instanceof EntryList) {
          child.stopIterators();
        }
      }
    }
  }
}
