import type {Primitive} from '../../../ast/ast.js';
import type {Entry} from '../../multiset.js';

/**
 * Indexes difference events by a key.
 */
export class DifferenceIndex<Key extends Primitive | undefined, V> {
  readonly #index = new Map<Key, Entry<V>[]>();
  readonly #getValueIdentity;

  constructor(getValueIdentity: (value: V) => string | number) {
    this.#getValueIdentity = getValueIdentity;
  }

  add(key: Key, value: Entry<V>) {
    let existing = this.#index.get(key);
    if (existing === undefined) {
      existing = [];
      this.#index.set(key, existing);
    }
    existing.push(value);
  }

  get(key: Key): Entry<V>[] | undefined {
    const ret = this.#index.get(key);
    if (ret === undefined || ret.length === 0) {
      return undefined;
    }

    return ret;
  }

  /**
   * Compaction is the process of summing multiplicities of entries with the same identity.
   * If the multiplicity of an entry becomes zero, it is removed from the index.
   *
   * Compaction is _not_ done when adding an item to the index as this would
   * break operators like `JOIN` that need to join against removals as well as additions.
   *
   * `JOIN` will compact its index at the end of each run.
   */
  compact(keys: Set<Key>) {
    // Go through all the keys that were requested to be compacted.
    for (const key of keys) {
      const values = this.#index.get(key);
      if (values === undefined) {
        continue;
      }
      const consolidated = this.#consolidateValues(values);
      if (consolidated.length === 0) {
        this.#index.delete(key);
      } else {
        this.#index.set(key, consolidated);
      }
    }
  }

  #consolidateValues(values: Entry<V>[]) {
    if (values.length === 1) {
      return values;
    }

    // Map to consolidate entries with the same identity
    const consolidated = new Map<string | number, Entry<V>>();

    for (const entry of values) {
      const identity = this.#getValueIdentity(entry[0]);
      const existing = consolidated.get(identity);
      if (existing !== undefined) {
        const newMultiplicity = existing[1] + entry[1];
        if (newMultiplicity === 0) {
          consolidated.delete(identity);
        } else {
          consolidated.set(identity, [entry[0], newMultiplicity]);
        }
      } else {
        consolidated.set(identity, entry);
      }
    }

    return [...consolidated.values()];
  }

  toString() {
    return JSON.stringify([...this.#index]);
  }
}
