import {Primitive} from '../../../ast/ast.js';
import {Entry, Multiset} from '../../multiset.js';
import {
  JoinResult,
  StringOrNumber,
  isJoinResult,
  joinSymbol,
} from '../../types.js';

/**
 * Indexes difference events by a key.
 */
export class DifferenceIndex<Key extends Primitive, V> {
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

  extend(index: DifferenceIndex<Key, V>) {
    for (const [key, value] of index.#index) {
      for (const entry of value) {
        this.add(key, entry);
      }
    }
  }

  get(key: Key): Entry<V>[] {
    return this.#index.get(key) ?? [];
  }

  join<
    VO,
    AAlias extends string | undefined,
    BAlias extends string | undefined,
  >(
    aAlias: AAlias | undefined,
    other: DifferenceIndex<Key, VO>,
    bAlias: BAlias | undefined,
    getBValueIdentity: (v: VO) => StringOrNumber,
  ): Multiset<JoinResult<V, VO, AAlias, BAlias>> {
    const ret: (readonly [JoinResult<V, VO, AAlias, BAlias>, number])[] = [];
    let outerIndex;
    let innerIndex;
    let getOuterValueIdentity;
    let getInnerValueIdentity;
    let outerAlias;
    let innerAlias;

    if (this.#index.size < other.#index.size) {
      outerIndex = this.#index;
      innerIndex = other.#index;
      getOuterValueIdentity = this.#getValueIdentity;
      getInnerValueIdentity = getBValueIdentity;
      outerAlias = aAlias;
      innerAlias = bAlias;
    } else {
      outerIndex = other.#index;
      innerIndex = this.#index;
      getOuterValueIdentity = getBValueIdentity;
      getInnerValueIdentity = this.#getValueIdentity;
      outerAlias = bAlias;
      innerAlias = aAlias;
    }

    for (const [key, outerEntry] of outerIndex) {
      const innerEntry = innerIndex.get(key);
      if (innerEntry === undefined) {
        continue;
      }
      for (const [outerValue, outerMult] of outerEntry) {
        for (const [innerValue, innerMult] of innerEntry) {
          // TODO: is there an alternate formulation of JoinResult that requires fewer allocations?
          let value: JoinResult<V, VO, AAlias, BAlias>;

          // Flatten our join results so we don't
          // end up arbitrarily deep after many joins.
          // This handles the case of: A JOIN B JOIN C ...
          // A JOIN B produces {a, b}
          // A JOIN B JOIN C would produce {a_b: {a, b}, c} if we didn't flatten here.
          if (isJoinResult(outerValue) && isJoinResult(innerValue)) {
            value = {
              ...outerValue,
              ...innerValue,
              id: this.#concatIds(outerValue.id, innerValue.id),
            } as JoinResult<V, VO, AAlias, BAlias>;
          } else if (isJoinResult(outerValue)) {
            value = {
              ...outerValue,
              [innerAlias!]: innerValue,
              id: this.#concatIds(
                outerValue.id,
                getInnerValueIdentity(innerValue as unknown as V & VO),
              ),
            } as JoinResult<V, VO, AAlias, BAlias>;
          } else if (isJoinResult(innerValue)) {
            value = {
              ...innerValue,
              [outerAlias!]: outerValue,
              id: this.#concatIds(
                getOuterValueIdentity(outerValue as unknown as V & VO),
                innerValue.id,
              ),
            } as JoinResult<V, VO, AAlias, BAlias>;
          } else {
            value = {
              [joinSymbol]: true,
              id: this.#concatIds(
                getOuterValueIdentity(outerValue as unknown as V & VO),
                getInnerValueIdentity(innerValue as unknown as V & VO),
              ),
              [outerAlias!]: outerValue,
              [innerAlias!]: innerValue,
            } as JoinResult<V, VO, AAlias, BAlias>;
          }
          ret.push([value, outerMult * innerMult] as const);
        }
      }
    }
    return ret;
  }

  #concatIds(idA: string | number, idB: string | number) {
    let ret;
    if (idA.toString() < idB.toString()) {
      ret = idA + '_' + idB;
    } else {
      ret = idB + '_' + idA;
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
  compact(keys: Key[]) {
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

  #consolidateValues(value: Entry<V>[]) {
    // Map to consolidate entries with the same identity
    const consolidated = new Map<string | number, Entry<V>>();

    for (const entry of value) {
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
