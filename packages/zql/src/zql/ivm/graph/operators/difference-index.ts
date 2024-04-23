import type {Primitive} from '../../../ast/ast.js';
import type {Entry, Multiset} from '../../multiset.js';
import {
  JoinResult,
  StringOrNumber,
  isJoinResult,
  joinSymbol,
} from '../../types.js';

export const joinType = {
  left: 0,
  inner: 1,
} as const;

export type JoinType = (typeof joinType)[keyof typeof joinType];

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

  // TODO: make Ret return two collections:
  // 1. the multiset of join results
  // 2. a list of the original items that were joined
  //
  // This latter list is needed for left-join to retract un-matches when matches are found.
  //
  // TODO: Make join lazy rather than actually computing the join and creating intermediate arrays.
  join<
    VO,
    AAlias extends string | undefined,
    BAlias extends string | undefined,
  >(
    type: JoinType,
    aAlias: AAlias | undefined,
    other: DifferenceIndex<Key | undefined, VO>,
    bAlias: BAlias | undefined,
    getBValueIdentity: (v: VO) => StringOrNumber,
  ): [
    Multiset<JoinResult<V, VO, AAlias, BAlias>>,
    (readonly [aValue: V | VO, bValue: V | VO | undefined])[],
  ] {
    const ret: (readonly [JoinResult<V, VO, AAlias, BAlias>, number])[] = [];
    const sourceRows: (readonly [
      aValue: V | VO,
      bValue: V | VO | undefined,
    ])[] = [];
    let outerIndex;
    let innerIndex;
    let getOuterValueIdentity;
    let getInnerValueIdentity;
    let outerAlias;
    let innerAlias;

    // If we're a left-join we can't re-order the loop.
    // We must use `this` as the outer index.
    // This means that the `join-operator` MUST always be smart
    // and use the `delta` set as the out loop and _never_ the `base` set.
    if (this.#index.size < other.#index.size || type === joinType.left) {
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
      // we do not match on undefined keys. This is to mimic SQL NULL behavior.
      const innerEntry = key !== undefined ? innerIndex.get(key) : undefined;
      if (innerEntry === undefined) {
        if (type !== joinType.left) {
          continue;
        }

        for (const [outerValue, outerMult] of outerEntry) {
          let value: JoinResult<V, VO, AAlias, BAlias>;
          if (isJoinResult(outerValue)) {
            value = outerValue as JoinResult<V, VO, AAlias, BAlias>;
          } else {
            value = {
              [joinSymbol]: true,
              id: getOuterValueIdentity(outerValue as unknown as V & VO),
              [outerAlias!]: outerValue,
            } as JoinResult<V, VO, AAlias, BAlias>;
          }
          ret.push([value, outerMult]);
          sourceRows.push([outerValue, undefined]);
        }
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
          sourceRows.push([outerValue, undefined]);
        }
      }
    }
    return [ret, sourceRows];
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
