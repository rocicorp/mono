import type {Primitive} from '../../../ast/ast.js';
import type {Entry} from '../../multiset.js';
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

  get(key: Key): Entry<V>[] {
    return this.#index.get(key) ?? [];
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

export function concatIds(idA: string | number, idB: string | number) {
  let ret;
  if (idA.toString() < idB.toString()) {
    ret = idA + '_' + idB;
  } else {
    ret = idB + '_' + idA;
  }

  return ret;
}

export function combineRows<
  AValue,
  BValue,
  AAlias extends string,
  BAlias extends string,
>(
  outerValue: AValue,
  innerValue: BValue | undefined,
  outerAlias: AAlias | undefined,
  innerAlias: BAlias | undefined,
  getOuterValueIdentity: (value: AValue) => StringOrNumber,
  getInnerValueIdentity: (value: BValue) => StringOrNumber,
): JoinResult<AValue, BValue, AAlias, BAlias> {
  // Flatten our join results so we don't
  // end up arbitrarily deep after many joins.
  // This handles the case of: A JOIN B JOIN C ...
  // A JOIN B produces {a, b}
  // A JOIN B JOIN C would produce {a_b: {a, b}, c} if we didn't flatten here.
  if (innerValue === undefined && isJoinResult(outerValue)) {
    return outerValue as JoinResult<AValue, BValue, AAlias, BAlias>;
  } else if (innerValue === undefined) {
    return {
      [joinSymbol]: true,
      id: getOuterValueIdentity(outerValue as unknown as AValue & BValue),
      [outerAlias!]: outerValue,
    } as JoinResult<AValue, BValue, AAlias, BAlias>;
  } else if (isJoinResult(outerValue) && isJoinResult(innerValue)) {
    return {
      ...outerValue,
      ...innerValue,
      id: concatIds(outerValue.id, innerValue.id),
    } as JoinResult<AValue, BValue, AAlias, BAlias>;
  } else if (isJoinResult(outerValue)) {
    return {
      ...outerValue,
      [innerAlias!]: innerValue,
      id: concatIds(
        outerValue.id,
        getInnerValueIdentity(innerValue as unknown as AValue & BValue),
      ),
    } as JoinResult<AValue, BValue, AAlias, BAlias>;
  } else if (isJoinResult(innerValue)) {
    return {
      ...innerValue,
      [outerAlias!]: outerValue,
      id: concatIds(
        getOuterValueIdentity(outerValue as unknown as AValue & BValue),
        innerValue.id,
      ),
    } as JoinResult<AValue, BValue, AAlias, BAlias>;
  }
  return {
    [joinSymbol]: true,
    id: concatIds(
      getOuterValueIdentity(outerValue as unknown as AValue & BValue),
      getInnerValueIdentity(innerValue as unknown as AValue & BValue),
    ),
    [outerAlias!]: outerValue,
    [innerAlias!]: innerValue,
  } as JoinResult<AValue, BValue, AAlias, BAlias>;
}
