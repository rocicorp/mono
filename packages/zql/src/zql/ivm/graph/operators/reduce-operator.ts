import {assert} from 'shared/src/asserts.js';
import {must} from 'shared/src/must.js';
import type {Selector} from '../../../ast/ast.js';
import {genCached, genFlatMap} from '../../../util/iterables.js';
import type {Entry, Multiset} from '../../multiset.js';
import {getValueFromEntity, selectorArraysAreEqual} from '../../source/util.js';
import type {PipelineEntity, StringOrNumber} from '../../types.js';
import type {DifferenceStream} from '../difference-stream.js';
import type {Reply} from '../message.js';
import {UnaryOperator} from './unary-operator.js';

/**
 * Applies a `reduce` function against a stream of values.
 *
 * Since `reduce` is a stateful operation, we need to keep track of all the
 * values that have been seen for a given key.
 *
 * If a given key has a member added or removed, we
 * re-run the reduction function against the entire set of
 * values for that key.
 *
 * In future iterations the reduction could also be made incremental.
 */
export class ReduceOperator<
  V extends PipelineEntity,
  O extends PipelineEntity = V,
> extends UnaryOperator<V, O> {
  /**
   * The set of all values that have been seen for a given key.
   *
   * Only positive multiplicities are expected to exist in this map.
   * If a negative multiplicity comes through the pipeline,
   * it reduces the multiplicity of the existing value in the map.
   */
  readonly #inIndex = new Map<
    StringOrNumber,
    Map<StringOrNumber, [V, number]>
  >();
  /**
   * Our prior reduction for a given key.
   *
   * This is used to retract reductions that are no longer valid.
   * E.g., if someone downstream of us is maintaining a count
   * then they'd need to know when a given reduction is no longer valid
   * so they can remove it from their count.
   */
  readonly #outIndex = new Map<StringOrNumber, O>();
  readonly #getValueIdentity;
  readonly #getGroupKey: (value: V) => StringOrNumber;
  readonly #keyColumns: Selector[];

  constructor(
    input: DifferenceStream<V>,
    output: DifferenceStream<O>,
    getValueIdentity: (value: V) => StringOrNumber,
    keyColumns: Selector[],
    f: (input: Iterable<V>) => O,
  ) {
    super(input, output, (_version, data, reply) =>
      this.#inner(data, f, reply),
    );
    this.#getValueIdentity = getValueIdentity;
    this.#keyColumns = keyColumns;
    this.#getGroupKey = makeKeyFunction(keyColumns);
  }

  #inner = (
    data: Multiset<V>,
    f: (input: Iterable<V>) => O,
    reply: Reply | undefined,
  ) => {
    const keysToProcess = new Set<StringOrNumber>();

    if (
      reply !== undefined &&
      this.#replyGroupingMatchesReduceGrouping(reply)
    ) {
      return this.#reduceOverContiguousGroups(data, f);
    }

    for (const [value, mult] of data) {
      const key = this.#getGroupKey(value);
      keysToProcess.add(key);
      this.#addToIndex(key, value, mult);
    }

    return genCached(
      genFlatMap(keysToProcess, k => {
        const dataIn = this.#inIndex.get(k);
        const existingOut = this.#outIndex.get(k);
        if (dataIn === undefined) {
          if (existingOut !== undefined) {
            // retract the reduction
            this.#outIndex.delete(k);
            return [[existingOut, -1]] as const;
          }
          return [];
        }

        const reduction = f(
          genFlatMap(dataIn, function* (mapEntry) {
            for (let i = 0; i < mapEntry[1][1]; i++) {
              yield mapEntry[1][0];
            }
          }),
        );
        const ret: Entry<O>[] = [];
        if (existingOut !== undefined) {
          // modified reduction
          ret.push([existingOut, -1]);
        }
        ret.push([reduction, 1]);
        this.#outIndex.set(k, reduction);
        return ret;
      }),
    );
  };

  #replyGroupingMatchesReduceGrouping(reply: Reply): boolean {
    return (
      reply.contiguousGroup !== undefined &&
      selectorArraysAreEqual(reply.contiguousGroup, this.#keyColumns)
    );
  }

  *#reduceOverContiguousGroups(
    data: Multiset<V>,
    f: (input: Iterable<V>) => O,
  ) {
    // The data coming in is contiguous on the key columns.
    // Which means each group is in a chunk.
    //
    // So we can iterate over the data until the key changes
    // which will make the end of a group.
    //
    // Once that group is done, we can reduce it and emit the result.
    //
    // We still need to maintain our indices so we can respond to deltas later / in a steady state.
    let lastKey: StringOrNumber | undefined;
    let havePendingReduction = false;

    const doReduction = () => {
      const key = must(lastKey);
      // We've removed the last item from the group.
      // So we can reduce the group and emit the result.
      const dataIn = this.#inIndex.get(key);
      assert(dataIn !== undefined, 'dataIn must be defined');
      const reduction = f(
        genFlatMap(dataIn, function* (mapEntry) {
          for (let i = 0; i < mapEntry[1][1]; i++) {
            yield mapEntry[1][0];
          }
        }),
      );
      const existingOut = this.#outIndex.get(key);
      const ret: Entry<O>[] = [];
      if (existingOut !== undefined) {
        // modified reduction
        ret.push([existingOut, -1]);
      }
      ret.push([reduction, 1]);
      this.#outIndex.set(key, reduction);
      lastKey = undefined;
      havePendingReduction = false;
      return ret;
    };

    for (const entry of data) {
      const [value, mult] = entry;
      const key = this.#getGroupKey(value);
      const hadExisting = this.#addToIndex(key, value, mult);
      havePendingReduction = true;
      if (!hadExisting && lastKey !== undefined) {
        yield* doReduction();
      }
      havePendingReduction = lastKey !== key || havePendingReduction;
      lastKey = key;
    }

    if (havePendingReduction) {
      yield* doReduction();
    }
  }

  #addToIndex(key: StringOrNumber, value: V, mult: number): boolean {
    let hadExisting = false;
    let existing = this.#inIndex.get(key);
    if (existing === undefined) {
      existing = new Map<string, [V, number]>();
      this.#inIndex.set(key, existing);
    } else {
      hadExisting = true;
    }

    const valueIdentity = this.#getValueIdentity(value);
    const prev = existing.get(valueIdentity);
    if (prev === undefined) {
      existing.set(valueIdentity, [value, mult]);
    } else {
      const [v, m] = prev;
      const newMult = m + mult;
      assert(
        newMult >= 0,
        'Should not end up with a negative multiplicity when tracking all events for an item',
      );
      if (newMult === 0) {
        existing.delete(valueIdentity);
        if (existing.size === 0) {
          this.#inIndex.delete(key);
          return hadExisting;
        }
      } else {
        existing.set(valueIdentity, [v, newMult]);
      }
    }

    return hadExisting;
  }
}

function makeKeyFunction(qualifiedColumns: Selector[]) {
  return (x: Record<string, unknown>) => {
    if (qualifiedColumns.length === 1) {
      const ret = getValueFromEntity(x, qualifiedColumns[0]);
      if (typeof ret === 'string' || typeof ret === 'number') {
        return ret;
      }
      return JSON.stringify(ret);
    }

    const ret: unknown[] = [];
    for (const qualifiedColumn of qualifiedColumns) {
      ret.push(getValueFromEntity(x, qualifiedColumn));
    }
    // Would it be better to come up with some hash function
    // which can handle complex types?
    return JSON.stringify(ret);
  };
}
