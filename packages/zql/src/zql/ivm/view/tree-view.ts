import {assert} from 'shared/src/asserts.js';
import {must} from 'shared/src/must.js';
import BTree from 'sorted-btree-roci';
import type {Ordering} from '../../ast/ast.js';
import type {Context} from '../../context/context.js';
import {fieldsMatch, makeComparator} from '../../query/statement.js';
import type {DifferenceStream} from '../graph/difference-stream.js';
import {Reply, createPullMessage} from '../graph/message.js';
import type {Entry, Multiset} from '../multiset.js';
import {selectorsAreEqual} from '../source/util.js';
import type {Comparator, PipelineEntity} from '../types.js';
import {AbstractView} from './abstract-view.js';

/**
 * A sink that maintains the list of values in-order.
 * Like any tree, insertion time is O(logn) no matter where the insertion happens.
 * Useful for maintaining large sorted lists.
 *
 * This sink is persistent in that each write creates a new version of the tree.
 * Copying the tree is relatively cheap (O(logn)) as we share structure with old versions
 * of the tree.
 */
let id = 0;
export class TreeView<T extends PipelineEntity> extends AbstractView<T, T[]> {
  #data: BTree<T, undefined>;

  #jsSlice: T[] = [];

  #limit: number | undefined;
  #min: T | undefined = undefined;
  #max: T | undefined = undefined;
  readonly #order;
  readonly id = id++;
  readonly #comparator: Comparator<T>;

  constructor(
    context: Context,
    stream: DifferenceStream<T>,
    comparator: Comparator<T>,
    order: Ordering | undefined,
    limit?: number | undefined,
    name: string = '',
  ) {
    super(context, stream, name);
    this.#limit = limit;
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    this.#data = new BTree(undefined, comparator);
    this.#comparator = comparator;
    this.#order = order;
    if (limit !== undefined) {
      this.#add = this.#limitedAdd;
      this.#remove = this.#limitedRemove;
    } else {
      this.#add = add;
      this.#remove = remove;
    }
  }

  #add: (data: BTree<T, undefined>, value: T) => BTree<T, undefined>;
  #remove: (data: BTree<T, undefined>, value: T) => BTree<T, undefined>;

  get value(): T[] {
    return this.#jsSlice;
  }

  protected _newDifference(
    data: Multiset<T>,
    reply?: Reply | undefined,
  ): boolean {
    let needsUpdate = this.hydrated === false;

    let newData = this.#data;
    [needsUpdate, newData] = this.#sink(data, newData, needsUpdate, reply);
    this.#data = newData;

    if (needsUpdate) {
      // idk.. would be more efficient for users to just use the
      // treap directly. We have a PersistentTreap variant for React users
      // or places where immutability is important.
      const arr: T[] = [];
      for (const key of this.#data.keys()) {
        arr.push(key);
      }
      this.#jsSlice = arr;
    }

    return needsUpdate;
  }

  #sink(
    c: Multiset<T>,
    data: BTree<T, undefined>,
    needsUpdate: boolean,
    reply?: Reply | undefined,
  ): [boolean, BTree<T, undefined>] {
    const process = (value: T, mult: number) => {
      let newData: BTree<T, undefined>;
      if (mult > 0) {
        newData = this.#add(data, value);
      } else if (mult < 0) {
        newData = this.#remove(data, value);
      } else {
        return;
      }
      if (newData !== data) {
        data = newData;
        needsUpdate = true;
      }
    };

    let iterator: Iterable<Entry<T>>;
    if (
      reply === undefined ||
      this.#limit === undefined ||
      !orderingsAreCompatible(reply.order, this.#order)
    ) {
      iterator = c;
    } else {
      // We only get the limited iterator if we're receiving historical data.
      iterator = this.#getLimitedIterator(c, reply, this.#limit);
    }

    for (const entry of iterator) {
      const [value, mult] = entry;
      process(value, mult);
    }

    return [needsUpdate, data];
  }

  /**
   * Limits the iterator to only pull `limit` items from the stream.
   * This is only used in cases where we're processing initial data
   * for initial query run. Initial data will never contain removes, only adds.
   */
  #getLimitedIterator(
    data: Multiset<T>,
    reply: Reply,
    limit: number,
  ): IterableIterator<Entry<T>> {
    const order = must(reply.order);
    const fields = order?.[0] ?? [];
    const iterator = data[Symbol.iterator]();
    let i = 0;
    let last: T | undefined = undefined;

    if (this.#order === undefined || fieldsMatch(fields, this.#order[0])) {
      return {
        [Symbol.iterator]() {
          return this;
        },
        next() {
          if (i >= limit) {
            return {done: true, value: undefined} as const;
          }
          const next = iterator.next();
          if (next.done) {
            return next;
          }
          const entry = next.value;
          i += entry[1];
          return next;
        },
      };
    }

    // source order may be a subset of desired order
    // e.g., [modified] vs [modified, created]
    // in which case we process until we hit the next thing after
    // the source order after we limit.
    if (!selectorsAreEqual(fields[0], this.#order[0][0])) {
      throw new Error(
        `Order must overlap on at least one field! Got: ${fields[0]} | ${
          this.#order[0][0]
        }`,
      );
    }

    // Partial order overlap
    const responseComparator = makeComparator(order[0], order[1]);
    return {
      [Symbol.iterator]() {
        return this;
      },
      next() {
        if (i >= limit) {
          // keep processing until `next` is greater than `last`
          // via the message's comparator.
          const next = iterator.next();
          if (next.done) {
            return next;
          }

          const entry = next.value;
          if (last === undefined) {
            return {
              done: true,
              value: undefined,
            } as const;
          }

          if (responseComparator(entry[0], last) > 0) {
            return {
              done: true,
              value: undefined,
            } as const;
          }

          return next;
        }

        const next = iterator.next();
        if (next.done) {
          return next;
        }
        const entry = next.value;
        i += Math.abs(entry[1]);
        last = entry[0];
        return next;
      },
    };
  }

  #limitedAdd(data: BTree<T, undefined>, value: T) {
    const limit = this.#limit || 0;
    // Under limit? We can just add.
    if (data.size < limit) {
      this.#updateMinMax(value);
      return data.with(value, undefined, true);
    }

    if (data.size > limit) {
      throw new Error(`Data size exceeded limit! ${data.size} | ${limit}`);
    }

    // at limit? We can only add if the value is under max
    assert(this.#max !== undefined);
    const comp = this.#comparator(value, this.#max);
    if (comp > 0) {
      return data;
    }

    // <= max we add.
    data = data.with(value, undefined, true);
    // and then remove the max since we were at limit
    data = data.without(this.#max!);
    // and then update max
    this.#max = data.maxKey() || undefined;

    // and what if the value was under min? We update our min.
    assert(this.#min !== undefined);
    if (this.#comparator(value, this.#min) <= 0) {
      this.#min = value;
    }
    return data;
  }

  #limitedRemove(data: BTree<T, undefined>, value: T) {
    // if we're outside the window, do not remove.
    const minComp = this.#min && this.#comparator(value, this.#min);
    const maxComp = this.#max && this.#comparator(value, this.#max);

    if (minComp !== undefined && minComp < 0) {
      return data;
    }

    if (maxComp !== undefined && maxComp > 0) {
      return data;
    }

    // inside the window?
    // do the removal and update min/max
    // only update min/max if the removals was equal to min/max tho
    // otherwise we removed a element that doesn't impact min/max

    data = data.without(value);
    // TODO: since we deleted we need to send a request upstream for more data!

    if (minComp === 0) {
      this.#min = value;
    }
    if (maxComp === 0) {
      this.#max = value;
    }

    return data;
  }

  pullHistoricalData(): void {
    this._materialite.tx(() => {
      this.stream.messageUpstream(
        createPullMessage(this.#order),
        this._listener,
      );
    });
  }

  #updateMinMax(value: T) {
    if (this.#min === undefined || this.#max === undefined) {
      this.#max = this.#min = value;
      return;
    }

    if (this.#comparator(value, this.#min) <= 0) {
      this.#min = value;
      return;
    }

    if (this.#comparator(value, this.#max) >= 0) {
      this.#max = value;
      return;
    }
  }
}

function add<T>(data: BTree<T, undefined>, value: T) {
  // A treap can't have dupes so we can ignore `mult`
  return data.with(value, undefined, true);
}

function remove<T>(data: BTree<T, undefined>, value: T) {
  // A treap can't have dupes so we can ignore `mult`
  return data.without(value);
}

/**
 * Orderings only need to be partially compatible.
 * As in, a prefix of sourceOrder matches a prefix of destOrder.
 */
function orderingsAreCompatible(
  sourceOrder: Ordering | undefined,
  destOrder: Ordering | undefined,
) {
  // destination doesn't care about order. Ok.
  if (destOrder === undefined) {
    return true;
  }

  // source is unordered, not ok.
  if (sourceOrder === undefined) {
    return false;
  }

  // asc/desc differ.
  if (sourceOrder[1] !== destOrder[1]) {
    return false;
  }

  const sourceFields = sourceOrder[0];
  const destFields = destOrder[0];

  // If at least the left most field is the same, we're compatible.
  if (selectorsAreEqual(sourceFields[0], destFields[0])) {
    return true;
  }

  return false;
}
