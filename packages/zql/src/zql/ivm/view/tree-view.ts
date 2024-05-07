import BTree from 'sorted-btree-roci';
import type {Ordering} from '../../ast/ast.js';
import type {Context} from '../../context/context.js';
import {fieldsMatch} from '../../query/statement.js';
import type {DifferenceStream} from '../graph/difference-stream.js';
import {createPullMessage, Reply} from '../graph/message.js';
import type {Multiset} from '../multiset.js';
import type {Comparator} from '../types.js';
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
export class TreeView<T extends object> extends AbstractView<T, T[]> {
  #data: BTree<T, undefined>;

  #jsSlice: T[] = [];

  #limit?: number | undefined;
  #min?: T | undefined;
  #max?: T | undefined;
  readonly #order;
  readonly id = id++;
  readonly #comparator;

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
    let needsUpdate = false || this.hydrated === false;

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
    let next;

    const process = (value: T, mult: number) => {
      if (mult > 0) {
        needsUpdate = true;
        data = this.#add(data, value);
      } else if (mult < 0) {
        needsUpdate = true;
        data = this.#remove(data, value);
      }
    };

    let iterator;
    if (reply === undefined || this.#limit === undefined) {
      iterator = c[Symbol.iterator]();
    } else {
      // We only get the limited iterator if we're receiving historical data.
      iterator = this.#getLimitedIterator(c, reply, this.#limit);
    }

    // TODO: process with a limit if we have a limit and we're in source order.
    // We're always in source order. At least to an extent.
    // We need to know to what extent.
    // I.e., all order by columns are present in source?
    // Only the first?
    // If only the first N, then we loop until we meet something beyond the first N and we're
    // at limimt.
    // So source must reply with its order param(s).
    // Then we can iterate correctly.
    // Join will update the reply.
    // Reply should also include contiguous groups or not.
    //   If we're sorted by `modified` but we have contiguous `issue id` groups
    //     this happens in the case of a join where the contiguous grouping can be something
    //     we are not sorted by. The sort drives the join order but the join key drives the grouping.
    while (!(next = iterator.next()).done) {
      const entry = next.value;
      const [value, mult] = entry;

      const nextNext = iterator.next();
      if (!nextNext.done) {
        const [nextValue, nextMult] = nextNext.value;
        if (
          Math.abs(mult) === 1 &&
          mult === -nextMult &&
          this.#comparator(nextValue, value) === 0
        ) {
          needsUpdate = true;
          // The tree doesn't allow dupes -- so this is a replace.
          data = data.with(nextMult > 0 ? nextValue : value, undefined, true);
          continue;
        }
      }

      process(value, mult);
      if (!nextNext.done) {
        const [value, mult] = nextNext.value;
        process(value, mult);
      }
    }

    return [needsUpdate, data];
  }

  /**
   * Limits the iterator to only pull `limit` items from the stream.
   * This is only used in cases where we're processing history
   * for initial query run.
   *
   * In those cases:
   * 1. We're in partially overlapping order with the source
   * 2. There are no deletes, only adds
   */
  #getLimitedIterator(data: Multiset<T>, reply: Reply, limit: number) {
    const {order} = reply;
    const fields = (order && order[0]) || [];
    const iterator = data[Symbol.iterator]();
    let i = 0;
    let last: T | undefined = undefined;
    const comparator = this.#comparator;

    // source order may be a subset of desired order
    // in which case we process until we hit the next thing after
    // the source order order after we hit the limit.

    if (this.#order === undefined || fieldsMatch(fields, this.#order[0])) {
      return {
        next() {
          if (i >= limit) {
            return {done: true, value: undefined} as const;
          }
          const next = iterator.next();
          if (next.done) {
            return next;
          }
          const entry = next.value;
          i += Math.abs(entry[1]);
          return next;
        },
      };
    }

    // The source order is a subset of the desired order.
    // We keep porcessing beyond `limit` until we hit the next
    // thing according to source order.
    if (fields[0] !== this.#order[0][0]) {
      throw new Error(
        `Order must overlap on at least one field! Got: ${fields[0]} | ${
          this.#order[0][0]
        }`,
      );
    }

    return {
      next() {
        if (i >= limit) {
          // keep processing until `next` is greater than `last`
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

          if (comparator(entry[0], last) > 0) {
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

  // TODO: if we're not in source order --
  // We should create a source in the order we need so we can always be in source order.
  #limitedAdd(data: BTree<T, undefined>, value: T) {
    const limit = this.#limit || 0;
    // Under limit? We can just add.
    if (data.size < limit) {
      this.#updateMinMax(value);
      data = data.with(value, undefined, true);
      return data;
    }

    if (data.size > limit) {
      throw new Error(`Data size exceeded limit! ${data.size} | ${limit}`);
    }

    // at limit? We can only add if the value is under max
    const comp = this.#comparator(value, this.#max!);
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
    if (this.#comparator(value, this.#min!) <= 0) {
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
  data = data.with(value, undefined, true);
  return data;
}

function remove<T>(data: BTree<T, undefined>, value: T) {
  // A treap can't have dupes so we can ignore `mult`
  data = data.without(value);
  return data;
}
