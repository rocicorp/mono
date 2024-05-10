import type {Ordering} from '../../ast/ast.js';
import type {Context} from '../../context/context.js';
import type {DifferenceStream} from '../graph/difference-stream.js';
import {createPullMessage} from '../graph/message.js';
import type {Multiset} from '../multiset.js';
import {AbstractView} from './abstract-view.js';
import type {ISortedMap} from 'sorted-btree';
import BTree from '../../btree-class.js';
import type {Comparator} from '../types.js';

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
export class MutableTreeView<T extends object> extends AbstractView<T, T[]> {
  #data: ISortedMap<T, undefined>;

  #jsSlice: T[] = [];

  #limit?: number | undefined;
  #min?: T | undefined;
  #max?: T | undefined;
  // readonly #order;
  readonly id = id++;
  readonly #comparator;

  constructor(
    context: Context,
    stream: DifferenceStream<T>,
    comparator: Comparator<T>,
    _order: Ordering | undefined,
    limit?: number | undefined,
    name: string = '',
  ) {
    super(context, stream, name);
    this.#limit = limit;
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    this.#data = new BTree(undefined, comparator);
    this.#comparator = comparator;
    // this.#order = order;
    if (limit !== undefined) {
      this.#addAll = this.#limitedAddAll;
      this.#removeAll = this.#limitedRemoveAll;
    } else {
      this.#addAll = addAll;
      this.#removeAll = removeAll;
    }
  }

  #addAll: (
    data: ISortedMap<T, undefined>,
    value: T,
  ) => ISortedMap<T, undefined>;
  #removeAll: (
    data: ISortedMap<T, undefined>,
    value: T,
  ) => ISortedMap<T, undefined>;

  get value(): T[] {
    return this.#jsSlice;
  }

  protected _newDifference(data: Multiset<T>): boolean {
    let needsUpdate = false || this.hydrated === false;

    let newData = this.#data;
    [needsUpdate, newData] = this.#sink(data, newData, needsUpdate);
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
    data: ISortedMap<T, undefined>,
    needsUpdate: boolean,
  ): [boolean, ISortedMap<T, undefined>] {
    const iterator = c[Symbol.iterator]();
    let next;

    const process = (value: T, mult: number) => {
      if (mult > 0) {
        needsUpdate = true;
        data = this.#addAll(data, value);
      } else if (mult < 0) {
        needsUpdate = true;
        data = this.#removeAll(data, value);
      }
    };

    // TODO: process with a limit if we have a limit and we're in source order.
    while (!(next = iterator.next()).done) {
      const [value, mult] = next.value;

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
          data.set(nextMult > 0 ? nextValue : value, undefined);
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

  // TODO: if we're not in source order --
  // We should create a source in the order we need so we can always be in source order.
  #limitedAddAll(data: ISortedMap<T, undefined>, value: T) {
    const limit = this.#limit || 0;
    // Under limit? We can just add.
    if (data.size < limit) {
      this.#updateMinMax(value);
      data.set(value, undefined);
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
    data.set(value, undefined);
    // and then remove the max since we were at limit
    data.delete(this.#max!);
    // and then update max
    this.#max = data.maxKey() || undefined;

    // and what if the value was under min? We update our min.
    if (this.#comparator(value, this.#min!) <= 0) {
      this.#min = value;
    }
    return data;
  }

  #limitedRemoveAll(data: ISortedMap<T, undefined>, value: T) {
    // if we're outside the window, do not remove.
    const minComp = this.#min && this.#comparator(value, this.#min);
    const maxComp = this.#max && this.#comparator(value, this.#max);

    if (minComp && minComp < 0) {
      return data;
    }

    if (maxComp && maxComp > 0) {
      return data;
    }

    // inside the window?
    // do the removal and update min/max
    // only update min/max if the removals was equal to min/max tho
    // otherwise we removed a element that doesn't impact min/max

    data.delete(value);
    // TODO: since we deleted we need to send a request upstream for more data!

    if (minComp && minComp === 0) {
      this.#min = value;
    }
    if (maxComp && maxComp === 0) {
      this.#max = value;
    }

    return data;
  }

  pullHistoricalData(): void {
    this._materialite.tx(() => {
      this.stream.messageUpstream(
        //this.#order
        createPullMessage(undefined, 'select'),
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

function addAll<T>(data: ISortedMap<T, undefined>, value: T) {
  // A treap can't have dupes so we can ignore `mult`
  data.set(value, undefined);
  return data;
}

function removeAll<T>(data: ISortedMap<T, undefined>, value: T) {
  // A treap can't have dupes so we can ignore `mult`
  data.delete(value);
  return data;
}
