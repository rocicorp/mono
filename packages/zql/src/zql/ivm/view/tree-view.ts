import {Treap} from '@vlcn.io/ds-and-algos/Treap';
import {Comparator, ITree} from '@vlcn.io/ds-and-algos/types';
import {Ordering} from '../../ast/ast.js';
import {DifferenceStream} from '../graph/difference-stream.js';
import {createPullMessage} from '../graph/message.js';
import {Materialite} from '../materialite.js';
import {Multiset} from '../multiset.js';
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
export class MutableTreeView<T extends object> extends AbstractView<T, T[]> {
  #data: ITree<T>;

  #jsSlice: T[] = [];

  #limit?: number;
  #min?: T;
  #max?: T;
  readonly #order;
  readonly id = id++;
  readonly #comparator;

  constructor(
    materialite: Materialite,
    stream: DifferenceStream<T>,
    comparator: Comparator<T>,
    order: Ordering | undefined,
    limit?: number | undefined,
    name: string = '',
  ) {
    super(materialite, stream, name);
    this.#limit = limit;
    this.#data = new Treap(comparator);
    this.#comparator = comparator;
    this.#order = order;
    if (limit !== undefined) {
      this.#addAll = this.#limitedAddAll;
      this.#removeAll = this.#limitedRemoveAll;
    } else {
      this.#addAll = addAll;
      this.#removeAll = removeAll;
    }
  }

  #addAll: (data: ITree<T>, value: T) => ITree<T>;
  #removeAll: (data: ITree<T>, value: T) => ITree<T>;

  get value(): T[] {
    return this.#jsSlice;
  }

  protected _newDifference(data: Multiset<T>): boolean {
    let changed = false;

    let newData = this.#data;
    [changed, newData] = this.#sink(data, newData, changed);
    this.#data = newData;

    if (changed) {
      // idk.. would be more efficient for users to just use the
      // treap directly. We have a PersistentTreap variant for React users
      // or places where immutability is important.
      this.#jsSlice = this.#data.toArray();
    }

    return changed;
  }

  #sink(c: Multiset<T>, data: ITree<T>, changed: boolean): [boolean, ITree<T>] {
    const iterator = c[Symbol.iterator]();
    let next;

    const process = (value: T, mult: number) => {
      if (mult > 0) {
        changed = true;
        data = this.#addAll(data, value);
      } else if (mult < 0) {
        changed = true;
        data = this.#removeAll(data, value);
      }
    };

    const fullRecompute = false;
    while (!(next = iterator.next()).done) {
      const [value, mult] = next.value;
      if (this.#limit !== undefined && fullRecompute && this.#order) {
        if (data.size >= this.#limit && mult > 0) {
          // bail early. During a re-compute with a source in the same order
          // as the view we can bail once we've consumed `LIMIT` items.
          break;
        }
      }

      // empty = false;
      const nextNext = iterator.next();
      if (!nextNext.done) {
        const [nextValue, nextMult] = nextNext.value;
        if (
          Math.abs(mult) === 1 &&
          mult === -nextMult &&
          this.#comparator(nextValue, value) === 0
        ) {
          changed = true;
          // The tree doesn't allow dupes -- so this is a replace.
          data = data.add(nextMult > 0 ? nextValue : value);
          continue;
        }
      }

      process(value, mult);
      if (!nextNext.done) {
        const [value, mult] = nextNext.value;
        process(value, mult);
      }
    }

    return [changed, data];
  }

  // TODO: if we're not in source order --
  // We should create a source in the order we need so we can always be in source order.
  #limitedAddAll(data: ITree<T>, value: T) {
    const limit = this.#limit || 0;
    // Under limit? We can just add.
    if (data.size < limit) {
      this.#updateMinMax(value);
      return data.add(value);
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
    data = data.add(value);
    // and then remove the max since we were at limit
    data = data.delete(this.#max!);
    // and then update max
    this.#max = data.getMax() || undefined;

    // and what if the value was under min? We update our min.
    if (this.#comparator(value, this.#min!) <= 0) {
      this.#min = value;
    }
    return data;
  }

  #limitedRemoveAll(data: ITree<T>, value: T) {
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

    data = data.delete(value);
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
    this.stream.messageUpstream(
      createPullMessage(this.#order, 'select'),
      this._listener,
    );
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

function addAll<T>(data: ITree<T>, value: T) {
  // A treap can't have dupes so we can ignore `mult`
  return data.add(value);
}

function removeAll<T>(data: ITree<T>, value: T) {
  // A treap can't have dupes so we can ignore `mult`
  return data.delete(value);
}
