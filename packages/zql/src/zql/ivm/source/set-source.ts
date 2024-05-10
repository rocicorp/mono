import {must} from 'shared/src/must.js';
import {makeComparator} from '../../query/statement.js';
import {DifferenceStream} from '../graph/difference-stream.js';
import {createPullResponseMessage, PullMsg, Request} from '../graph/message.js';
import type {MaterialiteForSourceInternal} from '../materialite.js';
import type {Entry, Multiset} from '../multiset.js';
import type {Comparator, Version} from '../types.js';
import type {Source, SourceInternal} from './source.js';
import BTree, {ISortedMap} from 'sorted-btree';

/**
 * A source that remembers what values it contains.
 *
 * This allows pipelines that are created after a source already
 * exists to be able to receive historical data.
 *
 */
let id = 0;
export class SetSource<T extends object> implements Source<T> {
  readonly #stream: DifferenceStream<T>;
  readonly #internal: SourceInternal;
  readonly #listeners = new Set<(data: ISortedMap<T, T>, v: Version) => void>();
  readonly #sorts = new Map<string, SetSource<T>>();
  readonly comparator: Comparator<T>;
  readonly #name: string | undefined;

  protected readonly _materialite: MaterialiteForSourceInternal;
  #id = id++;
  #historyRequests: Array<PullMsg> = [];
  #tree: ISortedMap<T, T>;
  #seeded = false;
  #pending: Entry<T>[] = [];

  constructor(
    materialite: MaterialiteForSourceInternal,
    comparator: Comparator<T>,
    name?: string | undefined,
  ) {
    this._materialite = materialite;
    this.#stream = new DifferenceStream<T>();
    this.#name = name;
    this.#stream.setUpstream({
      commit: () => {},
      messageUpstream: (message: Request) => {
        this.processMessage(message);
      },
      destroy: () => {},
    });
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    this.#tree = new BTree(undefined, comparator);
    this.comparator = comparator;

    this.#internal = {
      onCommitEnqueue: (version: Version) => {
        for (let i = 0; i < this.#pending.length; i++) {
          const [val, mult] = must(this.#pending[i]);
          // small optimization to reduce operations for replace
          if (i + 1 < this.#pending.length) {
            const [nextVal, nextMult] = must(this.#pending[i + 1]);
            if (
              Math.abs(mult) === 1 &&
              mult === -nextMult &&
              comparator(val, nextVal) === 0
            ) {
              // The tree doesn't allow dupes -- so this is a replace.
              this.#tree.set(
                nextMult > 0 ? nextVal : val,
                nextMult > 0 ? nextVal : val,
              );
              ++i;
              continue;
            }
          }
          if (mult < 0) {
            this.#tree.delete(val);
          } else if (mult > 0) {
            this.#tree.set(val, val);
          }
        }

        this.#stream.newDifference(version, this.#pending, undefined);
        this.#pending = [];
      },
      onCommitted: (version: Version) => {
        // In case we have direct source observers
        const tree = this.#tree;
        for (const l of this.#listeners) {
          l(tree, version);
        }

        // TODO(mlaw): only notify the path(s) that got data this tx?
        this.#stream.commit(version);
      },
      onRollback: () => {
        this.#pending = [];
      },
    };
  }

  withNewOrdering(comp: Comparator<T>): this {
    const ret = new SetSource(this._materialite, comp) as this;
    if (this.#seeded) {
      ret.seed(this.#tree.keys());
    }
    return ret;
  }

  get stream(): DifferenceStream<T> {
    return this.#stream;
  }

  get value() {
    return this.#tree;
  }

  destroy(): void {
    this.#listeners.clear();
    this.#stream.destroy();
  }

  on(cb: (value: ISortedMap<T, T>, version: Version) => void): () => void {
    this.#listeners.add(cb);
    return () => this.#listeners.delete(cb);
  }

  off(fn: (value: ISortedMap<T, T>, version: Version) => void): void {
    this.#listeners.delete(fn);
  }

  add(v: T): this {
    this.#pending.push([v, 1]);
    this._materialite.addDirtySource(this.#internal);

    for (const alternateSort of this.#sorts.values()) {
      alternateSort.add(v);
    }

    return this;
  }

  delete(v: T): this {
    this.#pending.push([v, -1]);
    this._materialite.addDirtySource(this.#internal);

    for (const alternateSort of this.#sorts.values()) {
      alternateSort.delete(v);
    }

    return this;
  }

  /**
   * Seeds the source with historical data.
   *
   * We have a separate path for seed to avoid copying
   * the entire set of `values` into the `pending` array before
   * sending it to the stream.
   *
   * We also have a separate path for `seed` so we know if the
   * source has history available or not yet.
   *
   * If a view is created and asks for history before the source
   * has history available, we need to wait for the seed to come in.
   *
   * This can happen since `experimentalWatch` will asynchronously call us
   * back with the seed/inital values.
   */
  seed(values: Iterable<T>): this {
    // TODO: invariant to ensure we are in a tx.

    for (const v of values) {
      this.#tree.set(v, v);
    }
    this._materialite.addDirtySource(this.#internal);

    this.#seeded = true;
    // Notify views that requested history, if any.
    for (const request of this.#historyRequests) {
      this.#sendHistoryTo(request);
    }
    this.#historyRequests = [];

    return this;
  }

  processMessage(message: Request): void {
    // TODO: invariant to ensure we are in a tx.

    switch (message.type) {
      case 'pull': {
        this._materialite.addDirtySource(this.#internal);
        if (this.#seeded) {
          // Already seeded? Immediately reply with history.
          this.#sendHistoryTo(message);
        } else {
          this.#historyRequests.push(message);
        }
        break;
      }
    }
  }

  #sendHistoryTo(request: PullMsg) {
    const newSort = this.#getOrCreateAndMaintainNewSort(request);

    this.#stream.newDifference(
      this._materialite.getVersion(),
      // TODO(mlaw): check asc/desc and iterate in correct direction
      asEntries(newSort.#tree, request),
      createPullResponseMessage(request),
    );
  }

  // TODO(mlaw): we need to validate that this ordering
  // is compatible with the source. I.e., it doesn't contain columns from other sources.
  // The latter can happen if the user is sorting on joined columns.
  // Join should do this for us when a `PullMsg` passes through it.
  #getOrCreateAndMaintainNewSort(request: PullMsg) {
    const ordering = request.order;
    if (ordering === undefined) {
      return this;
    }
    const fields = ordering[0];

    // If length is 1, we're sorted by ID.
    // TODO(mlaw): update AST structure so we can validate this.
    if (fields.length === 1) {
      return this;
    }

    const key = fields.join(',');
    const alternateSort = this.#sorts.get(key);
    if (alternateSort !== undefined) {
      return alternateSort;
    }

    // We ignore asc/desc as directionality can be achieved by reversing the order of iteration.
    // We do not need a separate source.
    const comparator = makeComparator(ordering[0], 'asc');
    const source = this.withNewOrdering(comparator);

    this.#sorts.set(key, source);
    return source;
  }

  awaitSeeding(): PromiseLike<void> {
    if (this.#seeded) {
      return Promise.resolve();
    }
    return new Promise(resolve => {
      const listener = () => {
        this.off(listener);
        resolve();
      };
      this.on(listener);
    });
  }

  isSeeded(): boolean {
    return this.#seeded;
  }

  get(key: T): T | undefined {
    const ret = this.#tree.get(key);
    return ret;
  }

  toString(): string {
    return this.#name ?? `SetSource(${this.#id})`;
  }
}

function asEntries<T>(
  m: ISortedMap<T, T>,
  _message?: Request | undefined,
): Multiset<T> {
  // message will contain hoisted expressions so we can do relevant
  // index selection against the source.
  // const after = hoisted.expressions.filter((e) => e._tag === "after")[0];
  // if (after && after.comparator === comparator) {
  //   return {
  //     [Symbol.iterator]() {
  //       return gen(m.iteratorAfter(after.cursor));
  //     },
  //   };
  // }
  // Optimizations we can do:
  // 1. if it compares on a unique field by equality, just send the single row
  // 2. if the view is in the same order as the source, start the iterator at the where clause
  // which matches this position in the source. (e.g., where id > x)
  return {
    [Symbol.iterator]() {
      return gen(m.keys());
    },
  };
}

function* gen<T>(m: Iterable<T>) {
  for (const v of m) {
    yield [v, 1] as const;
  }
}
