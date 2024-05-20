import {must} from 'shared/src/must.js';
import type {Ordering, Selector} from '../../ast/ast.js';
import {makeComparator} from '../../query/statement.js';
import {DifferenceStream} from '../graph/difference-stream.js';
import {createPullResponseMessage, PullMsg, Request} from '../graph/message.js';
import type {MaterialiteForSourceInternal} from '../materialite.js';
import type {Entry, Multiset} from '../multiset.js';
import type {Comparator, PipelineEntity, Version} from '../types.js';
import type {Source, SourceInternal} from './source.js';
import type {ISortedMap} from 'sorted-btree-roci';
import BTree from 'sorted-btree-roci';

/**
 * A source that remembers what values it contains.
 *
 * This allows pipelines that are created after a source already
 * exists to be able to receive historical data.
 *
 */
let id = 0;
export class SetSource<T extends PipelineEntity> implements Source<T> {
  readonly #stream: DifferenceStream<T>;
  readonly #internal: SourceInternal;
  readonly #listeners = new Set<
    (data: ISortedMap<T, undefined>, v: Version) => void
  >();
  readonly #sorts = new Map<string, SetSource<T>>();
  readonly comparator: Comparator<T>;
  readonly #name: string;
  readonly #order: Ordering;

  protected readonly _materialite: MaterialiteForSourceInternal;
  #id = id++;
  #historyRequests: Array<PullMsg> = [];
  #tree: BTree<T, undefined>;
  #seeded = false;
  #pending: Entry<T>[] = [];

  constructor(
    materialite: MaterialiteForSourceInternal,
    comparator: Comparator<T>,
    order: Ordering,
    name: string,
  ) {
    this.#order = order;
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
              this.#tree = this.#tree.with(
                nextMult > 0 ? nextVal : val,
                undefined,
                true,
              );
              ++i;
              continue;
            }
          }
          if (mult < 0) {
            this.#tree = this.#tree.without(val);
          } else if (mult > 0) {
            this.#tree = this.#tree.with(val, undefined, true);
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

  withNewOrdering(comp: Comparator<T>, ordering: Ordering): this {
    const ret = new SetSource(
      this._materialite,
      comp,
      ordering,
      this.#name,
    ) as this;
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

  on(
    cb: (value: ISortedMap<T, undefined>, version: Version) => void,
  ): () => void {
    this.#listeners.add(cb);
    return () => this.#listeners.delete(cb);
  }

  off(fn: (value: ISortedMap<T, undefined>, version: Version) => void): void {
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
      this.#tree = this.#tree.with(v, undefined, true);
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
    const [newSort, orderForReply] =
      this.#getOrCreateAndMaintainNewSort(request);

    this.#stream.newDifference(
      this._materialite.getVersion(),
      asEntries(newSort.#tree, request),
      createPullResponseMessage(request, this.#name, orderForReply),
    );
  }

  // TODO(mlaw): we need to validate that this ordering
  // is compatible with the source. I.e., it doesn't contain columns from other sources.
  // The latter can happen if the user is sorting on joined columns.
  // Join should do this for us when a `PullMsg` passes through it.
  #getOrCreateAndMaintainNewSort(
    request: PullMsg,
  ): [SetSource<T>, Ordering | undefined] {
    const ordering = request.order;
    if (ordering === undefined) {
      return [this, this.#order];
    }
    // only retain fields relevant to this source.
    const firstField = ordering[0][0];

    if (firstField[0] !== this.#name) {
      return [this, this.#order];
    }

    const key = firstField[1];
    // this is the canoncial sort.
    if (key === 'id') {
      return [this, this.#order];
    }
    const alternateSort = this.#sorts.get(key);
    const fields: Selector[] = [firstField, [this.#name, 'id']];
    if (alternateSort !== undefined) {
      return [alternateSort, [fields, ordering[1]]];
    }

    // We ignore asc/desc as directionality can be achieved by reversing the order of iteration.
    // We do not need a separate source.
    // Must append id for uniqueness.
    const newComparator = makeComparator(fields, 'asc');
    const source = this.withNewOrdering(newComparator, [fields, 'asc']);

    this.#sorts.set(key, source);
    return [source, [fields, ordering[1]]];
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
  m: BTree<T, undefined>,
  message?: Request | undefined,
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

  if (message?.order) {
    if (message.order[1] === 'desc') {
      return {
        [Symbol.iterator]() {
          return genFromEntries(m.entriesReversed());
        },
      };
    }
  }

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

function* genFromEntries<T>(m: Iterable<[T, undefined]>) {
  for (const pair of m) {
    yield [pair[0], 1] as const;
  }
}
