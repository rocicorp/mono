import type {ISortedMap} from 'btree';
import BTree from 'btree';
import {assert} from 'shared/src/asserts.js';
import {must} from 'shared/src/must.js';
import type {Ordering, Primitive, Selector} from '../../ast/ast.js';
import {makeComparator} from '../compare.js';
import {DifferenceStream} from '../graph/difference-stream.js';
import {
  HoistedCondition,
  PullMsg,
  Request,
  createPullResponseMessage,
} from '../graph/message.js';
import type {MaterialiteForSourceInternal} from '../materialite.js';
import type {Entry} from '../multiset.js';
import type {Comparator, PipelineEntity, Version} from '../types.js';
import {SourceHashIndex} from './source-hash-index.js';
import type {Source, SourceInternal} from './source.js';

let id = 0;

/**
 * A source that remembers what values it contains.
 *
 * This allows pipelines that are created after a source already exists to be
 * able to receive historical data.
 *
 * The source ordering is only sorted by the primary key and for alternative
 * sorts one field plus the id field to ensure stable sorts.
 */
export class SetSource<T extends PipelineEntity> implements Source<T> {
  readonly #stream: DifferenceStream<T>;
  readonly #internal: SourceInternal;
  readonly #listeners = new Set<
    (data: ISortedMap<T, undefined>, v: Version) => void
  >();
  readonly #sorts = new Map<string, SetSource<T>>();
  readonly #hashes = new Map<string, SourceHashIndex<Primitive, T>>();
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

    this.#tree = new BTree(undefined, comparator);
    this.comparator = comparator;

    this.#internal = {
      onCommitEnqueue: (version: Version) => {
        if (this.#pending.length === 0) {
          return;
        }
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
              for (const hash of this.#hashes.values()) {
                hash.add(val);
              }
              ++i;
              continue;
            }
          }
          if (mult < 0) {
            this.#tree = this.#tree.without(val);
            for (const hash of this.#hashes.values()) {
              hash.delete(val);
            }
          } else if (mult > 0) {
            this.#tree = this.#tree.with(val, undefined, true);
            for (const hash of this.#hashes.values()) {
              hash.add(val);
            }
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
   * back with the seed/initial values.
   */
  seed(values: Iterable<T>): this {
    // TODO: invariant to ensure we are in a tx.
    for (const v of values) {
      this.#tree = this.#tree.with(v, undefined, true);
      for (const hash of this.#hashes.values()) {
        hash.add(v);
      }
      for (const alternateSort of this.#sorts.values()) {
        alternateSort.add(v);
      }
    }

    this._materialite.addDirtySource(this.#internal);

    this.#seeded = true;
    // Notify views that requested history, if any.
    for (const request of this.#historyRequests) {
      this.#sendHistory(request);
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
          this.#sendHistory(message);
        } else {
          this.#historyRequests.push(message);
        }
        break;
      }
    }
  }

  #sendHistory(request: PullMsg) {
    const hoistedConditions = request?.hoistedConditions;
    const conditionsForThisSource = (hoistedConditions || []).filter(
      c => c.selector[0] === this.#name,
    );
    const primaryKeyEquality = getPrimaryKeyEquality(conditionsForThisSource);

    // Primary key lookup.
    if (primaryKeyEquality !== undefined) {
      const {value} = primaryKeyEquality;
      const entry = this.#tree.getPairOrNextHigher({
        id: value,
      } as unknown as T);
      this.#stream.newDifference(
        this._materialite.getVersion(),
        entry !== undefined
          ? entry[0].id !== value
            ? []
            : [[entry[0], 1]]
          : [],
        createPullResponseMessage(request, this.#name, this.#order),
      );
      return;
    }

    const newSort = this.#getOrCreateAndMaintainNewSort(request);
    const orderForReply = request.order ?? this.#order;

    // Is there a range constraint against the ordered field?
    const range = getRange(conditionsForThisSource, orderForReply);
    // const atEnd = createEndPredicate(
    //   range.field,
    //   range.endValue,
    //   orderForReply,
    // );

    this.#stream.newDifference(
      this._materialite.getVersion(),
      iterateBTreeWithOrder<T>(
        newSort.#tree,
        orderForReply,
        range.startValue,
        range.endValue,
      ),
      createPullResponseMessage(request, this.#name, orderForReply),
    );
  }

  #getOrCreateAndMaintainNewSort(request: PullMsg): SetSource<T> {
    const ordering = request.order ?? this.#order;
    if (ordering === undefined) {
      return this;
    }
    // only retain fields relevant to this source.
    const firstSelector = ordering[0][0];

    if (firstSelector[0] !== this.#name) {
      return this;
    }

    const key = firstSelector[1];
    // this is the canonical sort.
    if (key === 'id') {
      return this;
    }
    const alternateSort = this.#sorts.get(key);
    if (alternateSort !== undefined) {
      return alternateSort;
    }

    // We ignore asc/desc as directionality because the source is always `order
    // by key asc, id asc`. `desc` is achieved by iterating backwards. The
    // directions do not all need to be the same. The iterator knows how to deal
    // with no uniform order.
    //
    const orderBy: Ordering = [
      // We append id for uniqueness.
      [firstSelector, 'asc'],
      [[this.#name, 'id'], 'asc'],
    ];
    const newComparator = makeComparator(orderBy);
    const source = this.withNewOrdering(newComparator, orderBy);

    this.#sorts.set(key, source);
    return source;
  }

  // TODO: in the future we should collapse hash and sorted indices
  // so one can stand in for the other and we don't need to maintain both.
  getOrCreateAndMaintainNewHashIndex<K extends Primitive>(
    column: Selector,
  ): SourceHashIndex<K, T> {
    const existing = this.#hashes.get(column[1]);
    if (existing !== undefined) {
      return existing as SourceHashIndex<K, T>;
    }
    const index = new SourceHashIndex<K, T>(column);
    this.#hashes.set(column[1], index);
    if (this.#seeded) {
      for (const v of this.#tree.keys()) {
        index.add(v);
      }
    }

    return index;
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
    return this.#tree.get(key);
  }

  toString(): string {
    return this.#name ?? `SetSource(${this.#id})`;
  }
}

// TODO(mlaw): update `getPrimaryKeyEqualities` to support `IN`
function getPrimaryKeyEquality(
  conditions: HoistedCondition[],
): HoistedCondition | undefined {
  for (const c of conditions) {
    if (c.op === '=' && c.selector[1] === 'id') {
      return c;
    }
  }
  return undefined;
}

function getRange(
  conditions: HoistedCondition[],
  sourceOrder: Ordering,
): {
  field: Selector;
  startValue: unknown | undefined;
  endValue: unknown | undefined;
} {
  const field: Selector = sourceOrder[0][0];
  let startValue: unknown | undefined;
  let endValue: unknown | undefined;
  for (const c of conditions) {
    if (c.selector[1] === field[1]) {
      if (c.op === '>' || c.op === '>=' || c.op === '=') {
        startValue = c.value;
      }
      if (c.op === '<' || c.op === '<=' || c.op === '=') {
        endValue = c.value;
      }
    }
  }
  const reversed = sourceOrder[0][1] === 'desc';
  if (reversed) {
    return {field, startValue: endValue, endValue: startValue};
  }
  return {field, startValue, endValue};
}

function createEndPredicate<T extends object>(
  selector: Selector,
  end: unknown,
  order: Ordering,
): ((t: T) => boolean) | undefined {
  if (end === undefined) {
    return undefined;
  }
  const comp = makeComparator<T>([order[0]]);
  const r = {[selector[1]]: end} as T;
  return t => comp(t, r) > 0;
}

function maybeGetKey<T extends object>(
  order: Ordering,
  value: unknown,
): {
  maybeStartKey: T | undefined;
  maybeStartKeyComparator: Comparator<T> | undefined;
} {
  if (value === undefined) {
    return {maybeStartKey: undefined, maybeStartKeyComparator: undefined};
  }
  const selector = order[0][0];
  const key = selector[1];

  const startKey = {
    [key]: value,
  } as T;
  return {
    maybeStartKey: startKey,
    maybeStartKeyComparator: makeComparator<T>(order.slice(0, 1)),
  };
}

export function* iterateBTreeWithOrder<T extends object>(
  tree: BTree<T, undefined>,
  order: Ordering,
  rangeStartValue?: unknown,
  rangeEndValue?: unknown,
): Iterable<Entry<T>> {
  // The tree is always sorted asc by one field which is the first element of the order.

  const atEnd = createEndPredicate(order[0][0], rangeEndValue, order);

  const {maybeStartKey: startKey, maybeStartKeyComparator: startKeyComparator} =
    maybeGetKey<T>(order, rangeStartValue);

  const comp = makeComparator<Partial<T>>(order);
  const compFirst =
    order.length === 1 ? comp : makeComparator<T>(order.slice(0, 1));

  const buffer: T[] = [];

  /**
   * This returns the entries iterator to use. It takes {@linkcode startKey}
   * into account. Since we sort on the primary key and still support iterating
   * over secondary keys in arbitrary direction we start the entries iterator at
   * the first entry that matches the first selector of the startKey.
   */
  function getEntries(
    tree: BTree<T, undefined>,
    order: Ordering,
    startKey: T | undefined,
  ) {
    const reversed = order[0][1] === 'desc';

    if (!startKey) {
      return reversed ? tree.entriesReversed() : tree.entries();
    }

    // XXX: If desc then we need find the previous?
    if (reversed) {
      for (const entry of tree.entries(startKey)) {
        const value: T = entry[0];
        if (compFirst(value, startKey) > 0) {
          break;
        }
        startKey = value;
      }
      return tree.entriesReversed(startKey);
    }

    return tree.entries(startKey);

    // if (startKey) {
    //   const allAsc = order.every(o => o[1] === 'asc');
    //   if (!allAsc) {
    //     // If the direction of the sorts are not all the same we find the first
    //     // entry that starts with the startKey (first field) and use that as the
    //     // entry point to the BTree iterator
    //     const fieldName = order[0][0][1] as keyof T;
    //     const fakeKey = {
    //       [fieldName]: startKey[fieldName],
    //     };
    //     const entries = tree.entries(fakeKey as T);

    //     let iterStart = startKey;
    //     for (const entry of entries) {
    //       const value: T = entry[0];
    //       if (compFirst(value, startKey) < 0) {
    //         break;
    //       }
    //       iterStart = value;
    //     }
    //     return tree.entriesReversed(iterStart);

    //     // for (const entry of entries) {
    //     //   const value: T = entry[0];
    //     //   const v = compFirst(partialStartKey as T, value);
    //     //   if (v < 0) {
    //     //     break;
    //     //   }
    //     //   startKey = value;
    //     // }
    //   }
    // }
    return reversed ? tree.entriesReversed(startKey) : tree.entries(startKey);
  }

  function* sortAndYieldBuffer(): Generator<Entry<T>, boolean> {
    // TODO(arv): We should probably inline this since yield* is not free.
    buffer.sort(comp);
    for (const b of buffer) {
      if (atEnd?.(b)) {
        return true;
      }
      // startKey needs to be a real key that is in the tree.
      if (!startKey) {
        yield [b, 1];

        // The problem here is that startKey might be partial... for example:
        //
        // {x: 1}
        //
        // and our data might be
        //
        // {id: 1, x: 1}
        // {id: 2, x: 1}
        // {id: 3, x: 1}
        //
        // if id is ordered asc {x: 1} is less than all so that is fine .
        // if id is ordered desc {x: 1} is greater than all so that is a problem.
        // What we really want is
      } else {
        assert(startKeyComparator);
        if (startKeyComparator(startKey, b) <= 0) {
          // We don't need to skip anymore
          // startKey = undefined;
          yield [b, 1];
        }
      }
    }
    buffer.length = 0;
    return false;
  }

  const entries = getEntries(tree, order, startKey);
  for (const entry of entries) {
    const value: T = entry[0];

    if (buffer.length > 0 && compFirst(buffer[0], value) !== 0) {
      if (yield* sortAndYieldBuffer()) {
        return;
      }
    }
    buffer.push(value);
  }

  yield* sortAndYieldBuffer();
}
