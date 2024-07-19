import type {ISortedMap} from 'btree';
import BTree from 'btree';
import {must} from 'shared/src/must.js';
import {assert} from '../../../../../shared/src/asserts.js';
import type {Ordering, Primitive, Selector} from '../../ast/ast.js';
import {gen} from '../../util/iterables.js';
import {makeComparator} from '../compare.js';
import {DifferenceStream} from '../graph/difference-stream.js';
import {
  HoistedCondition,
  PullMsg,
  Request,
  createPullResponseMessage,
  mergeConditionLists,
} from '../graph/message.js';
import type {MaterialiteForSourceInternal} from '../materialite.js';
import type {Entry} from '../multiset.js';
import type {Comparator, PipelineEntity, Version} from '../types.js';
import {SourceHashIndex} from './source-hash-index.js';
import type {Source, SourceInternal} from './source.js';
import {getCommonPrefixOrdering} from './util.js';

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
  readonly #hashes = new Map<string, SourceHashIndex<Primitive, T>>();
  readonly comparator: Comparator<T>;
  readonly #name: string;
  readonly #order: Ordering;

  protected readonly _materialite: MaterialiteForSourceInternal;
  #id = id++;
  #historyRequests: Map<number, PullMsg> = new Map();
  #tree: BTree<T, undefined>;
  #seeded = false;
  #pending: Entry<T>[] = [];

  constructor(
    materialite: MaterialiteForSourceInternal,
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
    this.comparator = makeComparator(order);
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    this.#tree = new BTree(undefined, this.comparator);

    this.#internal = {
      onCommitEnqueue: (version: Version) => {
        if (this.#pending.length === 0 && this.#historyRequests.size === 0) {
          return;
        }

        if (this.#historyRequests.size > 0) {
          assert(
            this.#pending.length === 0,
            'It should be impossible to have pending changes and history requests in the same transaction.',
          );
          for (const request of this.#historyRequests.values()) {
            this.#sendHistory(request);
          }
          this.#historyRequests.clear();
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
              this.comparator(val, nextVal) === 0
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

  withNewOrdering(ordering: Ordering): this {
    const ret = new SetSource(this._materialite, ordering, this.#name) as this;
    if (this.#seeded) {
      ret.seed(this.#tree.keys(), true);
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
  seed(values: Iterable<T>, derived: boolean = false): this {
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

    if (!derived) {
      this._materialite.addDirtySource(this.#internal);
    }

    this.#seeded = true;
    return this;
  }

  processMessage(message: Request): void {
    // TODO: invariant to ensure we are in a tx.
    switch (message.type) {
      case 'pull': {
        this._materialite.addDirtySource(this.#internal);
        this.#historyRequests.set(
          message.id,
          mergeRequests(message, this.#historyRequests.get(message.id)),
        );
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

    const [newSort, orderForReply] =
      this.#getOrCreateAndMaintainNewSort(request);

    const version = this._materialite.getVersion();
    const reply = createPullResponseMessage(request, this.#name, orderForReply);

    // Is there a range constraint against the ordered field?
    const range = getRange(conditionsForThisSource, orderForReply);
    if (request.order === undefined || request.order[0][1] === 'asc') {
      this.#stream.newDifference(
        version,
        gen(() =>
          genFromBTreeEntries(
            newSort.#tree.entries(maybeGetKey(range.field, range.bottom)),
            createEndPredicate(range.field, range.top, 'asc'),
          ),
        ),
        reply,
      );
      return;
    }

    let maybeKey = maybeGetKey<T>(range.field, range.top);
    if (maybeKey !== undefined && newSort.#order.length > 1) {
      const entriesBelow = newSort.#tree.entries(maybeKey);
      let key: T | undefined;
      const specialComparator = makeComparator<T>([[range.field, 'asc']]);
      for (const entry of entriesBelow) {
        if (specialComparator(entry[0], maybeKey) > 0) {
          key = entry[0];
          break;
        }
      }
      maybeKey = key;
    }
    this.#stream.newDifference(
      version,
      gen(() =>
        genFromBTreeEntries(
          newSort.#tree.entriesReversed(maybeKey),
          createEndPredicate(range.field, range.bottom, 'desc'),
        ),
      ),
      reply,
    );
  }

  #getOrCreateAndMaintainNewSort(request: PullMsg): [SetSource<T>, Ordering] {
    const ordering = request.order;
    if (ordering === undefined) {
      return [this, this.#order];
    }
    // only retain fields relevant to this source.
    const firstSelector = ordering[0][0];

    if (firstSelector[0] !== this.#name) {
      return [this, this.#order];
    }

    const key = firstSelector[1];
    // this is the canonical sort.
    if (key === 'id') {
      return [this, this.#order];
    }
    const alternateSort = this.#sorts.get(key);
    if (alternateSort !== undefined) {
      // We omit the `id` part when responding to the view so the view can correctly
      // iterate past the common prefix.
      const orderForReply: Ordering = [ordering[0]];
      return [alternateSort, orderForReply];
    }

    // We ignore asc/desc as directionality can be achieved by reversing the order of iteration
    // rather than creating a separate source.
    // Must append id for uniqueness.
    const orderBy: Ordering = [
      [firstSelector, 'asc'],
      [[this.#name, 'id'], 'asc'],
    ];
    const source = this.withNewOrdering(orderBy);

    this.#sorts.set(key, source);
    // We omit the `id` part when responding to the view so the view can correctly
    // iterate past the common prefix.
    const orderForReply: Ordering = [ordering[0]];

    return [source, orderForReply];
    2;
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
    const ret = this.#tree.get(key);
    return ret;
  }

  toString(): string {
    return this.#name ?? `SetSource(${this.#id})`;
  }
}

function* genFromBTreeEntries<T>(
  m: Iterable<[T, undefined]>,
  end?: ((t: T) => boolean) | undefined,
): Iterator<Entry<T>> {
  for (const pair of m) {
    if (end !== undefined) {
      if (end(pair[0]) === false) {
        yield [pair[0], 1];
      } else {
        return false;
      }
    } else {
      yield [pair[0], 1];
    }
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

function getRange(conditions: HoistedCondition[], sourceOrder: Ordering) {
  let top: unknown | undefined;
  let bottom: unknown | undefined;
  const firstOrderField = sourceOrder[0][0];

  // TODO: Does this work correctly with multiple conditions?
  for (const c of conditions) {
    if (c.selector[1] === firstOrderField[1]) {
      if (c.op === '>' || c.op === '>=' || c.op === '=') {
        bottom = c.value;
      }
      if (c.op === '<' || c.op === '<=' || c.op === '=') {
        top = c.value;
      }
    }
  }

  return {
    field: firstOrderField,
    bottom,
    top,
  };
}

function createEndPredicate<T extends object>(
  selector: Selector,
  end: unknown,
  dir: 'asc' | 'desc',
): ((t: T) => boolean) | undefined {
  if (end === undefined) {
    return undefined;
  }
  const comp = makeComparator<T>([[selector, dir]]);
  const r = {[selector[1]]: end} as T;
  return l => comp(l, r) > 0;
}

function maybeGetKey<T>(selector: Selector, value: unknown): T | undefined {
  if (value === undefined) {
    return undefined;
  }
  return {
    [selector[1]]: value,
  } as T;
}

// TODO(mlaw): request selectors and orderings need to be de-aliased on the way up
// the graph.
export function mergeRequests(a: Request, b: Request | undefined) {
  if (b === undefined) {
    return a;
  }

  if (a === b) {
    return a;
  }

  const mergedConditions = mergeConditionLists(
    a.hoistedConditions,
    b.hoistedConditions,
  );
  const commonOrderPrefix = getCommonPrefixOrdering(a.order, b.order);

  if (
    mergedConditions !== a.hoistedConditions ||
    commonOrderPrefix !== a.order
  ) {
    const ret = {
      ...a,
    };
    if (mergedConditions !== a.hoistedConditions) {
      ret.hoistedConditions = mergedConditions;
    }
    if (commonOrderPrefix !== a.order) {
      ret.order = commonOrderPrefix;
    }

    return ret;
  }

  return a;
}
