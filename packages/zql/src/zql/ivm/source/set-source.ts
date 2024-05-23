import {must} from 'shared/src/must.js';
import type {Ordering, Selector} from '../../ast/ast.js';
import {DifferenceStream} from '../graph/difference-stream.js';
import {
  createPullResponseMessage,
  HoistedCondition,
  PullMsg,
  Request,
} from '../graph/message.js';
import type {MaterialiteForSourceInternal} from '../materialite.js';
import type {Entry, Multiset} from '../multiset.js';
import type {Comparator, PipelineEntity, Version} from '../types.js';
import type {Source, SourceInternal} from './source.js';
import type {ISortedMap} from 'sorted-btree-roci';
import BTree from 'sorted-btree-roci';
import {gen} from '../../util/iterables.js';
import {makeComparator} from '../compare.js';

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
   * back with the seed/initial values.
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

    const [newSort, orderForReply] =
      this.#getOrCreateAndMaintainNewSort(request);

    // Is there a range constraint against the ordered field?
    if (orderForReply !== undefined) {
      const range = getRange(conditionsForThisSource, orderForReply);
      if (request.order === undefined || request.order[1] === 'asc') {
        this.#stream.newDifference(
          this._materialite.getVersion(),
          gen(() =>
            genFromBTreeEntries(
              newSort.#tree.entries(maybeGetKey(range.field, range.bottom)),
              createEndPredicateAsc(range.field, range.top),
            ),
          ),
          createPullResponseMessage(request, this.#name, orderForReply),
        );
        return;
      }

      const maybeKey = maybeGetKey<T>(range.field, range.top);
      if (maybeKey !== undefined && newSort.#order[0].length > 1) {
        const entriesBelow = newSort.#tree.entries(maybeKey);
        let key: T | undefined;
        const specialComparator = makeComparator<T>([range.field], 'asc');
        for (const entry of entriesBelow) {
          if (specialComparator(entry[0], maybeKey) > 0) {
            key = entry[0];
            break;
          }
        }
        this.#stream.newDifference(
          this._materialite.getVersion(),
          gen(() =>
            genFromBTreeEntries(
              newSort.#tree.entriesReversed(key),
              createEndPredicateDesc(range.field, range.bottom),
            ),
          ),
          createPullResponseMessage(request, this.#name, orderForReply),
        );
      } else {
        this.#stream.newDifference(
          this._materialite.getVersion(),
          gen(() =>
            genFromBTreeEntries(
              newSort.#tree.entriesReversed(maybeKey),
              createEndPredicateDesc(range.field, range.bottom),
            ),
          ),
          createPullResponseMessage(request, this.#name, orderForReply),
        );
      }

      return;
    }

    this.#stream.newDifference(
      this._materialite.getVersion(),
      asEntries(newSort.#tree, request),
      createPullResponseMessage(request, this.#name, orderForReply),
    );
  }

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
    // this is the canonical sort.
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
  if (message?.order) {
    if (message.order[1] === 'desc') {
      return gen(() => genFromBTreeEntries(m.entriesReversed()));
    }
  }

  return gen<Entry<T>>(() => genFromBTreeEntries(m.entries()));
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
  const sourceOrderFields = sourceOrder[0];
  const firstOrderField = sourceOrderFields[0];

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

function createEndPredicateAsc<T extends object>(
  field: Selector,
  end: unknown,
): ((t: T) => boolean) | undefined {
  if (end === undefined) {
    return undefined;
  }
  const comp = makeComparator<T>([field], 'asc');
  return t => {
    const cmp = comp(t, {[field[1]]: end} as T);
    return cmp > 0;
  };
}

function createEndPredicateDesc<T extends object>(
  field: Selector,
  end: unknown,
): ((t: T) => boolean) | undefined {
  if (end === undefined) {
    return undefined;
  }
  const comp = makeComparator<T>([field], 'asc');
  return t => {
    const cmp = comp(t, {[field[1]]: end} as T);
    return cmp < 0;
  };
}

function maybeGetKey<T>(field: Selector, value: unknown): T | undefined {
  if (value === undefined) {
    return undefined;
  }
  return {
    [field[1]]: value,
  } as T;
}
