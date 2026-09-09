import {areEqual} from '../../../shared/src/arrays.ts';
import {assert, unreachable} from '../../../shared/src/asserts.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import {makeAddChange, makeRemoveChange, type Change} from './change.ts';
import {normalizeUndefined, type Node, type NormalizedValue} from './data.ts';
import {
  throwFilterOutput,
  type FilterInput,
  type FilterOperator,
  type FilterOutput,
} from './filter-operators.ts';
import type {SourceSchema} from './schema.ts';
import {
  type Stream,
  emptyPullStream,
  pullOf,
  type PullStream,
} from './stream.ts';

/**
 * The Exists operator filters data based on whether or not a relationship is
 * non-empty.
 */
export class Exists implements FilterOperator {
  readonly #input: FilterInput;
  readonly #relationshipName: string;
  readonly #not: boolean;
  readonly #parentJoinKey: CompoundKey;
  readonly #noSizeReuse: boolean;
  #cache: Map<string, boolean>;
  #cacheHitCountsForTesting: Map<string, number> | undefined;
  #output: FilterOutput = throwFilterOutput;

  /**
   * This instance variable is `true` when this operator is processing a `push`,
   * and is used to disable reuse of cached sizes across rows with the
   * same parent join key value.
   * This is necessary because during a push relationships can be inconsistent
   * due to push communicating changes (which may change multiple Nodes) one
   * Node at a time.
   */
  #inPush = false;

  constructor(
    input: FilterInput,
    relationshipName: string,
    parentJoinKey: CompoundKey,
    type: 'EXISTS' | 'NOT EXISTS',
    cacheHitCountsForTesting?: Map<string, number>,
  ) {
    this.#input = input;
    this.#relationshipName = relationshipName;
    this.#input.setFilterOutput(this);
    this.#cache = new Map();
    this.#cacheHitCountsForTesting = cacheHitCountsForTesting;
    assert(
      this.#input.getSchema().relationships[relationshipName],
      // log-leak-ignore -- relationship name is schema, allowed in errors
      `Input schema missing ${relationshipName}`,
    );
    this.#not = type === 'NOT EXISTS';
    this.#parentJoinKey = parentJoinKey;

    // If the parentJoinKey is the primary key, no sense in trying to reuse.
    this.#noSizeReuse = areEqual(
      parentJoinKey,
      this.#input.getSchema().primaryKey,
    );
  }

  setFilterOutput(output: FilterOutput): void {
    this.#output = output;
  }

  beginFilter() {
    this.#output.beginFilter();
  }

  endFilter() {
    this.#releasePending();
    this.#cache = new Map();
    this.#output.endFilter();
  }

  /** Closes the relationship stream a suspended `filter()` was reading. */
  #releasePending(): void {
    this.#pending?.count?.stream.close();
    this.#pending = undefined;
  }

  /**
   * The node `filterPull` is part-way through, and where it got to.
   *
   * Exists is the one filter that genuinely suspends: counting a relationship
   * pulls a child stream that can emit 'yield'. The generator this replaces
   * held that position implicitly; here it is explicit, so no generator is
   * created per node.
   */
  #pending:
    | {
        node: Node;
        key: string | undefined;
        count: {stream: PullStream<Node | 'yield'>; size: number} | undefined;
        exists: boolean | undefined;
      }
    | undefined;

  filter(node: Node): boolean | 'yield' {
    let p = this.#pending;
    if (p === undefined || p.node !== node) {
      this.#releasePending();
      p = {node, key: undefined, count: undefined, exists: undefined};
      this.#pending = p;
      if (!this.#noSizeReuse && !this.#inPush) {
        const key = this.#getCacheKey(node, this.#parentJoinKey);
        p.key = key;
        const cached = this.#cache.get(key);
        if (cached !== undefined) {
          p.exists = cached;
          if (this.#cacheHitCountsForTesting) {
            this.#cacheHitCountsForTesting.set(
              key,
              (this.#cacheHitCountsForTesting.get(key) ?? 0) + 1,
            );
          }
        }
      }
    }

    if (p.exists === undefined) {
      p.count ??= this.#startCount(node);
      const r = this.#countStep(p.count);
      if (r === 'yield') {
        return 'yield';
      }
      p.count.stream.close();
      p.count = undefined;
      p.exists = r > 0;
      if (p.key !== undefined) {
        this.#cache.set(p.key, p.exists);
      }
    }

    if (!(this.#not ? !p.exists : p.exists)) {
      this.#pending = undefined;
      return false;
    }
    const out = this.#output.filter(node);
    if (out === 'yield') {
      return 'yield';
    }
    this.#pending = undefined;
    return out;
  }

  /** Opens the relationship stream whose rows are being counted. */
  #startCount(node: Node): {
    stream: PullStream<Node | 'yield'>;
    size: number;
  } {
    const relationship = node.relationships[this.#relationshipName];
    assert(
      relationship,
      () =>
        `Exists: relationship "${this.#relationshipName}" not found on node`,
    );
    return {stream: relationship(), size: 0};
  }

  /**
   * Counts until the stream yields or ends: 'yield' to forward, otherwise the
   * final size. Shared by `filterPull` above and the push path's `#fetchSize`,
   * so the counting rule exists once.
   */
  #countStep(state: {
    stream: PullStream<Node | 'yield'>;
    size: number;
  }): 'yield' | number {
    for (;;) {
      const n = state.stream.next();
      if (n === undefined) {
        return state.size;
      }
      if (n === 'yield') {
        return 'yield';
      }
      state.size++;
    }
  }

  destroy(): void {
    this.#input.destroy();
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  *push(change: Change): Stream<'yield'> {
    assert(!this.#inPush, 'Unexpected re-entrancy');
    this.#inPush = true;
    try {
      switch (change[ChangeIndex.TYPE]) {
        // add, remove and edit cannot change the size of the
        // this.#relationshipName relationship, so simply #pushWithFilter
        case ChangeType.ADD:
        case ChangeType.EDIT:
        case ChangeType.REMOVE: {
          yield* this.#pushWithFilter(change);
          return;
        }
        case ChangeType.CHILD:
          // Only add and remove child changes for the
          // this.#relationshipName relationship, can change the size
          // of the this.#relationshipName relationship, for other
          // child changes simply #pushWithFilter
          if (
            change[ChangeIndex.CHILD_DATA].relationshipName !==
              this.#relationshipName ||
            change[ChangeIndex.CHILD_DATA].change[ChangeIndex.TYPE] ===
              ChangeType.EDIT ||
            change[ChangeIndex.CHILD_DATA].change[ChangeIndex.TYPE] ===
              ChangeType.CHILD
          ) {
            yield* this.#pushWithFilter(change);
            return;
          }
          switch (change[ChangeIndex.CHILD_DATA].change[ChangeIndex.TYPE]) {
            case ChangeType.ADD: {
              const size = yield* this.#fetchSize(change[ChangeIndex.NODE]);
              if (size === 1) {
                if (this.#not) {
                  // Since the add child change currently being processed is not
                  // pushed to output, the added child needs to be excluded from
                  // the remove being pushed to output (since the child has
                  // never been added to the output).
                  yield* this.#output.push(
                    makeRemoveChange({
                      row: change[ChangeIndex.NODE].row,
                      relationships: {
                        ...change[ChangeIndex.NODE].relationships,
                        [this.#relationshipName]: () =>
                          emptyPullStream<Node | 'yield'>(),
                      },
                    }),
                    this,
                  );
                } else {
                  yield* this.#output.push(
                    makeAddChange(change[ChangeIndex.NODE]),
                    this,
                  );
                }
              } else {
                yield* this.#pushWithFilter(change, size > 0);
              }
              return;
            }
            case ChangeType.REMOVE: {
              const size = yield* this.#fetchSize(change[ChangeIndex.NODE]);
              if (size === 0) {
                if (this.#not) {
                  yield* this.#output.push(
                    makeAddChange(change[ChangeIndex.NODE]),
                    this,
                  );
                } else {
                  // Since the remove child change currently being processed is
                  // not pushed to output, the removed child needs to be added to
                  // the remove being pushed to output.
                  yield* this.#output.push(
                    makeRemoveChange({
                      row: change[ChangeIndex.NODE].row,
                      relationships: {
                        ...change[ChangeIndex.NODE].relationships,
                        [this.#relationshipName]: () =>
                          pullOf([
                            change[ChangeIndex.CHILD_DATA].change[
                              ChangeIndex.NODE
                            ],
                          ]),
                      },
                    }),
                    this,
                  );
                }
              } else {
                yield* this.#pushWithFilter(change, size > 0);
              }
              return;
            }
          }
          return;
        default:
          unreachable(change);
      }
    } finally {
      this.#inPush = false;
    }
  }

  /**
   * Returns whether or not the node's this.#relationshipName
   * relationship passes the exist/not exists filter condition.
   * If the optional `size` is passed it is used.
   * Otherwise, if there is a stored size for the row it is used.
   * Otherwise the size is computed by streaming the node's
   * relationship with this.#relationshipName (this computed size is also
   * stored).
   */
  *#filter(node: Node, exists?: boolean): Generator<'yield', boolean> {
    exists = exists ?? (yield* this.#fetchExists(node));
    return this.#not ? !exists : exists;
  }

  #getCacheKey(node: Node, def: CompoundKey): string {
    const values: NormalizedValue[] = [];
    for (const key of def) {
      values.push(normalizeUndefined(node.row[key]));
    }
    return JSON.stringify(values);
  }

  /**
   * Pushes a change if this.#filter is true for its row.
   */
  *#pushWithFilter(change: Change, exists?: boolean): Stream<'yield'> {
    if (yield* this.#filter(change[ChangeIndex.NODE], exists)) {
      yield* this.#output.push(change, this);
    }
  }

  *#fetchExists(node: Node): Generator<'yield', boolean> {
    // While it seems like this should be able to fetch just 1 node
    // to check for exists, we can't because Take does not support
    // early return during initial fetch.
    return (yield* this.#fetchSize(node)) > 0;
  }

  *#fetchSize(node: Node): Generator<'yield', number> {
    const state = this.#startCount(node);
    try {
      for (;;) {
        const r = this.#countStep(state);
        if (r === 'yield') {
          yield 'yield';
          continue;
        }
        return r;
      }
    } finally {
      state.stream.close();
    }
  }
}
