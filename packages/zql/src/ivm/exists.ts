import {areEqual} from '../../../shared/src/arrays.ts';
import {assert, unreachable} from '../../../shared/src/asserts.ts';
import {makeEmptyIteratorWithReturn} from '../../../shared/src/iterables.ts';
import {emptyArray} from '../../../shared/src/sentinels.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import {makeAddChange, makeRemoveChange, type Change} from './change.ts';
import {
  normalizeUndefined,
  valueToKey,
  type Node,
  type NormalizedValue,
} from './data.ts';
import {
  throwFilterOutput,
  type FilterInput,
  type FilterOperator,
  type FilterOutput,
} from './filter-operators.ts';
import type {InputBase} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {andFinally, type Stream} from './stream.ts';

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
    this.#cache = new Map();
    this.#output.endFilter();
  }

  filter(node: Node): IterableIterator<'yield', boolean> {
    let exists: boolean | undefined;
    if (!this.#noSizeReuse && !this.#inPush) {
      const key = this.#getCacheKey(node, this.#parentJoinKey);
      exists = this.#cache.get(key);
      if (exists === undefined) {
        const size = this.#fetchSizeNow(node);
        if (typeof size !== 'number') {
          // The relationship yielded: the rest is read lazily.
          return this.#filterRest(node, key, size);
        }
        exists = size > 0;
        this.#cache.set(key, exists);
      } else if (this.#cacheHitCountsForTesting) {
        this.#cacheHitCountsForTesting.set(
          key,
          (this.#cacheHitCountsForTesting.get(key) ?? 0) + 1,
        );
      }
    }
    if (exists === undefined) {
      const size = this.#fetchSizeNow(node);
      if (typeof size !== 'number') {
        return this.#filterRest(node, undefined, size);
      }
      exists = size > 0;
    }
    return (this.#not ? !exists : exists)
      ? this.#output.filter(node)
      : emptyIteratorReturnFalse;
  }

  /** filter() from the point where reading the node's relationship yielded. */
  *#filterRest(
    node: Node,
    cacheKey: string | undefined,
    read: PartialRead,
  ): IterableIterator<'yield', boolean> {
    const exists = (yield* this.#fetchSizeRest(node, read)) > 0;
    if (cacheKey !== undefined) {
      this.#cache.set(cacheKey, exists);
    }
    return (this.#not ? !exists : exists) && (yield* this.#output.filter(node));
  }

  destroy(): void {
    this.#input.destroy();
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  reconcile(_pusher: InputBase): Stream<'yield'> {
    return this.#output.reconcile ? this.#output.reconcile(this) : emptyArray;
  }

  push(change: Change): Stream<'yield'> {
    assert(!this.#inPush, 'Unexpected re-entrancy');
    this.#inPush = true;
    let s: Stream<'yield'>;
    try {
      s = this.#pushInPush(change);
    } catch (e) {
      this.#inPush = false;
      throw e;
    }
    return andFinally(s, this.#endPush);
  }

  readonly #endPush = (): void => {
    this.#inPush = false;
  };

  #pushInPush(change: Change): Stream<'yield'> {
    switch (change[ChangeIndex.TYPE]) {
      // add, remove and edit cannot change the size of the
      // this.#relationshipName relationship, so simply #pushWithFilter
      case ChangeType.ADD:
      case ChangeType.EDIT:
      case ChangeType.REMOVE:
        return this.#pushWithFilter(change);
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
          return this.#pushWithFilter(change);
        }
        switch (change[ChangeIndex.CHILD_DATA].change[ChangeIndex.TYPE]) {
          case ChangeType.ADD:
            return this.#withSize(change[ChangeIndex.NODE], size => {
              if (size === 1) {
                if (this.#not) {
                  // Since the add child change currently being processed is not
                  // pushed to output, the added child needs to be excluded from
                  // the remove being pushed to output (since the child has
                  // never been added to the output).
                  return this.#output.push(
                    makeRemoveChange({
                      row: change[ChangeIndex.NODE].row,
                      relationships: {
                        ...change[ChangeIndex.NODE].relationships,
                        [this.#relationshipName]: () => [],
                      },
                    }),
                    this,
                  );
                }
                return this.#output.push(
                  makeAddChange(change[ChangeIndex.NODE]),
                  this,
                );
              }
              return this.#pushWithFilter(change, size > 0);
            });
          case ChangeType.REMOVE:
            return this.#withSize(change[ChangeIndex.NODE], size => {
              if (size === 0) {
                if (this.#not) {
                  return this.#output.push(
                    makeAddChange(change[ChangeIndex.NODE]),
                    this,
                  );
                }
                // Since the remove child change currently being processed is
                // not pushed to output, the removed child needs to be added to
                // the remove being pushed to output.
                return this.#output.push(
                  makeRemoveChange({
                    row: change[ChangeIndex.NODE].row,
                    relationships: {
                      ...change[ChangeIndex.NODE].relationships,
                      [this.#relationshipName]: () => [
                        change[ChangeIndex.CHILD_DATA].change[ChangeIndex.NODE],
                      ],
                    },
                  }),
                  this,
                );
              }
              return this.#pushWithFilter(change, size > 0);
            });
        }
        return emptyArray;
      default:
        unreachable(change);
    }
  }

  #getCacheKey(node: Node, def: CompoundKey): string {
    if (def.length === 1) {
      return valueToKey(node.row[def[0]]);
    }
    const values: NormalizedValue[] = [];
    for (const key of def) {
      values.push(normalizeUndefined(node.row[key]));
    }
    return JSON.stringify(values);
  }

  /**
   * Pushes a change if this.#filter is true for its row.
   */
  #pushWithFilter(change: Change, exists?: boolean): Stream<'yield'> {
    if (exists === undefined) {
      return this.#withSize(change[ChangeIndex.NODE], size =>
        this.#pushIfPasses(change, size > 0),
      );
    }
    return this.#pushIfPasses(change, exists);
  }

  #pushIfPasses(change: Change, exists: boolean): Stream<'yield'> {
    return (this.#not ? !exists : exists)
      ? this.#output.push(change, this)
      : emptyArray;
  }

  /**
   * Calls `rest` with the size of the node's relationship: right away, or
   * once the stream returned has been read if reading the relationship yields.
   */
  #withSize(
    node: Node,
    rest: (size: number) => Stream<'yield'>,
  ): Stream<'yield'> {
    const size = this.#fetchSizeNow(node);
    return typeof size === 'number'
      ? rest(size)
      : this.#withSizeRest(node, size, rest);
  }

  *#withSizeRest(
    node: Node,
    read: PartialRead,
    rest: (size: number) => Stream<'yield'>,
  ): Stream<'yield'> {
    yield* rest(yield* this.#fetchSizeRest(node, read));
  }

  /**
   * The size of the node's this.#relationshipName relationship, or, if
   * reading it yields, what was read up to there, for #fetchSizeRest.
   */
  #fetchSizeNow(node: Node): number | PartialRead {
    const relationship = node.relationships[this.#relationshipName];
    assert(
      relationship,
      () =>
        `Exists: relationship "${this.#relationshipName}" not found on node`,
    );
    if (this.#inPush) {
      // Only the size is needed, which Cap can answer from its state.
      let size = 0;
      const it = relationship(true)[Symbol.iterator]();
      for (;;) {
        const r = it.next();
        if (r.done) {
          return size;
        }
        if (r.value === 'yield') {
          return {it, size, children: undefined};
        }
        size++;
      }
    }
    // During a fetch, keep the children read to count them and make the
    // node's relationship return them, so that the consumer of the node does
    // not fetch them again. The exists subquery is limited, so this holds at
    // most a few nodes. `node.relationships` is created per node by the Join.
    const children: Node[] = [];
    const it = relationship()[Symbol.iterator]();
    for (;;) {
      const r = it.next();
      if (r.done) {
        break;
      }
      if (r.value === 'yield') {
        return {it, size: 0, children};
      }
      children.push(r.value);
    }
    node.relationships[this.#relationshipName] = () => children;
    return children.length;
  }

  /** #fetchSizeNow from the 'yield' it stopped at. */
  *#fetchSizeRest(
    node: Node,
    {it, size, children}: PartialRead,
  ): IterableIterator<'yield', number> {
    let ended = false;
    try {
      yield 'yield';
      for (;;) {
        const r = it.next();
        if (r.done) {
          ended = true;
          break;
        }
        if (r.value === 'yield') {
          yield 'yield';
        } else if (children) {
          children.push(r.value);
        } else {
          size++;
        }
      }
    } finally {
      // The consumer stopped reading at a 'yield'.
      if (!ended) {
        it.return?.();
      }
    }
    if (children) {
      node.relationships[this.#relationshipName] = () => children;
      return children.length;
    }
    return size;
  }
}

const emptyIteratorReturnFalse = makeEmptyIteratorWithReturn(false);

/** A relationship read that stopped at a 'yield'. */
type PartialRead = {
  readonly it: Iterator<Node | 'yield'>;
  /** Nodes counted so far, when only the size is wanted. */
  readonly size: number;
  /** Nodes read so far, when they are kept. */
  readonly children: Node[] | undefined;
};
