import {areEqual} from '../../../shared/src/arrays.ts';
import {assert, unreachable} from '../../../shared/src/asserts.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
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
import {canonicalKey} from './join-utils.ts';
import type {InputBase} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {type Stream} from './stream.ts';

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
  readonly #primaryKey: PrimaryKey;
  readonly #counts = new Map<string, number>();
  #sizeCache: Map<string, number>;
  #cacheHitCountsForTesting: Map<string, number> | undefined;
  #output: FilterOutput = throwFilterOutput;

  /**
   * This instance variable is `true` when this operator is processing a `push`,
   * and is used to disable reuse of cached sizes across rows with the
   * same parent join key value.
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
    this.#sizeCache = new Map();
    this.#cacheHitCountsForTesting = cacheHitCountsForTesting;
    assert(
      this.#input.getSchema().relationships[relationshipName],
      // log-leak-ignore -- relationship name is schema, allowed in errors
      `Input schema missing ${relationshipName}`,
    );
    this.#not = type === 'NOT EXISTS';
    this.#parentJoinKey = parentJoinKey;
    this.#primaryKey = input.getSchema().primaryKey;

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
    this.#sizeCache = new Map();
    this.#output.endFilter();
  }

  *filter(node: Node): IterableIterator<'yield', boolean> {
    let size: number | undefined;
    if (!this.#noSizeReuse && !this.#inPush) {
      const key = this.#getCacheKey(node, this.#parentJoinKey);
      size = this.#sizeCache.get(key);
      if (size === undefined) {
        size = yield* this.#fetchSize(node);
        this.#sizeCache.set(key, size);
      } else if (this.#cacheHitCountsForTesting) {
        this.#cacheHitCountsForTesting.set(
          key,
          (this.#cacheHitCountsForTesting.get(key) ?? 0) + 1,
        );
      }
    } else {
      size = yield* this.#fetchSize(node);
    }

    const pk = canonicalKey(node.row, this.#primaryKey);
    this.#counts.set(pk, size);

    const exists = size > 0;
    const passes = this.#not ? !exists : exists;
    const result = passes && (yield* this.#output.filter(node));
    return result;
  }

  destroy(): void {
    this.#counts.clear();
    this.#sizeCache.clear();
    this.#input.destroy();
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  *reconcile(_pusher: InputBase): Stream<'yield'> {
    if (this.#output.reconcile) {
      yield* this.#output.reconcile(this);
    }
  }

  *push(change: Change): Stream<'yield'> {
    assert(!this.#inPush, 'Unexpected re-entrancy');
    this.#inPush = true;
    try {
      switch (change[ChangeIndex.TYPE]) {
        case ChangeType.ADD: {
          const pk = canonicalKey(
            change[ChangeIndex.NODE].row,
            this.#primaryKey,
          );
          let size = 0;
          const rel =
            change[ChangeIndex.NODE].relationships[this.#relationshipName];
          if (rel) {
            for (const n of rel()) {
              if (n === 'yield') {
                yield 'yield';
                continue;
              }
              size++;
            }
          }
          this.#counts.set(pk, size);
          yield* this.#pushWithFilter(change, size > 0);
          return;
        }
        case ChangeType.REMOVE: {
          const pk = canonicalKey(
            change[ChangeIndex.NODE].row,
            this.#primaryKey,
          );
          const size = this.#counts.get(pk) ?? 0;
          this.#counts.delete(pk);
          yield* this.#pushWithFilter(change, size > 0);
          return;
        }
        case ChangeType.EDIT: {
          const oldPk = canonicalKey(
            change[ChangeIndex.OLD_NODE].row,
            this.#primaryKey,
          );
          const newPk = canonicalKey(
            change[ChangeIndex.NODE].row,
            this.#primaryKey,
          );
          const size = this.#counts.get(oldPk) ?? 0;
          if (oldPk !== newPk) {
            this.#counts.delete(oldPk);
          }
          this.#counts.set(newPk, size);
          yield* this.#pushWithFilter(change, size > 0);
          return;
        }
        case ChangeType.CHILD: {
          if (
            change[ChangeIndex.CHILD_DATA].relationshipName !==
              this.#relationshipName ||
            change[ChangeIndex.CHILD_DATA].change[ChangeIndex.TYPE] ===
              ChangeType.EDIT ||
            change[ChangeIndex.CHILD_DATA].change[ChangeIndex.TYPE] ===
              ChangeType.CHILD
          ) {
            const pk = canonicalKey(
              change[ChangeIndex.NODE].row,
              this.#primaryKey,
            );
            const size = this.#counts.get(pk) ?? 0;
            yield* this.#pushWithFilter(change, size > 0);
            return;
          }
          const pk = canonicalKey(
            change[ChangeIndex.NODE].row,
            this.#primaryKey,
          );
          const currentSize = this.#counts.get(pk) ?? 0;
          switch (change[ChangeIndex.CHILD_DATA].change[ChangeIndex.TYPE]) {
            case ChangeType.ADD: {
              const newSize = currentSize + 1;
              this.#counts.set(pk, newSize);
              if (currentSize === 0) {
                if (this.#not) {
                  yield* this.#output.push(
                    makeRemoveChange({
                      row: change[ChangeIndex.NODE].row,
                      relationships: {
                        ...change[ChangeIndex.NODE].relationships,
                        [this.#relationshipName]: () => [],
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
                yield* this.#pushWithFilter(change, true);
              }
              return;
            }
            case ChangeType.REMOVE: {
              const newSize = Math.max(0, currentSize - 1);
              this.#counts.set(pk, newSize);
              if (currentSize === 1 && newSize === 0) {
                if (this.#not) {
                  yield* this.#output.push(
                    makeAddChange(change[ChangeIndex.NODE]),
                    this,
                  );
                } else {
                  yield* this.#output.push(
                    makeRemoveChange({
                      row: change[ChangeIndex.NODE].row,
                      relationships: {
                        ...change[ChangeIndex.NODE].relationships,
                        [this.#relationshipName]: () => [
                          change[ChangeIndex.CHILD_DATA].change[
                            ChangeIndex.NODE
                          ],
                        ],
                      },
                    }),
                    this,
                  );
                }
              } else {
                yield* this.#pushWithFilter(change, newSize > 0);
              }
              return;
            }
          }
          return;
        }
        default:
          unreachable(change);
      }
    } finally {
      this.#inPush = false;
    }
  }

  *#filter(node: Node, exists?: boolean): IterableIterator<'yield', boolean> {
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

  *#pushWithFilter(change: Change, exists?: boolean): Stream<'yield'> {
    if (yield* this.#filter(change[ChangeIndex.NODE], exists)) {
      yield* this.#output.push(change, this);
    }
  }

  *#fetchExists(node: Node): IterableIterator<'yield', boolean> {
    return (yield* this.#fetchSize(node)) > 0;
  }

  *#fetchSize(node: Node): IterableIterator<'yield', number> {
    const relationship = node.relationships[this.#relationshipName];
    assert(
      relationship,
      () =>
        `Exists: relationship "${this.#relationshipName}" not found on node`,
    );
    let size = 0;
    for (const n of relationship()) {
      if (n === 'yield') {
        yield 'yield';
      } else {
        size++;
      }
    }
    return size;
  }
}
