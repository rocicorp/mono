import {assert, unreachable} from '../../../shared/src/asserts.ts';
import {hasOwn} from '../../../shared/src/has-own.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import {assertOrderingIncludesPK} from '../query/complete-ordering.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import {
  makeAddChange,
  makeRemoveChange,
  type Change,
  type EditChange,
} from './change.ts';
import type {Constraint} from './constraint.ts';
import {compareValues, type Comparator, type Node} from './data.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type InputBase,
  type Operator,
  type Output,
  type Storage,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {type Stream} from './stream.ts';
import type {TakeBoundProvider, TakeGate} from './take-gate.ts';

type TakeState = {
  size: number;
  bound: Row | undefined;
};

interface TakeStorage {
  get(key: string): TakeState | undefined;
  set(key: string, value: TakeState): void;
  del(key: string): void;
}

export type PartitionKey = PrimaryKey;

type DirtyPartitionState = {
  constraint: Constraint | undefined;
  /**
   * The active bound before any removals in this push cycle reduced the partition
   * size below limit. Upstream operators (like TakeGate) consult getBound() during
   * Phase 1 to cap child pushes. Preserving lastBound prevents TakeGate from prematurely
   * opening (becoming unbounded) while the partition has an unresolved deficit.
   */
  lastBound: Row | undefined;
};

/**
 * The Take operator is for implementing limit queries. It takes the first n
 * nodes of its input as determined by the input’s comparator. It then keeps
 * a *bound* of the last item it has accepted so that it can evaluate whether
 * new incoming pushes should be accepted or rejected.
 *
 * Take can count rows globally or by unique value of some field.
 *
 * Maintains the invariant that its output size is always <= limit, even
 * mid processing of a push.
 */
export class Take implements Operator, TakeBoundProvider {
  readonly #input: Input;
  readonly #storage: TakeStorage;
  readonly #limit: number;
  readonly #partitionKey: PartitionKey | undefined;
  readonly #partitionKeyComparator: Comparator | undefined;

  #takeGate: TakeGate | undefined;
  /**
   * Partitions that incurred removals in Phase 1 and require deficit refills in
   * Phase 2 (reconcile). Keyed by `takeStateKey` (see `getTakeStateKey`), matching
   * `#storage` keys: `'["take"]'` when unpartitioned, or `'["take", ...partitionValues]'`
   * when partitioned.
   */
  readonly #dirtyPartitions = new Map<string, DirtyPartitionState>();

  /**
   * When limit === 1, stores the currently active in-window Node for each partition
   * so that inline displacement during Phase 1 emits a complete REMOVE change with
   * relationships intact rather than stripping relationships.
   */
  readonly #boundNodes = new Map<string, Node>();

  #output: Output = throwOutput;

  setTakeGate(gate: TakeGate): void {
    this.#takeGate = gate;
  }

  constructor(
    input: Input,
    storage: Storage,
    limit: number,
    partitionKey?: PartitionKey,
  ) {
    assert(limit >= 0, 'Limit must be non-negative');
    const {sort} = input.getSchema();
    assert(sort !== undefined, 'Take requires sorted input');
    assertOrderingIncludesPK(sort, input.getSchema().primaryKey);
    input.setOutput(this);
    this.#input = input;
    this.#storage = storage as TakeStorage;
    this.#limit = limit;
    this.#partitionKey = partitionKey;
    this.#partitionKeyComparator =
      partitionKey && makePartitionKeyComparator(partitionKey);
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  getBound(constraint?: Constraint): Row | undefined {
    if (
      this.#partitionKey &&
      !constraintContainsPartitionKey(constraint, this.#partitionKey)
    ) {
      return undefined;
    }
    const takeStateKey = getTakeStateKey(this.#partitionKey, constraint);
    const dirty = this.#dirtyPartitions.get(takeStateKey);
    if (dirty) {
      // While dirty in Phase 1, return the bound prior to any removals so upstream
      // operators (e.g. TakeGate) remain capped rather than unbounding during push.
      return dirty.lastBound;
    }
    const takeState = this.#storage.get(takeStateKey);
    if (!takeState || takeState.size < this.#limit) {
      return undefined;
    }
    return takeState.bound;
  }

  *fetch(req: FetchRequest): Stream<Node | 'yield'> {
    assert(
      !this.#partitionKey ||
        (req.constraint !== undefined &&
          constraintContainsPartitionKey(req.constraint, this.#partitionKey)),
      'Partitioned take does not allow unpartitioned fetches',
    );

    const takeStateKey = getTakeStateKey(this.#partitionKey, req.constraint);
    const takeState = this.#storage.get(takeStateKey);
    if (!takeState) {
      if (constraintMatchesPartitionKey(req.constraint, this.#partitionKey)) {
        yield* this.#initialFetch(req);
      }
      return;
    }
    const bound =
      takeState.bound ?? this.#dirtyPartitions.get(takeStateKey)?.lastBound;
    if (bound === undefined) {
      return;
    }
    let count = 0;
    for (const inputNode of this.#input.fetch(req)) {
      if (inputNode === 'yield') {
        yield inputNode;
        continue;
      }
      if (this.getSchema().compareRows(bound, inputNode.row) < 0) {
        return;
      }
      if (this.#limit === 1) {
        this.#boundNodes.set(takeStateKey, inputNode);
      }
      yield inputNode;
      count++;
      if (count >= this.#limit) {
        return;
      }
    }
  }

  *#initialFetch(req: FetchRequest): Stream<Node | 'yield'> {
    assert(req.start === undefined, 'Start should be undefined');
    assert(!req.reverse, 'Reverse should be false');

    if (this.#limit === 0) {
      return;
    }

    assert(
      constraintMatchesPartitionKey(req.constraint, this.#partitionKey),
      'Constraint should match partition key',
    );

    const takeStateKey = getTakeStateKey(this.#partitionKey, req.constraint);
    assert(
      this.#storage.get(takeStateKey) === undefined,
      'Take state should be undefined',
    );

    let size = 0;
    let bound: Row | undefined;
    let singleNode: Node | undefined;
    let downstreamEarlyReturn = true;
    let exceptionThrown = false;
    try {
      for (const inputNode of this.#input.fetch(req)) {
        if (inputNode === 'yield') {
          yield 'yield';
          continue;
        }
        yield inputNode;
        bound = inputNode.row;
        if (this.#limit === 1) {
          singleNode = inputNode;
        }
        size++;
        if (size === this.#limit) {
          break;
        }
      }
      downstreamEarlyReturn = false;
    } catch (e) {
      exceptionThrown = true;
      throw e;
    } finally {
      if (!exceptionThrown) {
        this.#setTakeState(takeStateKey, size, bound);
        if (this.#limit === 1 && singleNode) {
          this.#boundNodes.set(takeStateKey, singleNode);
        }
        // If it becomes necessary to support downstream early return, this
        // assert should be removed, and replaced with code that consumes
        // the input stream until limit is reached or the input stream is
        // exhausted so that takeState is properly hydrated.
        assert(
          !downstreamEarlyReturn,
          'Unexpected early return prevented full hydration',
        );
      }
    }
  }

  #getStateAndConstraint(row: Row) {
    const takeStateKey = getTakeStateKey(this.#partitionKey, row);
    const takeState = this.#storage.get(takeStateKey);
    let constraint: Constraint | undefined;
    if (takeState) {
      constraint =
        this.#partitionKey &&
        Object.fromEntries(
          this.#partitionKey.map(key => [key, row[key]] as const),
        );
    }

    return {takeState, takeStateKey, constraint} as
      | {
          takeState: undefined;
          takeStateKey: string;
          constraint: undefined;
        }
      | {
          takeState: TakeState;
          takeStateKey: string;
          constraint: Constraint | undefined;
        };
  }

  *push(change: Change): Stream<'yield'> {
    if (change[ChangeIndex.TYPE] === ChangeType.EDIT) {
      yield* this.#pushEditChange(change);
      return;
    }

    const {takeState, takeStateKey, constraint} = this.#getStateAndConstraint(
      change[ChangeIndex.NODE].row,
    );
    if (!takeState) {
      return;
    }

    const {compareRows} = this.getSchema();

    if (change[ChangeIndex.TYPE] === ChangeType.ADD) {
      if (takeState.size < this.#limit) {
        if (this.#dirtyPartitions.has(takeStateKey)) {
          const dirty = this.#dirtyPartitions.get(takeStateKey)!;
          if (
            dirty.lastBound !== undefined &&
            compareRows(change[ChangeIndex.NODE].row, dirty.lastBound) > 0
          ) {
            return;
          }
        }
        const nextSize = takeState.size + 1;
        const nextBound =
          takeState.bound === undefined
            ? takeState.size === 0
              ? change[ChangeIndex.NODE].row
              : undefined
            : compareRows(takeState.bound, change[ChangeIndex.NODE].row) < 0
              ? change[ChangeIndex.NODE].row
              : takeState.bound;
        this.#setTakeState(takeStateKey, nextSize, nextBound);
        if (this.#limit === 1) {
          this.#boundNodes.set(takeStateKey, change[ChangeIndex.NODE]);
        }
        yield* this.#output.push(change, this);
        return;
      }

      // size >= limit
      const activeBound =
        takeState.bound ?? this.#dirtyPartitions.get(takeStateKey)?.lastBound;
      if (
        activeBound === undefined ||
        compareRows(change[ChangeIndex.NODE].row, activeBound) >= 0
      ) {
        return;
      }

      // added row < activeBound
      if (this.#limit === 1) {
        const oldNode = this.#boundNodes.get(takeStateKey);
        const removeChange = makeRemoveChange(
          oldNode ?? {
            row: takeState.bound ?? activeBound,
            relationships: {},
          },
        );
        this.#boundNodes.set(takeStateKey, change[ChangeIndex.NODE]);
        this.#setTakeState(takeStateKey, 1, change[ChangeIndex.NODE].row);
        yield* this.#output.push(removeChange, this);
        yield* this.#output.push(change, this);
        return;
      }

      if (!this.#dirtyPartitions.has(takeStateKey)) {
        this.#dirtyPartitions.set(takeStateKey, {
          constraint,
          lastBound: activeBound,
        });
      }

      this.#setTakeState(takeStateKey, takeState.size + 1, undefined);
      yield* this.#output.push(change, this);
      return;
    } else if (change[ChangeIndex.TYPE] === ChangeType.REMOVE) {
      if (
        takeState.bound === undefined &&
        !this.#dirtyPartitions.has(takeStateKey)
      ) {
        return;
      }
      const activeBound =
        takeState.bound ?? this.#dirtyPartitions.get(takeStateKey)?.lastBound;
      if (activeBound === undefined) {
        return;
      }
      const compToBound = compareRows(
        change[ChangeIndex.NODE].row,
        activeBound,
      );
      if (compToBound > 0) {
        // change is not in window
        return;
      }

      if (!this.#dirtyPartitions.has(takeStateKey)) {
        this.#dirtyPartitions.set(takeStateKey, {
          constraint,
          lastBound: activeBound,
        });
      }
      const nextSize = takeState.size - 1;
      const finalBound =
        nextSize === 0
          ? undefined
          : compToBound < 0
            ? takeState.bound
            : undefined;
      this.#setTakeState(takeStateKey, nextSize, finalBound);
      if (this.#limit === 1) {
        this.#boundNodes.delete(takeStateKey);
      }
      yield* this.#output.push(change, this);
      return;
    } else if (change[ChangeIndex.TYPE] === ChangeType.CHILD) {
      // A 'child' change should be pushed to output if its row
      // is <= bound.
      const activeBound =
        takeState.bound ?? this.#dirtyPartitions.get(takeStateKey)?.lastBound;
      if (
        activeBound &&
        compareRows(change[ChangeIndex.NODE].row, activeBound) <= 0
      ) {
        if (this.#limit === 1) {
          this.#boundNodes.set(takeStateKey, change[ChangeIndex.NODE]);
        }
        yield* this.#output.push(change, this);
      }
    }
  }

  *#pushEditChange(change: EditChange): Stream<'yield'> {
    assert(
      !this.#partitionKeyComparator ||
        this.#partitionKeyComparator(
          change[ChangeIndex.OLD_NODE].row,
          change[ChangeIndex.NODE].row,
        ) === 0,
      'Unexpected change of partition key',
    );

    const {takeState, takeStateKey, constraint} = this.#getStateAndConstraint(
      change[ChangeIndex.OLD_NODE].row,
    );
    if (!takeState) {
      return;
    }

    const activeBound =
      takeState.bound ?? this.#dirtyPartitions.get(takeStateKey)?.lastBound;
    assert(activeBound, 'Bound should be set');
    const {compareRows} = this.getSchema();
    const oldCmp = compareRows(change[ChangeIndex.OLD_NODE].row, activeBound);
    const newCmp = compareRows(change[ChangeIndex.NODE].row, activeBound);

    // Both outside bounds
    if (oldCmp > 0 && newCmp > 0) {
      return;
    }

    // Both inside bounds (or unchanged bound)
    if ((oldCmp < 0 && newCmp < 0) || (oldCmp === 0 && newCmp === 0)) {
      if (this.#limit === 1) {
        this.#boundNodes.set(takeStateKey, change[ChangeIndex.NODE]);
      }
      yield* this.#output.push(change, this);
      return;
    }

    // Old was inside/at bounds, new is outside bounds: old row leaves window
    if (oldCmp <= 0 && newCmp > 0) {
      yield* this.push(makeRemoveChange(change[ChangeIndex.OLD_NODE]));
      return;
    }

    // Old was outside bounds, new is inside bounds: new row enters window
    if (oldCmp > 0 && newCmp < 0) {
      yield* this.push(makeAddChange(change[ChangeIndex.NODE]));
      return;
    }

    // Old was the bound, new is inside bounds
    if (oldCmp === 0 && newCmp < 0) {
      if (this.#limit === 1) {
        this.#boundNodes.set(takeStateKey, change[ChangeIndex.NODE]);
        this.#setTakeState(
          takeStateKey,
          takeState.size,
          change[ChangeIndex.NODE].row,
        );
        yield* this.#output.push(change, this);
        return;
      }
      if (!this.#dirtyPartitions.has(takeStateKey)) {
        this.#dirtyPartitions.set(takeStateKey, {
          constraint,
          lastBound: activeBound,
        });
      }
      this.#setTakeState(takeStateKey, takeState.size, undefined);
      yield* this.#output.push(change, this);
      return;
    }

    unreachable();
  }

  #setTakeState(takeStateKey: string, size: number, bound: Row | undefined) {
    this.#storage.set(takeStateKey, {
      size,
      bound,
    });
  }

  destroy(): void {
    this.#boundNodes.clear();
    this.#input.destroy();
  }

  *reconcile(_pusher?: InputBase): Stream<'yield'> {
    if (this.#dirtyPartitions.size > 0) {
      const dirty = [...this.#dirtyPartitions.entries()];

      for (const [takeStateKey, {constraint, lastBound}] of dirty) {
        let takeState = this.#storage.get(takeStateKey);
        if (!takeState) {
          continue;
        }

        // 1. Handle overflow (if multiple adds at capacity caused size > limit)
        if (takeState.size > this.#limit) {
          const toRemove: Node[] = [];
          let newBound: Row | undefined;
          let count = 0;
          this.#takeGate?.open();
          try {
            for (const node of this.#input.fetch({
              constraint,
            })) {
              if (node === 'yield') {
                yield 'yield';
                continue;
              }
              count++;
              if (count === this.#limit) {
                newBound = node.row;
              } else if (count > this.#limit) {
                toRemove.push(node);
                if (count === takeState.size) {
                  break;
                }
              }
            }
          } finally {
            this.#takeGate?.close();
          }

          for (const node of toRemove) {
            yield* this.#output.push(makeRemoveChange(node), this);
          }
          this.#setTakeState(takeStateKey, this.#limit, newBound);
          takeState = this.#storage.get(takeStateKey)!;
        }

        // 2. Handle deficit (removals reduced size below limit)
        const deficit = this.#limit - takeState.size;
        if (deficit > 0) {
          const toPush: Node[] = [];
          this.#takeGate?.open();
          try {
            const stream = this.#input.fetch({
              start: lastBound
                ? {
                    row: lastBound,
                    basis: 'after',
                  }
                : undefined,
              constraint,
            });

            for (const node of stream) {
              if (node === 'yield') {
                yield 'yield';
                continue;
              }
              toPush.push(node);
              if (toPush.length === deficit) {
                break;
              }
            }
          } finally {
            this.#takeGate?.close();
          }

          const finalNode = toPush.at(-1);
          if (finalNode) {
            this.#setTakeState(
              takeStateKey,
              takeState.size + toPush.length,
              finalNode.row,
            );
            if (this.#limit === 1) {
              this.#boundNodes.set(takeStateKey, finalNode);
            }
            this.#dirtyPartitions.delete(takeStateKey);
          } else if (takeState.size === 0) {
            if (this.#limit === 1) {
              this.#boundNodes.delete(takeStateKey);
            }
            this.#dirtyPartitions.delete(takeStateKey);
          }

          for (const node of toPush) {
            yield* this.#output.push(makeAddChange(node), this);
          }
          takeState = this.#storage.get(takeStateKey)!;
        }

        // 3. If bound is still undefined but size > 0 (bound was removed or evicted and not yet updated):
        if (takeState.bound === undefined && takeState.size > 0) {
          let count = 0;
          for (const node of this.#input.fetch({
            constraint,
          })) {
            if (node === 'yield') {
              yield 'yield';
              continue;
            }
            count++;
            if (count === takeState.size) {
              this.#setTakeState(takeStateKey, takeState.size, node.row);
              if (this.#limit === 1) {
                this.#boundNodes.set(takeStateKey, node);
              }
              break;
            }
          }
        }

        this.#dirtyPartitions.delete(takeStateKey);
      }
    }

    if (this.#output.reconcile) {
      yield* this.#output.reconcile(this);
    }
  }
}

function getTakeStateKey(
  partitionKey: PartitionKey | undefined,
  rowOrConstraint: Row | Constraint | undefined,
): string {
  // The order must be consistent. We always use the order as defined by the
  // partition key.
  const partitionValues: Value[] = [];

  if (partitionKey && rowOrConstraint) {
    for (const key of partitionKey) {
      partitionValues.push(rowOrConstraint[key]);
    }
  }

  return JSON.stringify(['take', ...partitionValues]);
}

export function constraintMatchesPartitionKey(
  constraint: Constraint | undefined,
  partitionKey: PartitionKey | undefined,
): boolean {
  if (constraint === undefined || partitionKey === undefined) {
    return constraint === partitionKey;
  }
  if (partitionKey.length !== Object.keys(constraint).length) {
    return false;
  }
  for (const key of partitionKey) {
    if (!hasOwn(constraint, key)) {
      return false;
    }
  }
  return true;
}

export function constraintContainsPartitionKey(
  constraint: Constraint | undefined,
  partitionKey: PartitionKey | undefined,
): boolean {
  if (constraint === undefined || partitionKey === undefined) {
    return false;
  }
  for (const key of partitionKey) {
    if (!hasOwn(constraint, key)) {
      return false;
    }
  }
  return true;
}

export function makePartitionKeyComparator(
  partitionKey: PartitionKey,
): Comparator {
  return (a, b) => {
    for (const key of partitionKey) {
      const cmp = compareValues(a[key], b[key]);
      if (cmp !== 0) {
        return cmp;
      }
    }
    return 0;
  };
}
