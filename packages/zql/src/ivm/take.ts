import {assert, unreachable} from '../../../shared/src/asserts.ts';
import {hasOwn} from '../../../shared/src/has-own.ts';
import {emptyArray} from '../../../shared/src/sentinels.ts';
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
import {compareValues, valueToKey, type Comparator, type Node} from './data.ts';
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
import {andFinally, andThen, eachStream, type Stream} from './stream.ts';
import type {TakeBoundProvider, TakeGate} from './take-gate.ts';

type TakeState = {
  size: number;
  bound: Row | undefined;
};

interface TakeStorage {
  get(key: string): TakeState | undefined;
  set(key: string, value: TakeState): void;
  del(key: string): void;
  destroy(): void;
}

export type PartitionKey = PrimaryKey;

type DirtyPartitionState = {
  constraint: Constraint | undefined;
  /**
   * The bound to report to upstream TakeGate via getBound() until Phase 2
   * reconciliation completes.
   *
   * When removals reduce partition size below limit in Phase 1, normal getBound()
   * would return undefined (unbounded), which would cause upstream TakeGate to open
   * and allow unbounded parent pushes through. Preserving gateBound keeps TakeGate
   * capped at the pre-removal boundary until Phase 2 refills the deficit.
   */
  gateBound: Row | undefined;
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
  // Fetch overlay needed for some split push cases.
  #rowHiddenFromFetch: Row | undefined;

  #takeGate: TakeGate | undefined;
  /**
   * Partitions that incurred removals in Phase 1 and require deficit refills in
   * Phase 2 (reconcile). Keyed by `takeStateKey` (see `getTakeStateKey`), matching
   * `#storage` keys: `'["take"]'` when unpartitioned, `'take:' + valueToKey(value)` for one
   * partition column, or `'["take", ...partitionValues]'` for more.
   */
  readonly #dirtyPartitions = new Map<string, DirtyPartitionState>();

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
      // While dirty in Phase 1, return gateBound (the bound prior to removals) so
      // upstream operators (e.g. TakeGate) remain capped rather than unbounding during push.
      return dirty.gateBound;
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
        yield* this.#initialFetch(req, takeStateKey);
      }
      return;
    }
    if (takeState.bound === undefined) {
      return;
    }
    let count = 0;
    for (const inputNode of this.#input.fetch(req)) {
      if (inputNode === 'yield') {
        yield inputNode;
        continue;
      }
      if (this.getSchema().compareRows(takeState.bound, inputNode.row) < 0) {
        return;
      }
      if (
        this.#rowHiddenFromFetch &&
        this.getSchema().compareRows(
          this.#rowHiddenFromFetch,
          inputNode.row,
        ) === 0
      ) {
        continue;
      }
      yield inputNode;
      count++;
      if (count >= this.#limit) {
        return;
      }
    }
  }

  *#initialFetch(
    req: FetchRequest,
    takeStateKey: string,
  ): Stream<Node | 'yield'> {
    assert(req.start === undefined, 'Start should be undefined');
    assert(!req.reverse, 'Reverse should be false');

    if (this.#limit === 0) {
      return;
    }

    assert(
      constraintMatchesPartitionKey(req.constraint, this.#partitionKey),
      'Constraint should match partition key',
    );

    // The caller (fetch) has already checked that there is no state for
    // takeStateKey.

    let size = 0;
    let bound: Row | undefined;
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

  push(change: Change): Stream<'yield'> {
    if (change[ChangeIndex.TYPE] === ChangeType.EDIT) {
      return this.#pushEditChange(change);
    }

    const {takeState, takeStateKey, constraint} = this.#getStateAndConstraint(
      change[ChangeIndex.NODE].row,
    );
    if (!takeState) {
      return emptyArray;
    }

    const {compareRows} = this.getSchema();

    if (change[ChangeIndex.TYPE] === ChangeType.ADD) {
      if (takeState.size < this.#limit) {
        if (this.#dirtyPartitions.has(takeStateKey)) {
          // While dirty with an unresolved deficit in Phase 1, only admit rows that
          // sort strictly before the current window boundary. We refuse to expand the
          // bound forward during Phase 1 so that Phase 2 can backfill earlier candidate
          // rows from storage in sort order.
          if (
            takeState.bound === undefined ||
            compareRows(change[ChangeIndex.NODE].row, takeState.bound) >= 0
          ) {
            return emptyArray;
          }
        }
        const nextSize = takeState.size + 1;
        if (nextSize === this.#limit) {
          this.#dirtyPartitions.delete(takeStateKey);
        }
        this.#setTakeState(
          takeStateKey,
          nextSize,
          takeState.bound === undefined ||
            compareRows(takeState.bound, change[ChangeIndex.NODE].row) < 0
            ? change[ChangeIndex.NODE].row
            : takeState.bound,
        );
        return this.#output.push(change, this);
      }
      // size === limit
      if (
        takeState.bound === undefined ||
        compareRows(change[ChangeIndex.NODE].row, takeState.bound) >= 0
      ) {
        return emptyArray;
      }
      // added row < bound
      // The bound node and, unless the limit is 1, the node before it.
      return this.#withFirst(
        this.#limit === 1
          ? {
              start: {
                row: takeState.bound,
                basis: 'at',
              },
              constraint,
            }
          : {
              start: {
                row: takeState.bound,
                basis: 'at',
              },
              constraint,
              reverse: true,
            },
        this.#limit === 1 ? 1 : 2,
        false,
        ([boundNode, beforeBoundNode]) => {
          assert(
            boundNode !== undefined,
            'Take: boundNode must be found during fetch',
          );
          const removeChange = makeRemoveChange(boundNode);
          // Remove before add to maintain invariant that
          // output size <= limit.
          this.#setTakeState(
            takeStateKey,
            takeState.size,
            beforeBoundNode === undefined ||
              compareRows(change[ChangeIndex.NODE].row, beforeBoundNode.row) > 0
              ? change[ChangeIndex.NODE].row
              : beforeBoundNode.row,
          );
          return andThen(
            this.#pushWithRowHiddenFromFetch(
              change[ChangeIndex.NODE].row,
              removeChange,
            ),
            () => this.#output.push(change, this),
          );
        },
      );
    } else if (change[ChangeIndex.TYPE] === ChangeType.REMOVE) {
      if (takeState.bound === undefined) {
        // change is after bound
        return emptyArray;
      }
      const compToBound = compareRows(
        change[ChangeIndex.NODE].row,
        takeState.bound,
      );
      if (compToBound > 0 || (compToBound < 0 && this.#limit === 1)) {
        // change is not in window
        return emptyArray;
      }
      const {bound} = takeState;
      return this.#withFirst(
        {
          start: {
            row: bound,
            basis: 'after',
          },
          constraint,
          reverse: true,
        },
        1,
        false,
        ([beforeBoundNode]) => {
          const finalBound =
            takeState.size - 1 === 0
              ? undefined
              : compToBound < 0
                ? bound
                : beforeBoundNode?.row;
          if (!this.#dirtyPartitions.has(takeStateKey)) {
            // Snapshot the pre-removal bound as the gate bound on the first removal that
            // dirties this partition. Subsequent getBound() calls throughout Phase 1 will return
            // this bound to keep upstream TakeGate bounded until Phase 2 refills deficits.
            this.#dirtyPartitions.set(takeStateKey, {
              constraint,
              gateBound: bound,
            });
          }
          this.#setTakeState(takeStateKey, takeState.size - 1, finalBound);
          return this.#output.push(change, this);
        },
      );
    }
    // A 'child' change should be pushed to output if its row
    // is <= bound.
    if (
      takeState.bound &&
      compareRows(change[ChangeIndex.NODE].row, takeState.bound) <= 0
    ) {
      return this.#output.push(change, this);
    }
    return emptyArray;
  }

  /**
   * Reads the first `n` nodes of a fetch from the input and calls `rest` with
   * them (fewer if the fetch has fewer). With `gated`, the take gate is open
   * while the fetch is read.
   *
   * `rest` runs right away unless the fetch yields. Then the stream returned
   * holds the yields, with what is left of the fetch and `rest` behind them
   * (see `andThen` in stream.ts).
   */
  #withFirst(
    req: FetchRequest,
    n: number,
    gated: boolean,
    rest: (nodes: Node[]) => Stream<'yield'>,
  ): Stream<'yield'> {
    const nodes: Node[] = [];
    let closeGate = gated;
    if (gated) {
      this.#takeGate?.open();
    }
    try {
      const it = this.#input.fetch(req)[Symbol.iterator]();
      for (;;) {
        const r = it.next();
        if (r.done) {
          break;
        }
        if (r.value === 'yield') {
          closeGate = false;
          return this.#withFirstRest(it, nodes, n, gated, rest);
        }
        nodes.push(r.value);
        if (nodes.length === n) {
          it.return?.();
          break;
        }
      }
    } finally {
      if (closeGate) {
        this.#takeGate?.close();
      }
    }
    return rest(nodes);
  }

  /** #withFirst from the 'yield' it stopped at. */
  *#withFirstRest(
    it: Iterator<Node | 'yield'>,
    nodes: Node[],
    n: number,
    gated: boolean,
    rest: (nodes: Node[]) => Stream<'yield'>,
  ): Stream<'yield'> {
    let finished = false;
    try {
      yield 'yield';
      for (;;) {
        const r = it.next();
        if (r.done) {
          finished = true;
          break;
        }
        if (r.value === 'yield') {
          yield 'yield';
          continue;
        }
        nodes.push(r.value);
        if (nodes.length === n) {
          break;
        }
      }
    } finally {
      if (!finished) {
        it.return?.();
      }
      if (gated) {
        this.#takeGate?.close();
      }
    }
    yield* rest(nodes);
  }

  #pushEditChange(change: EditChange): Stream<'yield'> {
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
      return emptyArray;
    }

    assert(takeState.bound, 'Bound should be set');
    const {bound} = takeState;
    const {compareRows} = this.getSchema();
    const oldCmp = compareRows(change[ChangeIndex.OLD_NODE].row, bound);
    const newCmp = compareRows(change[ChangeIndex.NODE].row, bound);

    const replaceBoundAndForwardChange = () => {
      this.#setTakeState(
        takeStateKey,
        takeState.size,
        change[ChangeIndex.NODE].row,
      );
      return this.#output.push(change, this);
    };

    // The bounds row was changed.
    if (oldCmp === 0) {
      // The new row is the new bound.
      if (newCmp === 0) {
        // no need to update the state since we are keeping the bounds
        return this.#output.push(change, this);
      }

      if (newCmp < 0) {
        if (this.#limit === 1) {
          return replaceBoundAndForwardChange();
        }

        // New row will be in the result but it might not be the bounds any
        // more. We need to find the row before the bounds to determine the new
        // bounds.
        return this.#withFirst(
          {
            start: {
              row: bound,
              basis: 'after',
            },
            constraint,
            reverse: true,
          },
          1,
          false,
          ([beforeBoundNode]) => {
            assert(
              beforeBoundNode !== undefined,
              'Take: beforeBoundNode must be found during fetch',
            );

            this.#setTakeState(
              takeStateKey,
              takeState.size,
              beforeBoundNode.row,
            );
            return this.#output.push(change, this);
          },
        );
      }

      assert(newCmp > 0, 'New comparison must be greater than 0');
      // Find the first item at the old bounds. This will be the new bounds.
      return this.#withFirst(
        {
          start: {
            row: bound,
            basis: 'at',
          },
          constraint,
        },
        1,
        true,
        ([newBoundNode]) => {
          assert(
            newBoundNode !== undefined,
            'Take: newBoundNode must be found during fetch',
          );

          // The next row is the new row. We can replace the bounds and keep the
          // edit change.
          if (
            compareRows(newBoundNode.row, change[ChangeIndex.NODE].row) === 0
          ) {
            return replaceBoundAndForwardChange();
          }

          // The new row is now outside the bounds, so we need to remove the old
          // row and add the new bounds row.
          this.#setTakeState(takeStateKey, takeState.size, newBoundNode.row);
          return andThen(
            this.#pushWithRowHiddenFromFetch(
              newBoundNode.row,
              makeRemoveChange(change[ChangeIndex.OLD_NODE]),
            ),
            () => this.#output.push(makeAddChange(newBoundNode), this),
          );
        },
      );
    }

    if (oldCmp > 0) {
      assert(newCmp !== 0, 'Invalid state. Row has duplicate primary key');

      // Both old and new outside of bounds
      if (newCmp > 0) {
        return emptyArray;
      }

      // old was outside, new is inside. Pushing out the old bounds
      assert(newCmp < 0, 'New comparison must be less than 0');

      return this.#withFirst(
        {
          start: {
            row: bound,
            basis: 'at',
          },
          constraint,
          reverse: true,
        },
        2,
        false,
        ([oldBoundNode, newBoundNode]) => {
          assert(
            oldBoundNode !== undefined,
            'Take: oldBoundNode must be found during fetch',
          );
          assert(
            newBoundNode !== undefined,
            'Take: newBoundNode must be found during fetch',
          );

          // Remove before add to maintain invariant that
          // output size <= limit.
          this.#setTakeState(takeStateKey, takeState.size, newBoundNode.row);
          return andThen(
            this.#pushWithRowHiddenFromFetch(
              change[ChangeIndex.NODE].row,
              makeRemoveChange(oldBoundNode),
            ),
            () =>
              this.#output.push(makeAddChange(change[ChangeIndex.NODE]), this),
          );
        },
      );
    }

    if (oldCmp < 0) {
      assert(newCmp !== 0, 'Invalid state. Row has duplicate primary key');

      // Both old and new inside of bounds
      if (newCmp < 0) {
        return this.#output.push(change, this);
      }

      // old was inside, new is larger than old bound

      assert(newCmp > 0, 'New comparison must be greater than 0');

      // at this point we need to find the row after the bound and use that or
      // the newRow as the new bound.
      return this.#withFirst(
        {
          start: {
            row: bound,
            basis: 'after',
          },
          constraint,
        },
        1,
        true,
        ([afterBoundNode]) => {
          assert(
            afterBoundNode !== undefined,
            'Take: afterBoundNode must be found during fetch',
          );

          // The new row is the new bound. Use an edit change.
          if (
            compareRows(afterBoundNode.row, change[ChangeIndex.NODE].row) === 0
          ) {
            return replaceBoundAndForwardChange();
          }

          return andThen(
            this.#output.push(
              makeRemoveChange(change[ChangeIndex.OLD_NODE]),
              this,
            ),
            () => {
              this.#setTakeState(
                takeStateKey,
                takeState.size,
                afterBoundNode.row,
              );
              return this.#output.push(makeAddChange(afterBoundNode), this);
            },
          );
        },
      );
    }

    unreachable();
  }

  #pushWithRowHiddenFromFetch(row: Row, change: Change): Stream<'yield'> {
    this.#rowHiddenFromFetch = row;
    let s: Stream<'yield'>;
    try {
      s = this.#output.push(change, this);
    } catch (e) {
      this.#rowHiddenFromFetch = undefined;
      throw e;
    }
    return andFinally(s, this.#showHiddenRow);
  }

  readonly #showHiddenRow = (): void => {
    this.#rowHiddenFromFetch = undefined;
  };

  #setTakeState(takeStateKey: string, size: number, bound: Row | undefined) {
    this.#storage.set(takeStateKey, {
      size,
      bound,
    });
  }

  destroy(): void {
    this.#input.destroy();
    this.#storage.destroy();
  }

  reconcile(_pusher?: InputBase): Stream<'yield'> {
    if (this.#dirtyPartitions.size === 0) {
      return this.#output.reconcile ? this.#output.reconcile(this) : emptyArray;
    }
    const dirty = [...this.#dirtyPartitions.entries()];
    this.#dirtyPartitions.clear();
    let i = 0;
    return andThen(
      eachStream(() =>
        i === dirty.length ? undefined : this.#refill(dirty[i++]),
      ),
      () =>
        this.#output.reconcile ? this.#output.reconcile(this) : emptyArray,
    );
  }

  /** Refills a dirty partition with the rows that follow its bound. */
  #refill([takeStateKey, {constraint}]: [
    string,
    DirtyPartitionState,
  ]): Stream<'yield'> {
    const takeState = this.#storage.get(takeStateKey);
    assert(
      takeState !== undefined,
      'Take: dirty partition must exist in storage',
    );
    const deficit = this.#limit - takeState.size;
    assert(deficit > 0, 'Take: dirty partition must have a deficit');

    return this.#withFirst(
      {
        start: takeState.bound
          ? {
              row: takeState.bound,
              basis: 'after',
            }
          : undefined,
        constraint,
      },
      deficit,
      true,
      toPush => {
        let size = takeState.size;
        let j = 0;
        return eachStream(() => {
          if (j === toPush.length) {
            return undefined;
          }
          const node = toPush[j++];
          this.#setTakeState(takeStateKey, ++size, node.row);
          return this.#output.push(makeAddChange(node), this);
        });
      },
    );
  }
}

function getTakeStateKey(
  partitionKey: PartitionKey | undefined,
  rowOrConstraint: Row | Constraint | undefined,
): string {
  if (partitionKey?.length === 1 && rowOrConstraint) {
    return 'take:' + valueToKey(rowOrConstraint[partitionKey[0]]);
  }
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
