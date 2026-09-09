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
  type Operator,
  type Output,
  type Storage,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {
  emptyPullStream,
  LazyPullStream,
  type PullStream,
  PullStreamBase,
  type Stream,
} from './stream.ts';

const MAX_BOUND_KEY = 'maxBound';

type TakeState = {
  size: number;
  bound: Row | undefined;
};

interface TakeStorage {
  get(key: typeof MAX_BOUND_KEY): Row | undefined;
  get(key: string): TakeState | undefined;
  set(key: typeof MAX_BOUND_KEY, value: Row): void;
  set(key: string, value: TakeState): void;
  del(key: string): void;
}

export type PartitionKey = PrimaryKey;

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
export class Take implements Operator {
  readonly #input: Input;
  readonly #storage: TakeStorage;
  readonly #limit: number;
  readonly #partitionKey: PartitionKey | undefined;
  readonly #partitionKeyComparator: Comparator | undefined;
  // Fetch overlay needed for some split push cases.
  #rowHiddenFromFetch: Row | undefined;

  #output: Output = throwOutput;

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

  fetch(req: FetchRequest): PullStream<Node | 'yield'> {
    // Lazy: the generator read take state on first next(), and a push between
    // fetch() and iteration must still be visible.
    return new LazyPullStream(() => this.#startFetch(req));
  }

  #startFetch(req: FetchRequest): PullStream<Node | 'yield'> {
    const compareRows = this.getSchema().compareRows;
    if (
      !this.#partitionKey ||
      (req.constraint &&
        constraintMatchesPartitionKey(req.constraint, this.#partitionKey))
    ) {
      const takeStateKey = getTakeStateKey(this.#partitionKey, req.constraint);
      const takeState = this.#storage.get(takeStateKey);
      if (!takeState) {
        return this.#initialFetch(req, takeStateKey);
      }
      if (takeState.bound === undefined) {
        return emptyPullStream();
      }
      const bound = takeState.bound;
      const hidden = this.#rowHiddenFromFetch;
      return new TakeScanPull(this.#input.fetch(req), node =>
        compareRows(bound, node.row) < 0
          ? 'stop'
          : hidden && compareRows(hidden, node.row) === 0
            ? 'skip'
            : 'emit',
      );
    }
    const maxBound = this.#storage.get(MAX_BOUND_KEY);
    if (maxBound === undefined) {
      return emptyPullStream();
    }
    return new TakeScanPull(this.#input.fetch(req), node => {
      if (compareRows(node.row, maxBound) > 0) {
        return 'stop';
      }
      const takeState = this.#storage.get(
        getTakeStateKey(this.#partitionKey, node.row),
      );
      return takeState?.bound !== undefined &&
        compareRows(takeState.bound, node.row) >= 0
        ? 'emit'
        : 'skip';
    });
  }

  #initialFetch(
    req: FetchRequest,
    takeStateKey: string,
  ): PullStream<Node | 'yield'> {
    assert(req.start === undefined, 'Start should be undefined');
    assert(!req.reverse, 'Reverse should be false');
    if (this.#limit === 0) {
      return emptyPullStream();
    }
    assert(
      constraintMatchesPartitionKey(req.constraint, this.#partitionKey),
      'Constraint should match partition key',
    );
    assert(
      this.#storage.get(takeStateKey) === undefined,
      'Take state should be undefined',
    );
    return new TakeInitialPull(
      this.#input.fetch(req),
      this.#limit,
      (size, bound) =>
        this.#setTakeState(
          takeStateKey,
          size,
          bound,
          this.#storage.get(MAX_BOUND_KEY),
        ),
    );
  }

  #getStateAndConstraint(row: Row) {
    const takeStateKey = getTakeStateKey(this.#partitionKey, row);
    const takeState = this.#storage.get(takeStateKey);
    let maxBound: Row | undefined;
    let constraint: Constraint | undefined;
    if (takeState) {
      maxBound = this.#storage.get(MAX_BOUND_KEY);
      constraint =
        this.#partitionKey &&
        Object.fromEntries(
          this.#partitionKey.map(key => [key, row[key]] as const),
        );
    }

    return {takeState, takeStateKey, maxBound, constraint} as
      | {
          takeState: undefined;
          takeStateKey: string;
          maxBound: undefined;
          constraint: undefined;
        }
      | {
          takeState: TakeState;
          takeStateKey: string;
          maxBound: Row | undefined;
          constraint: Constraint | undefined;
        };
  }

  *push(change: Change): Stream<'yield'> {
    if (change[ChangeIndex.TYPE] === ChangeType.EDIT) {
      yield* this.#pushEditChange(change);
      return;
    }

    const {takeState, takeStateKey, maxBound, constraint} =
      this.#getStateAndConstraint(change[ChangeIndex.NODE].row);
    if (!takeState) {
      return;
    }

    const {compareRows} = this.getSchema();

    if (change[ChangeIndex.TYPE] === ChangeType.ADD) {
      if (takeState.size < this.#limit) {
        this.#setTakeState(
          takeStateKey,
          takeState.size + 1,
          takeState.bound === undefined ||
            compareRows(takeState.bound, change[ChangeIndex.NODE].row) < 0
            ? change[ChangeIndex.NODE].row
            : takeState.bound,
          maxBound,
        );
        yield* this.#output.push(change, this);
        return;
      }
      // size === limit
      if (
        takeState.bound === undefined ||
        compareRows(change[ChangeIndex.NODE].row, takeState.bound) >= 0
      ) {
        return;
      }
      // added row < bound
      let beforeBoundNode: Node | undefined;
      let boundNode: Node | undefined;
      if (this.#limit === 1) {
        {
          const __p246 = this.#input.fetch({
            start: {
              row: takeState.bound,
              basis: 'at',
            },
            constraint,
          });
          try {
            for (
              let node = __p246.next();
              node !== undefined;
              node = __p246.next()
            ) {
              if (node === 'yield') {
                yield node;
                continue;
              }
              boundNode = node;
              break;
            }
          } finally {
            __p246.close();
          }
        }
      } else {
        {
          const __p261 = this.#input.fetch({
            start: {
              row: takeState.bound,
              basis: 'at',
            },
            constraint,
            reverse: true,
          });
          try {
            for (
              let node = __p261.next();
              node !== undefined;
              node = __p261.next()
            ) {
              if (node === 'yield') {
                yield node;
                continue;
              } else if (boundNode === undefined) {
                boundNode = node;
              } else {
                beforeBoundNode = node;
                break;
              }
            }
          } finally {
            __p261.close();
          }
        }
      }
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
        maxBound,
      );
      yield* this.#pushWithRowHiddenFromFetch(
        change[ChangeIndex.NODE].row,
        removeChange,
      );
      yield* this.#output.push(change, this);
    } else if (change[ChangeIndex.TYPE] === ChangeType.REMOVE) {
      if (takeState.bound === undefined) {
        // change is after bound
        return;
      }
      const compToBound = compareRows(
        change[ChangeIndex.NODE].row,
        takeState.bound,
      );
      if (compToBound > 0) {
        // change is after bound
        return;
      }
      let beforeBoundNode: Node | undefined;
      {
        const __p315 = this.#input.fetch({
          start: {
            row: takeState.bound,
            basis: 'after',
          },
          constraint,
          reverse: true,
        });
        try {
          for (
            let node = __p315.next();
            node !== undefined;
            node = __p315.next()
          ) {
            if (node === 'yield') {
              yield node;
              continue;
            }
            beforeBoundNode = node;
            break;
          }
        } finally {
          __p315.close();
        }
      }

      let newBound: {node: Node; push: boolean} | undefined;
      if (beforeBoundNode) {
        const push = compareRows(beforeBoundNode.row, takeState.bound) > 0;
        newBound = {
          node: beforeBoundNode,
          push,
        };
      }
      if (!newBound?.push) {
        {
          const __p340 = this.#input.fetch({
            start: {
              row: takeState.bound,
              basis: 'at',
            },
            constraint,
          });
          try {
            for (
              let node = __p340.next();
              node !== undefined;
              node = __p340.next()
            ) {
              if (node === 'yield') {
                yield node;
                continue;
              }
              const push = compareRows(node.row, takeState.bound) > 0;
              newBound = {
                node,
                push,
              };
              if (push) {
                break;
              }
            }
          } finally {
            __p340.close();
          }
        }
      }

      if (newBound?.push) {
        yield* this.#output.push(change, this);
        this.#setTakeState(
          takeStateKey,
          takeState.size,
          newBound.node.row,
          maxBound,
        );
        yield* this.#output.push(makeAddChange(newBound.node), this);
        return;
      }
      this.#setTakeState(
        takeStateKey,
        takeState.size - 1,
        newBound?.node.row,
        maxBound,
      );
      yield* this.#output.push(change, this);
    } else if (change[ChangeIndex.TYPE] === ChangeType.CHILD) {
      // A 'child' change should be pushed to output if its row
      // is <= bound.
      if (
        takeState.bound &&
        compareRows(change[ChangeIndex.NODE].row, takeState.bound) <= 0
      ) {
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

    const {takeState, takeStateKey, maxBound, constraint} =
      this.#getStateAndConstraint(change[ChangeIndex.OLD_NODE].row);
    if (!takeState) {
      return;
    }

    assert(takeState.bound, 'Bound should be set');
    const {compareRows} = this.getSchema();
    const oldCmp = compareRows(
      change[ChangeIndex.OLD_NODE].row,
      takeState.bound,
    );
    const newCmp = compareRows(change[ChangeIndex.NODE].row, takeState.bound);

    const that = this;
    const replaceBoundAndForwardChange = function* () {
      that.#setTakeState(
        takeStateKey,
        takeState.size,
        change[ChangeIndex.NODE].row,
        maxBound,
      );
      yield* that.#output.push(change, that);
    };

    // The bounds row was changed.
    if (oldCmp === 0) {
      // The new row is the new bound.
      if (newCmp === 0) {
        // no need to update the state since we are keeping the bounds
        yield* this.#output.push(change, this);
        return;
      }

      if (newCmp < 0) {
        if (this.#limit === 1) {
          yield* replaceBoundAndForwardChange();
          return;
        }

        // New row will be in the result but it might not be the bounds any
        // more. We need to find the row before the bounds to determine the new
        // bounds.

        let beforeBoundNode: Node | undefined;
        {
          const __p447 = this.#input.fetch({
            start: {
              row: takeState.bound,
              basis: 'after',
            },
            constraint,
            reverse: true,
          });
          try {
            for (
              let node = __p447.next();
              node !== undefined;
              node = __p447.next()
            ) {
              if (node === 'yield') {
                yield node;
                continue;
              }
              beforeBoundNode = node;
              break;
            }
          } finally {
            __p447.close();
          }
        }
        assert(
          beforeBoundNode !== undefined,
          'Take: beforeBoundNode must be found during fetch',
        );

        this.#setTakeState(
          takeStateKey,
          takeState.size,
          beforeBoundNode.row,
          maxBound,
        );
        yield* this.#output.push(change, this);
        return;
      }

      assert(newCmp > 0, 'New comparison must be greater than 0');
      // Find the first item at the old bounds. This will be the new bounds.
      let newBoundNode: Node | undefined;
      {
        const __p480 = this.#input.fetch({
          start: {
            row: takeState.bound,
            basis: 'at',
          },
          constraint,
        });
        try {
          for (
            let node = __p480.next();
            node !== undefined;
            node = __p480.next()
          ) {
            if (node === 'yield') {
              yield node;
              continue;
            }
            newBoundNode = node;
            break;
          }
        } finally {
          __p480.close();
        }
      }
      assert(
        newBoundNode !== undefined,
        'Take: newBoundNode must be found during fetch',
      );

      // The next row is the new row. We can replace the bounds and keep the
      // edit change.
      if (compareRows(newBoundNode.row, change[ChangeIndex.NODE].row) === 0) {
        yield* replaceBoundAndForwardChange();
        return;
      }

      // The new row is now outside the bounds, so we need to remove the old
      // row and add the new bounds row.
      this.#setTakeState(
        takeStateKey,
        takeState.size,
        newBoundNode.row,
        maxBound,
      );
      yield* this.#pushWithRowHiddenFromFetch(
        newBoundNode.row,
        makeRemoveChange(change[ChangeIndex.OLD_NODE]),
      );
      yield* this.#output.push(makeAddChange(newBoundNode), this);
      return;
    }

    if (oldCmp > 0) {
      assert(newCmp !== 0, 'Invalid state. Row has duplicate primary key');

      // Both old and new outside of bounds
      if (newCmp > 0) {
        return;
      }

      // old was outside, new is inside. Pushing out the old bounds
      assert(newCmp < 0, 'New comparison must be less than 0');

      let oldBoundNode: Node | undefined;
      let newBoundNode: Node | undefined;
      {
        const __p535 = this.#input.fetch({
          start: {
            row: takeState.bound,
            basis: 'at',
          },
          constraint,
          reverse: true,
        });
        try {
          for (
            let node = __p535.next();
            node !== undefined;
            node = __p535.next()
          ) {
            if (node === 'yield') {
              yield node;
              continue;
            } else if (oldBoundNode === undefined) {
              oldBoundNode = node;
            } else {
              newBoundNode = node;
              break;
            }
          }
        } finally {
          __p535.close();
        }
      }
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
      this.#setTakeState(
        takeStateKey,
        takeState.size,
        newBoundNode.row,
        maxBound,
      );
      yield* this.#pushWithRowHiddenFromFetch(
        change[ChangeIndex.NODE].row,
        makeRemoveChange(oldBoundNode),
      );
      yield* this.#output.push(makeAddChange(change[ChangeIndex.NODE]), this);

      return;
    }

    if (oldCmp < 0) {
      assert(newCmp !== 0, 'Invalid state. Row has duplicate primary key');

      // Both old and new inside of bounds
      if (newCmp < 0) {
        yield* this.#output.push(change, this);
        return;
      }

      // old was inside, new is larger than old bound

      assert(newCmp > 0, 'New comparison must be greater than 0');

      // at this point we need to find the row after the bound and use that or
      // the newRow as the new bound.
      let afterBoundNode: Node | undefined;
      {
        const __p595 = this.#input.fetch({
          start: {
            row: takeState.bound,
            basis: 'after',
          },
          constraint,
        });
        try {
          for (
            let node = __p595.next();
            node !== undefined;
            node = __p595.next()
          ) {
            if (node === 'yield') {
              yield node;
              continue;
            }
            afterBoundNode = node;
            break;
          }
        } finally {
          __p595.close();
        }
      }
      assert(
        afterBoundNode !== undefined,
        'Take: afterBoundNode must be found during fetch',
      );

      // The new row is the new bound. Use an edit change.
      if (compareRows(afterBoundNode.row, change[ChangeIndex.NODE].row) === 0) {
        yield* replaceBoundAndForwardChange();
        return;
      }

      yield* this.#output.push(
        makeRemoveChange(change[ChangeIndex.OLD_NODE]),
        this,
      );
      this.#setTakeState(
        takeStateKey,
        takeState.size,
        afterBoundNode.row,
        maxBound,
      );
      yield* this.#output.push(makeAddChange(afterBoundNode), this);
      return;
    }

    unreachable();
  }

  *#pushWithRowHiddenFromFetch(row: Row, change: Change) {
    this.#rowHiddenFromFetch = row;
    try {
      yield* this.#output.push(change, this);
    } finally {
      this.#rowHiddenFromFetch = undefined;
    }
  }

  #setTakeState(
    takeStateKey: string,
    size: number,
    bound: Row | undefined,
    maxBound: Row | undefined,
  ) {
    this.#storage.set(takeStateKey, {
      size,
      bound,
    });
    if (
      bound !== undefined &&
      (maxBound === undefined ||
        this.getSchema().compareRows(bound, maxBound) > 0)
    ) {
      this.#storage.set(MAX_BOUND_KEY, bound);
    }
  }

  destroy(): void {
    this.#input.destroy();
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

/**
 * A Take scan in the pull protocol: forwards 'yield', and asks `decide` per
 * node whether to emit it, skip it, or stop (closing the input).
 */
class TakeScanPull extends PullStreamBase<Node | 'yield'> {
  readonly #input: PullStream<Node | 'yield'>;
  readonly #decide: (node: Node) => 'emit' | 'skip' | 'stop';
  #done = false;

  constructor(
    input: PullStream<Node | 'yield'>,
    decide: (node: Node) => 'emit' | 'skip' | 'stop',
  ) {
    super();
    this.#input = input;
    this.#decide = decide;
  }

  next(): Node | 'yield' | undefined {
    if (this.#done) {
      return undefined;
    }
    for (;;) {
      const v = this.#input.next();
      if (v === undefined) {
        this.#done = true;
        return undefined;
      }
      if (v === 'yield') {
        return v;
      }
      const d = this.#decide(v);
      if (d === 'emit') {
        return v;
      }
      if (d === 'stop') {
        this.close();
        return undefined;
      }
    }
  }

  close(): void {
    if (!this.#done) {
      this.#done = true;
      this.#input.close();
    }
  }
}

/**
 * Take's initial fetch in the pull protocol. Emits up to `limit` nodes and
 * records the take state once the scan completes -- which, as with the
 * generator, is when the consumer asks for the node after the last one. A
 * consumer that closes early still gets the state recorded and then the same
 * assertion the generator raised from its finally block: initial hydration
 * must run to completion.
 */
class TakeInitialPull extends PullStreamBase<Node | 'yield'> {
  readonly #input: PullStream<Node | 'yield'>;
  readonly #limit: number;
  readonly #finish: (size: number, bound: Row | undefined) => void;
  #size = 0;
  #bound: Row | undefined;
  #done = false;

  constructor(
    input: PullStream<Node | 'yield'>,
    limit: number,
    finish: (size: number, bound: Row | undefined) => void,
  ) {
    super();
    this.#input = input;
    this.#limit = limit;
    this.#finish = finish;
  }

  next(): Node | 'yield' | undefined {
    if (this.#done) {
      return undefined;
    }
    if (this.#size === this.#limit) {
      this.#complete();
      return undefined;
    }
    let v: Node | 'yield' | undefined;
    try {
      v = this.#input.next();
    } catch (e) {
      // As the generator did: an exception records no state.
      this.#done = true;
      throw e;
    }
    if (v === undefined) {
      this.#complete();
      return undefined;
    }
    if (v === 'yield') {
      return v;
    }
    this.#bound = v.row;
    this.#size++;
    return v;
  }

  #complete(): void {
    this.#done = true;
    this.#input.close();
    this.#finish(this.#size, this.#bound);
  }

  close(): void {
    if (!this.#done) {
      this.#complete();
      assert(false, 'Unexpected early return prevented full hydration');
    }
  }
}
