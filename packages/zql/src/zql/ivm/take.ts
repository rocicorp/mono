import {assert} from 'shared/src/asserts.js';
import type {Change, RemoveChange} from './change.js';
import {normalizeUndefined, type Node, type Row, type Value} from './data.js';
import type {
  Constraint,
  FetchRequest,
  Input,
  Operator,
  Output,
  Storage,
} from './operator.js';
import type {Schema} from './schema.js';
import {take, type Stream} from './stream.js';
import { assertOrderingIncludesPK } from '../builder/builder.js';

const MAX_BOUND_KEY = 'maxBound';

type TakeState = {
  size: number;
  bound: Row | undefined;
};

interface TakeStorage extends Storage {
  get(key: typeof MAX_BOUND_KEY): Row | undefined;
  get(key: string): TakeState | undefined;
  set(key: typeof MAX_BOUND_KEY, value: Row): void;
  set(key: string, value: TakeState): void;
}

/**
 * The Take operator is for implementing limit queries. It takes the first n
 * nodes of its input as determined by the input’s comparator. It then keeps
 * a *bound* of the last item it has accepted so that it can evaluate whether
 * new incoming pushes should be accepted or rejected.
 *
 * Take can count rows globally or by unique value of some field.
 */
export class Take implements Operator {
  readonly #input: Input;
  readonly #storage: TakeStorage;
  readonly #limit: number;
  readonly #partitionKey: string | undefined;

  #output: Output | null = null;

  constructor(
    input: Input,
    storage: Storage,
    limit: number,
    partitionKey?: string | undefined,
  ) {
    this.#input = input;
    this.#storage = storage as TakeStorage;
    this.#limit = limit;
    this.#partitionKey = partitionKey;
    assert(limit >= 0);
    assertOrderingIncludesPK(this.#input.getSchema().sort, this.#input.getSchema().primaryKey);
    this.#input.setOutput(this);
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): Schema {
    return this.#input.getSchema();
  }

  *fetch(req: FetchRequest): Stream<Node> {
    if (
      this.#partitionKey === undefined ||
      req.constraint?.key === this.#partitionKey
    ) {
      const partitionValue =
        this.#partitionKey === undefined ? undefined : req.constraint?.value;
      const takeStateKey = getTakeStateKey(partitionValue);
      const takeState = this.#storage.get(takeStateKey);
      if (takeState === undefined) {
        yield* this.#initialFetch(req);
        return;
      }
      if (takeState.bound === undefined) {
        return;
      }
      for (const inputNode of this.#input.fetch(req)) {
        if (this.getSchema().compareRows(takeState.bound, inputNode.row) < 0) {
          return;
        }
        yield inputNode;
      }
      return;
    }
    // There is a partition key, but the fetch is not constrained or constrained
    // on a different key.  Thus we don't have a single take state to bound by.
    // This currently only happens with nested sub-queries
    // e.g. issues include issuelabels include label.  We could remove this
    // case if we added a translation layer (powered by some state) in join.
    // Specifically we need joinKeyValue => parent constraint key
    const maxBound = this.#storage.get(MAX_BOUND_KEY);
    if (maxBound === undefined) {
      return;
    }
    for (const inputNode of this.#input.fetch(req)) {
      if (this.getSchema().compareRows(inputNode.row, maxBound) > 0) {
        return;
      }
      const partitionValue = inputNode.row[this.#partitionKey];
      const takeStateKey = getTakeStateKey(partitionValue);
      const takeState = this.#storage.get(takeStateKey);
      if (
        takeState &&
        takeState.bound !== undefined &&
        this.getSchema().compareRows(takeState.bound, inputNode.row) >= 0
      ) {
        yield inputNode;
      }
    }
  }

  *#initialFetch(req: FetchRequest): Stream<Node> {
    assert(req.start === undefined);
    assert(
      this.#partitionKey === undefined ||
        (req.constraint !== undefined &&
          req.constraint.key === this.#partitionKey),
    );

    if (this.#limit === 0) {
      return;
    }

    const partitionValue =
      this.#partitionKey === undefined ? undefined : req.constraint?.value;
    const takeStateKey = getTakeStateKey(partitionValue);
    assert(this.#storage.get(takeStateKey) === undefined);

    let size = 0;
    let bound: Row | undefined;
    let downstreamEarlyReturn = true;
    try {
      for (const inputNode of this.#input.fetch(req)) {
        yield inputNode;
        bound = inputNode.row;
        size++;
        if (size === this.#limit) {
          break;
        }
      }
      downstreamEarlyReturn = false;
    } finally {
      this.#setTakeState(
        takeStateKey,
        size,
        bound,
        this.#storage.get(MAX_BOUND_KEY),
      );
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

  *cleanup(req: FetchRequest): Stream<Node> {
    assert(req.start === undefined);
    assert(
      this.#partitionKey === undefined ||
        (req.constraint !== undefined &&
          req.constraint.key === this.#partitionKey),
    );

    let takeState: TakeState | undefined;
    if (this.#limit > 0) {
      const partitionValue =
        this.#partitionKey === undefined ? undefined : req.constraint?.value;
      const takeStateKey = getTakeStateKey(partitionValue);
      takeState = this.#storage.get(takeStateKey);
      assert(takeState !== undefined);
      this.#storage.del(takeStateKey);
    }
    for (const inputNode of this.#input.cleanup(req)) {
      if (
        takeState?.bound === undefined ||
        this.getSchema().compareRows(takeState.bound, inputNode.row) < 0
      ) {
        return;
      }
      yield inputNode;
    }
  }

  push(change: Change): void {
    assert(this.#output, 'Output not set');
    // When take below join is supported, this assert should be removed
    // and a 'child' change should be pushed to output if its row
    // is <= bound.
    assert(change.type !== 'child', 'child changes are not supported');
    const partitionValue =
      this.#partitionKey === undefined
        ? undefined
        : change.node.row[this.#partitionKey];
    const takeStateKey = getTakeStateKey(partitionValue);
    const takeState = this.#storage.get(takeStateKey);
    const maxBound = this.#storage.get(MAX_BOUND_KEY);
    const constraint: Constraint | undefined = this.#partitionKey
      ? {
          key: this.#partitionKey,
          value: change.node.row[this.#partitionKey],
        }
      : undefined;

    // The partition key was never fetched, so this push can be discarded.
    if (!takeState) {
      return;
    }

    if (change.type === 'add') {
      if (takeState.size < this.#limit) {
        this.#setTakeState(
          takeStateKey,
          takeState.size + 1,
          takeState.bound === undefined ||
            this.getSchema().compareRows(takeState.bound, change.node.row) < 0
            ? change.node.row
            : takeState.bound,
          maxBound,
        );
        this.#output.push(change);
        return;
      }
      // size === limit
      if (
        takeState.bound === undefined ||
        this.getSchema().compareRows(change.node.row, takeState.bound) >= 0
      ) {
        return;
      }
      // added row < bound
      let beforeBoundNode: Node | undefined;
      let boundNode: Node;
      if (this.#limit === 1) {
        [boundNode] = [
          ...take(
            this.#input.fetch({
              start: {
                row: takeState.bound,
                basis: 'at',
              },
              constraint,
            }),
            1,
          ),
        ];
      } else {
        [beforeBoundNode, boundNode] = [
          ...take(
            this.#input.fetch({
              start: {
                row: takeState.bound,
                basis: 'before',
              },
              constraint,
            }),
            2,
          ),
        ];
      }
      const removeChange: RemoveChange = {
        type: 'remove',
        node: boundNode,
      };
      this.#setTakeState(
        takeStateKey,
        takeState.size,
        beforeBoundNode === undefined ||
          this.getSchema().compareRows(change.node.row, beforeBoundNode.row) > 0
          ? change.node.row
          : beforeBoundNode.row,
        maxBound,
      );
      this.#output.push(removeChange);
      this.#output.push(change);
    } else if (change.type === 'remove') {
      if (takeState.bound === undefined) {
        // change is after bound
        return;
      }
      const compToBound = this.getSchema().compareRows(
        change.node.row,
        takeState.bound,
      );
      if (compToBound > 0) {
        // change is after bound
        return;
      }
      if (this.#limit === 1) {
        this.#storage.set(takeStateKey, {
          size: 0,
          bound: undefined,
        });
        this.#output.push(change);
        return;
      }
      // The bound is removed
      let beforeBoundNode: Node | undefined;
      let afterBoundNode: Node | undefined;
      if (compToBound === 0) {
        [beforeBoundNode, afterBoundNode] = take(
          this.#input.fetch({
            start: {
              row: takeState.bound,
              basis: 'before',
            },
            constraint,
          }),
          2,
        );
      } else {
        [beforeBoundNode, , afterBoundNode] = take(
          this.#input.fetch({
            start: {
              row: takeState.bound,
              basis: 'before',
            },
            constraint,
          }),
          3,
        );
      }

      if (afterBoundNode) {
        this.#setTakeState(
          takeStateKey,
          takeState.size,
          afterBoundNode.row,
          maxBound,
        );
        this.#output.push(change);
        this.#output.push({
          type: 'add',
          node: afterBoundNode,
        });
        return;
      }
      this.#setTakeState(
        takeStateKey,
        takeState.size - 1,
        compToBound === 0 ? beforeBoundNode.row : takeState.bound,
        maxBound,
      );
      this.#output.push(change);
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

function getTakeStateKey(partitionValue: Value): string {
  return JSON.stringify(['take', normalizeUndefined(partitionValue)]);
}
