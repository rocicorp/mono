import {assert} from 'shared/src/asserts.js';
import type {Node, Row} from './data.js';
import type {
  FetchRequest,
  HydrateRequest,
  Input,
  Operator,
  Output,
  Schema,
  Storage,
} from './operator.js';
import type {Stream} from './stream.js';
import type {Change} from './change.js';

const MAX_BOUND_KEY = ['maxBound'] as const;

type TakeState = {
  size: number;
  bound: Row;
};

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
  readonly #storage: Storage;
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
    this.#storage = storage;
    this.#limit = limit;
    this.#partitionKey = partitionKey;
    assert(limit >= 0);
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  get schema(): Schema {
    return this.schema;
  }

  *hydrate(req: HydrateRequest, _: Output): Stream<Node> {
    assert(
      this.#partitionKey === undefined ||
        (req.constraint !== undefined &&
          req.constraint.key === this.#partitionKey),
    );

    const partitionValue =
      this.#partitionKey === undefined ? undefined : req.constraint?.value;
    const takeStateKey = ['take', partitionValue];
    assert(this.#storage.get(takeStateKey) === undefined);
    if (this.#limit === 0) {
      this.#storage.set(takeStateKey, {count: 0});
      return;
    }
    let size = 0;
    let bound: Row | undefined;
    let downstreamEarlyReturn = true;
    try {
      for (const inputNode of this.#input.hydrate(req, this)) {
        yield inputNode;
        bound = inputNode.row;
        if (size++ === this.#limit) {
          break;
        }
      }
      downstreamEarlyReturn = false;
    } finally {
      this.#storage.set(takeStateKey, {
        size,
        bound,
      });
      const maxBound = this.#storage.get(MAX_BOUND_KEY) as Row;
      if (bound && this.schema.compareRows(bound, maxBound) > 0) {
        this.#storage.get(MAX_BOUND_KEY, bound);
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

  *fetch(req: FetchRequest, _: Output): Stream<Node> {
    if (
      this.#partitionKey === undefined ||
      req.constraint?.key === this.#partitionKey
    ) {
      const partitionValue =
        this.#partitionKey === undefined ? undefined : req.constraint?.value;
      const takeStateKey = ['take', partitionValue];
      const takeState = this.#storage.get(takeStateKey) as
        | TakeState
        | undefined;
      assert(takeState !== undefined);
      if (takeState.bound === undefined) {
        return;
      }
      for (const inputNode of this.#input.fetch(req, this)) {
        if (this.schema.compareRows(takeState.bound, inputNode.row) < 1) {
          return;
        }
        yield inputNode;
      }
      return;
    }
    const maxBound = this.#storage.get(MAX_BOUND_KEY) as Row;
    if (maxBound === undefined) {
      return;
    }
    // There is a partition key, but the fetch is not constrained on it.
    for (const inputNode of this.#input.fetch(req, this)) {
      if (this.schema.compareRows(inputNode.row, maxBound) > 0) {
        return;
      }
      const partitionValue = inputNode.row[this.#partitionKey];
      const takeStateKey = ['take', partitionValue];
      const takeState = this.#storage.get(takeStateKey) as
        | TakeState
        | undefined;
      if (
        takeState &&
        this.schema.compareRows(takeState.bound, inputNode.row) >= 0
      ) {
        yield inputNode;
      }
    }
  }

  *dehydrate(req: HydrateRequest, _: Output): Stream<Node> {
    assert(
      this.#partitionKey === undefined ||
        (req.constraint !== undefined &&
          req.constraint.key === this.#partitionKey),
    );

    const partitionValue =
      this.#partitionKey === undefined ? undefined : req.constraint?.value;
    const takeStateKey = ['take', partitionValue];
    const takeState = this.#storage.get(takeStateKey) as TakeState | undefined;
    this.#storage.del(takeStateKey);
    assert(takeState !== undefined);
    for (const inputNode of this.#input.dehydrate(req, this)) {
      if (
        takeState.bound === undefined ||
        this.schema.compareRows(takeState.bound, inputNode.row) < 1
      ) {
        return;
      }
      yield inputNode;
    }
  }

  push(change: Change, input: Input): void {
    assert(this.#output, 'Output not set');
    assert(input === this.#input, 'Wrong input');
    // When take below join is supported, this assert should be removed
    // and a 'child' change should be pushed to output if its row
    // is <= bound.
    assert(change.type !== 'child', 'child changes are not supported');
    if (this.#limit === 0) {
      return;
    }
    const partitionValue =
      this.#partitionKey === undefined
        ? undefined
        : change.node.row[this.#partitionKey];
    const takeStateKey = ['take', partitionValue];
    const takeState = this.#storage.get(takeStateKey) as TakeState | undefined;
    // I think this is right, if this partition key wasn't hydrated
    // we can ignore changes for it.
    if (!takeState) {
      return;
    }

    if (change.type === 'add') {
      if (takeState.size < this.#limit) {
        this.#storage.set(takeStateKey, {
          size: takeState.size + 1,
          bound:
            takeState.bound === undefined ||
            this.schema.compareRows(takeState.bound, change.node.row) < 0
              ? change.node.row
              : takeState.bound,
        });
        this.#output.push(change, this);
        return;
      }
      // size === limit
      if (
        takeState.bound === undefined ||
        this.schema.compareRows(change.node.row, takeState.bound) >= 0
      ) {
        return;
      }
      // added row < bound
      // TODO: We really just need the row not the whole node for the
      // beforeBound, but fetch doesn't let us retrieve this
      let beforeBoundNode: Node | undefined;
      let boundNode: Node;
      if (this.#limit === 1) {
        [boundNode] = take(
          this.#input.fetch(
            {
              start: {
                row: takeState.bound,
                basis: 'at',
              },
            },
            this,
          ),
          1,
        );
      } else {
        [beforeBoundNode, boundNode] = take(
          this.#input.fetch(
            {
              start: {
                row: takeState.bound,
                basis: 'before',
              },
            },
            this,
          ),
          2,
        );
      }
      const removeChange: Change = {
        type: 'remove',
        node: boundNode,
      };
      this.#storage.set(takeStateKey, {
        size: takeState.size,
        bound:
          beforeBoundNode === undefined ||
          this.schema.compareRows(change.node.row, beforeBoundNode.row) > 0
            ? change.node.row
            : beforeBoundNode.row,
      });
      // TODO: is the order of the remove/add here important?
      this.#output.push(removeChange, this);
      this.#output.push(change, this);
    } else if (change.type === 'remove') {
      if (takeState.bound === undefined) {
        // change is after bound
        return;
      }
      const compToBound = this.schema.compareRows(
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
        this.#output.push(change, this);
        return;
      }
      const [beforeBoundNode, boundNode, afterBoundNode] = take(
        this.#input.fetch(
          {
            start: {
              row: takeState.bound,
              basis: 'before',
            },
          },
          this,
        ),
        3,
      );
      if (afterBoundNode) {
        this.#storage.set(takeStateKey, {
          size: takeState.size,
          bound: afterBoundNode.row,
        });
        this.#output.push(change, this);
        this.#output.push(
          {
            type: 'add',
            node: afterBoundNode,
          },
          this,
        );
        return;
      }
      this.#storage.set(takeStateKey, {
        size: takeState.size - 1,
        bound: compToBound === 0 ? beforeBoundNode.row : boundNode.row,
      });
      this.#output.push(change, this);
    }
  }
}

function take<T>(stream: Stream<T>, limit: number): T[] {
  const result: T[] = [];
  if (limit < 1) {
    return result;
  }
  for (const v of stream) {
    result.push(v);
    if (result.length === limit) {
      break;
    }
  }
  return result;
}
