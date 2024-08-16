import BTree from 'btree';
import type {
  Output,
  FetchRequest,
  HydrateRequest,
  Constraint,
} from './operator.js';
import {makeComparator, valuesEqual, type Node, type Row} from './data.js';
import type {Ordering} from '../ast2/ast.js';
import {assert} from 'shared/src/asserts.js';
import {LookaheadIterator} from './lookahead-iterator.js';
import type {Stream} from './stream.js';
import {Source, SourceChange} from './source.js';
import {Schema} from './schema.js';

export type Overlay = {
  outputIndex: number;
  change: SourceChange;
};

/**
 * A `MemorySource` is a source that provides data to the pipeline from an
 * in-memory data source.
 *
 * This data is kept in sorted order as downstream pipelines will always expect
 * the data they receive from `pull` to be in sorted order.
 */
export class MemorySource implements Source {
  readonly #schema: Schema;
  readonly #data: BTree<Row, undefined>;
  readonly #outputs: Output[] = [];

  #overlay: Overlay | undefined;

  constructor(order: Ordering) {
    this.#schema = {
      compareRows: makeComparator(order),
      columns: {},
      primaryKey: [],
    };
    this.#data = new BTree(undefined, this.#schema.compareRows);
  }

  get schema(): Schema {
    return this.#schema;
  }

  addOutput(output: Output): void {
    this.#outputs.push(output);
  }

  hydrate(req: HydrateRequest, output: Output) {
    return this.fetch(req, output);
  }

  *fetch(req: FetchRequest, output: Output): Stream<Node> {
    let overlay: Overlay | undefined;

    // When we receive a push, we send it to each output one at a time. Once the
    // push is sent to an output, it should keep being sent until all datastores
    // have received it and the change has been made to the datastore.
    if (this.#overlay) {
      const callingOutputIndex = this.#outputs.indexOf(output);
      assert(callingOutputIndex !== -1, 'Output not found');
      if (callingOutputIndex <= this.#overlay.outputIndex) {
        overlay = this.#overlay;
      }
    }

    // If there is an overlay for this output, does it match the requested
    // constraints?
    if (overlay) {
      if (req.constraint) {
        const {key, value} = req.constraint;
        if (!valuesEqual(overlay.change.row[key], value)) {
          overlay = undefined;
        }
      }
    }

    const startAt = req.start?.row
      ? this.#data.nextLowerKey(req.start.row)
      : undefined;
    yield* generateWithStart(
      generateWithOverlay(
        startAt,
        this.#pullWithConstraint(startAt, req.constraint),
        req.constraint,
        overlay,
        this.#schema.compareRows,
      ),
      req,
      this.#schema.compareRows,
    );
  }

  dehydrate(req: HydrateRequest, output: Output): Stream<Node> {
    return this.fetch(req, output);
  }

  *#pullWithConstraint(
    startAt: Row | undefined,
    constraint: Constraint | undefined,
  ): IterableIterator<Row> {
    const it = this.#data.keys(startAt);

    // Process all items in the iterator, applying overlay as needed.
    for (const row of it) {
      if (!constraint || valuesEqual(row[constraint.key], constraint.value)) {
        yield row;
      }
    }
  }

  push(change: SourceChange) {
    if (change.type === 'add') {
      if (this.#data.has(change.row)) {
        throw new Error('Row already exists');
      }
    } else {
      change.type satisfies 'remove'; // ensures exuaustiveness of `if/else`
      if (!this.#data.has(change.row)) {
        throw new Error('Row not found');
      }
    }

    for (const [outputIndex, output] of this.#outputs.entries()) {
      this.#overlay = {outputIndex, change};
      output.push(
        {
          type: change.type,
          node: {
            row: change.row,
            relationships: {},
          },
        },
        this,
      );
    }
    this.#overlay = undefined;
    if (change.type === 'add') {
      const added = this.#data.add(change.row, undefined);
      // must suceed since we checked has() above.
      assert(added);
    } else {
      assert(change.type === 'remove');
      const removed = this.#data.delete(change.row);
      // must suceed since we checked has() above.
      assert(removed);
    }
  }
}

/**
 * If the request basis was `before` then the overlay might be the starting point of the stream.
 *
 * This can happen in a case like the following:
 * Store = [1,2,3, 5,6,7]
 * Overlay = [4]
 * Request = fetch starting before 5
 *
 * In this case, the overlay value of `4` should be the starting point of the stream, not `3`.
 */
export function* generateWithStart(
  it: Iterator<Node>,
  req: FetchRequest,
  compare: (r1: Row, r2: Row) => number,
): Stream<Node> {
  // Figure out the start row.
  const cursor = new LookaheadIterator(it, 2);

  let started = req.start === undefined ? true : false;
  for (const [curr, next] of cursor) {
    if (!started) {
      assert(req.start);
      if (req.start.basis === 'before') {
        if (next === undefined || compare(next.row, req.start.row) >= 0) {
          started = true;
        }
      } else if (req.start.basis === 'at') {
        if (compare(curr.row, req.start.row) >= 0) {
          started = true;
        }
      } else if (req.start.basis === 'after') {
        if (compare(curr.row, req.start.row) > 0) {
          started = true;
        }
      }
    }
    if (started) {
      yield curr;
    }
  }
}

/**
 * Takes an iterator and overlay.
 * Splices the overlay into the iterator at the correct position.
 *
 * @param startAt - if there is a lower bound to the stream. If the lower bound of the stream
 * is above the overlay, the overlay will be skipped.
 * @param rowIterator - the stream into which the overlay should be spliced
 * @param constraint - constraint that was applied to the rowIterator and should
 * also be applied to the overlay.
 * @param overlay - the overlay values to splice in
 * @param compare - the comparator to use to find the position for the overlay
 */
export function* generateWithOverlay(
  startAt: Row | undefined,
  rowIterator: IterableIterator<Row>,
  constraint: Constraint | undefined,
  overlay: Overlay | undefined,
  compare: (r1: Row, r2: Row) => number,
) {
  if (startAt && overlay && compare(overlay.change.row, startAt) < 0) {
    overlay = undefined;
  }

  if (overlay) {
    if (constraint) {
      const {key, value} = constraint;
      const {change} = overlay;
      if (!valuesEqual(change.row[key], value)) {
        overlay = undefined;
      }
    }
  }

  for (const row of rowIterator) {
    if (overlay) {
      const cmp = compare(overlay.change.row, row);
      if (overlay.change.type === 'add') {
        if (cmp < 0) {
          yield {row: overlay.change.row, relationships: {}};
          overlay = undefined;
        }
      } else if (overlay.change.type === 'remove') {
        if (cmp < 0) {
          overlay = undefined;
        } else if (cmp === 0) {
          overlay = undefined;
          continue;
        }
      }
    }
    yield {row, relationships: {}};
  }

  if (overlay && overlay.change.type === 'add') {
    yield {row: overlay.change.row, relationships: {}};
  }
}
