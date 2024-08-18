import BTree from 'btree';
import type {
  Input,
  Output,
  FetchRequest,
  HydrateRequest,
  Schema,
  Constraint,
} from './operator.js';
import {makeComparator, valuesEqual, type Node, type Row} from './data.js';
import type {Ordering} from '../ast2/ast.js';
import {assert} from 'shared/src/asserts.js';
import {LookaheadIterator} from './lookahead-iterator.js';
import type {Stream} from './stream.js';

export type SourceChange = {
  type: 'add' | 'remove';
  row: Row;
};

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
export class MemorySource implements Input {
  readonly #schema: Schema;
  readonly #data: BTree<Row, undefined>;
  readonly #outputs: Output[] = [];

  #overlay: Overlay | null = null;

  constructor(order: Ordering) {
    this.#schema = {
      compareRows: makeComparator(order),
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
    let overlay: Overlay | null = null;

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
          overlay = null;
        }
      }
    }

    const it = this.#pullWithOverlay(
      req.start?.row ? this.#data.nextLowerKey(req.start.row) : undefined,
      req.constraint,
      overlay ?? undefined,
    );

    // Figure out the start row.
    const cursor = new LookaheadIterator(it[Symbol.iterator](), 2);

    let started = req.start === undefined ? true : false;
    for (const [curr, next] of cursor) {
      if (!started) {
        assert(req.start);
        if (req.start.basis === 'before') {
          if (
            next === undefined ||
            this.#schema.compareRows(next.row, req.start.row) >= 0
          ) {
            started = true;
          }
        } else if (req.start.basis === 'at') {
          if (this.#schema.compareRows(curr.row, req.start.row) >= 0) {
            started = true;
          }
        } else if (req.start.basis === 'after') {
          if (this.#schema.compareRows(curr.row, req.start.row) > 0) {
            started = true;
          }
        }
      }
      if (started) {
        yield curr;
      }
    }
  }

  dehydrate(req: HydrateRequest, output: Output): Stream<Node> {
    return this.fetch(req, output);
  }

  *#pullWithOverlay(
    startAt: Row | undefined,
    constraint: Constraint | undefined,
    overlay: Overlay | undefined,
  ): Stream<Node> {
    const compare = this.#schema.compareRows;

    if (startAt && overlay && compare(overlay.change.row, startAt) < 0) {
      overlay = undefined;
    }

    for (const change of this.#pullWithConstraint(startAt, constraint)) {
      if (overlay) {
        const cmp = compare(overlay.change.row, change.row);
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
      yield change;
    }

    if (overlay && overlay.change.type === 'add') {
      yield {row: overlay.change.row, relationships: {}};
    }
  }

  *#pullWithConstraint(
    startAt: Row | undefined,
    constraint: Constraint | undefined,
  ): Stream<Node> {
    const it = this.#data.keys(startAt);

    // Process all items in the iterator, applying overlay as needed.
    for (const row of it) {
      if (!constraint || valuesEqual(row[constraint.key], constraint.value)) {
        yield {row, relationships: {}};
      }
    }
  }

  push(change: SourceChange) {
    if (change.type === 'add') {
      if (this.#data.has(change.row)) {
        throw new Error('Row already exists');
      }
    } else {
      assert(change.type === 'remove');
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
    this.#overlay = null;
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
