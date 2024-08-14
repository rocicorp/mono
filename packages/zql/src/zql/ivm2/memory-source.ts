import BTree from 'btree';
import type {
  Input,
  Output,
  FetchRequest,
  HydrateRequest,
  Schema,
} from './operator.js';
import {
  Comparator,
  makeComparator,
  valuesEqual,
  type Node,
  type Row,
} from './data.js';
import type {Ordering} from '../ast2/ast.js';
import {assert} from 'shared/src/asserts.js';
import {makeStream, type Stream} from './stream.js';

export type SourceChange = {
  type: 'add' | 'remove';
  row: Row;
};

type Overlay = {
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

  schema(): Schema {
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

    // Find the start of the iterator, taking into account 'start' param and
    // overlay.
    let it: Iterator<Row>;
    if (!req.start) {
      it = this.#data.keys();
    } else {
      assert(this.#data.has(req.start.row), 'Start row not found');
      const result = adjustStart(
        {
          prev: this.#data.nextLowerKey(req.start.row),
          basis: req.start.row,
          next: this.#data.nextHigherKey(req.start.row),
        },
        req.start.basis,
        overlay,
        this.#schema.compareRows,
      );
      overlay = result.overlay;
      if (result.start === 'begin') {
        it = this.#data.keys();
      } else if (result.start === 'end') {
        it = this.#data.keys(this.#data.maxKey());
        it.next();
      } else {
        it = this.#data.keys(result.start);
      }
    }

    // Process all items in the iterator, applying overlay as needed.
    for (const row of makeStream(it)) {
      if (overlay) {
        const cmp = this.#schema.compareRows(overlay.change.row, row);
        if (overlay.change.type === 'add') {
          if (cmp < 0) {
            yield {
              row: overlay.change.row,
              relationships: new Map(),
            };
            overlay = null;
          }
        } else if (overlay.change.type === 'remove') {
          if (cmp < 0) {
            overlay = null;
          } else if (cmp === 0) {
            overlay = null;
            continue;
          }
        }
      }

      if (
        !req.constraint ||
        valuesEqual(row[req.constraint.key], req.constraint.value)
      ) {
        yield {row, relationships: new Map()};
      }
    }

    // If there is an add overlay left, it's because it's greater than all the
    // rows.
    if (overlay && overlay.change.type === 'add') {
      yield {
        row: overlay.change.row,
        relationships: new Map(),
      };
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
            relationships: new Map(),
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

type Neighborhood = {
  prev: Row | undefined;
  basis: Row;
  next: Row | undefined;
};

// Helper to handle the 'start' parameter in fetch.
function adjustStart(
  hood: Neighborhood,
  startType: 'before' | 'at' | 'after',
  overlay: Overlay | null,
  compareRows: Comparator,
): {start: Row | 'begin' | 'end'; overlay: Overlay | null} {
  const overlayRow = overlay?.change?.row;

  if (startType === 'before') {
    // No previous row, so we're returning all rows. We start from beginning and
    // keep overlay.
    if (hood.prev === undefined) {
      return {start: 'begin', overlay};
    }

    if (!overlayRow) {
      return {start: hood.prev, overlay: null};
    }

    // Overlay is before previous, we discard it and start from previous.
    if (compareRows(overlayRow, hood.prev) < 0) {
      return {start: hood.prev, overlay: null};
    }

    // Overlay is equal to previous. We keep overlay and start from previous.
    if (compareRows(overlayRow, hood.prev) === 0) {
      return {start: hood.prev, overlay};
    }

    // Overlay between previous and start. We keep overlay and start from start,
    // not previous.
    if (compareRows(overlayRow, hood.basis) < 0) {
      return {start: hood.basis, overlay};
    }

    // Overlay >= start. Start from previous and keep overlay
    return {start: hood.prev, overlay};
  }

  if (startType === 'at') {
    if (!overlayRow) {
      return {start: hood.basis, overlay: null};
    }
    if (compareRows(overlayRow, hood.basis) < 0) {
      return {start: hood.basis, overlay: null};
    }
    return {start: hood.basis, overlay};
  }

  if (startType === 'after') {
    if (!overlayRow) {
      return {start: hood.next ?? 'end', overlay: null};
    }
    if (compareRows(overlayRow, hood.basis) < 0) {
      return {start: hood.next ?? 'end', overlay: null};
    }
    if (hood.next === undefined) {
      return {start: 'end', overlay};
    }
    return {start: hood.next, overlay};
  }

  assert(false, 'Unreachable');
}
