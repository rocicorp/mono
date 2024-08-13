import BTree from 'btree';
import type {
  Input,
  Output,
  FetchRequest,
  HydrateRequest,
  Schema,
} from './operator.js';
import {makeComparator, valuesEqual, type Node, type Row} from './data.js';
import type {Ordering} from '../ast2/ast.js';
import {assert} from 'shared/src/asserts.js';
import {makeStream, type Stream} from './stream.js';

export type SourceChange = {
  type: 'add' | 'remove';
  row: Row;
};

/**
 * A `MemorySource` is a source that provides data to the pipeline from
 * an in-memory data source.
 *
 * This data is kept in sorted order as downstream pipelines will
 * always expect the data they receive from `pull` to be in sorted order.
 */
export class MemorySource implements Input {
  readonly #schema: Schema;
  readonly #data: BTree<Row, undefined>;
  readonly #outputs: Output[] = [];

  #overlay: {
    output: Output;
    change: SourceChange;
  } | null = null;

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

  hydrate(req: HydrateRequest, _output: Output) {
    return this.#pullValues(req, this.#data.entries());
  }

  fetch(req: FetchRequest, _output: Output): Stream<Node> {
    const {start} = req;
    let it: Iterator<[Row, undefined]> | null = null;
    if (!start) {
      it = this.#data.entries();
    } else {
      const {row, basis} = start;
      if (basis === 'before') {
        const startKey = this.#data.nextLowerKey(row);
        it = this.#data.entries(startKey);
      } else if (basis === 'at') {
        it = this.#data.entries(row);
      } else {
        assert(basis === 'after');
        it = this.#data.entries(row);
        it.next();
      }
    }

    return this.#pullValues(req, it);
  }

  *#pullValues(
    req: HydrateRequest,
    input: Iterator<[Row, undefined]>,
  ): Stream<Node> {
    let usedOverlay = false;

    for (const [row] of makeStream(input)) {
      if (this.#overlay !== null && !usedOverlay) {
        const cmp = this.#schema.compareRows(row, this.#overlay.change.row);
        if (this.#overlay.change.type === 'add') {
          assert(cmp !== 0, 'Duplicate row in overlay');
          if (cmp > 0) {
            yield {
              row: this.#overlay.change.row,
              relationships: new Map(),
            };
            usedOverlay = true;
          }
        } else if (this.#overlay.change.type === 'remove') {
          if (cmp === 0) {
            yield {
              row: this.#overlay.change.row,
              relationships: new Map(),
            };
            usedOverlay = true;
          }
        }
      }

      if (
        !req.constraint ||
        valuesEqual(row[req.constraint.key], req.constraint.value)
      ) {
        yield {row, relationships: new Map()};
      }

      if (this.#overlay !== null && !usedOverlay) {
        if (this.#overlay.change.type === 'add') {
          yield {
            row: this.#overlay.change.row,
            relationships: new Map(),
          };
        } else {
          assert(false, 'Remove change did not affect any value');
        }
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

    for (const output of this.#outputs) {
      this.#overlay = {output, change};
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
