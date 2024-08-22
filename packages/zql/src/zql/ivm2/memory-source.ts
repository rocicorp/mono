import BTree from 'btree';
import type {
  Output,
  FetchRequest,
  HydrateRequest,
  Constraint,
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
import {LookaheadIterator} from './lookahead-iterator.js';
import type {Stream} from './stream.js';
import {Source, SourceChange} from './source.js';
import {Schema, ValueType} from './schema.js';

export type Overlay = {
  outputIndex: number;
  change: SourceChange;
};

type Index = {
  comparator: Comparator;
  data: BTree<Row, undefined>;
  usedBy: Set<Output>;
};

type OutputRegistration = {
  output: Output;
  sort: Ordering;
};

/**
 * A `MemorySource` is a source that provides data to the pipeline from an
 * in-memory data source.
 *
 * This data is kept in sorted order as downstream pipelines will always expect
 * the data they receive from `pull` to be in sorted order.
 */
export class MemorySource implements Source {
  readonly #columns: Record<string, ValueType>;
  readonly #primaryKeys: readonly string[];
  readonly #primaryIndexSort: Ordering;
  readonly #indexes: Map<string, Index> = new Map();
  readonly #outputs: OutputRegistration[] = [];

  #overlay: Overlay | undefined;

  constructor(
    columns: Record<string, ValueType>,
    primaryKeys: readonly string[],
  ) {
    this.#columns = columns;
    this.#primaryKeys = primaryKeys;
    this.#primaryIndexSort = primaryKeys.map(k => [k, 'asc']);
    this.#indexes = new Map();
    const comparator = makeComparator(this.#primaryIndexSort);
    this.#indexes.set(JSON.stringify(this.#primaryIndexSort), {
      comparator,
      data: new BTree<Row, undefined>([], comparator),
      usedBy: new Set(),
    });
  }

  #getRegistrationForOutput(output: Output): OutputRegistration {
    const reg = this.#outputs.find(r => r.output === output);
    assert(reg, 'Output not found');
    return reg;
  }

  getSchema(output: Output): Schema {
    const reg = this.#getRegistrationForOutput(output);
    return {
      columns: this.#columns,
      primaryKey: this.#primaryKeys,
      compareRows: makeComparator(reg.sort),
    };
  }

  addOutput(output: Output, sort: Ordering): void {
    this.#outputs.push({output, sort});
  }

  removeOutput(output: Output): void {
    const idx = this.#outputs.findIndex(r => r.output === output);
    assert(idx !== -1, 'Output not found');
    this.#outputs.splice(idx, 1);

    const primaryIndexKey = JSON.stringify(this.#primaryIndexSort);

    for (const [key, index] of this.#indexes) {
      if (key === primaryIndexKey) {
        continue;
      }
      index.usedBy.delete(output);
      if (index.usedBy.size === 0) {
        this.#indexes.delete(key);
      }
    }
  }

  hydrate(req: HydrateRequest, output: Output) {
    return this.fetch(req, output);
  }

  #getPrimaryIndex(): Index {
    const index = this.#indexes.get(JSON.stringify(this.#primaryIndexSort));
    assert(index, 'Primary index not found');
    return index;
  }

  #getOrCreateIndex(sort: Ordering, output: Output): Index {
    const key = JSON.stringify(sort);
    const index = this.#indexes.get(key);
    // Future optimization could use existing index if it's the same just sorted
    // in reverse of needed.
    if (index) {
      index.usedBy.add(output);
      return index;
    }

    const comparator = makeComparator(sort);

    // When creating these synchronously becomes a problem, a few options:
    // 1. Allow users to specify needed indexes up front
    // 2. Create indexes in a different thread asynchronously (this would require
    // modifying the BTree to be able to be passed over structured-clone, or using
    // a different library.)
    // 3. We could even theoretically do (2) on multiple threads and then merge the
    // results!
    const data = new BTree<Row, undefined>([], comparator);

    // I checked, there's no special path for adding data in bulk faster.
    // The constructor takes an array, but it just calls add/set over and over.
    for (const row of this.#getPrimaryIndex().data.keys()) {
      data.add(row, undefined);
    }

    const newIndex = {comparator, data, usedBy: new Set([output])};
    this.#indexes.set(key, newIndex);
    return newIndex;
  }

  // For unit testing that we correctly clean up indexes.
  getIndexKeys(): string[] {
    return [...this.#indexes.keys()];
  }

  *fetch(req: FetchRequest, output: Output): Stream<Node> {
    let overlay: Overlay | undefined;

    const callingOutputNum = this.#outputs.findIndex(r => r.output === output);
    assert(callingOutputNum !== -1, 'Output not found');
    const reg = this.#outputs[callingOutputNum];
    const {sort} = reg;

    const index = this.#getOrCreateIndex(sort, output);
    const {data, comparator} = index;

    // When we receive a push, we send it to each output one at a time. Once the
    // push is sent to an output, it should keep being sent until all datastores
    // have received it and the change has been made to the datastore.
    if (this.#overlay) {
      if (callingOutputNum <= this.#overlay.outputIndex) {
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
      ? data.nextLowerKey(req.start.row)
      : undefined;
    yield* generateWithStart(
      generateWithOverlay(
        startAt,
        this.#pullWithConstraint(data, startAt, req.constraint),
        req.constraint,
        overlay,
        comparator,
      ),
      req,
      comparator,
    );
  }

  dehydrate(req: HydrateRequest, output: Output): Stream<Node> {
    return this.fetch(req, output);
  }

  *#pullWithConstraint(
    data: BTree<Row, undefined>,
    startAt: Row | undefined,
    constraint: Constraint | undefined,
  ): IterableIterator<Row> {
    const it = data.keys(startAt);

    // Process all items in the iterator, applying overlay as needed.
    for (const row of it) {
      if (!constraint || valuesEqual(row[constraint.key], constraint.value)) {
        yield row;
      }
    }
  }

  push(change: SourceChange) {
    for (const [_, {data}] of this.#indexes) {
      if (change.type === 'add') {
        assert(!data.has(change.row), 'Row already exists');
      } else {
        change.type satisfies 'remove'; // ensures exuaustiveness of `if/else`
        assert(data.has(change.row), 'Row not found');
      }

      for (const [outputIndex, {output}] of this.#outputs.entries()) {
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
        const added = data.add(change.row, undefined);
        // must suceed since we checked has() above.
        assert(added);
      } else {
        assert(change.type === 'remove');
        const removed = data.delete(change.row);
        // must suceed since we checked has() above.
        assert(removed);
      }
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
