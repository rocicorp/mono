import BTree from 'btree';
import {assert} from 'shared/src/asserts.js';
import type {Ordering, SimpleCondition} from '../ast/ast.js';
import {
  Comparator,
  makeComparator,
  valuesEqual,
  type Node,
  type Row,
} from './data.js';
import {LookaheadIterator} from './lookahead-iterator.js';
import type {Constraint, FetchRequest, Input, Output} from './operator.js';
import type {PrimaryKeys, Schema, ValueType} from './schema.js';
import type {Source, SourceChange, SourceInput} from './source.js';
import type {Stream} from './stream.js';

export type Overlay = {
  outputIndex: number;
  change: SourceChange;
};

type Index = {
  comparator: Comparator;
  data: BTree<Row, undefined>;
  usedBy: Set<Connection>;
};

type Connection = {
  input: Input;
  output: Output | undefined;
  sort: Ordering;
  compareRows: Comparator;
};

/**
 * A `MemorySource` is a source that provides data to the pipeline from an
 * in-memory data source.
 *
 * This data is kept in sorted order as downstream pipelines will always expect
 * the data they receive from `pull` to be in sorted order.
 */
export class MemorySource implements Source {
  readonly #tableName: string;
  readonly #columns: Record<string, ValueType>;
  readonly #primaryKeys: PrimaryKeys;
  readonly #primaryIndexSort: Ordering;
  readonly #indexes: Map<string, Index> = new Map();
  readonly #connections: Connection[] = [];

  #overlay: Overlay | undefined;

  constructor(
    tableName: string,
    columns: Record<string, ValueType>,
    primaryKeys: PrimaryKeys,
  ) {
    this.#tableName = tableName;
    this.#columns = columns;
    this.#primaryKeys = primaryKeys;
    this.#primaryIndexSort = primaryKeys.map(k => [k, 'asc']);
    const comparator = makeComparator(this.#primaryIndexSort);
    this.#indexes.set(JSON.stringify(this.#primaryIndexSort), {
      comparator,
      data: new BTree<Row, undefined>([], comparator),
      usedBy: new Set(),
    });
  }

  // Mainly for tests.
  getSchemaInfo() {
    return {
      tableName: this.#tableName,
      columns: this.#columns,
      primaryKey: this.#primaryKeys,
    };
  }

  #getSchema(connection: Connection): Schema {
    return {
      tableName: this.#tableName,
      columns: this.#columns,
      primaryKey: this.#primaryKeys,
      compareRows: connection.compareRows,
      relationships: {},
      isHidden: false,
    };
  }

  connect(
    sort: Ordering,
    _optionalFilters?: SimpleCondition[] | undefined,
  ): SourceInput {
    const input: SourceInput = {
      getSchema: () => this.#getSchema(connection),
      fetch: req => this.#fetch(req, connection),
      cleanup: req => this.#cleanup(req, connection),
      setOutput: output => {
        connection.output = output;
      },
      destroy: () => {
        this.#disconnect(input);
      },
      appliedFilters: false,
    };

    const connection: Connection = {
      input,
      output: undefined,
      sort,
      compareRows: makeComparator(sort),
    };

    this.#connections.push(connection);
    return input;
  }

  #disconnect(input: Input): void {
    const idx = this.#connections.findIndex(c => c.input === input);
    assert(idx !== -1, 'Connection not found');
    const connection = this.#connections[idx];
    this.#connections.splice(idx, 1);

    const primaryIndexKey = JSON.stringify(this.#primaryIndexSort);

    for (const [key, index] of this.#indexes) {
      if (key === primaryIndexKey) {
        continue;
      }
      index.usedBy.delete(connection);
      if (index.usedBy.size === 0) {
        this.#indexes.delete(key);
      }
    }
  }

  #getPrimaryIndex(): Index {
    const index = this.#indexes.get(JSON.stringify(this.#primaryIndexSort));
    assert(index, 'Primary index not found');
    return index;
  }

  #getOrCreateIndex(sort: Ordering, usedBy: Connection): Index {
    const key = JSON.stringify(sort);
    const index = this.#indexes.get(key);
    // Future optimization could use existing index if it's the same just sorted
    // in reverse of needed.
    if (index) {
      index.usedBy.add(usedBy);
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

    const newIndex = {comparator, data, usedBy: new Set([usedBy])};
    this.#indexes.set(key, newIndex);
    return newIndex;
  }

  // For unit testing that we correctly clean up indexes.
  getIndexKeys(): string[] {
    return [...this.#indexes.keys()];
  }

  *#fetch(req: FetchRequest, from: Connection): Stream<Node> {
    let overlay: Overlay | undefined;

    const callingConnectionNum = this.#connections.indexOf(from);
    assert(callingConnectionNum !== -1, 'Output not found');
    const reg = this.#connections[callingConnectionNum];
    const {sort} = reg;

    const index = this.#getOrCreateIndex(sort, from);
    const {data, comparator} = index;

    // When we receive a push, we send it to each output one at a time. Once the
    // push is sent to an output, it should keep being sent until all datastores
    // have received it and the change has been made to the datastore.
    if (this.#overlay) {
      if (callingConnectionNum <= this.#overlay.outputIndex) {
        overlay = this.#overlay;
      }
    }

    const matchesConstraint = (row: Row) => {
      if (!req.constraint) {
        return true;
      }
      const {key, value} = req.constraint;
      return valuesEqual(row[key], value);
    };

    // If there is an overlay for this output, does it match the requested
    // constraints?
    if (overlay) {
      if (!matchesConstraint(overlay.change.row)) {
        overlay = undefined;
      }
    }

    const nextLowerKey = (row: Row | undefined) => {
      while (row !== undefined) {
        row = data.nextLowerKey(row);
        if (row && matchesConstraint(row)) {
          return row;
        }
      }
      return row;
    };

    const startAt =
      req.start?.basis === 'before'
        ? nextLowerKey(req.start.row)
        : req.start?.row;
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

  #cleanup(req: FetchRequest, connection: Connection): Stream<Node> {
    return this.#fetch(req, connection);
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
    const primaryIndex = this.#getPrimaryIndex();
    const {data} = primaryIndex;
    if (change.type === 'add') {
      if (data.has(change.row)) {
        throw new Error(`Row already exists: ` + JSON.stringify(change));
      }
    } else {
      change.type satisfies 'remove';
      if (!data.has(change.row)) {
        throw new Error(`Row not found: ` + JSON.stringify(change));
      }
    }

    for (const [outputIndex, {output}] of this.#connections.entries()) {
      if (output) {
        this.#overlay = {outputIndex, change};
        output.push({
          type: change.type,
          node: {
            row: change.row,
            relationships: {},
          },
        });
      }
    }
    this.#overlay = undefined;
    for (const {data} of this.#indexes.values()) {
      if (change.type === 'add') {
        const added = data.add(change.row, undefined);
        // must succeed since we checked has() above.
        assert(added);
      } else {
        change.type satisfies 'remove';
        const removed = data.delete(change.row);
        // must succeed since we checked has() above.
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
