import {BTree} from '../../../btree/src/mod.js';
import {assert, unreachable} from '../../../shared/src/asserts.js';
import type {
  Condition,
  Ordering,
  OrderPart,
  SimpleCondition,
} from '../../../zero-protocol/src/ast.js';
import type {Row, Value} from '../../../zero-protocol/src/data.js';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.js';
import {assertOrderingIncludesPK} from '../builder/builder.js';
import {createPredicate} from '../builder/filter.js';
import type {Change} from './change.js';
import {
  type Comparator,
  compareValues,
  makeComparator,
  type Node,
  valuesEqual,
} from './data.js';
import {LookaheadIterator} from './lookahead-iterator.js';
import type {Constraint, FetchRequest, Input, Output} from './operator.js';
import type {SourceSchema} from './schema.js';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.js';
import type {Source, SourceChange, SourceInput} from './source.js';
import type {Stream} from './stream.js';

export type Overlay = {
  outputIndex: number;
  change: SourceChange;
};

export type Overlays = {
  add: Row | undefined;
  remove: Row | undefined;
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
  optionalFilters: ((row: Row) => boolean)[];
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
  readonly #columns: Record<string, SchemaValue>;
  readonly #primaryKey: PrimaryKey;
  readonly #primaryIndexSort: Ordering;
  readonly #indexes: Map<string, Index> = new Map();
  readonly #connections: Connection[] = [];

  #overlay: Overlay | undefined;

  constructor(
    tableName: string,
    columns: Record<string, SchemaValue>,
    primaryKey: PrimaryKey,
  ) {
    this.#tableName = tableName;
    this.#columns = columns;
    this.#primaryKey = primaryKey;
    this.#primaryIndexSort = primaryKey.map(k => [k, 'asc']);
    const comparator = makeBoundComparator(this.#primaryIndexSort);
    this.#indexes.set(JSON.stringify(this.#primaryIndexSort), {
      comparator,
      data: new BTree<Row, undefined>([], comparator),
      usedBy: new Set(),
    });
    assertOrderingIncludesPK(this.#primaryIndexSort, this.#primaryKey);
  }

  // Mainly for tests.
  getSchemaInfo() {
    return {
      tableName: this.#tableName,
      columns: this.#columns,
      primaryKey: this.#primaryKey,
    };
  }

  #getSchema(connection: Connection): SourceSchema {
    return {
      tableName: this.#tableName,
      columns: this.#columns,
      primaryKey: this.#primaryKey,
      sort: connection.sort,
      relationships: {},
      isHidden: false,
      compareRows: connection.compareRows,
    };
  }

  connect(
    sort: Ordering,
    optionalFilters?: Condition | undefined,
  ): SourceInput {
    const input: SourceInput = {
      getSchema: () => schema,
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

    const predicates: ((row: Row) => boolean)[] = filteredOptionalFilters(
      optionalFilters,
    ).filters.map(c => createPredicate(c));

    const connection: Connection = {
      input,
      output: undefined,
      sort,
      compareRows: makeComparator(sort),
      optionalFilters: predicates,
    };
    const schema = this.#getSchema(connection);
    assertOrderingIncludesPK(sort, this.#primaryKey);
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

    const comparator = makeBoundComparator(sort);

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
    const conn = this.#connections[callingConnectionNum];
    const {sort: requestedSort} = conn;

    // If there is a constraint, we need an index sorted by it first.
    const indexSort: OrderPart[] = [];
    if (req.constraint) {
      indexSort.push([req.constraint.key, 'asc']);
    }

    // For the special case of constraining by PK, we don't need to worry about
    // any requested sort since there can only be one result. Otherwise we also
    // need the index sorted by the requested sort.
    if (
      this.#primaryKey.length > 1 ||
      req.constraint?.key !== this.#primaryKey[0]
    ) {
      indexSort.push(...requestedSort);
    }

    const index = this.#getOrCreateIndex(indexSort, from);
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

    const matchesFilters = (row: Row) =>
      conn.optionalFilters.every(f => f(row));

    const matchesConstraintAndFilters = (row: Row) =>
      matchesConstraint(row) && matchesFilters(row);
    // If there is an overlay for this output, does it match the requested
    // constraints and filters?
    if (overlay) {
      // TODO: This looks wrong given that we can have edit changes in the overlay.
      if (!matchesConstraintAndFilters(overlay.change.row)) {
        overlay = undefined;
      }
    }
    const nextLowerKey = (row: Row | undefined) => {
      if (!row) {
        return undefined;
      }
      let o = overlay;
      if (o) {
        if (comparator(o.change.row, row) >= 0) {
          o = undefined;
        }
      }
      while (row !== undefined) {
        row = data.nextLowerKey(row);
        if (row && matchesConstraintAndFilters(row)) {
          if (o && comparator(o.change.row, row) >= 0) {
            return o.change.row;
          }
          return row;
        }
      }
      return o?.change.row;
    };
    let startAt = req.start?.row;
    if (startAt) {
      if (req.constraint) {
        // There's no problem supporting startAt outside of constraints, but I
        // don't think we have a use case for this – if we see it, it's probably
        // a bug.
        if (!matchesConstraint(startAt)) {
          assert(false, 'Start row must match constraint');
        }
      }
      if (req.start!.basis === 'before') {
        startAt = nextLowerKey(startAt);
      }
    }

    // If there is a constraint, we want to start our scan at the first row that
    // matches the constraint. But because the next OrderPart can be `desc`,
    // it's not true that {[constraintKey]: constraintValue} is the first
    // matching row. Because in that case, the other fields will all be
    // `undefined`, and in Zero `undefined` is always less than any other value.
    // So if the second OrderPart is descending then `undefined` values will
    // actually be the *last* row. We need a way to stay "start at the first row
    // with this constraint value". RowBound with the corresponding compareBound
    // comparator accomplishes this. The right thing is probably to teach the
    // btree library to support this concept.
    let scanStart: RowBound | undefined;
    if (req.constraint) {
      scanStart = {};
      for (const [key, dir] of indexSort) {
        if (key === req.constraint.key) {
          scanStart[key] = req.constraint.value;
        } else {
          scanStart[key] = dir === 'asc' ? minValue : maxValue;
        }
      }
    } else {
      scanStart = startAt;
    }

    const withOverlay = generateWithOverlay(
      startAt,
      // 😬 - btree library doesn't support ideas like start "before" this
      // key.
      data.keys(scanStart as Row),
      req.constraint,
      overlay,
      comparator,
    );

    const withFilters = conn.optionalFilters.length
      ? generateWithFilter(withOverlay, matchesFilters)
      : withOverlay;

    yield* generateWithConstraint(
      generateWithStart(withFilters, req, comparator),
      req.constraint,
    );
  }

  #cleanup(req: FetchRequest, connection: Connection): Stream<Node> {
    return this.#fetch(req, connection);
  }

  push(change: SourceChange): void {
    const primaryIndex = this.#getPrimaryIndex();
    const {data} = primaryIndex;

    switch (change.type) {
      case 'add':
        if (data.has(change.row)) {
          throw new Error(`Row already exists: ` + JSON.stringify(change));
        }
        break;
      case 'remove':
        if (!data.has(change.row)) {
          throw new Error(`Row not found: ` + JSON.stringify(change));
        }
        break;
      case 'edit':
        if (!data.has(change.oldRow)) {
          throw new Error(`Row not found: ` + JSON.stringify(change));
        }
        break;
      default:
        unreachable(change);
    }

    const outputChange: Change =
      change.type === 'edit'
        ? change
        : {
            type: change.type,
            node: {
              row: change.row,
              relationships: {},
            },
          };
    for (const [outputIndex, {output}] of this.#connections.entries()) {
      if (output) {
        this.#overlay = {outputIndex, change};
        output.push(outputChange);
      }
    }
    this.#overlay = undefined;
    for (const {data} of this.#indexes.values()) {
      switch (change.type) {
        case 'add': {
          const added = data.add(change.row, undefined);
          // must succeed since we checked has() above.
          assert(added);
          break;
        }
        case 'remove': {
          const removed = data.delete(change.row);
          // must succeed since we checked has() above.
          assert(removed);
          break;
        }
        case 'edit': {
          // TODO: We could see if the PK (form the index tree's perspective)
          // changed and if not we could use set.

          // We cannot just do `set` with the new value since the `oldRow` might
          // not map to the same entry as the new `row` in the index btree.
          const removed = data.delete(change.oldRow);
          // must succeed since we checked has() above.
          assert(removed);
          data.add(change.row, undefined);
          break;
        }
        default:
          unreachable(change);
      }
    }
  }
}

function* generateWithConstraint(
  it: Stream<Node>,
  constraint: Constraint | undefined,
) {
  for (const node of it) {
    if (
      constraint &&
      !valuesEqual(node.row[constraint.key], constraint.value)
    ) {
      break;
    }
    yield node;
  }
}

function* generateWithFilter(it: Stream<Node>, filter: (row: Row) => boolean) {
  for (const node of it) {
    if (filter(node.row)) {
      yield node;
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
 * @param rows - the stream into which the overlay should be spliced
 * @param constraint - constraint that was applied to the rowIterator and should
 * also be applied to the overlay.
 * @param overlay - the overlay values to splice in
 * @param compare - the comparator to use to find the position for the overlay
 */
export function* generateWithOverlay(
  startAt: Row | undefined,
  rows: Iterable<Row>,
  constraint: Constraint | undefined,
  overlay: Overlay | undefined,
  compare: Comparator,
) {
  const overlays = computeOverlays(startAt, constraint, overlay, compare);
  yield* generateWithOverlayInner(rows, overlays, compare);
}

function computeOverlays(
  startAt: Row | undefined,
  constraint: Constraint | undefined,
  overlay: Overlay | undefined,
  compare: Comparator,
): Overlays {
  let overlays: Overlays = {
    add: undefined,
    remove: undefined,
  };
  switch (overlay?.change.type) {
    case 'add':
      overlays = {
        add: overlay.change.row,
        remove: undefined,
      };
      break;
    case 'remove':
      overlays = {
        add: undefined,
        remove: overlay.change.row,
      };
      break;
    case 'edit':
      overlays = {
        add: overlay.change.row,
        remove: overlay.change.oldRow,
      };
      break;
  }

  if (startAt) {
    overlays = overlaysForStartAt(overlays, startAt, compare);
  }

  if (constraint) {
    overlays = overlaysForConstraint(overlays, constraint);
  }

  return overlays;
}

export {overlaysForStartAt as overlaysForStartAtForTest};

function overlaysForStartAt(
  {add, remove}: Overlays,
  startAt: Row,
  compare: Comparator,
): Overlays {
  const undefinedIfBeforeStartAt = (row: Row | undefined) =>
    row === undefined || compare(row, startAt) < 0 ? undefined : row;
  return {
    add: undefinedIfBeforeStartAt(add),
    remove: undefinedIfBeforeStartAt(remove),
  };
}

export {overlaysForConstraint as overlaysForConstraintForTest};

function overlaysForConstraint(
  {add, remove}: Overlays,
  constraint: Constraint,
): Overlays {
  const undefinedIfDoesntMatchConstraint = (row: Row | undefined) =>
    row === undefined || !valuesEqual(row[constraint.key], constraint.value)
      ? undefined
      : row;

  return {
    add: undefinedIfDoesntMatchConstraint(add),
    remove: undefinedIfDoesntMatchConstraint(remove),
  };
}

export function* generateWithOverlayInner(
  rowIterator: Iterable<Row>,
  overlays: Overlays,
  compare: (r1: Row, r2: Row) => number,
) {
  let addOverlayYielded = false;
  let removeOverlaySkipped = false;
  for (const row of rowIterator) {
    if (!addOverlayYielded && overlays.add) {
      const cmp = compare(overlays.add, row);
      if (cmp < 0) {
        addOverlayYielded = true;
        yield {row: overlays.add, relationships: {}};
      }
    }

    if (!removeOverlaySkipped && overlays.remove) {
      const cmp = compare(overlays.remove, row);
      if (cmp === 0) {
        removeOverlaySkipped = true;
        continue;
      }
    }
    yield {row, relationships: {}};
  }

  if (!addOverlayYielded && overlays.add) {
    yield {row: overlays.add, relationships: {}};
  }
}

/**
 * A location to begin scanning an index from. Can either be a specific value
 * or the min or max possible value for the type. This is used to start a scan
 * at the beginning of the rows matching a constraint.
 */
type Bound = Value | MinValue | MaxValue;
type RowBound = Record<string, Bound>;
const minValue = Symbol('min-value');
type MinValue = typeof minValue;
const maxValue = Symbol('max-value');
type MaxValue = typeof maxValue;

function makeBoundComparator(sort: Ordering) {
  return (a: RowBound, b: RowBound) => {
    // Hot! Do not use destructuring
    for (const entry of sort) {
      const key = entry[0];
      const cmp = compareBounds(a[key], b[key]);
      if (cmp !== 0) {
        return entry[1] === 'asc' ? cmp : -cmp;
      }
    }
    return 0;
  };
}

function compareBounds(a: Bound, b: Bound): number {
  if (a === b) {
    return 0;
  }
  if (a === minValue) {
    return -1;
  }
  if (b === minValue) {
    return 1;
  }
  if (a === maxValue) {
    return 1;
  }
  if (b === maxValue) {
    return -1;
  }
  return compareValues(a, b);
}

/**
 * Only returns optional filters if:
 * 1. It's a simple condition
 * 2. It's an `and` condition with only simple conditions inside of it
 * 3. It's an `or` that is a no-op.
 *
 * Otherwise the filters are dropped.
 *
 * This is a short term solution until we update `fetch` to pass
 * the constraints rather than making them static in `connect`.
 *
 * The below way of doing things over-fetches as the optional filters
 * are widened to cover all branches of the pipeline.
 */
export function filteredOptionalFilters(
  optionalFilters: Condition | undefined,
): {filters: SimpleCondition[]; allApplied: boolean} {
  if (optionalFilters) {
    if (
      optionalFilters.type === 'or' &&
      optionalFilters.conditions.length === 1
    ) {
      optionalFilters = optionalFilters.conditions[0];
    }

    if (optionalFilters.type === 'and') {
      const filters = optionalFilters.conditions.filter(
        c => c.type === 'simple',
      );
      return {
        filters,
        allApplied: filters.length === optionalFilters.conditions.length,
      };
    }

    if (optionalFilters.type === 'simple') {
      return {
        filters: [optionalFilters],
        allApplied: true,
      };
    }

    return {filters: [], allApplied: false};
  }

  return {
    filters: [],
    allApplied: true,
  };
}
