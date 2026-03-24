import {
  assert,
  assertNumber,
  assertString,
  unreachable,
} from '../../../shared/src/asserts.ts';
import {BTreeSet} from '../../../shared/src/btree-set.ts';
import {hasOwn} from '../../../shared/src/has-own.ts';
import {once} from '../../../shared/src/iterables.ts';
import type {
  Condition,
  Ordering,
  OrderPart,
} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {SchemaValue} from '../../../zero-types/src/schema-value.ts';
import type {DebugDelegate} from '../builder/debug-delegate.ts';
import {
  createPredicate,
  transformFilters,
  type NoSubqueryCondition,
} from '../builder/filter.ts';
import {assertOrderingIncludesPK} from '../query/complete-ordering.ts';
import type {Change} from './change.ts';
import {
  constraintMatchesPrimaryKey,
  constraintMatchesRow,
  primaryKeyConstraintFromFilters,
  type Constraint,
} from './constraint.ts';
import {
  compareStringUTF8Fast,
  compareValues,
  makeComparator,
  valuesEqual,
  type Comparator,
  type Node,
} from './data.ts';
import {filterPush} from './filter-push.ts';
import {
  type FetchRequest,
  type Input,
  type Output,
  type Start,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {
  Source,
  SourceChange,
  SourceChangeAdd,
  SourceChangeEdit,
  SourceChangeRemove,
  SourceInput,
} from './source.ts';
import type {Stream} from './stream.ts';

// Shared frozen sentinel for nodes with no relationships. Avoids allocating
// a fresh {} on every node creation in the fetch and push hot paths.
const EMPTY_RELATIONSHIPS: Record<string, never> = Object.freeze({});

export type Overlay = {
  epoch: number;
  change: SourceChange;
};

export type Overlays = {
  add: Row | undefined;
  remove: Row | undefined;
};

type Index = {
  comparator: Comparator;
  data: BTreeSet<Row>;
  usedBy: Set<Connection>;
};

export type Connection = {
  input: Input;
  output: Output | undefined;
  sort: Ordering;
  splitEditKeys: Set<string> | undefined;
  compareRows: Comparator;
  filters:
    | {
        condition: NoSubqueryCondition;
        predicate: (row: Row) => boolean;
      }
    | undefined;
  readonly debug?: DebugDelegate | undefined;
  lastPushedEpoch: number;
  /** Pre-computed on connect so #fetch avoids re-deriving it every call. */
  pkConstraint: Constraint | undefined;
  /** Per-connection cache of constraint+sort -> Index to skip Map lookups. */
  indexCache: Map<string, Index>;
  /** Stringified sort key for this connection, cached to build index cache keys cheaply. */
  requestedSortKey: string;
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
  /** Cached JSON key for the primary index to avoid repeated JSON.stringify. */
  readonly #primaryIndexKey: string;
  readonly #indexes: Map<string, Index> = new Map();
  readonly #connections: Connection[] = [];

  #overlay: Overlay | undefined;
  #pushEpoch = 0;

  constructor(
    tableName: string,
    columns: Record<string, SchemaValue>,
    primaryKey: PrimaryKey,
    primaryIndexData?: BTreeSet<Row>,
  ) {
    this.#tableName = tableName;
    this.#columns = columns;
    this.#primaryKey = primaryKey;
    this.#primaryIndexSort = primaryKey.map(k => [k, 'asc']);
    this.#primaryIndexKey = JSON.stringify(this.#primaryIndexSort);
    const comparator = makeBoundComparator(this.#primaryIndexSort);
    this.#indexes.set(this.#primaryIndexKey, {
      comparator,
      data: primaryIndexData ?? new BTreeSet<Row>(comparator),
      usedBy: new Set(),
    });
  }

  get tableSchema() {
    return {
      name: this.#tableName,
      columns: this.#columns,
      primaryKey: this.#primaryKey,
    };
  }

  fork() {
    const primaryIndex = this.#getPrimaryIndex();
    return new MemorySource(
      this.#tableName,
      this.#columns,
      this.#primaryKey,
      primaryIndex.data.clone(),
    );
  }

  get data(): BTreeSet<Row> {
    return this.#getPrimaryIndex().data;
  }

  #getSchema(connection: Connection): SourceSchema {
    return {
      tableName: this.#tableName,
      columns: this.#columns,
      primaryKey: this.#primaryKey,
      sort: connection.sort,
      system: 'client',
      relationships: {},
      isHidden: false,
      compareRows: connection.compareRows,
    };
  }

  connect(
    sort: Ordering,
    filters?: Condition,
    splitEditKeys?: Set<string>,
  ): SourceInput {
    const transformedFilters = transformFilters(filters);

    const input: SourceInput = {
      getSchema: () => schema,
      fetch: req => this.#fetch(req, connection),
      setOutput: output => {
        connection.output = output;
      },
      destroy: () => {
        this.#disconnect(input);
      },
      fullyAppliedFilters: !transformedFilters.conditionsRemoved,
    };

    const requestedSortKey = sort.map(p => `${p[0]}:${p[1]}`).join('|');
    const connection: Connection = {
      input,
      output: undefined,
      sort,
      splitEditKeys,
      compareRows: makeComparator(sort),
      filters: transformedFilters.filters
        ? {
            condition: transformedFilters.filters,
            predicate: createPredicate(transformedFilters.filters),
          }
        : undefined,
      lastPushedEpoch: 0,
      pkConstraint: primaryKeyConstraintFromFilters(
        transformedFilters.filters,
        this.#primaryKey,
      ),
      indexCache: new Map(),
      requestedSortKey,
    };
    const schema = this.#getSchema(connection);
    assertOrderingIncludesPK(sort, this.#primaryKey);
    this.#connections.push(connection);
    return input;
  }

  #disconnect(input: Input): void {
    const idx = this.#connections.findIndex(c => c.input === input);
    assert(idx !== -1, 'Connection not found');
    this.#connections.splice(idx, 1);

    // TODO: We used to delete unused indexes here. But in common cases like
    // navigating into issue detail pages it caused a ton of constantly
    // building and destroying indexes.
    //
    // Perhaps some intelligent LRU or something is needed here but for now,
    // the opposite extreme of keeping all indexes for the lifetime of the
    // page seems better.
  }

  #getPrimaryIndex(): Index {
    const index = this.#indexes.get(this.#primaryIndexKey);
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
    const data = new BTreeSet<Row>(comparator);

    // I checked, there's no special path for adding data in bulk faster.
    // The constructor takes an array, but it just calls add/set over and over.
    for (const row of this.#getPrimaryIndex().data) {
      data.add(row);
    }

    const newIndex = {comparator, data, usedBy: new Set([usedBy])};
    this.#indexes.set(key, newIndex);
    return newIndex;
  }

  // For unit testing that we correctly clean up indexes.
  getIndexKeys(): string[] {
    return [...this.#indexes.keys()];
  }

  // Non-generator: returns an Iterable directly rather than using function*.
  // This avoids one generator frame allocation per fetch call. The returned
  // iterable comes from one of the fused generator helpers below, chosen
  // based on whether an overlay is active and whether the PK fast path applies.
  #fetch(
    req: FetchRequest,
    conn: Connection,
  ): Iterable<Node | 'yield'> {
    const {sort: requestedSort, compareRows} = conn;
    const connectionComparator: Comparator = req.reverse
      ? (r1, r2) => -compareRows(r1, r2)
      : compareRows;

    const pkConstraint = conn.pkConstraint;
    // The primary key constraint will be more limiting than the constraint
    // so swap out to that if it exists.
    const fetchOrPkConstraint = pkConstraint ?? req.constraint;

    // Determine overlay state once
    const overlay = this.#overlay;
    const hasActiveOverlay =
      overlay !== undefined && conn.lastPushedEpoch >= overlay.epoch;

    // PK fast path: direct BTree.get() for single-row constrained lookups.
    // When filters constrain to a single PK value and no overlay is active,
    // skip the entire generator pipeline and do a direct O(log n) lookup.
    if (pkConstraint && !hasActiveOverlay) {
      const row = this.#getPrimaryIndex().data.get(pkConstraint as Row);
      if (row !== undefined) {
        if (!conn.filters || conn.filters.predicate(row)) {
          if (!req.constraint || constraintMatchesRow(req.constraint, row)) {
            const start = req.start;
            if (
              !start ||
              (start.basis === 'at'
                ? connectionComparator(row, start.row) >= 0
                : connectionComparator(row, start.row) > 0)
            ) {
              return [{row, relationships: EMPTY_RELATIONSHIPS}];
            }
          }
        }
      }
      return [];
    }

    // Standard path: index-based scan
    const includeRequestedSort =
      this.#primaryKey.length > 1 ||
      !fetchOrPkConstraint ||
      !constraintMatchesPrimaryKey(fetchOrPkConstraint, this.#primaryKey);

    let constraintShapeKey = '';
    if (fetchOrPkConstraint) {
      for (const key of Object.keys(fetchOrPkConstraint)) {
        constraintShapeKey += key + '|';
      }
    }

    // If there is a constraint, we need an index sorted by it first.
    const indexSort: OrderPart[] = [];
    if (fetchOrPkConstraint) {
      for (const key of Object.keys(fetchOrPkConstraint)) {
        indexSort.push([key, 'asc']);
      }
    }

    if (includeRequestedSort) {
      indexSort.push(...requestedSort);
    }

    const indexCacheKey = `${constraintShapeKey}::${includeRequestedSort ? conn.requestedSortKey : 'pk'}`;
    let index = conn.indexCache.get(indexCacheKey);
    if (!index) {
      index = this.#getOrCreateIndex(indexSort, conn);
      conn.indexCache.set(indexCacheKey, index);
    }

    const {data, comparator: compare} = index;

    const startAt = req.start?.row;

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

    if (fetchOrPkConstraint) {
      scanStart = {};
      for (const [key, dir] of indexSort) {
        if (hasOwn(fetchOrPkConstraint, key)) {
          scanStart[key] = fetchOrPkConstraint[key];
        } else {
          if (req.reverse) {
            scanStart[key] = dir === 'asc' ? maxValue : minValue;
          } else {
            scanStart[key] = dir === 'asc' ? minValue : maxValue;
          }
        }
      }
    } else {
      scanStart = startAt;
    }

    // Fused fetch paths: eliminate generator frame overhead by combining
    // overlay/start/constraint/filter into minimal generators.
    if (!hasActiveOverlay) {
      // No overlay: fuse all 5 generator stages into 1
      return generateFetchDirect(
        data,
        scanStart,
        req.reverse,
        req.start,
        connectionComparator,
        req.constraint,
        conn.filters?.predicate,
      );
    }

    // Overlay active: compute overlay effects
    const indexComparator: Comparator = (r1, r2) =>
      compare(r1, r2) * (req.reverse ? -1 : 1);
    const overlays = computeOverlays(
      startAt,
      req.constraint,
      overlay,
      indexComparator,
      conn.filters?.predicate,
    );

    if (overlays.add === undefined && overlays.remove === undefined) {
      // Overlay doesn't affect this fetch: use fused no-overlay path
      return generateFetchDirect(
        data,
        scanStart,
        req.reverse,
        req.start,
        connectionComparator,
        req.constraint,
        conn.filters?.predicate,
      );
    }

    // Overlay has actual changes: use overlay inner + fused post-processing.
    // This reduces from 4 generators (overlay+start+constraint+filter) to 2.
    const rowsSource = data[req.reverse ? 'valuesFromReversed' : 'valuesFrom'](
      scanStart as Row | undefined,
    );
    const rowsIterable = pkConstraint ? once(rowsSource) : rowsSource;
    const overlayedNodes = generateWithOverlayInner(
      rowsIterable,
      overlays,
      indexComparator,
    );
    return generatePostOverlayFused(
      overlayedNodes,
      req.start,
      connectionComparator,
      req.constraint,
      conn.filters?.predicate,
    );
  }

  *push(change: SourceChange): Stream<'yield'> {
    for (const result of this.genPush(change)) {
      if (result === 'yield') {
        yield result;
      }
    }
  }

  *genPush(change: SourceChange) {
    const primaryIndex = this.#getPrimaryIndex();
    const {data} = primaryIndex;
    const exists = (row: Row) => data.has(row);
    const setOverlay = (o: Overlay | undefined) => (this.#overlay = o);
    const writeChange = (c: SourceChange) => this.#writeChange(c);
    yield* genPushAndWriteWithSplitEdit(
      this.#connections,
      change,
      exists,
      setOverlay,
      writeChange,
      () => ++this.#pushEpoch,
    );
  }

  #writeChange(change: SourceChange) {
    for (const {data} of this.#indexes.values()) {
      switch (change.type) {
        case 'add': {
          const added = data.add(change.row);
          // must succeed since we checked has() above.
          assert(
            added,
            'MemorySource: add must succeed since row existence was already checked',
          );
          break;
        }
        case 'remove': {
          const removed = data.delete(change.row);
          // must succeed since we checked has() above.
          assert(
            removed,
            'MemorySource: remove must succeed since row existence was already checked',
          );
          break;
        }
        case 'edit': {
          // TODO: We could see if the PK (form the index tree's perspective)
          // changed and if not we could use set.
          // We cannot just do `set` with the new value since the `oldRow` might
          // not map to the same entry as the new `row` in the index btree.
          const removed = data.delete(change.oldRow);
          // must succeed since we checked has() above.
          assert(
            removed,
            'MemorySource: edit remove must succeed since row existence was already checked',
          );
          data.add(change.row);
          break;
        }
        default:
          unreachable(change);
      }
    }
  }
}

export function* genPushAndWriteWithSplitEdit(
  connections: readonly Connection[],
  change: SourceChange,
  exists: (row: Row) => boolean,
  setOverlay: (o: Overlay | undefined) => Overlay | undefined,
  writeChange: (c: SourceChange) => void,
  getNextEpoch: () => number,
) {
  let shouldSplitEdit = false;
  if (change.type === 'edit') {
    for (const {splitEditKeys} of connections) {
      if (splitEditKeys) {
        for (const key of splitEditKeys) {
          if (!valuesEqual(change.row[key], change.oldRow[key])) {
            shouldSplitEdit = true;
            break;
          }
        }
      }
    }
  }

  if (change.type === 'edit' && shouldSplitEdit) {
    yield* genPushAndWrite(
      connections,
      {
        type: 'remove',
        row: change.oldRow,
      },
      exists,
      setOverlay,
      writeChange,
      getNextEpoch(),
    );
    yield* genPushAndWrite(
      connections,
      {
        type: 'add',
        row: change.row,
      },
      exists,
      setOverlay,
      writeChange,
      getNextEpoch(),
    );
  } else {
    yield* genPushAndWrite(
      connections,
      change,
      exists,
      setOverlay,
      writeChange,
      getNextEpoch(),
    );
  }
}

function* genPushAndWrite(
  connections: readonly Connection[],
  change: SourceChangeAdd | SourceChangeRemove | SourceChangeEdit,
  exists: (row: Row) => boolean,
  setOverlay: (o: Overlay | undefined) => Overlay | undefined,
  writeChange: (c: SourceChange) => void,
  pushEpoch: number,
) {
  for (const x of genPush(connections, change, exists, setOverlay, pushEpoch)) {
    yield x;
  }
  writeChange(change);
}

function* genPush(
  connections: readonly Connection[],
  change: SourceChange,
  exists: (row: Row) => boolean,
  setOverlay: (o: Overlay | undefined) => void,
  pushEpoch: number,
) {
  switch (change.type) {
    case 'add':
      assert(
        !exists(change.row),
        () => `Row already exists ${stringify(change)}`,
      );
      break;
    case 'remove':
      assert(exists(change.row), () => `Row not found ${stringify(change)}`);
      break;
    case 'edit':
      assert(exists(change.oldRow), () => `Row not found ${stringify(change)}`);
      break;
    default:
      unreachable(change);
  }

  // Reuse a small set of objects across the connection loop below to avoid
  // allocating fresh Node/Change objects per connection per push. The row
  // fields are overwritten before each use. This is safe because filterPush
  // and its downstream consumers process each change synchronously within
  // the generator chain -- yield* completes fully before the next iteration
  // mutates the objects. In a workload with 135 connections, this eliminates
  // thousands of short-lived allocations per push cycle.
  const reuseNode: Node = {
    row: undefined as unknown as Row,
    relationships: EMPTY_RELATIONSHIPS,
  };
  const reuseOldNode: Node = {
    row: undefined as unknown as Row,
    relationships: EMPTY_RELATIONSHIPS,
  };
  const reuseAddRemove = {
    type: undefined as unknown as 'add' | 'remove',
    node: reuseNode,
  };
  const reuseEdit = {
    type: 'edit' as const,
    oldNode: reuseOldNode,
    node: reuseNode,
  };

  for (const conn of connections) {
    const {output, filters, input} = conn;
    if (output) {
      conn.lastPushedEpoch = pushEpoch;
      setOverlay({epoch: pushEpoch, change});
      let outputChange: Change;
      if (change.type === 'edit') {
        reuseOldNode.row = change.oldRow;
        reuseNode.row = change.row;
        outputChange = reuseEdit;
      } else {
        reuseAddRemove.type = change.type;
        reuseNode.row = change.row;
        outputChange = reuseAddRemove;
      }
      yield* filterPush(outputChange, output, input, filters?.predicate);
      yield undefined;
    }
  }

  setOverlay(undefined);
}

export function* generateWithStart(
  nodes: Iterable<Node | 'yield'>,
  start: Start | undefined,
  compare: (r1: Row, r2: Row) => number,
): Stream<Node | 'yield'> {
  if (!start) {
    yield* nodes;
    return;
  }
  let started = false;
  for (const node of nodes) {
    if (node === 'yield') {
      yield node;
      continue;
    }
    if (!started) {
      if (start.basis === 'at') {
        if (compare(node.row, start.row) >= 0) {
          started = true;
        }
      } else if (start.basis === 'after') {
        if (compare(node.row, start.row) > 0) {
          started = true;
        }
      }
    }
    if (started) {
      yield node;
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
  lastPushedEpoch: number,
  compare: Comparator,
  filterPredicate?: (row: Row) => boolean | undefined,
) {
  if (!overlay || lastPushedEpoch < overlay.epoch) {
    for (const row of rows) {
      yield {row, relationships: EMPTY_RELATIONSHIPS};
    }
    return;
  }
  const overlays = computeOverlays(
    startAt,
    constraint,
    overlay,
    compare,
    filterPredicate,
  );
  if (overlays.add === undefined && overlays.remove === undefined) {
    for (const row of rows) {
      yield {row, relationships: EMPTY_RELATIONSHIPS};
    }
    return;
  }
  yield* generateWithOverlayInner(rows, overlays, compare);
}

function computeOverlays(
  startAt: Row | undefined,
  constraint: Constraint | undefined,
  overlay: Overlay | undefined,
  compare: Comparator,
  filterPredicate?: (row: Row) => boolean | undefined,
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

  if (filterPredicate) {
    overlays = overlaysForFilterPredicate(overlays, filterPredicate);
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
    row === undefined || !constraintMatchesRow(constraint, row)
      ? undefined
      : row;

  return {
    add: undefinedIfDoesntMatchConstraint(add),
    remove: undefinedIfDoesntMatchConstraint(remove),
  };
}

function overlaysForFilterPredicate(
  {add, remove}: Overlays,
  filterPredicate: (row: Row) => boolean | undefined,
): Overlays {
  const undefinedIfDoesntMatchFilter = (row: Row | undefined) =>
    row === undefined || !filterPredicate(row) ? undefined : row;

  return {
    add: undefinedIfDoesntMatchFilter(add),
    remove: undefinedIfDoesntMatchFilter(remove),
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
        yield {row: overlays.add, relationships: EMPTY_RELATIONSHIPS};
      }
    }

    if (!removeOverlaySkipped && overlays.remove) {
      const cmp = compare(overlays.remove, row);
      if (cmp === 0) {
        removeOverlaySkipped = true;
        continue;
      }
    }
    yield {row, relationships: EMPTY_RELATIONSHIPS};
  }

  if (!addOverlayYielded && overlays.add) {
    yield {row: overlays.add, relationships: EMPTY_RELATIONSHIPS};
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

/**
 * Compares two Bound values, handling minValue/maxValue sentinels,
 * null, and delegating to type-specific comparison. This merges the
 * logic of compareBounds + compareValues into a single function that
 * V8 can inline at the call site (well within TurboFan's 460-bytecode
 * inlining threshold).
 */
function compareBoundValue(a: Bound, b: Bound): number {
  if (a === b) return 0;
  if (a === minValue) return -1;
  if (b === minValue) return 1;
  if (a === maxValue) return 1;
  if (b === maxValue) return -1;
  const aN: Value = a ?? null;
  const bN: Value = b ?? null;
  if (aN === null) return bN === null ? 0 : -1;
  if (bN === null) return 1;
  if (typeof a === 'string') {
    assertString(b);
    return compareStringUTF8Fast(a, b);
  }
  if (typeof a === 'number') {
    assertNumber(b);
    return a - (b as number);
  }
  return compareValues(aN, bN);
}

/**
 * Creates a comparator for RowBound values used in BTree index scans.
 *
 * For single-key sorts (the common case), returns a direct comparator
 * that avoids the multi-key loop. The actual comparison logic lives in
 * compareBoundValue, which V8 inlines at the call site.
 */
function makeBoundComparator(sort: Ordering) {
  if (sort.length === 1) {
    const key = sort[0][0];
    const dir = sort[0][1];
    const cmp = (a: RowBound, b: RowBound) => compareBoundValue(a[key], b[key]);
    return dir === 'asc' ? cmp : (a: RowBound, b: RowBound) => -cmp(a, b);
  }
  return (a: RowBound, b: RowBound) => {
    for (const entry of sort) {
      const cmp = compareBoundValue(a[entry[0]], b[entry[0]]);
      if (cmp !== 0) {
        return entry[1] === 'asc' ? cmp : -cmp;
      }
    }
    return 0;
  };
}

export function stringify(change: SourceChange) {
  return JSON.stringify(change, (_, v) =>
    typeof v === 'bigint' ? v.toString() : v,
  );
}

// Fused fetch for no-overlay case.
// Replaces the 5-generator chain (generateRows -> generateWithOverlay ->
// generateWithStart -> generateWithConstraint -> generateWithFilter)
// with a single generator, eliminating 4 generator frame suspend/resume costs.
function* generateFetchDirect(
  data: BTreeSet<Row>,
  scanStart: RowBound | undefined,
  reverse: boolean | undefined,
  start: Start | undefined,
  connectionComparator: Comparator,
  constraint: Constraint | undefined,
  filterPredicate: ((row: Row) => boolean) | undefined,
): Stream<Node> {
  let started = !start;
  for (const row of data[reverse ? 'valuesFromReversed' : 'valuesFrom'](
    scanStart as Row | undefined,
  )) {
    if (!started) {
      const cmp = connectionComparator(row, start!.row);
      if (start!.basis === 'at' ? cmp >= 0 : cmp > 0) {
        started = true;
      } else {
        continue;
      }
    }
    if (constraint && !constraintMatchesRow(constraint, row)) {
      break;
    }
    if (filterPredicate && !filterPredicate(row)) {
      continue;
    }
    yield {row, relationships: EMPTY_RELATIONSHIPS};
  }
}

// Fused post-overlay processing.
// Replaces generateWithStart + generateWithConstraint + generateWithFilter
// (3 generators) with a single generator after overlay interleaving.
function* generatePostOverlayFused(
  nodes: Iterable<Node>,
  start: Start | undefined,
  connectionComparator: Comparator,
  constraint: Constraint | undefined,
  filterPredicate: ((row: Row) => boolean) | undefined,
): Stream<Node> {
  let started = !start;
  for (const node of nodes) {
    if (!started) {
      const cmp = connectionComparator(node.row, start!.row);
      if (start!.basis === 'at' ? cmp >= 0 : cmp > 0) {
        started = true;
      } else {
        continue;
      }
    }
    if (constraint && !constraintMatchesRow(constraint, node.row)) {
      break;
    }
    if (filterPredicate && !filterPredicate(node.row)) {
      continue;
    }
    yield node;
  }
}
