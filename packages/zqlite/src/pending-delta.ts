import {BTreeSet} from '../../shared/src/btree-set.ts';
import {hasOwn} from '../../shared/src/has-own.ts';
import type {Ordering, OrderPart} from '../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../zero-protocol/src/primary-key.ts';
import {
  constraintMatchesPrimaryKey,
  constraintMatchesRow,
  type Constraint,
} from '../../zql/src/ivm/constraint.ts';
import type {Comparator} from '../../zql/src/ivm/data.ts';
import {
  generateRows,
  makeBoundComparator,
  maxValue,
  minValue,
  type RowBound,
} from '../../zql/src/ivm/memory-source.ts';
import type {MultiConstraint} from '../../zql/src/ivm/operator.ts';

/**
 * The batch overlay that lets a `TableSource` derive a transaction without
 * writing to the database it is reading from.
 *
 * A `TableSource` normally makes each change durable in its backing SQLite
 * connection as soon as it has been vended to every output, because change
 * *k+1* has to read a base that already includes changes *1..k* -- IVM's
 * joins fetch from the source mid-push. On the server that backing connection
 * is a `BEGIN CONCURRENT` snapshot that is never committed and always rolled
 * back, so those writes exist only to serve reads for the rest of the pass.
 *
 * A `PendingDelta` holds them in memory instead. The base stays pinned at the
 * state it had when the pass began and this object supplies *1..k*, merged
 * into every leaf scan. The durable copy comes from the replicator's own
 * commit, which the source picks up when it leapfrogs to the next snapshot.
 *
 * State is coalesced by primary key: whatever the batch did to a row, all that
 * matters downstream is the row's final value, or that it is gone. A key that
 * appears here overrides the base unconditionally -- an added row has no base
 * to override, an edited row's base copy is suppressed in favor of the new
 * value, and a removed row's base copy is suppressed with nothing to put in
 * its place.
 *
 * Live rows are additionally kept in lazily built sorted indexes, one per
 * `[...constraintKeys, ...sort]` shape a fetch has asked for, so that merging
 * the delta into a constrained scan costs `O(log D + matches)` rather than a
 * walk of the whole delta. This mirrors `MemorySource`'s index handling; see
 * its `#fetch` for the bound construction the scans here reproduce.
 */
export class PendingDelta {
  readonly #primaryKey: PrimaryKey;
  readonly #singleColumnKey: string | undefined;

  /**
   * For a composite primary key, the first key column's value of every key in
   * `#byKey`. {@link overrides} runs for every base row of a merged fetch, and
   * building a composite key allocates, so this rules most rows out first.
   */
  readonly #firstKeyValues = new Set<Value>();

  /**
   * The coalesced net state, keyed by primary key. A `undefined` value is a
   * tombstone: the batch removed the row, so the base copy is suppressed and
   * nothing replaces it. Membership alone is what suppresses a base row, so
   * this map is also the "is this key overridden" test.
   */
  readonly #byKey = new Map<unknown, Row | undefined>();

  /** Sorted views over the live (non-tombstone) rows, keyed by `JSON(sort)`. */
  readonly #indexes = new Map<string, BTreeSet<Row>>();

  #liveCount = 0;
  #rowBytes = 0;
  #indexBytes = 0;

  constructor(primaryKey: PrimaryKey) {
    this.#primaryKey = primaryKey;
    this.#singleColumnKey = primaryKey.length === 1 ? primaryKey[0] : undefined;
  }

  /**
   * True when the source can take its untouched fast path: no suppression
   * test per base row, no merge, no index maintenance.
   */
  get isEmpty(): boolean {
    return this.#byKey.size === 0;
  }

  /** The number of rows the batch has touched, tombstones included. */
  get size(): number {
    return this.#byKey.size;
  }

  /**
   * Estimated retained bytes, including keys, tombstones, and sorted indexes.
   * This is a budgeting heuristic, not a measurement of the JavaScript heap.
   */
  get estimatedBytes(): number {
    // Budget 32 bytes per live row per index for references and B-tree nodes.
    return (
      this.#rowBytes +
      this.#indexBytes +
      this.#liveCount * this.#indexes.size * 32
    );
  }

  #key(row: Row): unknown {
    const single = this.#singleColumnKey;
    if (single !== undefined) {
      return row[single];
    }
    const key = this.#primaryKey.map(k => row[k]);
    return JSON.stringify(key);
  }

  #track(row: Row, key: unknown): void {
    if (this.#byKey.has(key)) {
      return;
    }
    this.#rowBytes += 64 + estimateBytes(key as Value);
    if (this.#singleColumnKey === undefined) {
      this.#firstKeyValues.add(row[this.#primaryKey[0]]);
    }
  }

  /** Records that `row` is now present with this value. */
  set(row: Row): void {
    const key = this.#key(row);
    const existing = this.#byKey.get(key);
    this.#track(row, key);
    this.#rowBytes +=
      estimateBytes(row) -
      (existing === undefined ? 0 : estimateBytes(existing));
    if (existing !== undefined) {
      // A live row for this key is already indexed under its old value, which
      // may sort differently. Retract it before inserting the new one.
      this.#removeFromIndexes(existing);
      this.#liveCount--;
    }
    this.#byKey.set(key, row);
    this.#addToIndexes(row);
    this.#liveCount++;
  }

  /** Records that `row` is now absent. */
  delete(row: Row): void {
    const key = this.#key(row);
    const existing = this.#byKey.get(key);
    this.#track(row, key);
    if (existing !== undefined) {
      this.#rowBytes -= estimateBytes(existing);
      this.#removeFromIndexes(existing);
      this.#liveCount--;
    }
    this.#byKey.set(key, undefined);
  }

  /**
   * Whether the batch overrides this base row. Callers use it to suppress base
   * rows during a merge, so it must be answered by primary key -- the value
   * carried by the base row is exactly what the delta is replacing.
   */
  overrides(row: Row): boolean {
    if (
      this.#singleColumnKey === undefined &&
      !this.#firstKeyValues.has(row[this.#primaryKey[0]])
    ) {
      return false;
    }
    return this.#byKey.has(this.#key(row));
  }

  /**
   * The batch's value for a row, by primary key. Returns `NOT_OVERRIDDEN` when
   * the batch has not touched the key at all (so the caller should consult the
   * base) and `undefined` when the batch removed it.
   */
  get(row: Row): Row | undefined | typeof NOT_OVERRIDDEN {
    const key = this.#key(row);
    if (!this.#byKey.has(key)) {
      return NOT_OVERRIDDEN;
    }
    return this.#byKey.get(key);
  }

  /**
   * Looks a row up by an arbitrary (not necessarily primary) unique key. Only
   * reachable when the delta is non-empty, which is never the case for the one
   * caller that uses a non-primary key, so a walk is fine here.
   */
  getByColumns(keyCols: readonly string[], keyRow: Row): Row | undefined {
    for (const row of this.#byKey.values()) {
      if (row !== undefined && keyCols.every(c => row[c] === keyRow[c])) {
        return row;
      }
    }
    return undefined;
  }

  #addToIndexes(row: Row): void {
    for (const data of this.#indexes.values()) {
      data.add(row);
    }
  }

  #removeFromIndexes(row: Row): void {
    for (const data of this.#indexes.values()) {
      data.delete(row);
    }
  }

  #getOrCreateIndex(sort: Ordering): BTreeSet<Row> {
    const indexKey = JSON.stringify(sort);
    let data = this.#indexes.get(indexKey);
    if (data === undefined) {
      data = new BTreeSet<Row>(makeBoundComparator(sort) as Comparator);
      for (const row of this.#byKey.values()) {
        if (row !== undefined) {
          data.add(row);
        }
      }
      this.#indexes.set(indexKey, data);
      this.#indexBytes += 128 + estimateBytes(indexKey) + estimateBytes(sort);
    }
    return data;
  }

  /**
   * The live rows this batch contributes to a fetch, in the same order the
   * backing SQL query returns its rows, so the two streams can be merged.
   *
   * `constraint`, `multiConstraints` and `filterPredicate` are the ones SQL
   * already applied to the base rows; they are applied here so the delta rows
   * are subject to the same query. `start` is the fetch's start row: the scan
   * begins there rather than at the start of the constraint span. It is
   * inclusive, so the caller still applies an `after` basis.
   */
  *rowsFor(
    sort: Ordering,
    constraint: Constraint | undefined,
    reverse: boolean | undefined,
    filterPredicate: ((row: Row) => boolean | undefined) | undefined,
    multiConstraints: readonly MultiConstraint[] | undefined,
    start?: Row | undefined,
  ): Iterable<Row> {
    if (this.#liveCount === 0) {
      return;
    }

    // Lead the index with the constraint keys so a constrained fetch scans
    // only the matching span; within that span rows come out in `sort` order,
    // which is the order SQL returned the base rows in.
    const indexSort: OrderPart[] = [];
    if (constraint) {
      for (const key of Object.keys(constraint)) {
        indexSort.push([key, 'asc']);
      }
    }
    // Constraining by the whole primary key admits at most one row, so the
    // requested sort adds nothing.
    const sorted =
      !constraint || !constraintMatchesPrimaryKey(constraint, this.#primaryKey);
    if (sorted) {
      indexSort.push(...sort);
    }

    const data = this.#getOrCreateIndex(indexSort);

    let scanStart: RowBound | undefined;
    if (
      start !== undefined &&
      sorted &&
      (!constraint || constraintMatchesRow(constraint, start))
    ) {
      // `start` carries every index column, and within the constraint span it
      // agrees with the constraint, so it is the scan's bound as it stands.
      // A `start` outside the span bounds nothing the scan can use.
      scanStart = start;
    } else if (constraint) {
      // The first row matching the constraint is not simply the constraint
      // values with everything else absent: a `desc` part puts absent values
      // last. The min/max sentinels say "the first row with these constraint
      // values" regardless of direction. Same construction as `MemorySource`.
      scanStart = {};
      for (const [key, dir] of indexSort) {
        if (hasOwn(constraint, key)) {
          scanStart[key] = constraint[key] as Value;
        } else if (reverse) {
          scanStart[key] = dir === 'asc' ? maxValue : minValue;
        } else {
          scanStart[key] = dir === 'asc' ? minValue : maxValue;
        }
      }
    }

    for (const row of generateRows(data, scanStart, reverse)) {
      if (constraint && !constraintMatchesRow(constraint, row)) {
        // Rows are sorted by the constraint keys first, so matches are
        // contiguous and the first miss ends the span.
        break;
      }
      if (multiConstraints && !matchesMultiConstraints(row, multiConstraints)) {
        continue;
      }
      if (filterPredicate && !filterPredicate(row)) {
        continue;
      }
      yield row;
    }
  }

  /**
   * A row with the primary key of each row the batch has touched, removed
   * rows included. A live row is returned as is.
   */
  *touchedKeys(): Iterable<Row> {
    const single = this.#singleColumnKey;
    for (const [key, row] of this.#byKey) {
      if (row !== undefined) {
        yield row;
      } else if (single !== undefined) {
        yield {[single]: key as Value};
      } else {
        const values = JSON.parse(key as string) as Value[];
        yield Object.fromEntries(
          this.#primaryKey.map((k, i) => [k, values[i]]),
        );
      }
    }
  }

  /** The rows the batch has added or edited, and not since removed. */
  *liveRows(): Iterable<Row> {
    for (const row of this.#byKey.values()) {
      if (row !== undefined) {
        yield row;
      }
    }
  }

  /** Drops the batch. Called when the source moves to a snapshot that has it. */
  clear(): void {
    this.#byKey.clear();
    this.#firstKeyValues.clear();
    this.#indexes.clear();
    this.#liveCount = 0;
    this.#rowBytes = 0;
    this.#indexBytes = 0;
  }
}

// Allow for object/property storage and UTF-16 strings without allocating a
// serialized copy. Shared values may be counted more than once intentionally.
function estimateBytes(value: Value): number {
  if (typeof value === 'string') {
    return 24 + value.length * 2;
  }
  if (value === null || typeof value !== 'object') {
    return 8;
  }
  let bytes = 32;
  if (Array.isArray(value)) {
    for (const item of value) {
      bytes += 8 + estimateBytes(item);
    }
  } else {
    for (const key in value) {
      bytes += 16 + estimateBytes(key) + estimateBytes((value as Row)[key]);
    }
  }
  return bytes;
}

export const NOT_OVERRIDDEN = Symbol('not-overridden');

function matchesMultiConstraints(
  row: Row,
  multiConstraints: readonly MultiConstraint[],
): boolean {
  for (const mc of multiConstraints) {
    if (mc.length === 0) {
      continue;
    }
    let any = false;
    for (const c of mc) {
      if (constraintMatchesRow(c, row)) {
        any = true;
        break;
      }
    }
    if (!any) {
      return false;
    }
  }
  return true;
}

/**
 * Merges a batch's rows into an ordered base stream and drops the base rows
 * the batch overrides.
 *
 * Both streams must already be in `compare` order. A delta row that ties with
 * a base row is the *same* row by primary key -- every fetch ordering includes
 * the primary key -- so the base copy is suppressed and the delta's value
 * takes its place at that position.
 */
export function* generateWithPendingDelta(
  baseRows: Iterable<Row>,
  deltaRows: Iterable<Row>,
  delta: PendingDelta,
  compare: Comparator,
): IterableIterator<Row> {
  const deltaIter = deltaRows[Symbol.iterator]();
  let pending = deltaIter.next();
  try {
    for (const row of baseRows) {
      while (!pending.done && compare(pending.value, row) < 0) {
        yield pending.value;
        pending = deltaIter.next();
      }
      if (!delta.overrides(row)) {
        yield row;
      }
    }
    while (!pending.done) {
      yield pending.value;
      pending = deltaIter.next();
    }
  } finally {
    // Propagate early termination so the delta iterator can release the BTree
    // cursor it is walking, the same contract `mergeSortedStreams` documents.
    if (!pending.done) {
      deltaIter.return?.(undefined);
    }
  }
}

/**
 * The unordered counterpart. Nothing downstream depends on position, so the
 * surviving base rows are emitted first and the batch's rows follow.
 */
export function* generateWithPendingDeltaUnordered(
  baseRows: Iterable<Row>,
  deltaRows: Iterable<Row>,
  delta: PendingDelta,
): IterableIterator<Row> {
  for (const row of baseRows) {
    if (!delta.overrides(row)) {
      yield row;
    }
  }
  yield* deltaRows;
}
