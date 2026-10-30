import {assert, unreachable} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import {assignProperty} from '../../../shared/src/objects.ts';
import {emptyArray} from '../../../shared/src/sentinels.ts';
import type {
  AST,
  CompoundKey,
  Ordering,
} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {SchemaValue} from '../../../zero-types/src/schema-value.ts';
import {
  AGGREGATE_COUNT_COLUMN,
  AGGREGATE_KEY_COLUMN,
  assertNotSyntheticColumn,
} from './aggregate.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import {
  makeAddChange,
  makeEditChange,
  makeRemoveChange,
  type Change,
} from './change.ts';
import {
  makeComparator,
  normalizeUndefined,
  valueToKey,
  type Node,
  type NormalizedValue,
} from './data.ts';
import {rowEqualsForCompoundKey} from './join-utils.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type InputBase,
  type Operator,
  type Output,
  type Storage,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {SourceInput} from './source.ts';
import type {Stream} from './stream.ts';

/**
 * The synthetic schema (columns, primary key and sort) of the rows
 * {@link Distinct} emits: the distinct `columns`, the key of the combination
 * (`_0_key`) and the number of input rows that have it (`_0_count`). Shared by
 * the operator (compute mode) and the client query builder
 * (`aggregatesFromSource` mode) so both agree on the synthetic source's shape.
 *
 * The key is the combination encoded as one string, rather than the columns
 * themselves, because a synced row key can't hold a `null`, and `null` is a
 * distinct value like any other.
 */
export function distinctSourceSchema(
  inputColumns: Record<string, SchemaValue>,
  ordering: Ordering,
): {
  columns: Record<string, SchemaValue>;
  primaryKey: PrimaryKey;
  sort: Ordering;
} {
  const columns: Record<string, SchemaValue> = {};
  for (const [col] of ordering) {
    assertNotSyntheticColumn(col);
    const colSchema = inputColumns[col];
    assert(colSchema, () => `Distinct: column "${col}" missing`);
    assignProperty(columns, col, colSchema);
  }
  columns[AGGREGATE_KEY_COLUMN] = {type: 'string'};
  columns[AGGREGATE_COUNT_COLUMN] = {type: 'number'};
  return {
    columns,
    primaryKey: [AGGREGATE_KEY_COLUMN],
    // The key is unique and determined by the columns, so it only makes the
    // order total.
    sort: [...ordering, [AGGREGATE_KEY_COLUMN, 'asc']],
  };
}

/**
 * The `_0_key` of a combination of distinct values, which also keys its
 * count in the operator's storage: as for Take and Cap, `valueToKey` of the
 * value when there is one column, and otherwise the values in column order,
 * JSON encoded. One query always has the same number of columns, so the two
 * forms never meet.
 */
export function distinctKey(values: readonly NormalizedValue[]): string {
  return values.length === 1 ? valueToKey(values[0]) : JSON.stringify(values);
}

/**
 * Distinct reduces its input to one row per distinct combination of
 * `columns`, as `SELECT DISTINCT` does: `{...columns, _0_key, _0_count}`,
 * where `_0_count` is the number of input rows that have the combination.
 * `null` is a value like any other.
 *
 * It sits at the end of a top-level pipeline whose rows are sorted by the
 * distinct columns first, so the rows of a combination are adjacent and a
 * fetch reads them in one pass. It keeps the count of every combination, so
 * a push adds a row when a combination gains its first input row, removes it
 * when it loses its last, and otherwise edits the count.
 */
export class Distinct implements Operator {
  readonly #input: Input;
  readonly #storage: Storage;
  readonly #columns: CompoundKey;
  readonly #schema: SourceSchema;
  // The source connection `input` reads straight from, when nothing between
  // them drops rows. It can count the combinations without the rows being
  // read (see #fetchGroups).
  readonly #groupSource: SourceInput | undefined;
  // Set by the first fetch. Until then nothing downstream has the rows, so a
  // push has nothing to update.
  #hydrated = false;

  #output: Output = throwOutput;

  /**
   * @param ordering The order of the distinct rows, which names each distinct
   *        column once. The input must be sorted by it first.
   */
  constructor(
    input: Input,
    storage: Storage,
    ordering: Ordering,
    tableName: string,
    groupSource?: SourceInput | undefined,
  ) {
    assert(ordering.length > 0, 'Distinct: needs at least one column');
    this.#input = input;
    this.#storage = storage;
    this.#columns = ordering.map(([col]) => col) as unknown as CompoundKey;
    this.#groupSource = groupSource;
    input.setOutput(this);

    const inputSchema = input.getSchema();
    const inputSort = inputSchema.sort ?? [];
    assert(
      ordering.every(
        ([col, dir], i) => inputSort[i]?.[0] === col && inputSort[i][1] === dir,
      ),
      'Distinct: the input must be sorted by the distinct columns first',
    );
    const {columns, primaryKey, sort} = distinctSourceSchema(
      inputSchema.columns,
      ordering,
    );
    this.#schema = {
      // The rows are routed (e.g. by the view-syncer streamer) to this
      // synthetic table, apart from the input table's rows.
      tableName,
      columns,
      primaryKey,
      relationships: {},
      isHidden: false,
      isAggregate: true,
      system: inputSchema.system,
      sort,
      compareRows: makeComparator(sort),
    };
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#schema;
  }

  destroy(): void {
    this.#input.destroy();
    this.#storage.destroy();
  }

  fetch(req: FetchRequest): Stream<Node | 'yield'> {
    // A top-level query without a bound, so the view reads every row once.
    assert(
      req.constraint === undefined && req.start === undefined && !req.reverse,
      'Distinct: fetch takes no constraint, start or reverse',
    );
    this.#hydrated = true;
    const groups = this.#groupSource?.groupCounts?.(this.#columns);
    if (groups === undefined) {
      return this.#fetchGroups();
    }
    const nodes = groups.map(({row, count}) =>
      this.#store(this.#groupValues(row), count),
    );
    nodes.sort((a, b) => this.#schema.compareRows(a.row, b.row));
    return nodes;
  }

  /** Reads the input and folds each run of a combination into its row. */
  *#fetchGroups(): Stream<Node | 'yield'> {
    // The first row of the current run, and how many rows it has.
    let first: Row | undefined;
    let count = 0;
    for (const node of this.#input.fetch({})) {
      if (node === 'yield') {
        yield node;
        continue;
      }
      if (
        first !== undefined &&
        rowEqualsForCompoundKey(first, node.row, this.#columns)
      ) {
        count++;
        continue;
      }
      if (first !== undefined) {
        yield this.#store(this.#groupValues(first), count);
      }
      first = node.row;
      count = 1;
    }
    if (first !== undefined) {
      yield this.#store(this.#groupValues(first), count);
    }
  }

  push(change: Change): Stream<'yield'> {
    if (!this.#hydrated) {
      return emptyArray;
    }
    switch (change[ChangeIndex.TYPE]) {
      case ChangeType.ADD:
        return this.#add(change[ChangeIndex.NODE].row);
      case ChangeType.REMOVE:
        return this.#remove(change[ChangeIndex.NODE].row);
      case ChangeType.EDIT: {
        const oldRow = change[ChangeIndex.OLD_NODE].row;
        const row = change[ChangeIndex.NODE].row;
        return rowEqualsForCompoundKey(oldRow, row, this.#columns)
          ? emptyArray
          : this.#move(oldRow, row);
      }
      case ChangeType.CHILD:
        // The input has no relationships the distinct rows show.
        return emptyArray;
      default:
        unreachable(change);
    }
  }

  reconcile(_pusher: InputBase): Stream<'yield'> {
    return this.#output.reconcile ? this.#output.reconcile(this) : emptyArray;
  }

  /** An edit that moves its row from one combination to another. */
  *#move(oldRow: Row, row: Row): Stream<'yield'> {
    yield* this.#remove(oldRow);
    yield* this.#add(row);
  }

  #add(row: Row): Stream<'yield'> {
    const values = this.#groupValues(row);
    const count = this.#count(values);
    const node = this.#store(values, count + 1);
    return this.#output.push(
      count === 0
        ? makeAddChange(node)
        : makeEditChange(node, this.#makeNode(values, count)),
      this,
    );
  }

  #remove(row: Row): Stream<'yield'> {
    const values = this.#groupValues(row);
    const count = this.#count(values);
    assert(count > 0, 'Distinct: removed a row of a combination it lacks');
    const old = this.#makeNode(values, count);
    if (count === 1) {
      this.#storage.del(distinctKey(values));
      return this.#output.push(makeRemoveChange(old), this);
    }
    return this.#output.push(
      makeEditChange(this.#store(values, count - 1), old),
      this,
    );
  }

  #count(values: readonly NormalizedValue[]): number {
    return (this.#storage.get(distinctKey(values)) as number | undefined) ?? 0;
  }

  /** Records `count` for the combination and returns its row. */
  #store(values: readonly NormalizedValue[], count: number): Node {
    this.#storage.set(distinctKey(values), count);
    return this.#makeNode(values, count);
  }

  #makeNode(values: readonly NormalizedValue[], count: number): Node {
    const row: Record<string, Value> = {};
    for (let i = 0; i < this.#columns.length; i++) {
      assignProperty(row, this.#columns[i], values[i]);
    }
    row[AGGREGATE_KEY_COLUMN] = distinctKey(values);
    row[AGGREGATE_COUNT_COLUMN] = count;
    return {row, relationships: {}};
  }

  #groupValues(row: Row): NormalizedValue[] {
    return this.#columns.map(col => normalizeUndefined(row[col]));
  }
}

/**
 * Turns the rows of a {@link Distinct}, or of the synthetic source a synced
 * client reads them from, into the rows a view of the query shows: only the
 * distinct columns, under the query's column names. The view keys a row by
 * them, and an edit that changes only the count doesn't reach it.
 */
export class DistinctProjection implements Operator {
  readonly #input: Input;
  // [the view's name, the input's name] of each distinct column.
  readonly #columns: readonly (readonly [string, string])[];
  readonly #schema: SourceSchema;

  #output: Output = throwOutput;

  constructor(input: Input, columns: readonly (readonly [string, string])[]) {
    this.#input = input;
    this.#columns = columns;
    input.setOutput(this);

    const inputSchema = input.getSchema();
    const viewName = new Map(columns.map(([out, from]) => [from, out]));
    const sort: Ordering = must(inputSchema.sort)
      .filter(([col]) => viewName.has(col))
      .map(([col, dir]) => [must(viewName.get(col)), dir] as const);
    const viewColumns: Record<string, SchemaValue> = {};
    for (const [out, from] of columns) {
      assignProperty(viewColumns, out, must(inputSchema.columns[from]));
    }
    this.#schema = {
      tableName: inputSchema.tableName,
      columns: viewColumns,
      primaryKey: columns.map(([out]) => out) as unknown as PrimaryKey,
      relationships: {},
      isHidden: false,
      system: inputSchema.system,
      sort,
      compareRows: makeComparator(sort),
    };
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#schema;
  }

  destroy(): void {
    this.#input.destroy();
  }

  *fetch(req: FetchRequest): Stream<Node | 'yield'> {
    for (const node of this.#input.fetch(req)) {
      yield node === 'yield' ? node : this.#project(node);
    }
  }

  push(change: Change): Stream<'yield'> {
    switch (change[ChangeIndex.TYPE]) {
      case ChangeType.ADD:
        return this.#output.push(
          makeAddChange(this.#project(change[ChangeIndex.NODE])),
          this,
        );
      case ChangeType.REMOVE:
        return this.#output.push(
          makeRemoveChange(this.#project(change[ChangeIndex.NODE])),
          this,
        );
      case ChangeType.EDIT:
        // A row's key is its combination of values, so an edit only moves
        // its count.
        return emptyArray;
      case ChangeType.CHILD:
        return emptyArray;
      default:
        unreachable(change);
    }
  }

  reconcile(_pusher: InputBase): Stream<'yield'> {
    return this.#output.reconcile ? this.#output.reconcile(this) : emptyArray;
  }

  #project(node: Node): Node {
    const row: Record<string, Value> = {};
    for (const [out, from] of this.#columns) {
      assignProperty(row, out, node.row[from]);
    }
    return {row, relationships: {}};
  }
}

/**
 * The order of a distinct query's rows (its `groupBy`, or the columns a count
 * of distinct combinations groups by): its `orderBy`, which names only those
 * columns, then the rest of them ascending. Other columns, such as
 * the primary key a completed ordering ends with, are left out.
 */
export function distinctOrdering(ast: AST): Ordering {
  const distinct = must(ast.groupBy ?? ast.aggregate?.distinct);
  const ordering: [string, 'asc' | 'desc'][] = [];
  for (const [col, dir] of ast.orderBy ?? []) {
    if (distinct.includes(col) && !ordering.some(([c]) => c === col)) {
      ordering.push([col, dir]);
    }
  }
  for (const col of distinct) {
    if (!ordering.some(([c]) => c === col)) {
      ordering.push([col, 'asc']);
    }
  }
  return ordering;
}
