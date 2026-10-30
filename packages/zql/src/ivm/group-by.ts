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
  AGGREGATE_KEY_COLUMN,
  AGGREGATE_VALUE_COLUMN,
  aggregateSourceSchema,
  isInvertible,
  type AggState,
  type AggStorage,
  type Reducer,
} from './aggregate.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import {
  makeAddChange,
  makeEditChange,
  makeRemoveChange,
  type Change,
} from './change.ts';
import type {Constraint} from './constraint.ts';
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
 * {@link GroupBy} emits: the group columns, the key of the group (`_0_key`)
 * and the aggregate's payload (`_0_value`, and for sum/avg its components).
 * Shared by the operator (compute mode) and the client query builder
 * (`aggregatesFromSource` mode) so both agree on the synthetic source's shape.
 *
 * The key is the group's values encoded as one string, rather than the
 * columns themselves, because a synced row key can't hold a `null`, and `null`
 * is a value like any other.
 */
export function groupSourceSchema(
  inputColumns: Record<string, SchemaValue>,
  ordering: Ordering,
  reducer: Pick<Reducer, 'fn' | 'field' | 'path'>,
): {
  columns: Record<string, SchemaValue>;
  primaryKey: PrimaryKey;
  sort: Ordering;
} {
  const {columns} = aggregateSourceSchema(
    inputColumns,
    ordering.map(([col]) => col),
    reducer.fn,
    reducer.field,
    reducer.path,
  );
  columns[AGGREGATE_KEY_COLUMN] = {type: 'string'};
  return {
    columns,
    primaryKey: [AGGREGATE_KEY_COLUMN],
    // The key is unique and determined by the columns, so it only makes the
    // order total.
    sort: [...ordering, [AGGREGATE_KEY_COLUMN, 'asc']],
  };
}

/**
 * The `_0_key` of a group, which also keys its state in the operator's
 * storage: as for Take and Cap, `valueToKey` of the value when there is one
 * column, and otherwise the values in column order, JSON encoded. One query
 * always has the same number of columns, so the two forms never meet.
 */
export function groupKey(values: readonly NormalizedValue[]): string {
  return values.length === 1 ? valueToKey(values[0]) : JSON.stringify(values);
}

/** A row's values of the group `columns`, in their order. */
export function groupValues(
  row: Row,
  columns: readonly string[],
): NormalizedValue[] {
  return columns.map(col => normalizeUndefined(row[col]));
}

/**
 * GroupBy groups its input by `columns` and reduces each group with an
 * aggregate function, as `GROUP BY` does: one row per combination of the
 * columns' values, `{...columns, _0_key, _0_value}`, plus the sum and count
 * components for sum/avg. `null` is a value like any other.
 *
 * A distinct is a GroupBy with a count, whose value a view doesn't show.
 *
 * It sits at the end of a top-level pipeline whose rows are sorted by the
 * group columns first, so the rows of a group are adjacent and a fetch reads
 * them in one pass. It keeps the state of every group, so a push adds a row
 * when a group gets its first input row, removes it when it loses its last,
 * and otherwise edits it when its value changes. Like {@link Aggregate}, a
 * min/max whose extreme a push removes is re-read from the input in
 * {@link reconcile}.
 */
export class GroupBy implements Operator {
  readonly #input: Input;
  readonly #storage: AggStorage;
  readonly #columns: CompoundKey;
  readonly #reducer: Reducer;
  readonly #schema: SourceSchema;
  // The source connection `input` reads straight from, when nothing between
  // them drops rows. It can count the groups without the rows being read
  // (see fetch).
  readonly #groupSource: SourceInput | undefined;
  // Set by the first fetch. Until then nothing downstream has the rows, so a
  // push has nothing to update.
  #hydrated = false;

  /**
   * min/max groups whose extreme a push removed, by storage key, with a row
   * of the group, to re-read in {@link reconcile}. Until then their storage
   * keeps the value last sent downstream, with the current count.
   */
  readonly #dirtyGroups = new Map<string, Row>();

  #output: Output = throwOutput;

  /**
   * @param ordering The order of the groups, which names each group column
   *        once. The input must be sorted by it first.
   */
  constructor(
    input: Input,
    storage: Storage,
    ordering: Ordering,
    tableName: string,
    reducer: Reducer,
    groupSource?: SourceInput | undefined,
  ) {
    assert(ordering.length > 0, 'GroupBy: needs at least one column');
    this.#input = input;
    this.#storage = storage as AggStorage;
    this.#columns = ordering.map(([col]) => col) as unknown as CompoundKey;
    this.#reducer = reducer;
    this.#groupSource = reducer.fn === 'count' ? groupSource : undefined;
    input.setOutput(this);

    const inputSchema = input.getSchema();
    const inputSort = inputSchema.sort ?? [];
    assert(
      ordering.every(
        ([col, dir], i) => inputSort[i]?.[0] === col && inputSort[i][1] === dir,
      ),
      'GroupBy: the input must be sorted by the group columns first',
    );
    const {columns, primaryKey, sort} = groupSourceSchema(
      inputSchema.columns,
      ordering,
      reducer,
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
      'GroupBy: fetch takes no constraint, start or reverse',
    );
    this.#hydrated = true;
    const groups = this.#groupSource?.groupCounts?.(this.#columns);
    if (groups === undefined) {
      return this.#fetchGroups();
    }
    const nodes = groups.map(({row, count}) =>
      this.#store(this.#groupValues(row), {...this.#reducer.empty(), count}),
    );
    nodes.sort((a, b) => this.#schema.compareRows(a.row, b.row));
    return nodes;
  }

  /** Reads the input and folds each run of a group into its row. */
  *#fetchGroups(): Stream<Node | 'yield'> {
    // The first row of the current run, and its group's state.
    let first: Row | undefined;
    let state: AggState | undefined;
    for (const node of this.#input.fetch({})) {
      if (node === 'yield') {
        yield node;
        continue;
      }
      if (
        first === undefined ||
        !rowEqualsForCompoundKey(first, node.row, this.#columns)
      ) {
        if (first !== undefined) {
          yield this.#store(this.#groupValues(first), must(state));
        }
        first = node.row;
        state = this.#reducer.empty();
      }
      this.#reducer.contribute(must(state), node.row);
    }
    if (first !== undefined) {
      yield this.#store(this.#groupValues(first), must(state));
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
          ? this.#update(oldRow, row)
          : this.#move(oldRow, row);
      }
      case ChangeType.CHILD:
        // The input has no relationships the group rows show.
        return emptyArray;
      default:
        unreachable(change);
    }
  }

  /**
   * Phase 2 of the two-phase push: re-read the min/max groups whose extreme
   * Phase 1 removed, now that everything upstream has settled, then forward
   * the signal for operators downstream.
   */
  reconcile(_pusher: InputBase): Stream<'yield'> {
    if (this.#dirtyGroups.size > 0) {
      return this.#reconcileDirty();
    }
    return this.#output.reconcile ? this.#output.reconcile(this) : emptyArray;
  }

  *#reconcileDirty(): Stream<'yield'> {
    const dirty = [...this.#dirtyGroups];
    this.#dirtyGroups.clear();
    for (const [key, groupRow] of dirty) {
      const stored = this.#state(key);
      if (stored === undefined) {
        continue; // its last row went in the meantime
      }
      const state = this.#reducer.empty();
      for (const node of this.#input.fetch({
        constraint: this.#constraint(groupRow),
      })) {
        if (node === 'yield') {
          yield node;
          continue;
        }
        if (rowEqualsForCompoundKey(node.row, groupRow, this.#columns)) {
          this.#reducer.contribute(state, node.row);
        }
      }
      yield* this.#emitIfChanged(this.#groupValues(groupRow), stored, state);
    }
    if (this.#output.reconcile) {
      yield* this.#output.reconcile(this);
    }
  }

  /** An edit that moves its row from one group to another. */
  *#move(oldRow: Row, row: Row): Stream<'yield'> {
    yield* this.#remove(oldRow);
    yield* this.#add(row);
  }

  #add(row: Row): Stream<'yield'> {
    const values = this.#groupValues(row);
    const key = groupKey(values);
    const stored = this.#state(key);
    if (stored === undefined) {
      const state = this.#reducer.empty();
      this.#reducer.contribute(state, row);
      return this.#output.push(makeAddChange(this.#store(values, state)), this);
    }
    const state = {...stored};
    if (this.#dirtyGroups.has(key)) {
      // Re-read in reconcile; only the count is kept up to date.
      state.count++;
      this.#storage.set(key, state);
      return emptyArray;
    }
    this.#reducer.contribute(state, row);
    return this.#emitIfChanged(values, stored, state);
  }

  #remove(row: Row): Stream<'yield'> {
    const values = this.#groupValues(row);
    const key = groupKey(values);
    const stored = must(
      this.#state(key),
      'GroupBy: removed a row of a group it lacks',
    );
    if (stored.count === 1) {
      this.#storage.del(key);
      this.#dirtyGroups.delete(key);
      return this.#output.push(
        makeRemoveChange(this.#makeNode(values, stored)),
        this,
      );
    }
    const state = {...stored};
    if (isInvertible(this.#reducer.fn)) {
      this.#reducer.applyInvertible(state, row, -1);
    } else if (
      this.#dirtyGroups.has(key) ||
      !this.#reducer.pushExtreme(
        state,
        makeRemoveChange({row, relationships: {}}),
      )
    ) {
      // Keep what was last sent downstream, with the count down by one.
      this.#dirtyGroups.set(key, row);
      this.#storage.set(key, {...stored, count: stored.count - 1});
      return emptyArray;
    }
    return this.#emitIfChanged(values, stored, state);
  }

  /** An edit within a group. */
  #update(oldRow: Row, row: Row): Stream<'yield'> {
    const values = this.#groupValues(row);
    const key = groupKey(values);
    const stored = this.#state(key);
    if (
      stored === undefined ||
      this.#dirtyGroups.has(key) ||
      this.#reducer.sameValue(oldRow, row)
    ) {
      return emptyArray;
    }
    const state = {...stored};
    if (!isInvertible(this.#reducer.fn)) {
      // The extreme may have moved in either direction.
      this.#dirtyGroups.set(key, row);
      return emptyArray;
    }
    this.#reducer.applyInvertible(state, oldRow, -1, true);
    this.#reducer.applyInvertible(state, row, 1, true);
    return this.#emitIfChanged(values, stored, state);
  }

  /** Stores `state` and sends the move from `stored` downstream, if any. */
  #emitIfChanged(
    values: readonly NormalizedValue[],
    stored: AggState,
    state: AggState,
  ): Stream<'yield'> {
    this.#storage.set(groupKey(values), state);
    if (!this.#reducer.changed(stored, state)) {
      return emptyArray;
    }
    return this.#output.push(
      makeEditChange(
        this.#makeNode(values, state),
        this.#makeNode(values, stored),
      ),
      this,
    );
  }

  #state(key: string): AggState | undefined {
    return this.#storage.get(key);
  }

  /** Records `state` for the group and returns its row. */
  #store(values: readonly NormalizedValue[], state: AggState): Node {
    this.#storage.set(groupKey(values), state);
    return this.#makeNode(values, state);
  }

  #makeNode(values: readonly NormalizedValue[], state: AggState): Node {
    const row: Record<string, Value> = {};
    for (let i = 0; i < this.#columns.length; i++) {
      assignProperty(row, this.#columns[i], values[i]);
    }
    row[AGGREGATE_KEY_COLUMN] = groupKey(values);
    this.#reducer.writePayload(row, state);
    return {row, relationships: {}};
  }

  /**
   * A constraint that reads the rows of `row`'s group. It leaves out the
   * `null` values, which a constraint never matches, so it can read rows of
   * other groups too, which the caller skips.
   */
  #constraint(row: Row): Constraint | undefined {
    let constraint: Record<string, Value> | undefined;
    for (const col of this.#columns) {
      const value = normalizeUndefined(row[col]);
      if (value !== null) {
        assignProperty((constraint ??= {}), col, value);
      }
    }
    return constraint;
  }

  #groupValues(row: Row): NormalizedValue[] {
    return groupValues(row, this.#columns);
  }
}

/**
 * Turns the rows of a {@link GroupBy}, or of the synthetic source a synced
 * client reads them from, into the rows a view of the query shows: the group
 * columns under the query's column names, and, for a `groupBy()` with an
 * aggregate, its value under `valueName` (`count`, `sum`, …). A `distinct()`
 * shows no value, so an edit, which only moves its count, doesn't reach the
 * view.
 */
export class GroupProjection implements Operator {
  readonly #input: Input;
  // [the view's name, the input's name] of each group column.
  readonly #columns: readonly (readonly [string, string])[];
  readonly #valueName: string | undefined;
  readonly #schema: SourceSchema;

  #output: Output = throwOutput;

  constructor(
    input: Input,
    columns: readonly (readonly [string, string])[],
    valueName?: string | undefined,
  ) {
    this.#input = input;
    this.#columns = columns;
    this.#valueName = valueName;
    // The value would overwrite the group column. The query builder rejects
    // such a query; a hand-built AST can still name one.
    assert(
      valueName === undefined || columns.every(([out]) => out !== valueName),
      () =>
        `groupBy(): the column "${valueName}" has the same name as the ${valueName}() value`,
    );
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
    if (valueName !== undefined) {
      assignProperty(
        viewColumns,
        valueName,
        must(inputSchema.columns[AGGREGATE_VALUE_COLUMN]),
      );
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
      case ChangeType.EDIT: {
        // A row's key is its group's values, so an edit only moves its
        // value, which a distinct doesn't show. Nothing is copied unless
        // the value changed.
        const node = change[ChangeIndex.NODE];
        const old = change[ChangeIndex.OLD_NODE];
        if (
          this.#valueName === undefined ||
          node.row[AGGREGATE_VALUE_COLUMN] === old.row[AGGREGATE_VALUE_COLUMN]
        ) {
          return emptyArray;
        }
        return this.#output.push(
          makeEditChange(this.#project(node), this.#project(old)),
          this,
        );
      }
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
    if (this.#valueName !== undefined) {
      assignProperty(row, this.#valueName, node.row[AGGREGATE_VALUE_COLUMN]);
    }
    return {row, relationships: {}};
  }
}

/**
 * The order of a grouped query's rows (its `groupBy`, or the columns a count
 * of distinct combinations groups by): its `orderBy`, which names only those
 * columns, then the rest of them ascending. Other columns, such as the
 * primary key a completed ordering ends with, are left out.
 */
export function groupOrdering(ast: AST): Ordering {
  const columns = must(ast.groupBy ?? ast.aggregate?.distinct);
  const ordering: [string, 'asc' | 'desc'][] = [];
  for (const [col, dir] of ast.orderBy ?? []) {
    if (columns.includes(col) && !ordering.some(([c]) => c === col)) {
      ordering.push([col, dir]);
    }
  }
  for (const col of columns) {
    if (!ordering.some(([c]) => c === col)) {
      ordering.push([col, 'asc']);
    }
  }
  return ordering;
}
