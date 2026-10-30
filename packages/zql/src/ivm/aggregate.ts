import {assert, unreachable} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import {assignProperty} from '../../../shared/src/objects.ts';
import {
  aggregateValue,
  type AggregateFunction,
  type Ordering,
} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {SchemaValue} from '../../../zero-types/src/schema-value.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import {makeEditChange, type Change} from './change.ts';
import type {Constraint} from './constraint.ts';
import {
  compareValues,
  makeComparator,
  normalizeUndefined,
  valueToKey,
  type Node,
  type NormalizedValue,
} from './data.ts';
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
import {type Stream} from './stream.ts';

/**
 * The name of the synthetic column that holds the aggregate result in the rows
 * this operator emits. The value is projected to a bare scalar in the view, so
 * this name is internal.
 *
 * The synthetic columns share the emitted row with the group key, whose names
 * come from the user's schema, so they use the reserved `_0_` prefix (as
 * `_0_version` does) rather than plain words a schema could also use.
 */
export const AGGREGATE_VALUE_COLUMN = '_0_value';

/**
 * A top-level (ungrouped) aggregate has no group key, but the synced path keys
 * every row in the CVR and forbids empty row keys (see zero-cache
 * `types/row-key.ts`). Since there is no group key, the single global row is
 * given one synthetic, constant-valued key column so it still has a (non-empty)
 * primary key. The materialized-view path ignores it (it projects only
 * `AGGREGATE_VALUE_COLUMN`), so this name is internal.
 *
 * The row holds nothing but this column and the payload columns, so any name
 * other than theirs (and `_0_version`) is collision-free. It must not be empty:
 * a SQLite-backed source cannot declare a zero-length column name.
 */
export const AGGREGATE_KEY_COLUMN = '_0_key';

/** The constant value of {@link AGGREGATE_KEY_COLUMN} (one global row). */
const AGGREGATE_KEY_VALUE = 0;

/**
 * `sum` and `avg` additionally carry their running components so the synced
 * client can adjust them optimistically: you can't move an average from the
 * final value alone, and a sum's value alone can't tell an empty group (`null`)
 * from one summing to 0. The value column stays the result (for display); these are the
 * sum of non-null field values and the count of non-null contributors.
 */
export const AGGREGATE_SUM_COLUMN = '_0_sum';
export const AGGREGATE_COUNT_COLUMN = '_0_count';

/**
 * Asserts that the aggregate group key column `col` is not one of the payload
 * columns. The payload columns are written over the group key in the emitted
 * row and stripped from it by the client's row key, so a collision would
 * silently merge every group into one. Their reserved `_0_` prefix keeps real
 * schemas clear of this; the check guards the invariant.
 */
export function assertNotPayloadColumn(col: string): void {
  assert(
    !AGGREGATE_PAYLOAD_COLUMNS.has(col),
    () =>
      `Aggregate: group key column "${col}" collides with the synthetic ` +
      `aggregate column of the same name`,
  );
}

/** Whether `fn`'s rows carry the sum and count components. */
function carriesComponents(fn: AggregateFunction): boolean {
  return fn === 'sum' || fn === 'avg';
}

/** The synthetic payload columns (not part of an aggregate row's key). */
export const AGGREGATE_PAYLOAD_COLUMNS: ReadonlySet<string> = new Set([
  AGGREGATE_VALUE_COLUMN,
  AGGREGATE_SUM_COLUMN,
  AGGREGATE_COUNT_COLUMN,
]);

/**
 * The type of the synthetic value column for an aggregate result: `count` is a
 * non-null number; `sum`/`avg` are nullable numbers; `min`/`max` take the
 * field's own type (`json` for a JSON path, whose leaf can be a number or a
 * string) and are nullable (empty group).
 */
function aggregateValueColumn(
  fn: AggregateFunction,
  field: string | undefined,
  inputColumns: Record<string, SchemaValue>,
  path: readonly (string | number)[] | undefined,
): SchemaValue {
  switch (fn) {
    case 'count':
      return {type: 'number'};
    case 'sum':
    case 'avg':
      return {type: 'number', optional: true};
    case 'min':
    case 'max': {
      if (path !== undefined) {
        // A JSON leaf: a number or a string, decided per value.
        return {type: 'json', optional: true};
      }
      const fieldSchema = inputColumns[must(field)];
      assert(fieldSchema, () => `Aggregate: ${fn} field "${field}" missing`);
      return {...fieldSchema, optional: true};
    }
    default:
      unreachable(fn);
  }
}

/**
 * The synthetic schema (columns + primary key) an aggregate produces, derived
 * from the *input* columns (the rows being aggregated), the group key, and the
 * fn/field. Shared by the {@link Aggregate} operator (compute mode) and the
 * client query builder (`aggregatesFromSource` mode) so both agree on the
 * synthetic source's shape. For a top-level (empty group key) aggregate the
 * single synthetic key column is added; otherwise the group key is the key.
 */
export function aggregateSourceSchema(
  inputColumns: Record<string, SchemaValue>,
  groupKey: readonly string[],
  fn: AggregateFunction,
  field: string | undefined,
  path?: readonly (string | number)[] | undefined,
): {columns: Record<string, SchemaValue>; primaryKey: readonly string[]} {
  const columns: Record<string, SchemaValue> = {};
  for (const col of groupKey) {
    assertNotPayloadColumn(col);
    const colSchema = inputColumns[col];
    assert(colSchema, () => `Aggregate: group key column "${col}" missing`);
    assignProperty(columns, col, colSchema);
  }
  const keyColumns = groupKey.length > 0 ? groupKey : [AGGREGATE_KEY_COLUMN];
  if (groupKey.length === 0) {
    columns[AGGREGATE_KEY_COLUMN] = {type: 'number'};
  }
  columns[AGGREGATE_VALUE_COLUMN] = aggregateValueColumn(
    fn,
    field,
    inputColumns,
    path,
  );
  if (carriesComponents(fn)) {
    // Components for optimistic adjustment on the synced client.
    columns[AGGREGATE_SUM_COLUMN] = {type: 'number'};
    columns[AGGREGATE_COUNT_COLUMN] = {type: 'number'};
  }
  return {columns, primaryKey: keyColumns};
}

/**
 * Per-group running state. We keep enough to derive any supported function:
 *  - `count`    → count(*)                (all rows)
 *  - `sum`      → sum of non-null         (NULL when nonNull === 0)
 *  - `avg`      → sum / nonNull           (NULL when nonNull === 0)
 *  - `min`/`max`→ extreme of non-null     (NULL when nonNull === 0)
 */
type AggState = {
  count: number;
  sum: number;
  nonNull: number;
  /** Current min/max of the non-null field values (null when nonNull === 0). */
  extreme: NormalizedValue;
};

/** Typed view of the operator's {@link Storage} (mirrors Take). */
interface AggStorage {
  get(key: string): AggState | undefined;
  set(key: string, value: AggState): void;
  destroy(): void;
}

/**
 * Whether a change to one input row maps to a fixed delta of the aggregate
 * (`count`/`sum`/`avg`). Shared with the synced client, which only applies
 * optimistic deltas to these functions.
 */
export const isInvertible = (fn: AggregateFunction) =>
  fn === 'count' || fn === 'sum' || fn === 'avg';

/**
 * Aggregate is a reducing operator for `count(*)` / `sum` / `avg` / `min` /
 * `max` over a correlated relationship.
 *
 * It sits at the end of a related-subquery pipeline (where a normal `related`
 * would emit child rows) and collapses the child rows for each parent into a
 * *single* synthetic row `{ ...groupKey, value }`. Carrying the group key keeps
 * the {@link Join} unchanged — it routes child changes by childField.
 *
 * Two maintenance strategies share the same fetch/emit machinery:
 *
 *  - **Invertible** (count/sum/avg): add/remove/edit adjust running numbers in
 *    O(1); no re-fetch.
 *  - **Non-invertible** (min/max): add and removing a non-extreme value are
 *    O(1), but removing (or editing away) the current extreme is not
 *    invertible — the new extreme is unknown. In that case the group is marked
 *    dirty and re-fetched from the source in {@link reconcile}, as {@link Take}
 *    refills a partition. Re-fetching during the push itself could read
 *    operators upstream (e.g. an `EXISTS` under an `OR`) before they have
 *    settled the rest of the change.
 *
 * Null/empty semantics match SQL: every function except `count(*)` ignores null
 * field values and is NULL for an empty (or all-null) group; `count(*)` counts
 * all rows (0 when empty). An EDIT is emitted only when the derived value
 * actually changes.
 */
export class Aggregate implements Operator {
  readonly #input: Input;
  readonly #storage: AggStorage;
  // Empty for a top-level (ungrouped) aggregate — a single global group.
  readonly #groupKey: readonly string[];
  readonly #fn: AggregateFunction;
  readonly #field: string | undefined;
  readonly #path: readonly (string | number)[] | undefined;
  readonly #schema: SourceSchema;
  // The source connection `input` reads straight from, when nothing between
  // them drops rows. A `count` asks it for the number instead of reading every
  // row (see #compute).
  readonly #countSource: SourceInput | undefined;

  #output: Output = throwOutput;

  /**
   * min/max groups whose extreme a push removed, by storage key, to recompute
   * in {@link reconcile}. Until then their storage keeps the state last sent
   * downstream, which a fetch keeps returning.
   */
  readonly #dirtyGroups = new Map<string, readonly NormalizedValue[]>();

  constructor(
    input: Input,
    storage: Storage,
    groupKey: readonly string[],
    fn: AggregateFunction,
    field?: string,
    tableName?: string,
    countSource?: SourceInput,
    path?: readonly (string | number)[],
  ) {
    assert(
      fn === 'count' ? field === undefined : field !== undefined,
      () =>
        `Aggregate: ${fn} ${fn === 'count' ? 'takes no' : 'requires a'} field`,
    );
    assert(
      path === undefined || field !== undefined,
      'Aggregate: a JSON path requires a field',
    );
    this.#input = input;
    this.#storage = storage as AggStorage;
    this.#groupKey = groupKey;
    this.#fn = fn;
    this.#field = field;
    this.#countSource = fn === 'count' ? countSource : undefined;
    this.#path = path;
    input.setOutput(this);

    const inputSchema = input.getSchema();
    // Top-level (ungrouped) aggregate synthesizes a single constant-valued key
    // column (so the one global row has a non-empty primary key, required by the
    // synced/CVR path); grouped/relationship aggregates use the group key. The
    // same derivation runs on the client (aggregatesFromSource), so the two
    // sides agree on the synthetic source shape.
    const {columns, primaryKey: keyColumns} = aggregateSourceSchema(
      inputSchema.columns,
      groupKey,
      fn,
      field,
      path,
    );

    // Exactly one row per key, so the key is the primary key and a valid
    // (stable) sort.
    const sort: Ordering = keyColumns.map(col => [col, 'asc'] as const);
    this.#schema = {
      // The synthetic rows are routed (e.g. by the view-syncer streamer) to a
      // synthetic table, distinct from the child table, so they are never
      // confused with real child rows.
      tableName: tableName ?? inputSchema.tableName,
      columns,
      // The group key (relationship aggregate) or the synthetic singleton key
      // (top-level aggregate) — one row per key in either case.
      primaryKey: keyColumns as unknown as PrimaryKey,
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

  /** The scalar result for the current state, per the aggregate function. */
  #value(s: AggState): Value {
    switch (this.#fn) {
      case 'count':
        return s.count;
      case 'sum':
        return s.nonNull === 0 ? null : s.sum;
      case 'avg':
        return s.nonNull === 0 ? null : s.sum / s.nonNull;
      case 'min':
      case 'max':
        return s.extreme; // null when nonNull === 0
      default:
        unreachable(this.#fn);
    }
  }

  /** Whether `v` should replace the current extreme for a min/max. */
  #beats(v: NormalizedValue, current: NormalizedValue): boolean {
    const c = compareAggregateValues(v, current);
    return this.#fn === 'max' ? c > 0 : c < 0;
  }

  /**
   * The value `row` contributes: its field, or the JSON leaf at the path (see
   * `aggregateValue`), `null` when it contributes nothing.
   */
  #read(row: Row): NormalizedValue {
    const field = must(this.#field);
    return normalizeUndefined(
      this.#path === undefined
        ? row[field]
        : (aggregateValue(
            {fn: this.#fn, field, path: this.#path},
            row,
          ) as Value),
    );
  }

  /** Add one row's contribution to a (freshly accumulating) state. */
  #contribute(s: AggState, row: Row): void {
    s.count++;
    if (this.#field === undefined) {
      return;
    }
    const v = this.#read(row);
    if (v === null) {
      return;
    }
    s.nonNull++;
    switch (this.#fn) {
      case 'sum':
      case 'avg':
        assert(
          typeof v === 'number',
          () => `Aggregate: ${this.#fn} needs a number`,
        );
        s.sum += v;
        break;
      case 'min':
      case 'max':
        if (s.extreme === null || this.#beats(v, s.extreme)) {
          s.extreme = v;
        }
        break;
      default:
        break;
    }
  }

  /** Recompute the whole group state from the source (already up to date). */
  *#compute(constraint: Constraint | undefined): Generator<'yield', AggState> {
    const s: AggState = {count: 0, sum: 0, nonNull: 0, extreme: null};
    const count = this.#countSource?.count?.(constraint);
    if (count !== undefined) {
      s.count = count;
      return s;
    }
    for (const node of this.#input.fetch({constraint})) {
      if (node === 'yield') {
        yield node;
        continue;
      }
      this.#contribute(s, node.row);
    }
    return s;
  }

  *fetch(req: FetchRequest): Stream<Node | 'yield'> {
    const groupValues = this.#groupValuesFromConstraint(req.constraint);
    const key = stateKey(groupValues);

    let state = this.#storage.get(key);
    if (state === undefined) {
      state = yield* this.#compute(req.constraint);
      this.#storage.set(key, state);
    }

    yield this.#makeNode(groupValues, state);
  }

  /**
   * Phase 2 of the two-phase push: recompute the min/max groups whose extreme
   * Phase 1 removed, now that everything upstream has settled, then forward the
   * signal for bounded operators downstream.
   */
  *reconcile(_pusher: InputBase): Stream<'yield'> {
    if (this.#dirtyGroups.size > 0) {
      const dirty = [...this.#dirtyGroups];
      this.#dirtyGroups.clear();
      for (const [key, groupValues] of dirty) {
        const stored = must(this.#storage.get(key));
        const state = yield* this.#compute(
          this.#constraintFromGroup(groupValues),
        );
        this.#storage.set(key, state);
        yield* this.#emitIfChanged(groupValues, stored, state);
      }
    }
    if (this.#output.reconcile) {
      yield* this.#output.reconcile(this);
    }
  }

  *push(change: Change): Stream<'yield'> {
    const groupValues = this.#groupValuesFromRow(change[ChangeIndex.NODE].row);
    const key = stateKey(groupValues);
    const stored = this.#storage.get(key);

    // If we have no state for this group, nothing downstream has materialized
    // it, so there is nothing to update. When it is later fetched, fetch()
    // computes the true state from the source (which has already applied this
    // change). Mirrors Take's no-state no-op.
    if (stored === undefined) {
      return;
    }

    const state: AggState = {...stored};

    if (isInvertible(this.#fn)) {
      switch (change[ChangeIndex.TYPE]) {
        case ChangeType.ADD:
          this.#applyInvertible(state, change[ChangeIndex.NODE].row, 1);
          break;
        case ChangeType.REMOVE:
          this.#applyInvertible(state, change[ChangeIndex.NODE].row, -1);
          break;
        case ChangeType.EDIT: {
          // Group key unchanged (Join guarantees it); only the field moves the
          // value. count(*) is unaffected, and so is a sum/avg whose field kept
          // its value: subtracting and re-adding it would only add rounding
          // error.
          if (
            this.#field === undefined ||
            compareAggregateValues(
              this.#read(change[ChangeIndex.OLD_NODE].row),
              this.#read(change[ChangeIndex.NODE].row),
            ) === 0
          ) {
            return;
          }
          // Remove the old contribution, add the new.
          this.#applyInvertible(
            state,
            change[ChangeIndex.OLD_NODE].row,
            -1,
            true,
          );
          this.#applyInvertible(state, change[ChangeIndex.NODE].row, 1, true);
          break;
        }
        case ChangeType.CHILD:
          return;
        default:
          unreachable(change);
      }
    } else {
      // min/max — non-invertible. A dirty group is recomputed from the source
      // in reconcile, which covers this change too.
      if (this.#dirtyGroups.has(key) || !this.#pushExtreme(state, change)) {
        this.#dirtyGroups.set(key, groupValues);
        return;
      }
    }

    assert(state.count >= 0, 'Aggregate: removed a row from an empty group');
    this.#storage.set(key, state);
    yield* this.#emitIfChanged(groupValues, stored, state);
  }

  /** Sends downstream the move of a group from `stored` to `state`, if any. */
  *#emitIfChanged(
    groupValues: readonly NormalizedValue[],
    stored: AggState,
    state: AggState,
  ): Stream<'yield'> {
    // Emit when the value changes, or — for sum/avg — when the components
    // change even though the value doesn't (e.g. adding a 0 to a sum, or a row
    // equal to the average), so the synced client's optimistic-delta base stays
    // in sync.
    const changed =
      this.#value(stored) !== this.#value(state) ||
      (carriesComponents(this.#fn) &&
        (stored.sum !== state.sum || stored.nonNull !== state.nonNull));
    if (changed) {
      yield* this.#output.push(
        makeEditChange(
          this.#makeNode(groupValues, state),
          this.#makeNode(groupValues, stored),
        ),
        this,
      );
    }
  }

  /** Invertible delta for count/sum/avg. */
  #applyInvertible(
    state: AggState,
    row: Row,
    sign: 1 | -1,
    fieldOnly = false,
  ): void {
    if (!fieldOnly) {
      state.count += sign;
    }
    if (this.#field !== undefined) {
      const v = this.#read(row);
      if (v !== null) {
        assert(
          typeof v === 'number',
          () => `Aggregate: ${this.#fn}(${this.#field}) requires a number`,
        );
        state.sum += sign * v;
        state.nonNull += sign;
      }
    }
  }

  /**
   * min/max maintenance, applied to `state` in place. Cheap for adds and for
   * removing a non-extreme value. Returns false when the current extreme is
   * removed or edited away: the new extreme is not recoverable from running
   * state, so the group has to be re-fetched.
   */
  #pushExtreme(state: AggState, change: Change): boolean {
    switch (change[ChangeIndex.TYPE]) {
      case ChangeType.ADD:
        // Adding can only extend the extreme outward — O(1).
        this.#contribute(state, change[ChangeIndex.NODE].row);
        return true;
      case ChangeType.REMOVE: {
        const v = this.#read(change[ChangeIndex.NODE].row);
        state.count--;
        if (v === null) {
          return true; // null never contributed to the extreme
        }
        if (
          state.extreme !== null &&
          compareAggregateValues(v, state.extreme) === 0
        ) {
          return false; // removed (one of) the extreme
        }
        state.nonNull--; // a non-extreme value left; extreme unchanged
        return true;
      }
      case ChangeType.EDIT: {
        const oldV = this.#read(change[ChangeIndex.OLD_NODE].row);
        const newV = this.#read(change[ChangeIndex.NODE].row);
        // When the field moved, the extreme may have changed in either
        // direction.
        return compareAggregateValues(oldV, newV) === 0;
      }
      case ChangeType.CHILD:
        return true;
      default:
        unreachable(change);
    }
  }

  #makeNode(groupValues: readonly NormalizedValue[], state: AggState): Node {
    const row: Record<string, Value> = {};
    for (let i = 0; i < this.#groupKey.length; i++) {
      assignProperty(row, this.#groupKey[i], groupValues[i]);
    }
    if (this.#groupKey.length === 0) {
      // Top-level (ungrouped): the single row carries the synthetic key column.
      row[AGGREGATE_KEY_COLUMN] = AGGREGATE_KEY_VALUE;
    }
    row[AGGREGATE_VALUE_COLUMN] = this.#value(state);
    if (carriesComponents(this.#fn)) {
      // Carry the running components so the synced client can adjust them.
      row[AGGREGATE_SUM_COLUMN] = state.sum;
      row[AGGREGATE_COUNT_COLUMN] = state.nonNull;
    }
    return {row, relationships: {}};
  }

  #constraintFromGroup(groupValues: readonly NormalizedValue[]): Constraint {
    const constraint: Record<string, Value> = {};
    for (let i = 0; i < this.#groupKey.length; i++) {
      assignProperty(constraint, this.#groupKey[i], groupValues[i]);
    }
    return constraint;
  }

  #groupValuesFromConstraint(
    constraint: Constraint | undefined,
  ): NormalizedValue[] {
    if (this.#groupKey.length === 0) {
      return []; // top-level (ungrouped) — one global group, no constraint
    }
    assert(
      constraint,
      'Aggregate requires a constraint on the group key (relationship aggregate only)',
    );
    return this.#groupKey.map(col => {
      assert(
        Object.hasOwn(constraint, col),
        () => `Aggregate: constraint missing group key column "${col}"`,
      );
      return normalizeUndefined(constraint[col]);
    });
  }

  #groupValuesFromRow(row: Row): NormalizedValue[] {
    return this.#groupKey.map(col => normalizeUndefined(row[col]));
  }
}

/**
 * Orders the values a min/max reduces. A JSON path's leaves can be numbers or
 * strings, decided per value; numbers order before strings, as in JS and
 * SQLite (z2s compiles the same rule rather than Postgres's jsonb ordering,
 * which puts strings first). Otherwise it is {@link compareValues}.
 */
function compareAggregateValues(
  a: NormalizedValue,
  b: NormalizedValue,
): number {
  if (
    a !== null &&
    b !== null &&
    typeof a !== typeof b &&
    (typeof a === 'number' || typeof a === 'string') &&
    (typeof b === 'number' || typeof b === 'string')
  ) {
    return typeof a === 'number' ? -1 : 1;
  }
  return compareValues(a, b);
}

/**
 * The storage key of a group's state. A relationship is usually correlated on
 * one column, which gets the cheaper single-value encoding Take and Cap use;
 * an operator's group key has a fixed length, so the two forms never meet.
 */
function stateKey(groupValues: readonly NormalizedValue[]): string {
  return groupValues.length === 1
    ? valueToKey(groupValues[0])
    : JSON.stringify(groupValues);
}
