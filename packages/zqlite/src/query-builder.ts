import type {SQLQuery} from '@databases/sql';
import {assert} from '../../shared/src/asserts.ts';
import type {
  Condition,
  Ordering,
  SimpleCondition,
  ValuePosition,
} from '../../zero-protocol/src/ast.ts';
import type {
  SchemaValue,
  ValueType,
} from '../../zero-schema/src/table-schema.ts';
import type {Constraint} from '../../zql/src/ivm/constraint.ts';
import type {
  FetchRequest,
  MultiConstraint,
  Start,
} from '../../zql/src/ivm/operator.ts';
import {format, sql} from './internal/sql.ts';
import {normalizeWhitespace} from './internal/statement-cache.ts';

/**
 * Condition type without correlated subqueries.
 * This matches the output of transformFilters from zql/builder/filter.ts
 */
export type NoSubqueryCondition = Exclude<
  Condition,
  {type: 'correlatedSubquery'}
>;

/** The parts of a {@link FetchRequest} that a select query is built from. */
export type SelectRequest = Pick<
  FetchRequest,
  'constraint' | 'start' | 'reverse' | 'multiConstraints'
> & {readonly filter?: NoSubqueryCondition | undefined};

/** Reads the value of a statement parameter from a request. */
export type Binding = (req: SelectRequest) => unknown;

/**
 * A select query compiled for one shape of request: its SQL, with whitespace
 * normalized as the statement cache does, and how to read each of its
 * parameters from a request of that shape. See {@link compileSelectQuery}.
 */
export type CompiledSelectQuery = {
  readonly text: string;
  readonly bindings: readonly Binding[];
};

/**
 * Emits the SQL for a parameter. `value` is its value in the request the
 * query is being built for, and `get` reads it from any request of the same
 * shape.
 */
type Bind = (value: unknown, get: Binding) => SQLQuery;

const bindValue: Bind = value => sql`${value}`;

class Slot {
  readonly get: Binding;
  constructor(get: Binding) {
    this.get = get;
  }
}

const bindSlot: Bind = (_, get) => sql`${new Slot(get)}`;

export function buildSelectQuery(
  tableName: string,
  columns: Record<string, SchemaValue>,
  constraint: Constraint | undefined,
  filters: NoSubqueryCondition | undefined,
  order: Ordering | undefined,
  reverse: boolean | undefined,
  start: Start | undefined,
  multiConstraints?: readonly MultiConstraint[] | undefined,
  fetchFilters?: NoSubqueryCondition | undefined,
) {
  return selectQuery(
    tableName,
    columns,
    {constraint, reverse, start, multiConstraints, filter: fetchFilters},
    filters,
    order,
    bindValue,
  );
}

/**
 * Compiles the select query for `req` so that it can be reused for any
 * request of the same shape, i.e. one that differs from `req` only in the
 * values of its constraint, multi-constraints and start row, and in no way
 * that changes the SQL: the same constraint and multi-constraint keys (in the
 * same order), the same number of multi-constraint entries, the same start
 * basis and NULL start values, the same `reverse`, and the same `filter`.
 *
 * The values of `filters` and `req.filter` are compiled in as they are.
 */
export function compileSelectQuery(
  tableName: string,
  columns: Record<string, SchemaValue>,
  req: SelectRequest,
  filters: NoSubqueryCondition | undefined,
  order: Ordering | undefined,
): CompiledSelectQuery {
  const {text, values} = format(
    selectQuery(tableName, columns, req, filters, order, bindSlot),
  );
  return {
    text: normalizeWhitespace(text),
    bindings: values.map(v => (v instanceof Slot ? v.get : () => v)),
  };
}

function selectQuery(
  tableName: string,
  columns: Record<string, SchemaValue>,
  req: SelectRequest,
  filters: NoSubqueryCondition | undefined,
  order: Ordering | undefined,
  bind: Bind,
) {
  const {
    constraint,
    reverse,
    start,
    multiConstraints,
    filter: fetchFilters,
  } = req;
  let query = sql`SELECT ${sql.join(
    Object.keys(columns).map(c => sql.ident(c)),
    sql`,`,
  )} FROM ${sql.ident(tableName)}`;
  const constraints: SQLQuery[] = constraintsToSQL(constraint, columns, bind);

  if (multiConstraints) {
    for (let i = 0; i < multiConstraints.length; i++) {
      const mc = multiConstraints[i];
      if (mc.length > 0) {
        constraints.push(multiConstraintToSQL(mc, columns, bind, i));
      }
    }
  }

  if (start) {
    assert(order !== undefined, 'start requires ordering');
    constraints.push(
      gatherStartConstraints(start, reverse, order, columns, bind),
    );
  }

  if (filters) {
    constraints.push(filtersToSQL(filters));
  }

  if (fetchFilters) {
    constraints.push(filtersToSQL(fetchFilters));
  }

  if (constraints.length > 0) {
    query = sql`${query} WHERE ${sql.join(constraints, sql` AND `)}`;
  }

  if (order && order.length > 0) {
    return sql`${query} ${orderByToSQL(order, !!reverse)}`;
  }
  return query;
}

export function constraintsToSQL(
  constraint: Constraint | undefined,
  columns: Record<string, SchemaValue>,
  bind: Bind = bindValue,
) {
  if (!constraint) {
    return [];
  }

  const constraints: SQLQuery[] = [];
  for (const [key, value] of Object.entries(constraint)) {
    const {type} = columns[key];
    constraints.push(
      sql`${sql.ident(key)} = ${bind(toSQLiteType(value, type), req =>
        toSQLiteType(req.constraint?.[key], type),
      )}`,
    );
  }

  return constraints;
}

/**
 * Builds a single batched IN clause from a `MultiConstraint`. All entries
 * are assumed to share the same shape (the keys of the first entry);
 * FlippedJoin derives them from the same parentKey for all children.
 *
 * Single-column form: `col IN (?, ?, ?)`
 * Compound form:      `(a, b) IN (VALUES (?, ?), (?, ?), …)`
 *
 * NOTE: SQLite optimizes `col IN (literal-list)` using the column's index;
 * verified via EXPLAIN QUERY PLAN — see query-builder.test.ts.
 */
export function multiConstraintToSQL(
  multiConstraint: MultiConstraint,
  columns: Record<string, SchemaValue>,
  bind: Bind = bindValue,
  // The index of `multiConstraint` in the request's `multiConstraints`.
  index = 0,
): SQLQuery {
  const param = (i: number, key: string, type: ValueType) =>
    bind(toSQLiteType(multiConstraint[i][key], type), req =>
      toSQLiteType(req.multiConstraints?.[index][i][key], type),
    );

  assert(multiConstraint.length > 0, 'multiConstraint must be non-empty');
  // All entries share the same keys; pull the column list from the first.
  const keys = Object.keys(multiConstraint[0]);
  assert(keys.length > 0, 'multiConstraint entries must have at least one key');
  // Subsequent entries must share the first entry's shape — the SQL form
  // is `(col_a, col_b, …) IN VALUES (…)`, with one binding per key per
  // entry. Heterogeneous keys would silently produce incorrect bindings.
  for (let i = 1; i < multiConstraint.length; i++) {
    const entry = multiConstraint[i];
    assert(
      Object.keys(entry).length === keys.length && keys.every(k => k in entry),
      () =>
        `multiConstraint entries must share the same keys (entry 0: [${keys.join(
          ',',
        )}], entry ${i}: [${Object.keys(entry).join(',')}])`,
    );
  }

  if (keys.length === 1) {
    const key = keys[0];
    const colType = columns[key].type;
    return sql`${sql.ident(key)} IN (${sql.join(
      multiConstraint.map((_, i) => param(i, key, colType)),
      sql`,`,
    )})`;
  }

  // Compound: `(col_a, col_b, …) IN (VALUES (?, ?, …), …)`
  const colList = sql`(${sql.join(
    keys.map(k => sql.ident(k)),
    sql`,`,
  )})`;
  const rows = multiConstraint.map(
    (_, i) =>
      sql`(${sql.join(
        keys.map(k => param(i, k, columns[k].type)),
        sql`,`,
      )})`,
  );
  return sql`${colList} IN (VALUES ${sql.join(rows, sql`,`)})`;
}

export function orderByToSQL(order: Ordering, reverse: boolean): SQLQuery {
  if (reverse) {
    return sql`ORDER BY ${sql.join(
      order.map(
        s =>
          sql`${sql.ident(s[0])} ${sql.__dangerous__rawValue(
            s[1] === 'asc' ? 'desc' : 'asc',
          )}`,
      ),
      sql`, `,
    )}`;
  } else {
    return sql`ORDER BY ${sql.join(
      order.map(
        s => sql`${sql.ident(s[0])} ${sql.__dangerous__rawValue(s[1])}`,
      ),
      sql`, `,
    )}`;
  }
}

/**
 * Converts filters (conditions) to SQL WHERE clause.
 * This applies all filters present in the AST for a query to the source.
 */
export function filtersToSQL(filters: NoSubqueryCondition): SQLQuery {
  switch (filters.type) {
    case 'simple':
      return simpleConditionToSQL(filters);
    case 'and':
      return filters.conditions.length > 0
        ? sql`(${sql.join(
            filters.conditions.map(condition =>
              filtersToSQL(condition as NoSubqueryCondition),
            ),
            sql` AND `,
          )})`
        : sql`TRUE`;
    case 'or':
      return filters.conditions.length > 0
        ? sql`(${sql.join(
            filters.conditions.map(condition =>
              filtersToSQL(condition as NoSubqueryCondition),
            ),
            sql` OR `,
          )})`
        : sql`FALSE`;
  }
}

function simpleConditionToSQL(filter: SimpleCondition): SQLQuery {
  const {op} = filter;
  if (op === 'IN' || op === 'NOT IN') {
    switch (filter.right.type) {
      case 'literal':
        return sql`${valuePositionToSQL(
          filter.left,
        )} ${sql.__dangerous__rawValue(
          filter.op,
        )} (SELECT value FROM json_each(${JSON.stringify(
          filter.right.value,
        )}))`;
      case 'static':
        throw new Error(
          'Static parameters must be replaced before conversion to SQL',
        );
    }
  }
  if (
    op === 'LIKE' ||
    op === 'NOT LIKE' ||
    op === 'ILIKE' ||
    op === 'NOT ILIKE'
  ) {
    return likeConditionToSQL(filter);
  }

  if (
    (op === 'IS' || op === 'IS NOT') &&
    filter.right.type === 'literal' &&
    filter.right.value === null
  ) {
    return sql`${valuePositionToSQL(filter.left)} ${sql.__dangerous__rawValue(
      op,
    )} NULL`;
  }

  return sql`${valuePositionToSQL(filter.left)} ${sql.__dangerous__rawValue(
    filter.op,
  )} ${valuePositionToSQL(filter.right)}`;
}

function likeConditionToSQL(filter: SimpleCondition): SQLQuery {
  const {op} = filter;
  // Mirror Postgres pattern-matching semantics:
  //  * LIKE is case-sensitive. The replica connection runs with
  //    `PRAGMA case_sensitive_like = ON` (see db.ts), so the bare LIKE
  //    operator is case-sensitive.
  //  * ILIKE is case-insensitive. We lower() both operands using the
  //    Unicode-aware lower() that @rocicorp/zero-sqlite3 provides via ICU,
  //    mirroring the toLowerCase() used by the in-memory IVM matcher
  //    (see zql/src/builder/like.ts).
  //  * Backslash is the default escape character in Postgres and in the IVM
  //    matcher, but SQLite has no default, so we specify `ESCAPE '\'`
  //    explicitly. The SQL literal '\' is a single backslash (SQLite does not
  //    process backslash escapes inside string literals).
  const caseInsensitive = op === 'ILIKE' || op === 'NOT ILIKE';
  const negated = op === 'NOT LIKE' || op === 'NOT ILIKE';
  const likeOp = sql.__dangerous__rawValue(negated ? 'NOT LIKE' : 'LIKE');

  const left = valuePositionToSQL(filter.left);
  const right = valuePositionToSQL(filter.right);
  if (caseInsensitive) {
    return sql`lower(${left}) ${likeOp} lower(${right}) ESCAPE '\\'`;
  }
  return sql`${left} ${likeOp} ${right} ESCAPE '\\'`;
}

function valuePositionToSQL(value: ValuePosition): SQLQuery {
  switch (value.type) {
    case 'column':
      return sql.ident(value.name);
    case 'literal':
      return sql`${toSQLiteType(value.value, getJsType(value.value))}`;
    case 'static':
      throw new Error(
        'Static parameters must be replaced before conversion to SQL',
      );
  }
}

function getJsType(value: unknown): ValueType {
  if (value === null) {
    return 'null';
  }
  return typeof value === 'string'
    ? 'string'
    : typeof value === 'number'
      ? 'number'
      : typeof value === 'boolean'
        ? 'boolean'
        : 'json';
}

export function toSQLiteType(v: unknown, type: ValueType): unknown {
  switch (type) {
    case 'boolean':
      return v === null ? null : v ? 1 : 0;
    case 'number':
    case 'string':
    case 'null':
      return v;
    case 'json':
      return JSON.stringify(v);
  }
}

function nullableAwareEquality(
  field: string,
  value: unknown,
  param: SQLQuery,
  columnType: SchemaValue,
): SQLQuery {
  if (value === null) {
    // A NULL bound value proves the column is nullable regardless of the
    // column metadata, and `=` never matches NULL — `IS` selects the NULL
    // tie-break group a cursor anchored on a NULL value needs.
    return sql`${sql.ident(field)} IS NULL`;
  }
  // Use = instead of IS for non-nullable columns to enable better
  // index usage in SQLite.
  return columnType.optional === true
    ? sql`${sql.ident(field)} IS ${param}`
    : sql`${sql.ident(field)} = ${param}`;
}

function nullableAwareRangeComparison(
  field: string,
  value: unknown,
  param: SQLQuery,
  operator: '>' | '<',
  columnType: SchemaValue,
): SQLQuery {
  if (value === null) {
    return operator === '>' ? sql`${sql.ident(field)} IS NOT NULL` : sql`FALSE`;
  }

  // For non-nullable columns, skip IS NULL checks to avoid breaking
  // SQLite's MULTI-INDEX OR optimization, which falls back to a full
  // table scan when any OR branch involves NULL.
  // See: https://github.com/rocicorp/mono/pull/5542
  const comparison = sql`${sql.ident(field)} ${sql.__dangerous__rawValue(
    operator,
  )} ${param}`;
  if (columnType.optional !== true) {
    return comparison;
  }

  // The bound is non-NULL here. NULLs sort before every non-NULL value, so
  // `>` already excludes them and needs no guard, while `<` must admit the
  // NULL group explicitly — a bare `col < ?` would silently drop NULL rows
  // from a backward walk.
  return operator === '>'
    ? comparison
    : sql`(${sql.ident(field)} IS NULL OR ${comparison})`;
}

function sargableLeadingStartBound(
  field: string,
  value: unknown,
  param: SQLQuery,
  operator: '>' | '<',
  columnType: SchemaValue,
): SQLQuery | undefined {
  // A NULL bound value proves the column is nullable regardless of the
  // column metadata, and a bare range bound is not sound there: `col >= NULL`
  // is never true, so instead of being redundant it would annihilate the
  // whole start constraint. A nullable column also cannot use a `<` bound,
  // because the start constraint must retain the NULL group. For `>`, NULLs
  // sort before the non-NULL bound, so `col >= value` remains sound.
  if (value === null || (columnType.optional === true && operator === '<')) {
    return undefined;
  }

  const inclusiveOperator = operator === '>' ? '>=' : '<=';
  return sql`${sql.ident(field)} ${sql.__dangerous__rawValue(
    inclusiveOperator,
  )} ${param}`;
}

/**
 * The ordering could be complex such as:
 * `ORDER BY a ASC, b DESC, c ASC`
 *
 * In those cases, we need to encode the constraints as various
 * `OR` clauses.
 *
 * E.g.,
 *
 * to get the row after (a = 1, b = 2, c = 3) would be:
 *
 * `WHERE a > 1 OR (a = 1 AND b < 2) OR (a = 1 AND b = 2 AND c > 3)`
 *
 * - after vs before flips the comparison operators.
 * - inclusive adds a final `OR` clause for the exact match.
 */
function gatherStartConstraints(
  start: Start,
  reverse: boolean | undefined,
  order: Ordering,
  columnTypes: Record<string, SchemaValue>,
  bind: Bind,
): SQLQuery {
  const constraints: SQLQuery[] = [];
  const {row: from, basis} = start;
  let leadingBound: SQLQuery | undefined;

  // The value of the start row in `field`, and its parameter.
  const startValue = (field: string): [unknown, SQLQuery] => {
    const {type} = columnTypes[field];
    const value = toSQLiteType(from[field] ?? null, type);
    return [
      value,
      bind(value, req => toSQLiteType(req.start?.row[field] ?? null, type)),
    ];
  };

  for (let i = 0; i < order.length; i++) {
    const group: SQLQuery[] = [];
    const [iField, iDirection] = order[i];
    for (let j = 0; j <= i; j++) {
      if (j === i) {
        const columnType = columnTypes[iField];
        const [constraintValue, param] = startValue(iField);
        const operator =
          iDirection === 'asc' ? (reverse ? '<' : '>') : reverse ? '>' : '<';
        if (i === 0) {
          leadingBound = sargableLeadingStartBound(
            iField,
            constraintValue,
            param,
            operator,
            columnType,
          );
        }
        group.push(
          nullableAwareRangeComparison(
            iField,
            constraintValue,
            param,
            operator,
            columnType,
          ),
        );
      } else {
        const [jField] = order[j];
        const [value, param] = startValue(jField);
        group.push(
          nullableAwareEquality(jField, value, param, columnTypes[jField]),
        );
      }
    }
    constraints.push(sql`(${sql.join(group, sql` AND `)})`);
  }

  if (basis === 'at') {
    constraints.push(
      sql`(${sql.join(
        order.map(([field]) => {
          const [value, param] = startValue(field);
          return nullableAwareEquality(field, value, param, columnTypes[field]);
        }),
        sql` AND `,
      )})`,
    );
  }

  const lexicographicStart = sql`(${sql.join(constraints, sql` OR `)})`;
  return leadingBound === undefined
    ? lexicographicStart
    : sql`(${leadingBound} AND ${lexicographicStart})`;
}
