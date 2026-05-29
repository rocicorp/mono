import type {SQLQuery} from '@databases/sql';
import {assert} from 'shared/src/asserts.ts';
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
import type {MultiConstraint, Start} from '../../zql/src/ivm/operator.ts';
import {sql} from './internal/sql.ts';

/**
 * Condition type without correlated subqueries.
 * This matches the output of transformFilters from zql/builder/filter.ts
 */
export type NoSubqueryCondition = Exclude<
  Condition,
  {type: 'correlatedSubquery'}
>;

export function buildSelectQuery(
  tableName: string,
  columns: Record<string, SchemaValue>,
  constraint: Constraint | undefined,
  filters: NoSubqueryCondition | undefined,
  order: Ordering | undefined,
  reverse: boolean | undefined,
  start: Start | undefined,
  multiConstraints?: readonly MultiConstraint[] | undefined,
) {
  let query = sql`SELECT ${sql.join(
    Object.keys(columns).map(c => sql.ident(c)),
    sql`,`,
  )} FROM ${sql.ident(tableName)}`;
  const constraints: SQLQuery[] = constraintsToSQL(constraint, columns);

  if (multiConstraints) {
    for (const mc of multiConstraints) {
      if (mc.length > 0) {
        constraints.push(multiConstraintToSQL(mc, columns));
      }
    }
  }

  if (start) {
    assert(order !== undefined, 'start requires ordering');
    constraints.push(gatherStartConstraints(start, reverse, order, columns));
  }

  if (filters) {
    constraints.push(filtersToSQL(filters));
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
) {
  if (!constraint) {
    return [];
  }

  const constraints: SQLQuery[] = [];
  for (const [key, value] of Object.entries(constraint)) {
    constraints.push(
      sql`${sql.ident(key)} = ${toSQLiteType(value, columns[key].type)}`,
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
): SQLQuery {
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
      multiConstraint.map(c => sql`${toSQLiteType(c[key], colType)}`),
      sql`,`,
    )})`;
  }

  // Compound: `(col_a, col_b, …) IN (VALUES (?, ?, …), …)`
  const colList = sql`(${sql.join(
    keys.map(k => sql.ident(k)),
    sql`,`,
  )})`;
  const rows = multiConstraint.map(
    c =>
      sql`(${sql.join(
        keys.map(k => sql`${toSQLiteType(c[k], columns[k].type)}`),
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
  return sql`${valuePositionToSQL(filter.left)} ${sql.__dangerous__rawValue(
    // SQLite's LIKE operator is case-insensitive by default, so we
    // convert ILIKE to LIKE and NOT ILIKE to NOT LIKE.
    filter.op === 'ILIKE'
      ? 'LIKE'
      : filter.op === 'NOT ILIKE'
        ? 'NOT LIKE'
        : filter.op,
  )} ${valuePositionToSQL(filter.right)}`;
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
  columnType: SchemaValue,
): SQLQuery {
  // Use = instead of IS for non-nullable columns to enable better
  // index usage in SQLite.
  return columnType.optional === true
    ? sql`${sql.ident(field)} IS ${value}`
    : sql`${sql.ident(field)} = ${value}`;
}

function nullableAwareRangeComparison(
  field: string,
  value: unknown,
  operator: '>' | '<',
  columnType: SchemaValue,
): SQLQuery {
  // For non-nullable columns, skip IS NULL checks to avoid breaking
  // SQLite's MULTI-INDEX OR optimization, which falls back to a full
  // table scan when any OR branch involves NULL.
  // See: https://github.com/rocicorp/mono/pull/5542
  const comparison = sql`${sql.ident(field)} ${sql.__dangerous__rawValue(
    operator,
  )} ${value}`;
  if (columnType.optional !== true) {
    return comparison;
  }

  return operator === '>'
    ? sql`(${value} IS NULL OR ${comparison})`
    : sql`(${sql.ident(field)} IS NULL OR ${comparison})`;
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
): SQLQuery {
  const constraints: SQLQuery[] = [];
  const {row: from, basis} = start;

  for (let i = 0; i < order.length; i++) {
    const group: SQLQuery[] = [];
    const [iField, iDirection] = order[i];
    for (let j = 0; j <= i; j++) {
      if (j === i) {
        const columnType = columnTypes[iField];
        const constraintValue = toSQLiteType(from[iField], columnType.type);
        const operator =
          iDirection === 'asc' ? (reverse ? '<' : '>') : reverse ? '>' : '<';
        group.push(
          nullableAwareRangeComparison(
            iField,
            constraintValue,
            operator,
            columnType,
          ),
        );
      } else {
        const [jField] = order[j];
        const columnType = columnTypes[jField];
        const value = toSQLiteType(from[jField], columnType.type);
        group.push(nullableAwareEquality(jField, value, columnType));
      }
    }
    constraints.push(sql`(${sql.join(group, sql` AND `)})`);
  }

  if (basis === 'at') {
    constraints.push(
      sql`(${sql.join(
        order.map(([field]) => {
          const columnType = columnTypes[field];
          const value = toSQLiteType(from[field], columnType.type);
          return nullableAwareEquality(field, value, columnType);
        }),
        sql` AND `,
      )})`,
    );
  }

  return sql`(${sql.join(constraints, sql` OR `)})`;
}
