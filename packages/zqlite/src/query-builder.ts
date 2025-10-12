import type {SQLQuery} from '@databases/sql';
import type {
  Condition,
  Ordering,
  SimpleCondition,
  ValuePosition,
} from '../../zero-protocol/src/ast.ts';
import type {ValueType} from '../../zero-schema/src/table-schema.ts';
import type {PlannerConstraint} from '../../zql/src/planner/planner-constraint.ts';
import {sql} from './internal/sql.ts';

/**
 * Condition type without correlated subqueries.
 * This matches the output of transformFilters from zql/builder/filter.ts
 */
export type NoSubqueryCondition = Exclude<
  Condition,
  {type: 'correlatedSubquery'}
>;

/**
 * Builds a SELECT query from components.
 * Used for both fetching data and estimating costs via scanstatus.
 */
export function buildSelectQuery(
  tableName: string,
  columns: readonly string[],
  constraint: PlannerConstraint | undefined,
  filters: NoSubqueryCondition | undefined,
  ordering: Ordering,
): SQLQuery {
  const allColumns = sql.join(
    columns.map(c => sql.ident(c)),
    sql`,`,
  );

  let query = sql`SELECT ${allColumns} FROM ${sql.ident(tableName)}`;
  const constraints: SQLQuery[] = [];

  // Add constraint clauses
  if (constraint) {
    for (const [key, value] of Object.entries(constraint)) {
      constraints.push(
        sql`${sql.ident(key)} = ${toSQLiteType(value, getJsType(value))}`,
      );
    }
  }

  // Add filter clauses
  if (filters) {
    constraints.push(filtersToSQL(filters));
  }

  // Add WHERE clause if we have constraints
  if (constraints.length > 0) {
    query = sql`${query} WHERE ${sql.join(constraints, sql` AND `)}`;
  }

  // Add ORDER BY clause
  query = sql`${query} ORDER BY ${sql.join(
    ordering.map(
      s => sql`${sql.ident(s[0])} ${sql.__dangerous__rawValue(s[1])}`,
    ),
    sql`, `,
  )}`;

  return query;
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
