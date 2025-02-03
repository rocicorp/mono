import {assert} from '../../shared/src/asserts.ts';
import type {
  CorrelatedSubqueryCondition,
  Ordering,
  ValuePosition,
} from '../../zero-protocol/src/ast.ts';
import {
  type AST,
  type Condition,
  type CorrelatedSubquery,
  type SimpleCondition,
} from '../../zero-protocol/src/ast.ts';
import {sql} from './sql.ts';
import type {SQLQuery} from '@databases/sql';

/**
 * Compiles to the Postgres dialect of SQL
 * - IS, IS NOT can only compare against `NULL`, `TRUE`, `FALSE` so use
 *   `IS DISTINCT FROM` and `IS NOT DISTINCT FROM` instead
 * - IN is changed to ANY to allow binding array literals
 * - subqueries are aggregated using PG's `array_agg` and `row_to_json` functions
 */
export function compile(ast: AST) {
  return select(ast, undefined);
}

export function select(ast: AST, correlation: SQLQuery | undefined) {
  const selectionSet = related(ast.related ?? [], ast.table);
  selectionSet.push(sql.__dangerous__rawValue('*'));
  return sql`SELECT ${sql.join(selectionSet, ',')} FROM ${sql.ident(
    ast.table,
  )} ${ast.where ? sql`WHERE ${where(ast.where, ast.table)}` : sql``} ${
    correlation
      ? sql`${ast.where ? sql`AND` : sql`WHERE`} (${correlation})`
      : sql``
  } ${orderBy(ast.orderBy)} ${limit(ast.limit)}`;
}

export function orderBy(orderBy: Ordering | undefined): SQLQuery {
  if (!orderBy) {
    return sql``;
  }
  return sql`ORDER BY ${sql.join(
    orderBy.map(([col, dir]) =>
      dir === 'asc' ? sql`${sql.ident(col)} ASC` : sql`${sql.ident(col)} DESC`,
    ),
    ', ',
  )}`;
}

export function limit(limit: number | undefined): SQLQuery {
  if (!limit) {
    return sql``;
  }
  return sql`LIMIT ${sql.value(limit)}`;
}

export function related(
  relationships: readonly CorrelatedSubquery[],
  parentTable: string,
): SQLQuery[] {
  return relationships.map(
    relationship => sql`(
    SELECT array_agg(row_to_json(${sql.ident(
      `inner_${relationship.subquery.alias}`,
    )})) FROM (${select(
      relationship.subquery,
      correlate(
        parentTable,
        relationship.correlation.parentField,
        relationship.subquery.table,
        relationship.correlation.childField,
      ),
    )}) ${sql.ident(`inner_${relationship.subquery.alias}`)}
  ) as ${sql.ident(relationship.subquery.alias)}`,
  );
}

export function where(
  condition: Condition | undefined,
  parentTable: string,
): SQLQuery {
  if (!condition) {
    return sql``;
  }

  switch (condition.type) {
    case 'and':
      return sql`(${sql.join(
        condition.conditions.map(c => where(c, parentTable)),
        ' AND ',
      )})`;
    case 'or':
      return sql`(${sql.join(
        condition.conditions.map(c => where(c, parentTable)),
        ' OR ',
      )})`;
    case 'correlatedSubquery':
      return exists(condition, parentTable);
    case 'simple':
      return simple(condition);
  }
}

export function simple(condition: SimpleCondition): SQLQuery {
  switch (condition.op) {
    case '!=':
    case '<':
    case '<=':
    case '=':
    case '>':
    case '>=':
    case 'ILIKE':
    case 'LIKE':
    case 'NOT ILIKE':
    case 'NOT LIKE':
      return sql`${valuePosition(condition.left)} ${sql.__dangerous__rawValue(
        condition.op,
      )} ${valuePosition(condition.right)}`;
    case 'NOT IN':
    case 'IN':
      return any(condition);
    case 'IS':
    case 'IS NOT':
      return distinctFrom(condition);
  }
}

export function distinctFrom(condition: SimpleCondition): SQLQuery {
  return sql`${valuePosition(condition.left)} ${
    condition.op === 'IS' ? sql`IS NOT DISTINCT FROM` : sql`IS DISTINCT FROM`
  } ${valuePosition(condition.right)}`;
}

export function any(condition: SimpleCondition): SQLQuery {
  return sql`${valuePosition(condition.left)} ${
    condition.op === 'IN' ? sql`= ANY` : sql`!= ANY`
  } (${valuePosition(condition.right)})`;
}

export function valuePosition(value: ValuePosition): SQLQuery {
  switch (value.type) {
    case 'column':
      return sql.ident(value.name);
    case 'literal':
      return sql.value(value.value);
    case 'static':
      throw new Error(
        'Static parameters must be bound to a value before compiling to SQL',
      );
  }
}

export function exists(
  condition: CorrelatedSubqueryCondition,
  parentTable: string,
): SQLQuery {
  switch (condition.op) {
    case 'EXISTS':
      return sql`EXISTS (${select(
        condition.related.subquery,
        correlate(
          parentTable,
          condition.related.correlation.parentField,
          condition.related.subquery.table,
          condition.related.correlation.childField,
        ),
      )})`;
    case 'NOT EXISTS':
      return sql`NOT EXISTS (${select(condition.related.subquery, undefined)})`;
  }
}

export function correlate(
  parentTable: string,
  parentColumns: readonly string[],
  childTable: string,
  childColumns: readonly string[],
) {
  return sql.join(
    zip(parentColumns, childColumns).map(
      ([parentColumn, childColumn]) =>
        sql`${sql.ident(parentTable)}.${sql.ident(parentColumn)} = ${sql.ident(
          childTable,
        )}.${sql.ident(childColumn)}`,
    ),
    ' AND ',
  );
}

function zip<T>(a1: readonly T[], a2: readonly T[]): [T, T][] {
  assert(a1.length === a2.length);
  const result: [T, T][] = [];
  for (let i = 0; i < a1.length; i++) {
    result.push([a1[i], a2[i]]);
  }
  return result;
}
