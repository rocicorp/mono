import type {SQLQuery} from '@databases/sql';
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
import {named, sql} from './internal/sql.ts';
import type {Constraint} from '../../zql/src/ivm/constraint.ts';
import type {Start} from '../../zql/src/ivm/operator.ts';

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
  order: Ordering,
  reverse: boolean | undefined,
  start: Start | undefined,
) {
  let query = sql`SELECT ${sql.join(
    Object.keys(columns).map(c => sql.ident(c)),
    sql`,`,
  )} FROM ${sql.ident(tableName)}`;
  const constraints: SQLQuery[] = constraintsToSQL(constraint, columns);

  if (start) {
    constraints.push(gatherStartConstraints(start, reverse, order, columns));
  }

  // Note: do filters first
  // Perma-bind them
  // Get max index so we know where to start constraints and start from
  if (filters) {
    constraints.push(filtersToSQL(filters));
  }

  if (constraints.length > 0) {
    query = sql`${query} WHERE ${sql.join(constraints, sql` AND `)}`;
  }

  return sql`${query} ${orderByToSQL(order, !!reverse)}`;
}

export function constraintsToSQL(
  constraint: Constraint | undefined,
  columns: Record<string, SchemaValue>,
) {
  if (!constraint) {
    return [];
  }

  // Sort keys for consistent ordering - enables cache key matching
  const sortedKeys = Object.keys(constraint).sort();
  return sortedKeys.map(
    key =>
      sql`${sql.ident(key)} = ${named(
        `c_${key}`,
        toSQLiteType(constraint[key], columns[key].type),
      )}`,
  );
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
 *
 * Named placeholder scheme: `f_{n}` where n is incremented for each value.
 * Pass a counter object to track the current index across recursive calls.
 */
export function filtersToSQL(
  filters: NoSubqueryCondition,
  counter: {n: number} = {n: 0},
): SQLQuery {
  switch (filters.type) {
    case 'simple':
      return simpleConditionToSQL(filters, counter);
    case 'and':
      return filters.conditions.length > 0
        ? sql`(${sql.join(
            filters.conditions.map(condition =>
              filtersToSQL(condition as NoSubqueryCondition, counter),
            ),
            sql` AND `,
          )})`
        : sql`TRUE`;
    case 'or':
      return filters.conditions.length > 0
        ? sql`(${sql.join(
            filters.conditions.map(condition =>
              filtersToSQL(condition as NoSubqueryCondition, counter),
            ),
            sql` OR `,
          )})`
        : sql`FALSE`;
  }
}

function simpleConditionToSQL(
  filter: SimpleCondition,
  counter: {n: number},
): SQLQuery {
  const {op} = filter;
  if (op === 'IN' || op === 'NOT IN') {
    switch (filter.right.type) {
      case 'literal':
        return sql`${valuePositionToSQL(
          filter.left,
          counter,
        )} ${sql.__dangerous__rawValue(
          filter.op,
        )} (SELECT value FROM json_each(${named(
          `f_${counter.n++}`,
          JSON.stringify(filter.right.value),
        )}))`;
      case 'static':
        throw new Error(
          'Static parameters must be replaced before conversion to SQL',
        );
    }
  }
  return sql`${valuePositionToSQL(filter.left, counter)} ${sql.__dangerous__rawValue(
    // SQLite's LIKE operator is case-insensitive by default, so we
    // convert ILIKE to LIKE and NOT ILIKE to NOT LIKE.
    filter.op === 'ILIKE'
      ? 'LIKE'
      : filter.op === 'NOT ILIKE'
        ? 'NOT LIKE'
        : filter.op,
  )} ${valuePositionToSQL(filter.right, counter)}`;
}

function valuePositionToSQL(
  value: ValuePosition,
  counter: {n: number},
): SQLQuery {
  switch (value.type) {
    case 'column':
      return sql.ident(value.name);
    case 'literal':
      return sql`${named(
        `f_${counter.n++}`,
        toSQLiteType(value.value, getJsType(value.value)),
      )}`;
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
 *
 * Named placeholder scheme: `s_{colName}` for each column in start row.
 * SQLite binds the same value to all occurrences of the same param name.
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
        const constraintValue = toSQLiteType(
          from[iField],
          columnTypes[iField].type,
        );
        const paramName = `s_${iField}`;
        if (iDirection === 'asc') {
          if (!reverse) {
            group.push(
              sql`(${named(paramName, constraintValue)} IS NULL OR ${sql.ident(iField)} > ${named(paramName, constraintValue)})`,
            );
          } else {
            reverse satisfies true;
            group.push(
              sql`(${sql.ident(iField)} IS NULL OR ${sql.ident(iField)} < ${named(paramName, constraintValue)})`,
            );
          }
        } else {
          iDirection satisfies 'desc';
          if (!reverse) {
            group.push(
              sql`(${sql.ident(iField)} IS NULL OR ${sql.ident(iField)} < ${named(paramName, constraintValue)})`,
            );
          } else {
            reverse satisfies true;
            group.push(
              sql`(${named(paramName, constraintValue)} IS NULL OR ${sql.ident(iField)} > ${named(paramName, constraintValue)})`,
            );
          }
        }
      } else {
        const [jField] = order[j];
        group.push(
          sql`${sql.ident(jField)} IS ${named(
            `s_${jField}`,
            toSQLiteType(from[jField], columnTypes[jField].type),
          )}`,
        );
      }
    }
    constraints.push(sql`(${sql.join(group, sql` AND `)})`);
  }

  if (basis === 'at') {
    constraints.push(
      sql`(${sql.join(
        order.map(
          s =>
            sql`${sql.ident(s[0])} IS ${named(
              `s_${s[0]}`,
              toSQLiteType(from[s[0]], columnTypes[s[0]].type),
            )}`,
        ),
        sql` AND `,
      )})`,
    );
  }

  return sql`(${sql.join(constraints, sql` OR `)})`;
}
