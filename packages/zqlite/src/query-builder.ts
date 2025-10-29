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
import {sql} from './internal/sql.ts';
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

  const constraints: SQLQuery[] = [];
  for (const [key, value] of Object.entries(constraint)) {
    constraints.push(
      sql`${sql.ident(key)} = ${toSQLiteType(value, columns[key].type)}`,
    );
  }

  return constraints;
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
        const constraintValue = toSQLiteType(
          from[iField],
          columnTypes[iField].type,
        );
        if (iDirection === 'asc') {
          if (!reverse) {
            group.push(
              sql`(${constraintValue} IS NULL OR ${sql.ident(iField)} > ${constraintValue})`,
            );
          } else {
            reverse satisfies true;
            group.push(
              sql`(${sql.ident(iField)} IS NULL OR ${sql.ident(iField)} < ${constraintValue})`,
            );
          }
        } else {
          iDirection satisfies 'desc';
          if (!reverse) {
            group.push(
              sql`(${sql.ident(iField)} IS NULL OR ${sql.ident(iField)} < ${constraintValue})`,
            );
          } else {
            reverse satisfies true;
            group.push(
              sql`(${constraintValue} IS NULL OR ${sql.ident(iField)} > ${constraintValue})`,
            );
          }
        }
      } else {
        const [jField] = order[j];
        group.push(
          sql`${sql.ident(jField)} IS ${toSQLiteType(
            from[jField],
            columnTypes[jField].type,
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
            sql`${sql.ident(s[0])} IS ${toSQLiteType(
              from[s[0]],
              columnTypes[s[0]].type,
            )}`,
        ),
        sql` AND `,
      )})`,
    );
  }

  return sql`(${sql.join(constraints, sql` OR `)})`;
}
