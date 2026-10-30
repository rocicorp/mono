import type {SQLQuery} from '@databases/sql';
import {last, zip} from '../../shared/src/arrays.ts';
import {assert, unreachable} from '../../shared/src/asserts.ts';
import {
  parse as parseBigIntJson,
  type JSONValue as BigIntJSONValue,
} from '../../shared/src/bigint-json.ts';
import {hasOwn} from '../../shared/src/has-own.ts';
import {type JSONValue} from '../../shared/src/json.ts';
import {must} from '../../shared/src/must.ts';
import {isPgTextRepresentedType} from '../../zero-cache/src/types/pg-data-type.ts';
import {
  isLikeOperator,
  isNegatedOperator,
  jsonLiteralType,
  type Aggregate,
  type AST,
  type Bound,
  type Condition,
  type CorrelatedSubquery,
  type CorrelatedSubqueryCondition,
  type Correlation,
  type JsonPathReference,
  type LiteralReference,
  type Ordering,
  type SimpleCondition,
  type ValuePosition,
} from '../../zero-protocol/src/ast.ts';
import {
  clientToServer,
  type NameMapper,
} from '../../zero-schema/src/name-mapper.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {ServerSchema} from '../../zero-types/src/server-schema.ts';
import {groupOrdering} from '../../zql/src/ivm/group-by.ts';
import type {Format} from '../../zql/src/ivm/view.ts';
import {completeOrdering} from '../../zql/src/query/complete-ordering.ts';
import {
  isBuiltinTypeSchema,
  pgTypeForLiteralType,
  sql,
  sqlConvertColumnArg,
  sqlConvertPluralLiteralArg,
  sqlConvertSingularLiteralArg,
  type PluralLiteralType,
} from './sql.ts';

type Table = {
  zql: string;
  alias: string;
};

type QualifiedColumn = {
  table: Table;
  zql: string;
};

type ServerSpec = {
  schema: ServerSchema;
  // maps zql names to server names
  mapper: NameMapper;
};

export type Spec = {
  server: ServerSpec;
  zql: Schema['tables'];
  aliasCount: number;
};

const ZQL_RESULT_KEY = 'zql_result';
const ZQL_RESULT_KEY_IDENT = sql.ident(ZQL_RESULT_KEY);

const ZQL_RESULT_TABLE_KEY = 'zql_root';
const ZQL_RESULT_TABLE_IDENT = sql.ident(ZQL_RESULT_TABLE_KEY);

export function compile(
  serverSchema: ServerSchema,
  zqlSchema: Schema,
  ast: AST,
  format?: Format,
): SQLQuery {
  assert(
    ast.groupBy === undefined || ast.aggregate?.distinct === undefined,
    'a count of distinct values per group is not supported yet',
  );
  // The value would be selected under a group column's name. The query
  // builder rejects such a query; a hand-built AST can still name one.
  assert(
    ast.groupBy === undefined ||
      ast.aggregate === undefined ||
      !ast.groupBy.includes(ast.aggregate.fn),
    () =>
      `groupBy(): the column "${ast.aggregate?.fn}" has the same name as the ${ast.aggregate?.fn}() value`,
  );
  // As in the IVM builder: the rows are grouped in the group's order, not the
  // one a bound would be taken in.
  assert(
    (ast.groupBy === undefined && ast.aggregate?.distinct === undefined) ||
      (ast.start === undefined && ast.limit === undefined),
    'groupBy and a count of distinct values do not support start or limit',
  );
  ast = completeOrdering(
    ast,
    tableName => zqlSchema.tables[tableName].primaryKey,
  );
  const spec: Spec = {
    aliasCount: 0,
    server: {
      schema: serverSchema,
      mapper: clientToServer(zqlSchema.tables),
    },
    zql: zqlSchema.tables,
  };
  if (ast.groupBy) {
    const groups = ast.aggregate
      ? selectGroups(spec, ast, ast.aggregate)
      : selectDistinct(spec, ast);
    return sql`SELECT ${toJSON(ZQL_RESULT_TABLE_KEY)}::text AS ${ZQL_RESULT_KEY_IDENT}
      FROM (${groups}) ${ZQL_RESULT_TABLE_IDENT}`;
  }

  if (ast.aggregate) {
    if (ast.aggregate.distinct) {
      // A count of the distinct combinations counts the rows a distinct()
      // of them gives.
      const groups = selectDistinct(spec, {
        ...ast,
        groupBy: ast.aggregate.distinct,
        aggregate: undefined,
      });
      return sql`SELECT to_json(COUNT(*))::text AS ${ZQL_RESULT_KEY_IDENT}
        FROM (${groups}) ${ZQL_RESULT_TABLE_IDENT}`;
    }
    // Top-level (ungrouped) aggregate: the result is a single scalar, not rows.
    // It reduces the same normalized projection a relationship aggregate does,
    // so values such as timestamps come back in the form ZQL uses. Like the
    // IVM operator, it reduces the rows left after `start`/`limit` and ignores
    // `related`. The order only matters for picking those rows.
    const bounded = ast.limit !== undefined || ast.start !== undefined;
    const inner = select(
      spec,
      {
        ...ast,
        aggregate: undefined,
        related: undefined,
        orderBy: bounded ? ast.orderBy : undefined,
      },
      undefined,
    );
    // An aggregate over no rows (other than COUNT) is SQL NULL, and to_json()
    // of NULL is NULL rather than the JSON text `null` the result parser reads.
    return sql`SELECT COALESCE(to_json(${aggregateExpr(spec, ast.table, ast.aggregate)}), 'null'::json)::text AS ${ZQL_RESULT_KEY_IDENT}
      FROM (${inner}) ${ZQL_RESULT_TABLE_IDENT}`;
  }

  return sql`SELECT 
    ${toJSON(ZQL_RESULT_TABLE_KEY, format?.singular)}::text AS ${ZQL_RESULT_KEY_IDENT}
    FROM (${select(spec, ast, format)}) ${ZQL_RESULT_TABLE_IDENT}`;
}

/**
 * The groups of `ast.groupBy` among the rows that pass `ast.where`, each with
 * the aggregate under the function's name (`count`, `sum`, …), in the order a
 * grouped query's rows have (see `groupOrdering`). Like a top-level
 * aggregate, it reduces the normalized projection {@link select} gives, so a
 * timestamp comes back in epoch milliseconds, and groups by those values.
 */
function selectGroups(spec: Spec, ast: AST, agg: Aggregate): SQLQuery {
  const inner = select(
    spec,
    {
      ...ast,
      groupBy: undefined,
      aggregate: undefined,
      related: undefined,
      orderBy: undefined,
    },
    undefined,
  );
  const cols = must(ast.groupBy).map(col => sql.ident(col));
  return sql`SELECT ${sql.join(cols, ',')}, ${aggregateExpr(spec, ast.table, agg)} AS ${sql.ident(agg.fn)}
    FROM (${inner}) ${sql.ident('zql_group')}
    GROUP BY ${sql.join(cols, ', ')}
    ORDER BY ${sql.join(
      groupOrdering(ast).map(([col, dir]) => {
        const value = orderedValue(spec, ast.table, col, sql.ident(col));
        return dir === 'asc'
          ? sql`${value} ASC NULLS FIRST`
          : sql`${value} DESC NULLS LAST`;
      }),
      ', ',
    )}`;
}

/**
 * The distinct values of `ast.groupBy` among the rows that pass `ast.where`,
 * one row of those columns each, in the order a distinct query's rows have
 * (see `groupOrdering`). It groups by the columns rather than selecting
 * DISTINCT, which would need the converted values {@link selectIdent} selects
 * to appear in the ORDER BY too. Like the IVM operator, it ignores `related`.
 */
function selectDistinct(spec: Spec, ast: AST): SQLQuery {
  const table = makeTable(spec, ast.table);
  const columns = must(ast.groupBy).map(zql => ({table, zql}));
  return sql`SELECT ${sql.join(
    columns.map(c => selectIdent(spec.server, c)),
    ',',
  )}
    FROM ${fromIdent(spec.server, table)}
    ${ast.where ? sql`WHERE ${where(spec, ast.where, table)}` : sql``}
    GROUP BY ${sql.join(
      columns.map(c => colIdent(spec.server, c)),
      ', ',
    )}
    ORDER BY ${sql.join(
      groupOrdering(ast).map(([col, dir]) => {
        const value = orderedValue(
          spec,
          ast.table,
          col,
          colIdent(spec.server, {table, zql: col}),
        );
        return dir === 'asc'
          ? sql`${value} ASC NULLS FIRST`
          : sql`${value} DESC NULLS LAST`;
      }),
      ', ',
    )}`;
}

/**
 * The SQL aggregate for `aggregate`, over a subquery that exposes the
 * aggregated column by its client name (as {@link select} does).
 *
 * `COUNT(*)` is left as Postgres's 64-bit count: the result parser rejects a
 * value outside the safe integer range rather than rounding it.
 *
 * `SUM`/`AVG` add the values as float8, as the IVM operator adds JS numbers:
 * Postgres would otherwise sum a `real` in float4 and a `numeric` exactly.
 *
 * `MIN`/`MAX` of an enum, or of a type Zero represents as text (`uuid`,
 * `inet`, …), compare that text byte by byte, as the IVM operator compares
 * strings. Otherwise an enum would use its declaration order, an `inet` its
 * network order, and a `uuid` would fail (it has no `MIN`).
 */
function aggregateExpr(spec: Spec, zqlTable: string, agg: Aggregate): SQLQuery {
  assert(
    agg.distinct === undefined,
    'a count of distinct values is not supported in a relationship yet',
  );
  const {fn, field, path} = agg;
  if (path !== undefined) {
    return jsonAggregateExpr(spec, zqlTable, agg, path);
  }
  switch (fn) {
    case 'count':
      return sql`COUNT(*)`;
    case 'sum':
      return sql`SUM(${sql.ident(must(field))}::float8)`;
    case 'avg':
      return sql`AVG(${sql.ident(must(field))}::float8)`;
    case 'min':
      return sql`MIN(${orderedAggregateArg(spec, zqlTable, must(field))})`;
    case 'max':
      return sql`MAX(${orderedAggregateArg(spec, zqlTable, must(field))})`;
    default:
      unreachable(fn);
  }
}

/**
 * The SQL aggregate over the JSON leaf at `path` inside the (client-named)
 * json column `field` of the subquery. Only leaves the aggregate takes count
 * (see `aggregateValue`): numbers for `sum`/`avg`, numbers and strings for
 * `min`/`max`, each gated on `jsonb_typeof` so a mismatched leaf is SQL NULL
 * (skipped) rather than a cast error.
 *
 * `min`/`max` reduce numbers and strings separately and combine them with the
 * client's cross-type order, numbers before strings (`compareAggregateValues`),
 * instead of Postgres's jsonb order, which puts strings first. The result is
 * jsonb so either type comes back as itself.
 */
function jsonAggregateExpr(
  spec: Spec,
  zqlTable: string,
  {fn, field}: Aggregate,
  path: readonly (string | number)[],
): SQLQuery {
  const name = must(field);
  const {type, isArray} = getServerColumn(
    spec.server,
    {zql: zqlTable, alias: zqlTable},
    name,
  );
  assert(
    isArray || type === 'json' || type === 'jsonb',
    () => `JSON path on non-JSON column "${name}" of type ${type}`,
  );
  const {raw, jsonType} = jsonNavigate(
    jsonBase(sql.ident(name), type, isArray),
    path,
  );
  const num = sql`(CASE WHEN ${jsonType} = 'number' THEN ${raw}::float8 END)`;
  const str = sql`(CASE WHEN ${jsonType} = 'string' THEN ${raw} END)`;
  switch (fn) {
    case 'sum':
      return sql`SUM(${num})::float8`;
    case 'avg':
      return sql`AVG(${num})::float8`;
    case 'min':
      return sql`COALESCE(to_jsonb(MIN(${num})), to_jsonb(MIN(${str})))`;
    case 'max':
      return sql`COALESCE(to_jsonb(MAX(${str})), to_jsonb(MAX(${num})))`;
    case 'count':
      throw new Error('count() takes no JSON path');
    default:
      unreachable(fn);
  }
}

function orderedAggregateArg(
  spec: Spec,
  zqlTable: string,
  field: string,
): SQLQuery {
  return orderedValue(spec, zqlTable, field, sql.ident(field));
}

/**
 * `value`, the value of `zqlTable`'s column `field`, in a form Postgres
 * orders as the IVM does: an enum, or a type Zero represents as text, by that
 * text byte by byte. Otherwise an enum would use its declaration order and an
 * `inet` its network order.
 */
function orderedValue(
  spec: Spec,
  zqlTable: string,
  field: string,
  value: SQLQuery,
): SQLQuery {
  const {server} = spec;
  const column =
    server.schema[server.mapper.tableName(zqlTable)][
      server.mapper.columnName(zqlTable, field)
    ];
  // `format` gives the type's own text: `::text` adds the netmask to an `inet`.
  return column.isEnum || isPgTextRepresentedType(column.type)
    ? sql`format('%s', ${value}) COLLATE "C"`
    : value;
}

function select(
  spec: Spec,
  ast: AST,
  format: Format | undefined,
  correlate?: (childTable: Table) => SQLQuery,
): SQLQuery {
  const table = makeTable(spec, ast.table);
  const selectionSet = related(spec, ast.related ?? [], format, table);
  const tableSchema = spec.zql[ast.table];
  const usedAliases = new Set<string>(
    ast.related?.map(r => r.subquery.alias ?? ''),
  );
  for (const column of Object.keys(tableSchema.columns)) {
    if (!usedAliases.has(column)) {
      selectionSet.push(
        selectIdent(spec.server, {
          table,
          zql: column,
        }),
      );
    }
  }

  let appliedWhere = false;
  function maybeWhere(test: unknown) {
    if (!test) {
      return sql``;
    }

    const ret = appliedWhere ? sql`AND` : sql`WHERE`;
    appliedWhere = true;
    return ret;
  }

  return sql`SELECT ${sql.join(selectionSet, ',')}
    FROM ${fromIdent(spec.server, table)}
    ${maybeWhere(ast.where)} ${where(spec, ast.where, table)}${
      ast.start
        ? sql`
    ${maybeWhere(ast.start)} ${start(spec, ast.start, ast.orderBy, table)}`
        : sql``
    }
    ${maybeWhere(correlate)} ${correlate ? correlate(table) : sql``}
    ${orderBy(spec, ast.orderBy, table)}
    ${limit(ast.limit, format?.singular)}`;
}

export function limit(
  limit: number | undefined,
  singular: boolean | undefined,
): SQLQuery {
  if (limit === 0) {
    return sql`LIMIT 0`;
  }
  if (singular) {
    return sql`LIMIT 1`;
  }
  if (limit === undefined) {
    return sql``;
  }
  return sql`LIMIT ${sqlConvertSingularLiteralArg(limit)}`;
}

function makeTable(spec: Spec, zql: string, alias?: string): Table {
  alias = alias ?? zql + '_' + spec.aliasCount++;
  return {
    zql,
    alias,
  };
}

export function orderBy(
  spec: Spec,
  orderBy: Ordering | undefined,
  table: Table,
): SQLQuery {
  if (!orderBy) {
    return sql``;
  }
  return sql`ORDER BY ${sql.join(
    orderBy.map(([col, dir]) =>
      dir === 'asc'
        ? // Oh postgres. The table must be referred to by client name but the column by server name.
          // E.g., `SELECT server_col as client_col FROM server_table as client_table ORDER BY client_Table.server_col`
          sql`${colIdent(spec.server, {
            table,
            zql: col,
          })} ASC NULLS FIRST`
        : sql`${colIdent(spec.server, {
            table,
            zql: col,
          })} DESC NULLS LAST`,
    ),
    ', ',
  )}`;
}

export function start(
  spec: Spec,
  bound: Bound | undefined,
  orderBy: Ordering | undefined,
  table: Table,
): SQLQuery {
  if (!bound) {
    return sql``;
  }
  assert(
    orderBy !== undefined && orderBy.length > 0,
    'start requires ordering',
  );

  const constraints: SQLQuery[] = [];
  for (let i = 0; i < orderBy.length; i++) {
    const group: SQLQuery[] = [];
    const [iField, iDirection] = orderBy[i];
    for (let j = 0; j <= i; j++) {
      const [field] = orderBy[j];
      if (j === i) {
        group.push(
          startRangeComparison(
            spec,
            table,
            iField,
            bound.row[iField] ?? null,
            iDirection === 'asc' ? '>' : '<',
          ),
        );
      } else {
        group.push(startEquality(spec, table, field, bound.row[field] ?? null));
      }
    }
    constraints.push(sql`(${sql.join(group, ' AND ')})`);
  }

  if (!bound.exclusive) {
    constraints.push(
      sql`(${sql.join(
        orderBy.map(([field]) =>
          startEquality(spec, table, field, bound.row[field] ?? null),
        ),
        ' AND ',
      )})`,
    );
  }

  return sql`(${sql.join(constraints, ' OR ')})`;
}

function related(
  spec: Spec,
  relationships: readonly CorrelatedSubquery[],
  format: Format | undefined,
  parentTable: Table,
): SQLQuery[] {
  return relationships.map(relationship =>
    relationshipSubquery(
      spec,
      relationship,
      format?.relationships[must(relationship.subquery.alias)],
      parentTable,
    ),
  );
}

function relationshipSubquery(
  spec: Spec,
  relationship: CorrelatedSubquery,
  format: Format | undefined,
  parentTable: Table,
): SQLQuery {
  const innerAlias = `inner_${relationship.subquery.alias}`;
  if (relationship.aggregate) {
    // Aggregate relationship: emit a correlated scalar subquery returning the
    // aggregate over the matching child rows, instead of aggregating the rows
    // to JSON. No rows are materialized. The inner subquery exposes columns by
    // their client names, so the aggregated field is referenced as-is.
    const {field} = relationship.aggregate;
    const isJunction = !!relationship.subquery.related?.length;
    const aggExpr = aggregateExpr(
      spec,
      isJunction
        ? must(relationship.subquery.related)[0].subquery.table
        : relationship.subquery.table,
      relationship.aggregate,
    );

    let inner: SQLQuery;
    if (isJunction) {
      // Junction (many-to-many) aggregate: the value lives on the destination,
      // one hop past the junction. Aggregate over the junction join, exposing
      // the destination field by its client name so `aggExpr` references it. A
      // `where` on the destination filters the joined rows before aggregating.
      const {join, participatingTables} = makeJunctionJoin(spec, relationship);
      const lastTable = must(last(participatingTables)).table;
      const destWhere = relationship.subquery.related[0].subquery.where;
      inner = sql`SELECT ${
        field
          ? selectIdent(spec.server, {table: lastTable, zql: field})
          : sql`NULL`
      } FROM ${join} WHERE (${makeCorrelator(
        spec,
        relationship.correlation.parentField.map(f => ({
          table: parentTable,
          zql: f,
        })),
        relationship.correlation.childField,
      )(participatingTables[0].table)})${
        destWhere ? sql` AND (${where(spec, destWhere, lastTable)})` : sql``
      }`;
    } else {
      // As for a top-level aggregate, the order only matters for picking the
      // rows a `limit`/`start` keeps; otherwise it is a needless sort per parent.
      const {subquery} = relationship;
      const bounded =
        subquery.limit !== undefined || subquery.start !== undefined;
      inner = select(
        spec,
        bounded ? subquery : {...subquery, orderBy: undefined},
        undefined,
        makeCorrelator(
          spec,
          relationship.correlation.parentField.map(f => ({
            table: parentTable,
            zql: f,
          })),
          relationship.correlation.childField,
        ),
      );
    }
    return sql`(
        SELECT ${aggExpr} FROM (${inner}) ${sql.ident(innerAlias)}
      ) as ${sql.ident(relationship.subquery.alias)}`;
  }
  if (relationship.hidden) {
    const {join, participatingTables} = makeJunctionJoin(spec, relationship);
    const lastTable = must(last(participatingTables)).table;

    assert(
      relationship.subquery.related,
      'hidden relationship must be a junction',
    );
    const nestedAst = relationship.subquery.related[0].subquery;
    const selectionSet = related(
      spec,
      nestedAst.related ?? [],
      format,
      lastTable,
    );
    const tableSchema = spec.zql[nestedAst.table];
    for (const column of Object.keys(tableSchema.columns)) {
      selectionSet.push(
        selectIdent(spec.server, {
          table: lastTable,
          zql: column,
        }),
      );
    }

    return sql`(
        SELECT ${toJSON(innerAlias, format?.singular)} FROM (SELECT ${sql.join(
          selectionSet,
          ',',
        )} FROM ${join} WHERE (${makeCorrelator(
          spec,
          relationship.correlation.parentField.map(f => ({
            table: parentTable,
            zql: f,
          })),
          relationship.correlation.childField,
        )(participatingTables[0].table)}) ${
          nestedAst.where
            ? sql`AND ${where(spec, nestedAst.where, lastTable)}`
            : sql``
        }${
          nestedAst.start
            ? sql` AND ${start(
                spec,
                nestedAst.start,
                nestedAst.orderBy,
                lastTable,
              )}`
            : sql``
        } ${orderBy(spec, nestedAst.orderBy, lastTable)} ${limit(
          last(participatingTables)?.limit,
          format?.singular,
        )} ) ${sql.ident(innerAlias)}
      ) as ${sql.ident(relationship.subquery.alias)}`;
  }

  return sql`(
      SELECT ${toJSON(innerAlias, format?.singular)} FROM (${select(
        spec,
        relationship.subquery,
        format,
        makeCorrelator(
          spec,
          relationship.correlation.parentField.map(f => ({
            table: parentTable,
            zql: f,
          })),
          relationship.correlation.childField,
        ),
      )}) ${sql.ident(innerAlias)}
    ) as ${sql.ident(relationship.subquery.alias)}`;
}

function where(
  spec: Spec,
  condition: Condition | undefined,
  table: Table,
): SQLQuery {
  if (!condition) {
    return sql``;
  }

  switch (condition.type) {
    case 'and':
      return sql`(${sql.join(
        condition.conditions.map(c => where(spec, c, table)),
        ' AND ',
      )})`;
    case 'or':
      return sql`(${sql.join(
        condition.conditions.map(c => where(spec, c, table)),
        ' OR ',
      )})`;
    case 'correlatedSubquery':
      // `scalar` is deliberately ignored here. It is a planner hint for the
      // IVM engines, which have no cost-based planner and benefit from
      // pre-resolving an at-most-one-row subquery to a literal comparison
      // (see zqlite's `resolveSimpleScalarSubqueries`). Postgres decorrelates
      // EXISTS into a semi-join on its own, so the hint buys nothing here —
      // and honoring it as `parentField = (SELECT childField … LIMIT 1)` is
      // only sound when the subquery matches at most one row *globally*.
      // Applying it unconditionally silently dropped rows whenever the
      // subquery was not pinned to a unique key.
      return exists(spec, condition, table);
    case 'simple':
      return simple(spec, condition, table);
  }
}

function exists(
  spec: Spec,
  condition: CorrelatedSubqueryCondition,
  parentTable: Table,
): SQLQuery {
  switch (condition.op) {
    case 'EXISTS':
      return sql`EXISTS (${select(
        spec,
        condition.related.subquery,
        undefined,
        makeCorrelator(
          spec,
          condition.related.correlation.parentField.map(f => ({
            table: parentTable,
            zql: f,
          })),
          condition.related.correlation.childField,
        ),
      )})`;
    case 'NOT EXISTS':
      return sql`NOT EXISTS (${select(
        spec,
        condition.related.subquery,
        undefined,
        makeCorrelator(
          spec,
          condition.related.correlation.parentField.map(f => ({
            table: parentTable,
            zql: f,
          })),
          condition.related.correlation.childField,
        ),
      )})`;
  }
}

export function makeCorrelator(
  spec: Spec,
  parentFields: readonly QualifiedColumn[],
  childZqlFields: readonly string[],
): (childTable: Table) => SQLQuery {
  return (childTable: Table) => {
    const childFields = childZqlFields.map(zqlField => ({
      table: childTable,
      zql: zqlField,
    }));
    return sql.join(
      zip(parentFields, childFields).map(
        ([parentColumn, childColumn]) =>
          sql`${colIdent(spec.server, parentColumn)} = ${colIdent(
            spec.server,
            childColumn,
          )}`,
      ),
      ' AND ',
    );
  };
}

export function simple(
  spec: Spec,
  condition: SimpleCondition,
  table: Table,
): SQLQuery {
  if (condition.left.type === 'json') {
    const special = jsonPathCondition(spec, condition.left, condition, table);
    if (special) {
      return special;
    }
  }
  switch (condition.op) {
    case 'ILIKE':
    case 'LIKE':
    case 'NOT ILIKE':
    case 'NOT LIKE': {
      // The pattern is a string, not a value of the column's type. Converted
      // to the `bpchar` of a `char(n)` column it would lose its trailing
      // spaces. Like ZQL's own LIKE, non-string patterns match as strings.
      const {right} = condition;
      assert(
        right.type === 'literal',
        'Static parameters must be bound to a value before compiling to SQL',
      );
      return sql`${valueComparison(
        spec,
        condition.left,
        table,
        right,
        false,
      )} ${sql.__dangerous__rawValue(condition.op)} ${sqlConvertSingularLiteralArg(
        right.value === null ? null : String(right.value),
      )}`;
    }
    case '!=':
    case '<':
    case '<=':
    case '=':
    case '>':
    case '>=':
      return sql`${valueComparison(
        spec,
        condition.left,
        table,
        condition.right,
        false,
      )} ${sql.__dangerous__rawValue(condition.op)} ${valueComparison(
        spec,
        condition.right,
        table,
        condition.left,
        false,
      )}`;
    case 'NOT IN':
    case 'IN':
      return any(spec, condition, table);
    case 'IS':
    case 'IS NOT':
      return distinctFrom(spec, condition, table);
  }
}

export function any(
  spec: Spec,
  condition: SimpleCondition,
  table: Table,
): SQLQuery {
  return sql`${condition.op === 'NOT IN' ? sql`NOT` : sql``}
    (
      ${valueComparison(spec, condition.left, table, condition.right, false)} = ANY 
      (${valueComparison(spec, condition.right, table, condition.left, true)})
    )`;
}

export function distinctFrom(
  spec: Spec,
  condition: SimpleCondition,
  table: Table,
): SQLQuery {
  return sql`${valueComparison(spec, condition.left, table, condition.right, false)} ${
    condition.op === 'IS' ? sql`IS NOT DISTINCT FROM` : sql`IS DISTINCT FROM`
  } ${valueComparison(spec, condition.right, table, condition.left, false)}`;
}

function startEquality(
  spec: Spec,
  table: Table,
  field: string,
  value: unknown,
): SQLQuery {
  return sql`${colIdent(spec.server, {
    table,
    zql: field,
  })} IS NOT DISTINCT FROM ${startValue(spec, table, field, value)}`;
}

function startRangeComparison(
  spec: Spec,
  table: Table,
  field: string,
  value: unknown,
  operator: '>' | '<',
): SQLQuery {
  const column = colIdent(spec.server, {table, zql: field});
  if (value === null) {
    return operator === '>' ? sql`${column} IS NOT NULL` : sql`FALSE`;
  }
  const typedValue = startValue(spec, table, field, value);
  return operator === '>'
    ? sql`${column} > ${typedValue}`
    : sql`(${column} IS NULL OR ${column} < ${typedValue})`;
}

function startValue(
  spec: Spec,
  table: Table,
  field: string,
  value: unknown,
): SQLQuery {
  return sqlConvertColumnArg(
    getServerColumn(spec.server, table, field),
    value,
    false,
    true,
  );
}

function valueComparison(
  spec: Spec,
  valuePos: ValuePosition,
  table: Table,
  otherValuePos: ValuePosition,
  plural: boolean,
): SQLQuery {
  const valuePosType = valuePos.type;
  switch (valuePosType) {
    case 'column': {
      const qualified: QualifiedColumn = {
        table,
        zql: valuePos.name,
      };
      return colIdent(spec.server, qualified);
    }
    case 'json':
      return jsonPathLeaf(spec, valuePos, table, otherValuePos);
    case 'literal':
      return literalValueComparison(
        spec,
        valuePos,
        table,
        otherValuePos,
        plural,
      );
    case 'static':
      throw new Error(
        'Static parameters must be bound to a value before compiling to SQL',
      );
    default:
      unreachable(valuePosType);
  }
}

function literalValueComparison(
  spec: Spec,
  valuePos: LiteralReference,
  table: Table,
  otherValuePos: ValuePosition,
  plural: boolean,
): SQLQuery {
  const otherType = otherValuePos.type;
  switch (otherType) {
    case 'column':
      return sqlConvertColumnArg(
        getServerColumn(spec.server, table, otherValuePos.name),
        valuePos.value,
        plural,
        true,
      );
    case 'json': {
      // The other side is a JSON path leaf, which is dynamically typed. Render
      // this literal by its OWN JS type so its cast matches the leaf's cast in
      // `jsonPathLeaf` (`leafType`). LiteralValue is primitives or a
      // homogeneous primitive list (enforced at the wire and by the builder).
      assert(
        plural === Array.isArray(valuePos.value),
        'Expected plural flag to match whether value is an array',
      );
      const {value} = valuePos;
      if (Array.isArray(value)) {
        return sqlConvertPluralLiteralArg(
          jsonLiteralType(value) ?? 'string',
          value as PluralLiteralType[],
        );
      }
      // `Array.isArray` does not narrow a `readonly` array out of the union.
      return sqlConvertSingularLiteralArg(
        value as string | number | boolean | null,
      );
    }
    case 'literal': {
      assert(
        plural === Array.isArray(valuePos.value),
        'Expected plural flag to match whether value is an array',
      );
      if (Array.isArray(valuePos.value)) {
        if (valuePos.value.length > 0) {
          // If the array is non-empty base its type on its first
          // element
          return sqlConvertPluralLiteralArg(
            typeof valuePos.value[0] as PluralLiteralType,
            valuePos.value as PluralLiteralType[],
          );
        }
        // If the array is empty, base its type on the other value
        // position's type (as long as the other value position is non-null,
        // cannot have a null[]).
        if (otherValuePos.value !== null) {
          return sqlConvertPluralLiteralArg(
            typeof otherValuePos.value as PluralLiteralType,
            [],
          );
        }
        // If the other value position is null, it can be compared to any
        // type of empty array, chose 'string' arbitrarily.
        return sqlConvertPluralLiteralArg('string', []);
      }
      if (
        typeof valuePos.value === 'string' ||
        typeof valuePos.value === 'number' ||
        typeof valuePos.value === 'boolean'
      ) {
        return sqlConvertSingularLiteralArg(valuePos.value);
      }
      throw new Error(`Literal of unexpected type: ${typeof valuePos.value}`);
    }
    case 'static':
      throw new Error(
        'Static parameters must be bound to a value before compiling to SQL',
      );
    default:
      unreachable(otherType);
  }
}

/**
 * The two SQL pieces every JSON path comparison is built from: the leaf's text
 * (`raw`, SQL NULL for a missing key or a JSON null) and its JSON type
 * (`jsonType`, via `jsonb_typeof`).
 *
 * Navigation chains `->` with a *typed* operand per segment (a text operand
 * for an object key, an integer operand for an array index), so Postgres
 * applies the same strict rule as the in-memory reader and SQLite: a string
 * segment never indexes an array and a number never reads an object key. (A
 * `#>>` text[] path would instead resolve a digit string by the container's
 * runtime type, including `'-1'` from the end.) The last step uses `->>` for
 * the text form.
 *
 * The base is the column as jsonb: a `json` column is cast, and a Postgres
 * array column (which zero maps to a `json()` column and the replica stores
 * as JSON text) is converted with `to_jsonb`.
 */
function jsonPathParts(
  spec: Spec,
  ref: JsonPathReference,
  table: Table,
): {value: SQLQuery; raw: SQLQuery; jsonType: SQLQuery} {
  const {name} = ref.value;
  const col = colIdent(spec.server, {table, zql: name});
  const {type, isArray} = getServerColumn(spec.server, table, name);
  assert(
    isArray || type === 'json' || type === 'jsonb',
    () => `JSON path on non-JSON column "${name}" of type ${type}`,
  );
  return jsonNavigate(jsonBase(col, type, isArray), ref.path);
}

/** A json/jsonb/array column (or an expression of one) as jsonb. */
function jsonBase(col: SQLQuery, type: string, isArray: boolean): SQLQuery {
  return isArray
    ? sql`to_jsonb(${col})`
    : type === 'json'
      ? sql`${col}::jsonb`
      : col;
}

/** Navigates `path` from the jsonb `base`; see {@link jsonPathParts}. */
function jsonNavigate(
  base: SQLQuery,
  path: readonly (string | number)[],
): {value: SQLQuery; raw: SQLQuery; jsonType: SQLQuery} {
  let obj = base;
  const operand = (seg: string | number): SQLQuery =>
    typeof seg === 'number'
      ? // A validated non-negative int32 (see isValidJsonPathIndex), so it is
        // safe to inline; `->` needs an integer operand to index an array.
        sql.__dangerous__rawValue(String(seg))
      : sqlConvertSingularLiteralArg(seg);
  const last = path.length - 1;
  for (let i = 0; i < last; i++) {
    obj = sql`(${obj} -> ${operand(path[i])})`;
  }
  const leaf = operand(path[last]);
  return {
    // The leaf as jsonb; `raw` is its text form.
    value: sql`(${obj} -> ${leaf})`,
    raw: sql`(${obj} ->> ${leaf})`,
    jsonType: sql`jsonb_typeof(${obj} -> ${leaf})`,
  };
}

/**
 * The JS type the leaf must have to be compared against the literal on the
 * other side (`jsonLiteralType`, shared across engines). It is also the
 * `jsonb_typeof` name and the key into `pgTypeForLiteralType` for the cast, so
 * the leaf's cast and the literal's own cast cannot drift. `undefined` for a
 * `null` literal (`IS NULL`: a missing key and a JSON null must both read as
 * SQL NULL), an empty list, or a non-literal.
 */
function leafType(other: ValuePosition): PluralLiteralType | undefined {
  return other.type === 'literal' ? jsonLiteralType(other.value) : undefined;
}

/**
 * Compiles a JSON path reference to its text form, cast to the comparison type
 * derived from the literal on the other side (`leafType`) so it lines up with
 * that literal's own cast.
 *
 * `->>` maps **both** a missing key and a JSON `null` to SQL `NULL`, matching
 * the SQLite `json_extract` pushdown and the in-memory predicate, so `IS NULL`
 * agrees across all three.
 *
 * The cast is gated on the leaf's JSON type (`CASE WHEN jsonb_typeof(...)`), so a
 * leaf of a different type than the literal is SQL NULL, i.e. a non-match. This
 * is the type-strict comparison documented on {@link JsonPathReference}: `->>`
 * renders a numeric `42` as the text `'42'`, so an ungated text comparison
 * would wrongly match it against the string `'42'`. For `number`/`boolean` the
 * gate is also what keeps the cast from throwing: Postgres errors when casting
 * non-conforming text (a string `"n/a"`, an object's JSON text) to
 * `double precision`/`boolean`, which would fail the whole query on one
 * mismatched row. Negated operators need a different form; see
 * `jsonPathCondition`.
 */
function jsonPathLeaf(
  spec: Spec,
  ref: JsonPathReference,
  table: Table,
  other: ValuePosition,
): SQLQuery {
  const {raw, jsonType} = jsonPathParts(spec, ref, table);
  const t = leafType(other);
  if (t === undefined) {
    return raw;
  }
  return sql`(CASE WHEN ${jsonType} = ${sqlConvertSingularLiteralArg(
    t,
  )} THEN ${raw}::${sql.__dangerous__rawValue(pgTypeForLiteralType(t))} END)`;
}

/**
 * The comparisons on a JSON path left operand that cannot go through the
 * generic operator rendering with the gated leaf (`jsonPathLeaf`); returns
 * `undefined` for the ones that can.
 *
 * - `IN`/`NOT IN` with a `null` literal is constant-false, as in the
 *   in-memory predicate (the generic forms would assert, or match every
 *   non-null leaf).
 * - `=`/`IN` against a string or boolean literal compare the leaf *as jsonb*:
 *   jsonb equality is type-strict by itself (`"42"` never equals `42`, a JSON
 *   `null` never equals a string) and a missing key is SQL NULL, so the
 *   `jsonb_typeof` gate is unnecessary. Without the `CASE` around the
 *   extraction, an expression index on `(col -> 'key')` can serve the
 *   predicate. Numbers keep the gated `double precision` form: jsonb compares
 *   numbers exactly (`numeric`), which would diverge from the client's and
 *   SQLite's IEEE-754 equality above 2^53 or beyond double precision.
 * - An empty `NOT IN` list matches every non-null leaf: SQL's
 *   `NOT (x = ANY('{}'))` is TRUE even for a NULL leaf, whereas the
 *   predicate's null guard excludes it.
 * - The LIKE family compares text, so the leaf must be a string whatever the
 *   literal's type, and the pattern is the literal's text form, as in the
 *   in-memory predicate (`String(pattern)`) and SQLite. Deriving the leaf type
 *   from a numeric or boolean literal instead would cast the leaf to
 *   `double precision`/`boolean` and hand Postgres a LIKE it cannot execute.
 * - The negated operators (`!=`, `NOT LIKE`, `NOT ILIKE`, `NOT IN`): the gate
 *   makes a mismatched leaf SQL NULL, which a positive comparison correctly
 *   excludes. But NULL also excludes under a negated operator, where a
 *   mismatch must *match* (JS `42 !== '42'` is true). `jsonb_typeof` is NULL
 *   for a missing key and `'null'` for a JSON null, so one `COALESCE` covers
 *   the null guard without a second extraction: a null/missing leaf is
 *   false, the same type is the real (negated) comparison, another type is
 *   true.
 */
function jsonPathCondition(
  spec: Spec,
  left: JsonPathReference,
  condition: SimpleCondition,
  table: Table,
): SQLQuery | undefined {
  const {op} = condition;
  let {right} = condition;
  if (
    (op === 'IN' || op === 'NOT IN') &&
    right.type === 'literal' &&
    right.value === null
  ) {
    return sql`false`;
  }
  if (
    isLikeOperator(op) &&
    right.type === 'literal' &&
    right.value !== null &&
    !Array.isArray(right.value)
  ) {
    right = {type: 'literal', value: String(right.value)};
    if (!isNegatedOperator(op)) {
      return sql`${valueComparison(
        spec,
        left,
        table,
        right,
        false,
      )} ${sql.__dangerous__rawValue(op)} ${valueComparison(
        spec,
        right,
        table,
        left,
        false,
      )}`;
    }
  }
  if ((op === '=' || op === 'IN') && right.type === 'literal') {
    const t = leafType(right);
    if (t === 'string' || t === 'boolean') {
      const {value} = jsonPathParts(spec, left, table);
      const lit = valueComparison(spec, right, table, left, op === 'IN');
      return op === 'IN'
        ? sql`${value} = ANY (ARRAY(SELECT to_jsonb(v) FROM unnest(${lit}) AS v))`
        : sql`${value} = to_jsonb(${lit})`;
    }
  }
  // A non-literal (an unbound static parameter) falls through to the generic
  // rendering, which rejects it; it must not be mistaken for an empty list.
  if (!isNegatedOperator(op) || right.type !== 'literal') {
    return undefined;
  }
  const t = leafType(right);
  const {raw, jsonType} = jsonPathParts(spec, left, table);
  if (t === undefined) {
    // Only an empty NOT IN list reaches here: `!=`/`NOT LIKE` against null
    // are constant-false through the generic NULL comparison.
    return op === 'NOT IN' ? sql`${raw} IS NOT NULL` : undefined;
  }
  const leaf = sql`${raw}::${sql.__dangerous__rawValue(pgTypeForLiteralType(t))}`;
  const plural = op === 'NOT IN';
  const lit = valueComparison(spec, right, table, left, plural);
  const cmp = plural
    ? sql`NOT (${leaf} = ANY (${lit}))`
    : sql`${leaf} ${sql.__dangerous__rawValue(op)} ${lit}`;
  return sql`(CASE COALESCE(${jsonType}, 'null') WHEN 'null' THEN false WHEN ${sqlConvertSingularLiteralArg(
    t,
  )} THEN ${cmp} ELSE true END)`;
}

export function makeJunctionJoin(
  spec: Spec,
  relationship: CorrelatedSubquery,
): {
  join: SQLQuery;
  participatingTables: ReturnType<typeof pullTablesForJunction>;
} {
  const participatingTables = pullTablesForJunction(spec, relationship);
  const joins: SQLQuery[] = [];

  for (const {table} of participatingTables) {
    if (joins.length === 0) {
      joins.push(fromIdent(spec.server, table));
      continue;
    }
    joins.push(
      sql` JOIN ${fromIdent(spec.server, table)} ON ${makeCorrelator(
        spec,
        participatingTables[joins.length].correlation.parentField.map(f => ({
          table: participatingTables[joins.length - 1].table,
          zql: f,
        })),
        participatingTables[joins.length].correlation.childField,
      )(participatingTables[joins.length].table)}`,
    );
  }

  return {
    join: sql`${sql.join(joins, '')}`,
    participatingTables,
    // lastTable: participatingTables[participatingTables.length - 1].table,
    // lastLimit: participatingTables[participatingTables.length - 1].limit,
  };
}

export function pullTablesForJunction(
  spec: Spec,
  relationship: CorrelatedSubquery,
): [
  {
    table: Table;
    correlation: Correlation;
    limit: number | undefined;
  },
  {table: Table; correlation: Correlation; limit: number | undefined},
] {
  assert(
    relationship.subquery.related?.length === 1,
    'Too many related tables for a junction edge',
  );
  const otherRelationship = relationship.subquery.related[0];
  assert(
    !otherRelationship.hidden,
    'Expected junction edge relationship to not be hidden',
  );
  return [
    {
      table: makeTable(spec, relationship.subquery.table),
      correlation: relationship.correlation,
      limit: relationship.subquery.limit,
    },
    {
      table: makeTable(spec, otherRelationship.subquery.table),
      correlation: otherRelationship.correlation,
      limit: otherRelationship.subquery.limit,
    },
  ];
}

function toJSON(table: string, singular = false): SQLQuery {
  return sql`${
    singular ? sql`` : sql`COALESCE(json_agg`
  }(row_to_json(${sql.ident(table)}))${singular ? sql`` : sql`, '[]'::json)`}`;
}

function selectIdent(server: ServerSpec, column: QualifiedColumn): SQLQuery {
  const serverColumnSchema =
    server.schema[server.mapper.tableName(column.table.zql)][
      server.mapper.columnName(column.table.zql, column.zql)
    ];
  const serverType = serverColumnSchema.type;
  // A user-defined type can have a built-in name such as `timestamp`; only
  // convert the values of the built-in types.
  if (
    !serverColumnSchema.isEnum &&
    isBuiltinTypeSchema(serverColumnSchema.typeSchema)
  ) {
    let needsNormalization = false;
    switch (serverType) {
      case 'timetz':
      // @ts-expect-error Fallthrough intended
      case 'time with time zone':
        needsNormalization = true;
      // fallthrough

      case 'date':
      case 'time':
      case 'time without time zone':
      case 'timestamp':
      case 'timestamp without time zone':
      case 'timestamptz':
      case 'timestamp with time zone': {
        // EXTRACT(EPOCH FROM timetz) can be negative when the UTC offset is
        // positive (e.g. 01:00+02 = 23:00 UTC prev day = -3600s). Wrap with
        // modular arithmetic to normalize to 0..86400000.
        const toMs = (epochExpr: SQLQuery): SQLQuery =>
          needsNormalization
            ? sql`((${epochExpr})::bigint + 86400000) % 86400000`
            : epochExpr;

        if (serverColumnSchema.isArray) {
          const col = colIdent(server, column);
          return sql`CASE WHEN ${col} IS NULL THEN NULL ELSE ARRAY(SELECT ${toMs(
            sql`EXTRACT(EPOCH FROM unnest(${col})) * 1000`,
          )}) END as ${sql.ident(column.zql)}`;
        }

        return sql`${toMs(
          sql`EXTRACT(EPOCH FROM ${colIdent(server, column)}) * 1000`,
        )} as ${sql.ident(column.zql)}`;
      }
    }
  }

  return sql`${colIdent(server, column)} as ${sql.ident(column.zql)}`;
}

function colIdent(server: ServerSpec, column: QualifiedColumn) {
  return sql.ident(
    column.table.alias,
    server.mapper.columnName(column.table.zql, column.zql),
  );
}

function fromIdent(server: ServerSpec, table: Table) {
  return sql`${sql.ident(server.mapper.tableName(table.zql))} AS ${sql.ident(table.alias)}`;
}

function getServerColumn(spec: ServerSpec, table: Table, zqlColumn: string) {
  return spec.schema[spec.mapper.tableName(table.zql)][
    spec.mapper.columnName(table.zql, zqlColumn)
  ];
}

// oxlint-disable-next-line @typescript-eslint/no-explicit-any
export function extractZqlResult(pgResult: Array<any>): JSONValue {
  const bigIntJson: BigIntJSONValue = parseBigIntJson(
    pgResult[0][ZQL_RESULT_KEY],
  );
  assertJSONValue(bigIntJson);
  return bigIntJson;
}

function assertJSONValue(v: BigIntJSONValue): asserts v is JSONValue {
  const path = findPathToBigInt(v);
  if (path) {
    throw new Error(`Value exceeds safe Number range. ${path}`);
  }
}

function findPathToBigInt(v: BigIntJSONValue): string | undefined {
  const typeOfV = typeof v;
  switch (typeOfV) {
    case 'bigint':
      return ` = ${v}`;
    case 'object': {
      if (v === null) {
        return;
      }
      if (Array.isArray(v)) {
        for (let i = 0; i < v.length; i++) {
          const path = findPathToBigInt(v[i]);
          if (path) {
            return `[${i}]${path}`;
          }
        }
        return undefined;
      }

      const o = v as Record<string, BigIntJSONValue>;
      for (const k in o) {
        if (hasOwn(o, k)) {
          const path = findPathToBigInt(o[k]);
          if (path) {
            return `['${k}']${path}`;
          }
        }
      }
      return undefined;
    }
    case 'number':
      return undefined;
    case 'boolean':
      return undefined;
    default:
      return undefined;
  }
}
