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
import {pgToZqlStringTypeMap} from '../../zero-cache/src/types/pg-data-type.ts';
import type {
  AST,
  Condition,
  CorrelatedSubquery,
  CorrelatedSubqueryCondition,
  Correlation,
  LiteralReference,
  Ordering,
  ScalarSubqueryCondition,
  SimpleCondition,
  ValuePosition,
} from '../../zero-protocol/src/ast.ts';
import {
  clientToServer,
  type NameMapper,
} from '../../zero-schema/src/name-mapper.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {
  ServerColumnSchema,
  ServerSchema,
} from '../../zero-types/src/server-schema.ts';
import type {Format} from '../../zql/src/ivm/view.ts';
import {completeOrdering} from '../../zql/src/query/complete-ordering.ts';
import {
  sql,
  sqlConvertColumnArg,
  sqlConvertPluralLiteralArg,
  sqlConvertSingularLiteralArg,
  Z2S_COLLATION,
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
  return sql`SELECT 
    ${toJSON(ZQL_RESULT_TABLE_KEY, format?.singular)}::text AS ${ZQL_RESULT_KEY_IDENT}
    FROM (${select(spec, ast, format)}) ${ZQL_RESULT_TABLE_IDENT}`;
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
  function maybeWhere(test: unknown | undefined) {
    if (!test) {
      return sql``;
    }

    const ret = appliedWhere ? sql`AND` : sql`WHERE`;
    appliedWhere = true;
    return ret;
  }

  return sql`SELECT ${sql.join(selectionSet, ',')}
    FROM ${fromIdent(spec.server, table)}
    ${maybeWhere(ast.where)} ${where(spec, ast.where, table)}
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
    orderBy.map(([col, dir]) => {
      const serverColumnSchema = getServerColumn(spec.server, table, col);
      return dir === 'asc'
        ? // Oh postgres. The table must be referred to by client name but the column by server name.
          // E.g., `SELECT server_col as client_col FROM server_table as client_table ORDER BY client_Table.server_col`
          sql`${colIdent(spec.server, {
            table,
            zql: col,
          })}${maybeCollate(serverColumnSchema)} ASC NULLS FIRST`
        : sql`${colIdent(spec.server, {
            table,
            zql: col,
          })}${maybeCollate(serverColumnSchema)} DESC NULLS LAST`;
    }),
    ', ',
  )}`;
}

function maybeCollate(serverColumnSchema: ServerColumnSchema): SQLQuery {
  if (serverColumnSchema.type === 'uuid' || serverColumnSchema.isEnum) {
    return sql`::text COLLATE ${sql.ident(Z2S_COLLATION)}`;
  }
  if (Object.hasOwn(pgToZqlStringTypeMap, serverColumnSchema.type)) {
    return sql` COLLATE ${sql.ident(Z2S_COLLATION)}`;
  }

  return sql``;
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
      return exists(spec, condition, table);
    case 'scalarSubquery':
      return scalarSubquery(spec, condition, table);
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

function scalarSubquery(
  spec: Spec,
  condition: ScalarSubqueryCondition,
  parentTable: Table,
): SQLQuery {
  const parentCol = colIdent(spec.server, {
    table: parentTable,
    zql: condition.parentField,
  });

  const subqueryTable = makeTable(spec, condition.subquery.table);
  const childCol = colIdent(spec.server, {
    table: subqueryTable,
    zql: condition.childField,
  });

  const op = sql.__dangerous__rawValue(condition.op);

  const subqueryWhere = condition.subquery.where
    ? sql`WHERE ${where(spec, condition.subquery.where, subqueryTable)}`
    : sql``;
  const subqueryOrderBy = orderBy(
    spec,
    condition.subquery.orderBy,
    subqueryTable,
  );

  return sql`${parentCol} ${op} (SELECT ${childCol} FROM ${fromIdent(spec.server, subqueryTable)} ${subqueryWhere} ${subqueryOrderBy} LIMIT 1)`;
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
      const serverColumnSchema = getServerColumn(
        spec.server,
        table,
        valuePos.name,
      );
      const qualified: QualifiedColumn = {
        table,
        zql: valuePos.name,
      };
      if (serverColumnSchema.type === 'uuid' || serverColumnSchema.isEnum) {
        return sql`${colIdent(spec.server, qualified)}::text`;
      }
      return colIdent(spec.server, qualified);
    }
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
      break;
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
    case 'literal': {
      assert(plural === Array.isArray(valuePos.value));
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
      throw new Error(
        `Literal of unexpected type. ${valuePos.value} of type ${typeof valuePos.value}`,
      );
    }
    case 'static':
      throw new Error(
        'Static parameters must be bound to a value before compiling to SQL',
      );
    default:
      unreachable(otherType);
  }
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
  assert(!otherRelationship.hidden);
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
  if (
    !serverColumnSchema.isEnum &&
    (serverType === 'date' ||
      serverType === 'timestamp' ||
      serverType === 'timestamp without time zone' ||
      serverType === 'timestamptz' ||
      serverType === 'timestamp with time zone')
  ) {
    if (serverColumnSchema.isArray) {
      // Map EXTRACT(EPOCH FROM ...) * 1000 over array elements
      return sql`ARRAY(SELECT EXTRACT(EPOCH FROM unnest(${colIdent(server, column)})) * 1000) as ${sql.ident(column.zql)}`;
    }
    return sql`EXTRACT(EPOCH FROM ${colIdent(server, column)}) * 1000 as ${sql.ident(column.zql)}`;
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
