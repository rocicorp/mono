import type {SQLQuery} from '@databases/sql';
import {zip} from '../../shared/src/arrays.ts';
import {assert, unreachable} from '../../shared/src/asserts.ts';
import {type JSONValue} from '../../shared/src/json.ts';
import {must} from '../../shared/src/must.ts';
import type {
  CorrelatedSubqueryCondition,
  Correlation,
  LiteralReference,
  Ordering,
  ValuePosition,
} from '../../zero-protocol/src/ast.ts';
import {
  type AST,
  type Condition,
  type CorrelatedSubquery,
  type SimpleCondition,
} from '../../zero-protocol/src/ast.ts';
import {clientToServer, NameMapper} from '../../zero-schema/src/name-mapper.ts';
import type {TableSchema} from '../../zero-schema/src/table-schema.ts';
import type {Format} from '../../zql/src/ivm/view.ts';
import {
  sql,
  sqlConvertColumnArg,
  type PluralLiteralType,
  sqlConvertSingularLiteralArg,
  sqlConvertPluralLiteralArg,
  Z2S_COLLATION,
} from './sql.ts';
import {
  type JSONValue as BigIntJSONValue,
  parse as parseBigIntJson,
} from '../../zero-cache/src/types/bigint-json.ts';
import {hasOwn} from '../../shared/src/has-own.ts';
import type {ServerColumnSchema, ServerSchema} from './schema.ts';

type Tables = Record<string, TableSchema>;

const ZQL_RESULT_KEY = 'zql_result';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
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

/**
 * Compiles to the Postgres dialect of SQL
 * - IS, IS NOT can only compare against `NULL`, `TRUE`, `FALSE` so use
 *   `IS DISTINCT FROM` and `IS NOT DISTINCT FROM` instead
 * - IN is changed to ANY to allow binding array literals
 * - subqueries are aggregated using PG's `array_agg` and `row_to_json` functions
 */
export function compile(
  ast: AST,
  tables: Tables,
  serverSchema: ServerSchema,
  format?: Format | undefined,
): SQLQuery {
  const compiler = new Compiler(tables, serverSchema);
  return compiler.compile(ast, format);
}

export class Compiler {
  readonly #tables: Tables;
  readonly #serverSchema: ServerSchema;
  readonly #nameMapper: NameMapper;

  constructor(tables: Tables, serverSchema: ServerSchema) {
    this.#tables = tables;
    this.#serverSchema = serverSchema;
    this.#nameMapper = clientToServer(tables);
  }

  compile(ast: AST, format?: Format | undefined): SQLQuery {
    return sql`SELECT ${this.#toJSON(
      `root`,
      format?.singular,
    )}::text as ${sql.ident(ZQL_RESULT_KEY)} FROM (${this.select(
      ast,
      format,
      undefined,
    )})${sql.ident(`root`)}`;
  }

  select(
    ast: AST,
    // Is this a singular or plural query?
    format: Format | undefined,
    // If a select is being used as a subquery, this is the correlation to the parent query
    correlation: SQLQuery | undefined,
  ): SQLQuery {
    const selectionSet = this.related(ast.related ?? [], format, ast.table);
    const tableSchema = this.#tables[ast.table];
    for (const column of Object.keys(tableSchema.columns)) {
      selectionSet.push(this.#selectCol(ast.table, column));
    }
    return sql`SELECT ${sql.join(selectionSet, ',')} FROM ${this.#mapTable(
      ast.table,
    )} ${ast.where ? sql`WHERE ${this.where(ast.where, ast.table)}` : sql``} ${
      correlation
        ? sql`${ast.where ? sql`AND` : sql`WHERE`} (${correlation})`
        : sql``
    } ${this.orderBy(ast.orderBy, ast.table)} ${
      format?.singular ? this.limit(1) : this.limit(ast.limit)
    }`;
  }

  orderBy(orderBy: Ordering | undefined, table: string): SQLQuery {
    if (!orderBy) {
      return sql``;
    }
    return sql`ORDER BY ${sql.join(
      orderBy.map(([col, dir]) => {
        const serverColumnSchema =
          this.#serverSchema[this.#nameMapper.tableName(table)][
            this.#nameMapper.columnName(table, col)
          ];
        return dir === 'asc'
          ? // Oh postgres. The table must be referred to be client name but the column by server name.
            // E.g., `SELECT server_col as client_col FROM server_table as client_table ORDER BY client_Table.server_col`
            sql`${sql.ident(table)}.${this.#mapColumnNoAlias(table, col)}${this.#maybeCollate(serverColumnSchema)} ASC`
          : sql`${sql.ident(table)}.${this.#mapColumnNoAlias(table, col)}${this.#maybeCollate(serverColumnSchema)} DESC`;
      }),
      ', ',
    )}`;
  }

  #maybeCollate(serverColumnSchema: ServerColumnSchema) {
    if (
      serverColumnSchema.type === 'text' ||
      serverColumnSchema.type === 'char' ||
      serverColumnSchema.type === 'varchar'
    ) {
      return sql` COLLATE ${sql.ident(Z2S_COLLATION)}`;
    }
    if (serverColumnSchema.type === 'uuid' || serverColumnSchema.isEnum) {
      return sql`::text COLLATE ${sql.ident(Z2S_COLLATION)}`;
    }

    return sql``;
  }

  limit(limit: number | undefined): SQLQuery {
    if (!limit) {
      return sql``;
    }
    return sql`LIMIT ${sqlConvertSingularLiteralArg(limit)}`;
  }

  related(
    relationships: readonly CorrelatedSubquery[],
    format: Format | undefined,
    parentTable: string,
  ): SQLQuery[] {
    return relationships.map(relationship =>
      this.relationshipSubquery(
        relationship,
        format?.relationships[must(relationship.subquery.alias)],
        parentTable,
      ),
    );
  }

  relationshipSubquery(
    relationship: CorrelatedSubquery,
    format: Format | undefined,
    parentTable: string,
  ): SQLQuery {
    if (relationship.hidden) {
      const [join, lastAlias, lastLimit, lastTable] =
        this.makeJunctionJoin(relationship);
      const lastClientColumns = Object.keys(this.#tables[lastTable].columns);
      /**
       * This aggregates the relationship subquery into an array of objects.
       * This looks roughly like:
       *
       * SELECT COALESCE(json_agg(row_to_json("inner_owner")) , '[]'::json) FROM
       * (SELECT mytable.col as client_col, mytable.col2 as client_col2 FROM mytable) inner_mytable
       */
      return sql`(
        SELECT ${this.#toJSON(
          `inner_${relationship.subquery.alias}`,
          format?.singular,
        )} FROM (SELECT ${sql.join(
          lastClientColumns.map(
            c => sql`${sql.ident(lastAlias)}.${this.#mapColumn(lastTable, c)}`,
          ),
          ',',
        )} FROM ${join} WHERE (${this.correlate(
          parentTable,
          parentTable,
          relationship.correlation.parentField,
          relationship.subquery.table,
          relationship.subquery.table,
          relationship.correlation.childField,
        )}) ${
          relationship.subquery.where
            ? sql`AND ${this.where(
                relationship.subquery.where,
                relationship.subquery.table,
              )}`
            : sql``
        } ${this.orderBy(
          relationship.subquery.orderBy,
          relationship.subquery.table,
        )} ${
          format?.singular ? this.limit(1) : this.limit(lastLimit)
        } ) ${sql.ident(`inner_${relationship.subquery.alias}`)}
      ) as ${sql.ident(relationship.subquery.alias)}`;
    }
    return sql`(
      SELECT ${
        format?.singular ? sql`` : sql`COALESCE(json_agg`
      }(row_to_json(${sql.ident(`inner_${relationship.subquery.alias}`)})) ${
        format?.singular ? sql`` : sql`, '[]'::json)`
      } FROM (${this.select(
        relationship.subquery,
        format,
        this.correlate(
          parentTable,
          parentTable,
          relationship.correlation.parentField,
          relationship.subquery.table,
          relationship.subquery.table,
          relationship.correlation.childField,
        ),
      )}) ${sql.ident(`inner_${relationship.subquery.alias}`)}
    ) as ${sql.ident(relationship.subquery.alias)}`;
  }

  pullTablesForJunction(
    relationship: CorrelatedSubquery,
    tables: [string, Correlation, number | undefined][] = [],
  ) {
    tables.push([
      relationship.subquery.table,
      relationship.correlation,
      relationship.subquery.limit,
    ]);
    assert(
      relationship.subquery.related?.length || 0 <= 1,
      'Too many related tables for a junction edge',
    );
    for (const subRelationship of relationship.subquery.related ?? []) {
      this.pullTablesForJunction(subRelationship, tables);
    }
    return tables;
  }

  makeJunctionJoin(
    relationship: CorrelatedSubquery,
  ): [
    join: SQLQuery,
    lastAlis: string,
    lastLimit: number | undefined,
    lastTable: string,
  ] {
    const participatingTables = this.pullTablesForJunction(relationship);
    const joins: SQLQuery[] = [];

    function alias(index: number) {
      if (index === 0) {
        return participatingTables[0][0];
      }
      return `table_${index}`;
    }

    for (const [table, _correlation] of participatingTables) {
      if (joins.length === 0) {
        joins.push(this.#mapTable(table));
        continue;
      }
      joins.push(
        sql` JOIN ${this.#mapTableNoAlias(table)} as ${sql.ident(
          alias(joins.length),
        )} ON ${this.correlate(
          participatingTables[joins.length - 1][0],
          alias(joins.length - 1),
          participatingTables[joins.length][1].parentField,
          participatingTables[joins.length][0],
          alias(joins.length),
          participatingTables[joins.length][1].childField,
        )}`,
      );
    }

    return [
      sql.join(joins, ''),
      alias(joins.length - 1),
      participatingTables[participatingTables.length - 1][2],
      participatingTables[participatingTables.length - 1][0],
    ] as const;
  }

  where(condition: Condition | undefined, table: string): SQLQuery {
    if (!condition) {
      return sql``;
    }

    switch (condition.type) {
      case 'and':
        return sql`(${sql.join(
          condition.conditions.map(c => this.where(c, table)),
          ' AND ',
        )})`;
      case 'or':
        return sql`(${sql.join(
          condition.conditions.map(c => this.where(c, table)),
          ' OR ',
        )})`;
      case 'correlatedSubquery':
        return this.exists(condition, table);
      case 'simple':
        return this.simple(condition, table);
    }
  }

  simple(condition: SimpleCondition, table: string): SQLQuery {
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
        return sql`${this.valueComparison(
          condition.left,
          table,
          condition.right,
          false,
        )} ${sql.__dangerous__rawValue(condition.op)} ${this.valueComparison(
          condition.right,
          table,
          condition.left,
          false,
        )}`;
      case 'NOT IN':
      case 'IN':
        return this.any(condition, table);
      case 'IS':
      case 'IS NOT':
        return this.distinctFrom(condition, table);
    }
  }

  distinctFrom(condition: SimpleCondition, table: string): SQLQuery {
    return sql`${this.valueComparison(condition.left, table, condition.right, false)} ${
      condition.op === 'IS' ? sql`IS NOT DISTINCT FROM` : sql`IS DISTINCT FROM`
    } ${this.valueComparison(condition.right, table, condition.left, false)}`;
  }

  any(condition: SimpleCondition, table: string): SQLQuery {
    return sql`${this.valueComparison(condition.left, table, condition.right, false)} ${
      condition.op === 'IN' ? sql`= ANY` : sql`!= ANY`
    } (${this.valueComparison(condition.right, table, condition.left, true)})`;
  }

  valueComparison(
    valuePos: ValuePosition,
    table: string,
    otherValuePos: ValuePosition,
    plural: boolean,
  ): SQLQuery {
    const valuePosType = valuePos.type;
    switch (valuePosType) {
      case 'column': {
        const serverColumnSchema =
          this.#serverSchema[this.#nameMapper.tableName(table)][
            this.#nameMapper.columnName(table, valuePos.name)
          ];
        if (serverColumnSchema.type === 'uuid' || serverColumnSchema.isEnum) {
          return sql`${this.#mapColumnNoAlias(table, valuePos.name)}::text`;
        }
        return this.#mapColumnNoAlias(table, valuePos.name);
      }
      case 'literal':
        return this.#literalValueComparison(
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

  #literalValueComparison(
    valuePos: LiteralReference,
    table: string,
    otherValuePos: ValuePosition,
    plural: boolean,
  ) {
    {
      const otherType = otherValuePos.type;
      switch (otherType) {
        case 'column':
          return sqlConvertColumnArg(
            this.#serverSchema[this.#nameMapper.tableName(table)][
              this.#nameMapper.columnName(table, otherValuePos.name)
            ],
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
  }

  exists(
    condition: CorrelatedSubqueryCondition,
    parentTable: string,
  ): SQLQuery {
    switch (condition.op) {
      case 'EXISTS':
        return sql`EXISTS (${this.select(
          condition.related.subquery,
          undefined,
          this.correlate(
            parentTable,
            parentTable,
            condition.related.correlation.parentField,
            condition.related.subquery.table,
            condition.related.subquery.table,
            condition.related.correlation.childField,
          ),
        )})`;
      case 'NOT EXISTS':
        return sql`NOT EXISTS (${this.select(
          condition.related.subquery,
          undefined,
          undefined,
        )})`;
    }
  }

  correlate(
    // The table being correlated could be aliased to some other name
    // in the case of a junction. Hence we pass `xTableAlias`. The original
    // name of the table is required so we can look up the server names of the columns
    // to be used in the correlation.
    parentTable: string,
    parentTableAlias: string,
    parentColumns: readonly string[],
    childTable: string,
    childTableAlias: string,
    childColumns: readonly string[],
  ): SQLQuery {
    return sql.join(
      zip(parentColumns, childColumns).map(
        ([parentColumn, childColumn]) =>
          sql`${sql.ident(parentTableAlias)}.${this.#mapColumnNoAlias(
            parentTable,
            parentColumn,
          )} = ${sql.ident(childTableAlias)}.${this.#mapColumnNoAlias(
            childTable,
            childColumn,
          )}`,
      ),
      ' AND ',
    );
  }

  #mapColumn(table: string, column: string) {
    const mapped = this.#nameMapper.columnName(table, column);
    if (mapped === column) {
      return sql.ident(column);
    }

    return sql`${sql.ident(mapped)} as ${sql.ident(column)}`;
  }

  #mapColumnNoAlias(table: string, column: string) {
    const mapped = this.#nameMapper.columnName(table, column);
    return sql.ident(mapped);
  }

  #mapTable(table: string) {
    const mapped = this.#nameMapper.tableName(table);
    if (mapped === table) {
      return sql.ident(table);
    }

    return sql`${sql.ident(mapped)} as ${sql.ident(table)}`;
  }

  #mapTableNoAlias(table: string) {
    const mapped = this.#nameMapper.tableName(table);
    return sql.ident(mapped);
  }

  #selectCol(table: string, column: string) {
    const serverColumnSchema =
      this.#serverSchema[this.#nameMapper.tableName(table)][
        this.#nameMapper.columnName(table, column)
      ];
    const serverType = serverColumnSchema.type;
    if (
      !serverColumnSchema.isEnum &&
      (serverType === 'date' ||
        serverType === 'timestamp' ||
        serverType === 'timestamptz' ||
        serverType === 'timestamp with time zone' ||
        serverType === 'timestamp without time zone')
    ) {
      return sql`EXTRACT(EPOCH FROM ${sql.ident(
        table,
      )}.${this.#mapColumnNoAlias(
        table,
        column,
      )}::timestamp AT TIME ZONE 'UTC') * 1000 as ${sql.ident(column)}`;
    }
    return sql`${sql.ident(table)}.${this.#mapColumn(table, column)}`;
  }

  #toJSON(table: string, singular = false): SQLQuery {
    return sql`${
      singular ? sql`` : sql`COALESCE(json_agg`
    }(row_to_json(${sql.ident(table)}))${
      singular ? sql`` : sql`, '[]'::json)`
    }`;
  }
}
