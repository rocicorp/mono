import {assert} from '../../shared/src/asserts.ts';
import {must} from '../../shared/src/must.ts';
import type {
  CorrelatedSubqueryCondition,
  Correlation,
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
import {sql} from './sql.ts';
import type {SQLQuery} from '@databases/sql';

type Tables = Record<string, TableSchema>;

/**
 * Compiles to the Postgres dialect of SQL
 * - IS, IS NOT can only compare against `NULL`, `TRUE`, `FALSE` so use
 *   `IS DISTINCT FROM` and `IS NOT DISTINCT FROM` instead
 * - IN is changed to ANY to allow binding array literals
 * - subqueries are aggregated using PG's `array_agg` and `row_to_json` functions
 */
export function compile(ast: AST, tables: Tables, format?: Format | undefined) {
  const compiler = new Compiler(tables);
  return compiler.compile(ast, format);
}

export class Compiler {
  readonly #tables: Tables;
  readonly #nameMapper: NameMapper;

  constructor(tables: Tables) {
    this.#tables = tables;
    this.#nameMapper = clientToServer(tables);
  }

  compile(ast: AST, format?: Format | undefined) {
    return this.select(ast, format, undefined);
  }

  select(
    ast: AST,
    // Is this a singular or plural query?
    format: Format | undefined,
    // If a select is being used as a subquery, this is the correlation to the parent query
    correlation: SQLQuery | undefined,
  ) {
    const selectionSet = this.related(ast.related ?? [], format, ast.table);
    const table = this.#tables[ast.table];
    for (const column of Object.keys(table.columns)) {
      selectionSet.push(
        sql`${sql.ident(ast.table)}.${this.#mapColumn(ast.table, column)}`,
      );
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
      orderBy.map(([col, dir]) =>
        dir === 'asc'
          ? // Oh postgres. The table must be referred to be client name but the column by server name.
            // E.g., `SELECT server_col as client_col FROM server_table as client_table ORDER BY client_Table.server_col`
            sql`${sql.ident(table)}.${this.#mapColumnNoAlias(table, col)} ASC`
          : sql`${sql.ident(table)}.${this.#mapColumnNoAlias(table, col)} DESC`,
      ),
      ', ',
    )}`;
  }

  limit(limit: number | undefined): SQLQuery {
    if (!limit) {
      return sql``;
    }
    return sql`LIMIT ${sql.value(limit)}`;
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
  ) {
    if (relationship.hidden) {
      const [join, lastAlias, lastLimit, lastTable] =
        this.makeJunctionJoin(relationship);
      const lastClientColumns = Object.keys(this.#tables[lastTable].columns);
      /**
       * This aggregates the relationship subquery into an array of objects.
       * This looks roughly like:
       *
       * SELECT COALESCE(array_agg(row_to_json("inner_table")) , ARRAY[]::json[]) FROM
       *  (SELECT inner.col as client_col, inner.col2 as client_col2 FROM table) inner_table;
       */
      return sql`(
        SELECT ${
          format?.singular ? sql`` : sql`COALESCE(array_agg`
        }(row_to_json(${sql.ident(`inner_${relationship.subquery.alias}`)})) ${
          format?.singular ? sql`` : sql`, ARRAY[]::json[])`
        } FROM (SELECT ${sql.join(
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
        format?.singular ? sql`` : sql`COALESCE(array_agg`
      }(row_to_json(${sql.ident(`inner_${relationship.subquery.alias}`)})) ${
        format?.singular ? sql`` : sql`, ARRAY[]::json[])`
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
        return sql`${this.valuePosition(
          condition.left,
          table,
        )} ${sql.__dangerous__rawValue(condition.op)} ${this.valuePosition(
          condition.right,
          table,
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
    return sql`${this.valuePosition(condition.left, table)} ${
      condition.op === 'IS' ? sql`IS NOT DISTINCT FROM` : sql`IS DISTINCT FROM`
    } ${this.valuePosition(condition.right, table)}`;
  }

  any(condition: SimpleCondition, table: string): SQLQuery {
    return sql`${this.valuePosition(condition.left, table)} ${
      condition.op === 'IN' ? sql`= ANY` : sql`!= ANY`
    } (${this.valuePosition(condition.right, table)})`;
  }

  valuePosition(value: ValuePosition, table: string): SQLQuery {
    switch (value.type) {
      case 'column':
        return this.#mapColumnNoAlias(table, value.name);
      case 'literal':
        return sql.value(value.value);
      case 'static':
        throw new Error(
          'Static parameters must be bound to a value before compiling to SQL',
        );
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
  ) {
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
}

function zip<T>(a1: readonly T[], a2: readonly T[]): [T, T][] {
  assert(a1.length === a2.length);
  const result: [T, T][] = [];
  for (let i = 0; i < a1.length; i++) {
    result.push([a1[i], a2[i]]);
  }
  return result;
}
