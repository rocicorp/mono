import {assert} from 'shared/src/asserts.js';
import {CustomKeyMap} from 'shared/src/custom-key-map.js';
import {CustomKeySet} from 'shared/src/custom-key-set.js';
import type {AST, Selector} from 'zql/src/zql/ast/ast.js';
import {stringify, type JSONObject} from '../../types/bigint-json.js';
import {rowIDHash} from '../../types/row-key.js';
import {deaggregateArrays} from '../../zql/deaggregation.js';
import {
  ALIAS_COMPONENT_SEPARATOR,
  expandSelection,
} from '../../zql/expansion.js';
import {
  computeInvalidationInfo,
  type InvalidationInfo,
} from '../../zql/invalidation.js';
import {Normalized} from '../../zql/normalize.js';
import type {ServerAST} from '../../zql/server-ast.js';
import {ZERO_VERSION_COLUMN_NAME} from '../replicator/schema/replication-state.js';
import type {TableSpec} from '../replicator/tables/specs.js';
import type {QueryRecord, RowID, RowRecord} from './schema/types.js';

export class InvalidQueryError extends Error {}

export type TransformedQuery = {
  /**
   * Note that multiple client queries can be normalized into the same transformed
   * query. For example, all of the following statements:
   *
   * ```sql
   * SELECT id FROM foo WHERE bar = 1;
   * SELECT id AS whatever FROM foo WHERE bar = 1;
   * SELECT id, bar FROM foo WHERE bar = 1;
   * ```
   *
   * are transformed to the equivalent server-side query; the server ignores the
   * final aliases and fetches all of the columns necessary for the client
   * (re-)compute the results.
   *
   * This, a transformed query may be associated with multiple (client) `queryIDs`.
   */
  readonly queryIDs: readonly string[];
  readonly transformedAST: Normalized;
  readonly transformationHash: string;
  readonly columnAliases: Map<string, AliasInfo>;
  readonly invalidationInfo: InvalidationInfo;
};

export class QueryHandler {
  readonly #tables: TableSchemas;

  constructor(tables: readonly TableSpec[]) {
    this.#tables = new TableSchemas(tables);
  }

  #getTableSpecOrError(schema = 'public', table: string, ast: AST) {
    const t = this.#tables.spec(schema, table);
    if (!t) {
      throw new InvalidQueryError(
        `Unknown table "${table}" in ${JSON.stringify(ast)}`,
      );
    }
    return t;
  }

  /**
   * Transforms the client-desired queries into normalized, expanded versions that
   * includes primary key columns, the row version column, and all columns required
   * to compute the query.
   *
   * Returns a mapping from `transformationHash` to {@link TransformedQuery}.
   */
  transform(
    queries: readonly (QueryRecord | {id: string; ast: AST})[],
  ): Map<string, TransformedQuery> {
    // Mutable version for constructing the object.
    type TransformedQueryBuilder = TransformedQuery & {queryIDs: string[]};
    const transformed = new Map<string, TransformedQueryBuilder>();

    for (const q of queries) {
      const requiredColumns = (
        schema = 'public',
        table: string,
      ): Selector[] => {
        const t = this.#getTableSpecOrError(schema, table, q.ast);
        return [
          ...t.primaryKey.map(pk => [table, pk] as const),
          [table, ZERO_VERSION_COLUMN_NAME] as const,
        ];
      };

      const isPrimaryKey = (schema = 'public', table: string, col: string) => {
        const t = this.#getTableSpecOrError(schema, table, q.ast);
        return t.primaryKey.length === 1 && t.primaryKey[0] === col;
      };

      const deaggregated = deaggregateArrays(q.ast, isPrimaryKey);
      const expanded = expandSelection(deaggregated, requiredColumns);
      const {ast: minified, columnAliases} = minifyAliases(expanded);
      const transformedAST = new Normalized(minified);
      const transformationHash = transformedAST.hash();

      const exists = transformed.get(transformationHash);
      if (exists) {
        exists.queryIDs = union(exists.queryIDs, [q.id]);
      } else {
        const invalidationInfo = computeInvalidationInfo(transformedAST);
        transformed.set(transformationHash, {
          queryIDs: [q.id],
          transformedAST,
          transformationHash,
          columnAliases,
          invalidationInfo,
        });
      }
    }
    return transformed;
  }

  /**
   * Returns an object for deconstructing each result from executed queries
   * into its constituent tables and rows.
   */
  resultParser(
    queryIDs: readonly string[],
    columnAliases: Map<string, AliasInfo>,
  ) {
    return new ResultParser(this.#tables, queryIDs, columnAliases);
  }

  tableSpec(schema: string, table: string) {
    return this.#tables.spec(schema, table);
  }
}

export type ParsedRow = {
  record: Omit<RowRecord, 'patchVersion'>;
  contents: JSONObject;
};

class ResultParser {
  readonly #tables: TableSchemas;
  readonly #queryIDs: readonly string[];
  readonly #columnAliases: Map<string, AliasInfo>;
  // Maps sub-query names to row-paths to dedupe redundant rows from deaggregations.
  readonly #subQueryRows = new Map<string, Set<RowID>>();

  /**
   * @param queryIDs The query ID(s) with which the query is associated. See
   *        {@link TransformedQuery.queryIDs} for why there may be more than one.
   */
  constructor(
    tables: TableSchemas,
    queryIDs: readonly string[],
    columnAliases: Map<string, AliasInfo>,
  ) {
    this.#tables = tables;
    this.#queryIDs = queryIDs;
    this.#columnAliases = columnAliases;
  }

  /**
   * Parses the query results by decomposing each result into its constituent
   * rows, according to the column naming schema defined by {@link expandSelection}.
   * Multiple views of rows from different queries are merged, with the query to column
   * mapping tracked in the `record` field of the returned {@link ParsedRow}.
   *
   * Returns a mapping from the CVR row ID to {@link ParsedRow}.
   *
   */
  parseResults(results: readonly JSONObject[]): Map<RowID, ParsedRow> {
    type ExtractedRow = {
      aliasInfo: AliasInfo;
      rowWithVersion: JSONObject;
    };

    const parsed = new CustomKeyMap<RowID, ParsedRow>(rowIDHash);
    for (const result of results) {
      // Partition the result's column-aliases into their respective sub-queried rows.
      const subQueries = new Map<string, ExtractedRow>();
      for (const [alias, value] of Object.entries(result)) {
        const aliasInfo = this.#columnAliases.get(alias);
        assert(aliasInfo, `Unexpected column alias ${alias}`);

        const {subQueryName, column} = aliasInfo;
        let row = subQueries.get(subQueryName);
        if (!row) {
          row = {aliasInfo, rowWithVersion: {}};
          subQueries.set(subQueryName, row);
        }
        row.rowWithVersion = {...row.rowWithVersion, [column]: value};
      }

      // Now, merge each row into its corresponding ParsedRow by row key.
      for (const {aliasInfo, rowWithVersion} of subQueries.values()) {
        // Exclude the _0_version column from what is sent to the client.
        const {[ZERO_VERSION_COLUMN_NAME]: rowVersion, ...row} = rowWithVersion;
        if (rowVersion === null) {
          // Non-existent rows result from non-INNER JOINs.
          continue;
        }
        if (typeof rowVersion !== 'string' || rowVersion.length === 0) {
          throw new Error(`Invalid _0_version in ${stringify(rowWithVersion)}`);
        }

        const {subQueryName, schema, table} = aliasInfo;
        const id = this.#tables.rowID(schema, table, row);

        let subQuery = this.#subQueryRows.get(subQueryName);
        if (!subQuery) {
          subQuery = new CustomKeySet<RowID>(id => rowIDHash(id));
          this.#subQueryRows.set(subQueryName, subQuery);
        } else if (subQuery.has(id)) {
          continue; // Redundant row for this sub-query (i.e. from de-aggregation)
        }
        subQuery.add(id);

        let rowResult = parsed.get(id);
        if (!rowResult) {
          rowResult = {
            record: {id, rowVersion, queriedColumns: {}},
            contents: {},
          };
          parsed.set(id, rowResult);
        }
        for (const id of this.#queryIDs) {
          rowResult.record.queriedColumns ??= {}; // Appease the compiler
          rowResult.record.queriedColumns[id] = union(
            rowResult.record.queriedColumns[id],
            Object.keys(row),
          ).sort();
        }
        rowResult.contents = {...rowResult.contents, ...row};
      }
    }
    return parsed;
  }
}

class TableSchemas {
  readonly #tables: Map<string, TableSpec>;

  constructor(tables: readonly TableSpec[]) {
    this.#tables = new Map(tables.map(t => [`${t.schema}.${t.name}`, t]));
  }

  spec(schema: string, table: string): TableSpec | undefined {
    return this.#tables.get(`${schema}.${table}`);
  }

  rowID(schema: string, table: string, row: JSONObject): RowID {
    const t = this.spec(schema, table);
    assert(t, `No TableSpec for "${schema}.${table}"`);

    const rowKey = Object.fromEntries(
      t.primaryKey.map(col => {
        const val = row[col];
        assert(
          val,
          `Primary key "${col}" missing from row in ${schema}.${table}`,
        );
        return [col, val];
      }),
    );
    return {schema: t.schema, table: t.name, rowKey};
  }
}

export function union<T>(...arrs: (readonly T[] | undefined)[]): T[] {
  const set = new Set(arrs.flatMap(a => a ?? []));
  return [...set];
}

export function splitLastComponent(
  str: string,
): [prefix: string, suffix: string] {
  const lastSlash = str.lastIndexOf(ALIAS_COMPONENT_SEPARATOR);
  return lastSlash < 0
    ? ['', str]
    : [str.substring(0, lastSlash), str.substring(lastSlash + 1)];
}

export type AliasInfo = {
  subQueryName: string;
  schema: string;
  table: string;
  column: string;
};

const aliasFirstChar = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';

/**
 * Parses the aliases created by the query expansion step into the information
 * necessary to construct the individual rows from each result. The aliases are
 * minified and mapped to their {@link AliasInfo} objects to reduce the
 * serialization and memory overhead per result.
 */
// Exported for testing.
export function minifyAliases(ast: ServerAST): {
  ast: ServerAST;
  columnAliases: Map<string, AliasInfo>;
} {
  let aliasCount = 0;
  const columnAliases = new Map<string, AliasInfo>();

  const reAlias = (orig: string) => {
    const cycle = Math.floor(aliasCount / aliasFirstChar.length);
    const newAlias =
      cycle === 0
        ? aliasFirstChar[aliasCount]
        : aliasFirstChar[aliasCount % aliasFirstChar.length] + String(cycle);
    aliasCount++;

    const parts = orig.split(ALIAS_COMPONENT_SEPARATOR);
    assert(parts.length >= 3);
    columnAliases.set(newAlias, {
      subQueryName: parts
        .slice(0, parts.length - 3)
        .join(ALIAS_COMPONENT_SEPARATOR),
      schema: parts.at(-3)!,
      table: parts.at(-2)!,
      column: parts.at(-1)!,
    });
    return newAlias;
  };

  return {
    ast: {
      ...ast,
      select: (ast.select ?? []).map(s => [s[0], reAlias(s[1])] as const),
    },
    columnAliases,
  };
}
