import type {LogContext} from '@rocicorp/logger';
import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import {assert} from 'shared/src/asserts.js';
import {stringify, type JSONObject} from '../../types/bigint-json.js';
import {deaggregate} from '../../zql/deaggregation.js';
import {
  ALIAS_COMPONENT_SEPARATOR,
  expandSelection,
} from '../../zql/expansion.js';
import {
  computeInvalidationInfo,
  type InvalidationInfo,
} from '../../zql/invalidation.js';
import {Normalized} from '../../zql/normalize.js';
import {ZERO_VERSION_COLUMN_NAME} from '../replicator/schema/replication.js';
import type {TableSpec} from '../replicator/tables/specs.js';
import {CVRPaths} from './schema/paths.js';
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
  readonly invalidationInfo: InvalidationInfo;
};

export class QueryHandler {
  readonly #tables: TableSchemas;

  constructor(tables: readonly TableSpec[]) {
    this.#tables = new TableSchemas(tables);
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
      const requiredColumns = (schema = 'public', table: string) => {
        const t = this.#tables.spec(schema, table);
        if (!t) {
          throw new InvalidQueryError(
            `Unknown table "${table}" in ${JSON.stringify(q.ast)}`,
          );
        }
        return [...t.primaryKey, ZERO_VERSION_COLUMN_NAME];
      };

      const deaggregated = deaggregate(q.ast);
      const expanded = expandSelection(deaggregated, requiredColumns);
      const transformedAST = new Normalized(expanded);
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
  resultParser(lc: LogContext, cvrID: string) {
    return new ResultParser(lc, this.#tables, cvrID);
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
  readonly #lc: LogContext;
  readonly #tables: TableSchemas;
  readonly #paths: CVRPaths;

  constructor(lc: LogContext, tables: TableSchemas, cvrID: string) {
    this.#lc = lc;
    this.#tables = tables;
    this.#paths = new CVRPaths(cvrID);
  }

  /**
   * Parses the query results by decomposing each result into its constituent
   * rows, according to the column naming schema defined by {@link expandSelection}.
   * Multiple views of rows from different queries are merged, with the query to column
   * mapping tracked in the `record` field of the returned {@link ParsedRow}.
   *
   * Returns a mapping from the CVR row record path to {@link ParsedRow}.
   *
   * @param queryIDs The query ID(s) with which the query is associated. See
   *        {@link TransformedQuery.queryIDs} for why there may be more than one.
   */
  parseResults(
    queryIDs: readonly string[],
    results: readonly JSONObject[],
  ): Map<string, ParsedRow> {
    const parsed = new Map<string, ParsedRow>(); // Maps CVRPath.row() => RowResult
    for (const result of results) {
      // Partitions the values of the full result into individual "subquery/table" keys.
      // For example, a result:
      // ```
      // {
      //   "public/issues/id": 1,
      //   "public/issues/name": "foo",
      //   "owner/public/users/id": 3,
      //   "owner/public/users/name: "moar",
      //   "parent/public/issues/id": 5,
      //   "parent/public/issues/name" "trix",
      // }
      // ```
      //
      // is partitioned into:
      //
      // ```
      // "public/issues": {id: 1, name: "foo"}
      // "owners/public/users": {id: 3, name: "moar"}
      // "parent/public/issues": {id: 5, name: "trix"}
      //```
      const rows = new Map<string, JSONObject>();

      for (const [alias, value] of Object.entries(result)) {
        const [rowAlias, columnName] = splitLastComponent(alias);
        rows.set(rowAlias, {
          ...rows.get(rowAlias),
          [columnName]: value,
        });
      }

      // Now, merge each row into its corresponding RowResult by row key.
      for (const [rowAlias, rowWithVersion] of rows.entries()) {
        // Exclude the _0_version column from what is sent to the client.
        const {[ZERO_VERSION_COLUMN_NAME]: rowVersion, ...row} = rowWithVersion;
        if (rowVersion === null) {
          this.#lc.debug?.(
            `skipping non-existent row ${rowAlias}`, // This can happen for LEFT JOINs
            rowWithVersion,
          );
          continue;
        }
        if (typeof rowVersion !== 'string') {
          throw new Error(`Invalid _0_version in ${stringify(rowWithVersion)}`);
        }

        const [prefix, table] = splitLastComponent(rowAlias);
        const [_, schema] = splitLastComponent(prefix);
        const id = this.#tables.rowID(schema, table, row);
        const key = this.#paths.row(id);

        let rowResult = parsed.get(key);
        if (!rowResult) {
          rowResult = {
            record: {id, rowVersion, queriedColumns: {}},
            contents: {},
          };
          parsed.set(key, rowResult);
        }
        for (const col of Object.keys(row)) {
          rowResult.record.queriedColumns ??= {}; // Appease the compiler
          rowResult.record.queriedColumns[col] = union(
            rowResult.record.queriedColumns[col],
            queryIDs,
          );
        }
        rowResult.contents = {...rowResult.contents, ...row};
      }
    }
    this.#lc
      .withContext('queryIDs', queryIDs)
      .debug?.(`processed ${results.length} results`);
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

function splitLastComponent(str: string): [prefix: string, suffix: string] {
  const lastSlash = str.lastIndexOf(ALIAS_COMPONENT_SEPARATOR);
  return lastSlash < 0
    ? ['', str]
    : [str.substring(0, lastSlash), str.substring(lastSlash + 1)];
}
