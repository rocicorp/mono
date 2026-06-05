import {ident as id} from 'pg-format';
import {clientToServer} from '../../../zero-schema/src/name-mapper.ts';
import type {
  Cardinality,
  Relationship,
  Schema,
} from '../../../zero-types/src/schema.ts';
import {liteTableName} from '../types/names.ts';

/**
 * Minimal, DB-agnostic description of a table that the relationship-index
 * checker needs. Adapted from a `PublishedSchema` table (or a replica's
 * table spec) by the caller.
 *
 * Names are *server* names (i.e. the names as they exist in the upstream
 * database), and `schema`/`name` are kept separate so that `CREATE INDEX`
 * statements can be schema-qualified correctly.
 */
export type CheckTableSpec = {
  /** Upstream Postgres schema, e.g. `public`. */
  readonly schema: string;
  /** Upstream table name (server name). */
  readonly name: string;
  /** Primary key columns (server names). A primary key always has an index. */
  readonly primaryKey: readonly string[];
};

/**
 * Minimal, DB-agnostic description of an index. Adapted from a
 * `PublishedSchema` index (or a replica `LiteIndexSpec`) by the caller.
 */
export type CheckIndexSpec = {
  /** Upstream Postgres schema of the indexed table, e.g. `public`. */
  readonly schema: string;
  /** Upstream table name (server name) that the index is on. */
  readonly tableName: string;
  /** Index columns in index order (server names). */
  readonly columns: readonly string[];
};

/**
 * A relationship join field (on one side of one hop) that is not backed by
 * an index. A join over an unindexed field forces a full table scan every
 * time Zero incrementally maintains the `related()`/`whereExists()` query.
 */
export type MissingRelationshipIndex = {
  /** The table on which the relationship is declared (client name). */
  readonly ownerTable: string;
  /** The relationship name. */
  readonly relationship: string;
  /** 1-based hop index. Junction (many-to-many) relationships have 2 hops. */
  readonly hop: number;
  /** Total number of hops in the relationship (1 for direct, 2 for junction). */
  readonly hopCount: number;
  /**
   * Which side of the join is missing the index:
   * - `source`: the parent/junction lookup (used when a row on the *dest*
   *   side changes and Zero must find the affected source rows).
   * - `dest`: the child lookup (used when fetching/refetching related rows).
   */
  readonly side: 'source' | 'dest';
  readonly cardinality: Cardinality;
  /** The join fields, in client names. */
  readonly clientColumns: readonly string[];
  /** Upstream Postgres schema of the table needing the index. */
  readonly serverSchema: string;
  /** Upstream table name (server name) needing the index. */
  readonly serverTable: string;
  /** The join fields, in server names — i.e. the columns to index. */
  readonly serverColumns: readonly string[];
};

export type RelationshipIndexCheck = {
  readonly missing: readonly MissingRelationshipIndex[];
  /**
   * Client table names that are referenced by a relationship but are absent
   * from the published/synced schema, so their indexes could not be checked.
   */
  readonly unsyncedTables: readonly string[];
};

/**
 * Checks that every relationship's join fields are backed by an index, in
 * *both* directions of the relationship.
 *
 * Why both directions? Zero maintains `related()`/`whereExists()` queries
 * incrementally and reacts to changes from either table. For a relationship
 * `A.sourceField -> B.destField`:
 * - fetching related `B` rows for an `A` row looks `B` up by `destField`, and
 * - reacting to a changed `B` row looks the matching `A` rows up by `sourceField`.
 *
 * So an index is needed on `B.destField` *and* on `A.sourceField`. Typically
 * one side is the primary key (already indexed) and the other is a foreign
 * key that needs an explicit index. Junction (many-to-many) relationships
 * have two hops, and every field on both hops needs to be covered.
 *
 * @param schema The application's Zero schema (provides `relationships` and
 *     the client<->server name mapping).
 * @param tables The published/synced tables (server names + primary keys).
 * @param indexes The published/synced indexes (server names).
 */
export function findMissingRelationshipIndexes(
  schema: Schema,
  tables: readonly CheckTableSpec[],
  indexes: readonly CheckIndexSpec[],
): RelationshipIndexCheck {
  const clientToServerNames = clientToServer(schema.tables);

  // Index column-lists and Postgres identifiers, keyed by the canonical
  // "lite" table name (which is what `clientToServer` resolves table names
  // to). A primary key is always backed by an index, so it is included.
  const indexesByTable = new Map<string, string[][]>();
  const pgIdentByTable = new Map<string, {schema: string; table: string}>();

  const addIndex = (lite: string, columns: readonly string[]) => {
    const list = indexesByTable.get(lite);
    if (list) {
      list.push([...columns]);
    } else {
      indexesByTable.set(lite, [[...columns]]);
    }
  };

  for (const table of tables) {
    const lite = liteTableName({schema: table.schema, name: table.name});
    pgIdentByTable.set(lite, {schema: table.schema, table: table.name});
    if (table.primaryKey.length > 0) {
      addIndex(lite, table.primaryKey);
    }
  }
  for (const index of indexes) {
    addIndex(
      liteTableName({schema: index.schema, name: index.tableName}),
      index.columns,
    );
  }

  const missing: MissingRelationshipIndex[] = [];
  const unsyncedTables = new Set<string>();

  // Checks a single join field set on one side of one hop. `table`/`columns`
  // are the (client) table and fields being looked up; `ownerTable` is the
  // table the relationship is declared on (which may differ from `table` for
  // the dest side and for junction hops).
  const checkSide = (args: {
    ownerTable: string;
    relationship: string;
    hop: number;
    hopCount: number;
    cardinality: Cardinality;
    side: 'source' | 'dest';
    table: string;
    columns: readonly string[];
  }) => {
    const serverTable = clientToServerNames.tableNameIfKnown(args.table);
    if (serverTable === undefined) {
      // The relationship references a table that isn't in `schema.tables`.
      // `createSchema()` validates against this, so it shouldn't happen, but
      // guard rather than throw from a best-effort warning.
      return;
    }
    const pgIdent = pgIdentByTable.get(serverTable);
    if (pgIdent === undefined) {
      // The table is in the Zero schema but isn't published/synced, so we
      // have no index information for it.
      unsyncedTables.add(args.table);
      return;
    }
    const serverColumns = clientToServerNames.columns(args.table, [
      ...args.columns,
    ]);
    if (hasCoveringIndex(indexesByTable.get(serverTable), serverColumns)) {
      return;
    }
    missing.push({
      ownerTable: args.ownerTable,
      relationship: args.relationship,
      hop: args.hop,
      hopCount: args.hopCount,
      side: args.side,
      cardinality: args.cardinality,
      clientColumns: [...args.columns],
      serverSchema: pgIdent.schema,
      serverTable: pgIdent.table,
      serverColumns,
    });
  };

  for (const [ownerTable, relationships] of Object.entries(
    schema.relationships,
  )) {
    for (const [relationship, connections] of Object.entries(relationships)) {
      const conns = connections as Relationship;
      let sourceTable = ownerTable;
      for (let hop = 0; hop < conns.length; hop++) {
        const conn = conns[hop];
        const common = {
          ownerTable,
          relationship,
          hop: hop + 1,
          hopCount: conns.length,
          cardinality: conn.cardinality,
        };
        checkSide({
          ...common,
          side: 'source',
          table: sourceTable,
          columns: conn.sourceField,
        });
        checkSide({
          ...common,
          side: 'dest',
          table: conn.destSchema,
          columns: conn.destField,
        });
        sourceTable = conn.destSchema;
      }
    }
  }

  return {missing, unsyncedTables: [...unsyncedTables]};
}

/**
 * Returns true if any of `indexLists` can satisfy an equality lookup over
 * exactly `fields`. An index covers the lookup when `fields` (as a set) are
 * the leading columns of the index — additional trailing index columns are
 * fine, but every join field must be part of the leading prefix.
 */
function hasCoveringIndex(
  indexLists: readonly (readonly string[])[] | undefined,
  fields: readonly string[],
): boolean {
  if (indexLists === undefined) {
    return false;
  }
  const wanted = new Set(fields);
  return indexLists.some(columns => {
    if (columns.length < fields.length) {
      return false;
    }
    const prefix = columns.slice(0, fields.length);
    return prefix.length === wanted.size && prefix.every(c => wanted.has(c));
  });
}

/**
 * Builds a `CREATE INDEX` statement that would back the missing join field.
 * The table is schema-qualified when it isn't in the `public` schema.
 */
export function createIndexSQL(missing: {
  readonly serverSchema: string;
  readonly serverTable: string;
  readonly serverColumns: readonly string[];
}): string {
  const target =
    missing.serverSchema === 'public'
      ? id(missing.serverTable)
      : `${id(missing.serverSchema)}.${id(missing.serverTable)}`;
  const columns = missing.serverColumns.map(c => id(c)).join(', ');
  return `CREATE INDEX ON ${target} (${columns});`;
}
