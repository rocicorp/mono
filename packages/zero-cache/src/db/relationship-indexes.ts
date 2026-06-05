import {ident as id} from 'pg-format';
import type {
  CheckIndexesResult,
  MissingIndex,
} from '../../../zero-protocol/src/inspect-down.ts';
import type {IndexRequirement} from '../../../zero-protocol/src/inspect-up.ts';
import {enumerateRelationshipIndexRequirements} from '../../../zero-schema/src/relationship-index-requirements.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';

export type {CheckIndexesResult, MissingIndex};

/**
 * A table that exists in the database, with its primary key (server names). A
 * primary key always has a backing index, so it counts as a covering index.
 * `table` is the canonical "lite" (server) table name.
 */
export type CheckTableSpec = {
  readonly table: string;
  readonly primaryKey: readonly string[];
};

/**
 * An index that exists in the database (server names). `table` is the
 * canonical "lite" (server) table name; `columns` are the index columns in
 * index order.
 */
export type CheckIndexSpec = {
  readonly table: string;
  readonly columns: readonly string[];
};

/**
 * Given the relationship join-field requirements (see
 * {@link enumerateRelationshipIndexRequirements}) and the database's actual
 * tables and indexes, returns the requirements that are *not* backed by a
 * covering index, each with a `CREATE INDEX` statement that would fix it.
 *
 * Used by both the `zero-check-indexes` CLI (against the upstream Postgres
 * indexes) and the inspector's `check-indexes` handler (against the replica's
 * indexes).
 */
export function filterMissingRequirements(
  requirements: readonly IndexRequirement[],
  tables: readonly CheckTableSpec[],
  indexes: readonly CheckIndexSpec[],
): CheckIndexesResult {
  // Index column-lists keyed by lite table name. A primary key is always
  // backed by an index, so it is included.
  const indexesByTable = new Map<string, string[][]>();
  // Every table that exists in the database (so we can tell "unindexed" from
  // "not synced").
  const knownTables = new Set<string>();

  const addIndex = (table: string, columns: readonly string[]) => {
    const list = indexesByTable.get(table);
    if (list) {
      list.push([...columns]);
    } else {
      indexesByTable.set(table, [[...columns]]);
    }
  };

  for (const t of tables) {
    knownTables.add(t.table);
    if (t.primaryKey.length > 0) {
      addIndex(t.table, t.primaryKey);
    }
  }
  for (const i of indexes) {
    knownTables.add(i.table);
    addIndex(i.table, i.columns);
  }

  const missing: MissingIndex[] = [];
  const unsyncedTables = new Set<string>();

  for (const req of requirements) {
    if (!knownTables.has(req.serverTable)) {
      // The table isn't published/synced, so we have no index info for it.
      unsyncedTables.add(req.clientTable);
      continue;
    }
    if (
      hasCoveringIndex(indexesByTable.get(req.serverTable), req.serverColumns)
    ) {
      continue;
    }
    missing.push({...req, createIndexSQL: createIndexSQL(req)});
  }

  return {missing, unsyncedTables: [...unsyncedTables]};
}

/**
 * Checks that every relationship's join fields are backed by an index, in both
 * directions. Convenience composition of
 * {@link enumerateRelationshipIndexRequirements} and
 * {@link filterMissingRequirements}, used by the `zero-check-indexes` CLI.
 *
 * @param schema The application's Zero schema.
 * @param tables The published/synced tables (lite/server names + primary keys).
 * @param indexes The published/synced indexes (lite/server names).
 */
export function findMissingRelationshipIndexes(
  schema: Schema,
  tables: readonly CheckTableSpec[],
  indexes: readonly CheckIndexSpec[],
): CheckIndexesResult {
  return filterMissingRequirements(
    enumerateRelationshipIndexRequirements(schema),
    tables,
    indexes,
  );
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
 * The (lite) `serverTable` is schema-qualified when it isn't in the `public`
 * schema (i.e. when it has the `schema.table` form).
 */
export function createIndexSQL(req: {
  readonly serverTable: string;
  readonly serverColumns: readonly string[];
}): string {
  const dot = req.serverTable.indexOf('.');
  const target =
    dot === -1
      ? id(req.serverTable)
      : `${id(req.serverTable.slice(0, dot))}.${id(req.serverTable.slice(dot + 1))}`;
  const columns = req.serverColumns.map(c => id(c)).join(', ');
  return `CREATE INDEX ON ${target} (${columns});`;
}
