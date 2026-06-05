import type {CheckIndexesResult} from '../../../zero-protocol/src/inspect-down.ts';
import type {IndexRequirement} from '../../../zero-protocol/src/inspect-up.ts';
import type {Database} from '../../../zqlite/src/db.ts';
import {listIndexes, listTables} from './lite-tables.ts';
import {filterMissingRequirements} from './relationship-indexes.ts';

/**
 * Checks the given relationship index requirements against the indexes that
 * actually exist in the replica `db`. Used by the inspector's `check-indexes`
 * handler: the client computes the requirements from the schema and the server
 * checks them against the replica it queries.
 *
 * Both the replica's primary keys (via {@link listTables}) and its secondary
 * indexes (via {@link listIndexes}) count as covering indexes — a primary key
 * may be an implicit SQLite auto-index that doesn't appear in `listIndexes`.
 */
export function findMissingIndexesInReplica(
  db: Database,
  requirements: readonly IndexRequirement[],
): CheckIndexesResult {
  return filterMissingRequirements(
    requirements,
    listTables(db, false, false).map(t => ({
      table: t.name,
      primaryKey: t.primaryKey ?? [],
    })),
    listIndexes(db).map(i => ({
      table: i.tableName,
      columns: Object.keys(i.columns),
    })),
  );
}
