import {deepEqual} from '../../../../shared/src/json.ts';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import type {JSONObject} from '../../../../shared/src/bigint-json.ts';
import {ErrorForClient} from '../../types/error-for-client.ts';
import type {RowID, RowRecord} from './schema/types.ts';

/**
 * KeyColumns track the key columns used to reference rows in the CVR.
 * This is then used to potentially compute a backwards compatible row
 * key in the event that pipelines produce a row with a schema that has
 * a different row key.
 *
 * An invariant that is assumed and maintained is that only one key
 * (i.e. set of columns) is used per table in a given CVR
 * (not counting deleted rows with non-null refCounts).
 *
 * This invariant is maintained by the fact that a full hydration
 * (and thus full CVR scan) always follows any schema change:
 * (1) initial hydration when the client connects
 * (2) re-hydration upon a receiving ResetPipelinesSignal during an
 *     advancement (i.e. schema change)
 *
 * The ensuing CVR update is then responsible for replacing or deleting
 * all obsolete row keys.
 */
export class KeyColumns {
  readonly #cvrKeyColumns = new Map<string, readonly string[]>();
  readonly #sameKey = new Map<string, boolean>();

  constructor(allRowRecords: Iterable<RowRecord>) {
    for (const existing of allRowRecords) {
      const {schema, table, rowKey} = existing.id;
      const fullTableName = `${schema}.${table}`;
      if (!this.#cvrKeyColumns.has(fullTableName)) {
        this.#cvrKeyColumns.set(fullTableName, Object.keys(rowKey).sort());
      }
    }
  }

  /**
   * Gets an "old" RowID (i.e. compatible with what's in the CVR) from the
   * given `id` and `row` produced by the pipeline. Returns `null` if there
   * is no old row being replaced.
   */
  getOldRowID(id: RowID, row: JSONObject): RowID | null {
    const {schema, table, rowKey} = id;
    const fullTableName = `${schema}.${table}`;
    const cvrKey = this.#cvrKeyColumns.get(fullTableName);
    if (!cvrKey) {
      return null; // No rows in the CVR for the given table.
    }
    let sameKey = this.#sameKey.get(fullTableName);
    if (sameKey === undefined) {
      const newKey = Object.keys(rowKey).sort();
      sameKey = deepEqual(cvrKey, newKey);
      this.#sameKey.set(fullTableName, sameKey);
    }
    if (sameKey) {
      return null;
    }
    const cvrRowKey = Object.fromEntries(
      cvrKey.map(col => {
        const val = row[col];
        if (val === undefined) {
          // If the column used by the row key in the CVR no longer exists in
          // the replica, this CVR should have ideally been rejected via the
          // schema versioning mechanism. However, since there is no guarantee
          // of that protection, this sanity check here drops the CVR entirely.
          throw new ErrorForClient({
            kind: ErrorKind.ClientNotFound,
            message: `CVR contains key column "${col}" that is no longer in the replica`,
          });
        }
        return [col, val];
      }),
    );
    return {...id, rowKey: cvrRowKey};
  }
}
