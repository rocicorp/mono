/**
 * Replication metadata, used for incremental view maintenance and catchup.
 *
 * These tables are created atomically in {@link setupReplicationTables}
 * after the logical replication handoff when initial data synchronization has completed.
 */

import {Database} from 'better-sqlite3';
import * as v from 'shared/src/valita.js';
import {toLexiVersion} from 'zero-cache/src/types/lsn.js';

export const ZERO_VERSION_COLUMN_NAME = '_0_version';

const CREATE_REPLICATION_STATE_SCHEMA =
  // publications     : JSON stringified array of publication names
  // watermark        : Opaque, upstream-specific watermark denoting the point from which replication
  //                    should continue. For a Postgres upstream, for example, this is the LSN string.
  // nextStateVersion : The value to use for the _0_version column of rows in the _next_ transaction.
  //                    This is generally a lexicographically sortable representation of the watermark.
  // lock             : Auto-magic column for enforcing single-row semantics.
  `
  CREATE TABLE "_zero.ReplicationState" (
    publications TEXT NOT NULL,
    watermark TEXT NOT NULL,
    nextStateVersion TEXT NOT NULL,
    lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
  )
  `;

const stringArray = v.array(v.string());

const replicationStateSchema = v
  .object({
    publications: v.string(),
    watermark: v.string(),
    nextStateVersion: v.string(),
  })
  .map(s => ({
    ...s,
    publications: v.parse(JSON.parse(s.publications), stringArray),
  }));

export type ReplicationState = v.Infer<typeof replicationStateSchema>;

export function initReplicationState(
  db: Database,
  publications: string[],
  lsn: string,
) {
  db.exec(CREATE_REPLICATION_STATE_SCHEMA);
  db.prepare(
    `
      INSERT INTO "_zero.ReplicationState" 
        (publications, watermark, nextStateVersion) VALUES (?,?,?)
    `,
  ).run(JSON.stringify(publications), lsn, toLexiVersion(lsn));
}

export function updateReplicationWatermark(db: Database, lsn: string) {
  db.prepare(
    `UPDATE "_zero.ReplicationState" SET watermark = ?, nextStateVersion = ?`,
  ).run(lsn, toLexiVersion(lsn));
}

export function getReplicationState(db: Database): ReplicationState {
  const result = db
    .prepare(
      `SELECT publications, watermark, nextStateVersion FROM "_zero.ReplicationState"`,
    )
    .get();
  return v.parse(result, replicationStateSchema);
}

export function getNextStateVersion(db: Database): string {
  const result = db
    .prepare(`SELECT nextStateVersion FROM "_zero.ReplicationState"`)
    .get();
  return v.parse(result.nextStateVersion, v.string());
}
