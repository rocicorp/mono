/**
 * Replication metadata, used for incremental view maintenance and catchup.
 *
 * These tables are created atomically in {@link setupReplicationTables}
 * after the logical replication handoff when initial data synchronization has completed.
 */

import {Database} from 'better-sqlite3';
import * as v from 'shared/src/valita.js';
import {StatementRunner} from 'zero-cache/src/db/statements.js';
import {toLexiVersion} from 'zero-cache/src/types/lsn.js';

export const ZERO_VERSION_COLUMN_NAME = '_0_version';

const CREATE_REPLICATION_STATE_SCHEMA =
  // publications     : JSON stringified array of publication names
  // lock             : Auto-magic column for enforcing single-row semantics.
  `
  CREATE TABLE "_zero.ReplicationConfig" (
    publications TEXT NOT NULL,
    lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
  );
  ` +
  // watermark        : Opaque, upstream-specific watermark denoting the point from which replication
  //                    should continue. For a Postgres upstream, for example, this is the LSN string.
  // stateVersion     : The value of the _0_version column for the newest rows in the database.
  // nextStateVersion : The value to use for the _0_version column of rows in the _next_ transaction.
  //                    This is generally a lexicographically sortable representation of the watermark.
  `
  CREATE TABLE "_zero.ReplicationState" (
    watermark TEXT NOT NULL,
    stateVersion TEXT NOT NULL,
    nextStateVersion TEXT NOT NULL,
    lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
  );
  `;

const stringArray = v.array(v.string());

const subscriptionStateSchema = v
  .object({publications: v.string(), watermark: v.string()})
  .map(s => ({
    ...s,
    publications: v.parse(JSON.parse(s.publications), stringArray),
  }));

const versionsSchema = v.object({
  stateVersion: v.string(),
  nextStateVersion: v.string(),
});

export type ReplicationVersions = v.Infer<typeof versionsSchema>;

export function initReplicationState(
  db: Database,
  publications: string[],
  lsn: string,
) {
  db.exec(CREATE_REPLICATION_STATE_SCHEMA);
  db.prepare(
    `INSERT INTO "_zero.ReplicationConfig" (publications) VALUES (?)`,
  ).run(JSON.stringify(publications));
  db.prepare(
    `
      INSERT INTO "_zero.ReplicationState" 
        (watermark, stateVersion, nextStateVersion) VALUES (?,'00',?)
    `,
  ).run(lsn, toLexiVersion(lsn));
}

export function getSubscriptionState(db: StatementRunner) {
  const result = db.get(
    `
      SELECT c.publications, s.watermark 
        FROM "_zero.ReplicationConfig" as c
        JOIN "_zero.ReplicationState" as s
        ON c.lock = s.lock
    `,
  );
  return v.parse(result, subscriptionStateSchema);
}

export function updateReplicationWatermark(db: StatementRunner, lsn: string) {
  // The previous `nextStateVersion` needs to be set as the next `stateVersion`.
  // Rather than explicitly looking that up with an additional statement, use an
  // UPSERT for which the INSERT fails so that the value of `nextStateVersion`
  // from the original row can be used to set the new `stateVersion`.
  db.run(
    `
      INSERT INTO "_zero.ReplicationState" 
        (lock, watermark, stateVersion, nextStateVersion) VALUES (1,'','','')
        ON CONFLICT (lock)
        DO UPDATE SET watermark=?, stateVersion=nextStateVersion, nextStateVersion=?
    `,
    lsn,
    toLexiVersion(lsn),
  );
}

export function getReplicationVersions(
  db: StatementRunner,
): ReplicationVersions {
  const result = db.get(
    `SELECT stateVersion, nextStateVersion FROM "_zero.ReplicationState"`,
  );
  return v.parse(result, versionsSchema);
}
