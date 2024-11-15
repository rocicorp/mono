/**
 * Replication metadata, used for incremental view maintenance and catchup.
 *
 * These tables are created atomically in {@link setupReplicationTables}
 * after the logical replication handoff when initial data synchronization has completed.
 */

import * as v from '../../../../../shared/src/valita.js';
import {Database} from '../../../../../zqlite/src/db.js';
import {StatementRunner} from '../../../db/statements.js';

export const ZERO_VERSION_COLUMN_NAME = '_0_version';

const CREATE_REPLICATION_STATE_SCHEMA =
  // replicaVersion   : A value identifying the version at which the initial sync happened, i.e.
  //                    at which all rows were copied and initialized with `_0_version: "00"`. This
  //                    value is used to distinguish data from other replicas (e.g. if a replica is
  //                    reset or if there are ever multiple replicas). Data from replicas with
  //                    different versions are incompatible, as their "00" version will correspond
  //                    to different snapshots of the upstream database.
  // publications     : JSON stringified array of publication names
  // lock             : Auto-magic column for enforcing single-row semantics.
  `
  CREATE TABLE "_zero.replicationConfig" (
    replicaVersion TEXT NOT NULL,
    publications TEXT NOT NULL,
    lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
  );
  ` +
  // watermark        : Lexicographically sortable watermark denoting the point from which replication
  //                    should continue. For a Postgres upstream, for example, this is the
  //                    LexiVersion-encoded LSN. This is also used as the state version for rows
  //                    modified in the **next** transaction.
  // stateVersion     : The value of the _0_version column for the newest rows in the database.
  `
  CREATE TABLE "_zero.replicationState" (
    watermark TEXT NOT NULL,
    stateVersion TEXT NOT NULL,
    lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
  );
  `;

const stringArray = v.array(v.string());

const subscriptionStateSchema = v
  .object({
    replicaVersion: v.string(),
    publications: v.string(),
    watermark: v.string(),
  })
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
  watermark: string,
) {
  db.exec(CREATE_REPLICATION_STATE_SCHEMA);
  db.prepare(
    `
    INSERT INTO "_zero.replicationConfig" 
       (replicaVersion, publications) VALUES (?, ?)
    `,
  ).run(watermark, JSON.stringify(publications.sort()));
  db.prepare(
    `
    INSERT INTO "_zero.replicationState" 
       (watermark, stateVersion) VALUES (?,'00')
    `,
  ).run(watermark);
}

export function getSubscriptionState(db: StatementRunner) {
  const result = db.get(
    `
      SELECT c.replicaVersion, c.publications, s.watermark 
        FROM "_zero.replicationConfig" as c
        JOIN "_zero.replicationState" as s
        ON c.lock = s.lock
    `,
  );
  return v.parse(result, subscriptionStateSchema);
}

export function updateReplicationWatermark(
  db: StatementRunner,
  watermark: string,
) {
  db.run(
    `UPDATE "_zero.replicationState" SET stateVersion=watermark, watermark=?`,
    watermark,
  );
}

export function getReplicationVersions(
  db: StatementRunner,
): ReplicationVersions {
  const result = db.get(
    `SELECT stateVersion, watermark as nextStateVersion FROM "_zero.replicationState"`,
  );
  return v.parse(result, versionsSchema);
}
