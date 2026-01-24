/**
 * Replication metadata, used for incremental view maintenance and catchup.
 *
 * These tables are created atomically in {@link setupReplicationTables}
 * after the logical replication handoff when initial data synchronization has completed.
 */

import * as v from '../../../../../shared/src/valita.ts';
import type {Database} from '../../../../../zqlite/src/db.ts';
import type {StatementRunner} from '../../../db/statements.ts';
import {CREATE_CHANGELOG_SCHEMA} from './change-log.ts';
import {CREATE_COLUMN_METADATA_TABLE} from './column-metadata.ts';
import {ZERO_VERSION_COLUMN_NAME} from './constants.ts';

export {ZERO_VERSION_COLUMN_NAME};

export type RuntimeEvent = 'sync' | 'upgrade' | 'vacuum';

// event     : The RuntimeEvent. Only one row per event is tracked.
//             Inserting an event will REPLACE any row for the same event.
// timestamp : SQLite timestamp string, e.g. "2024-04-12 11:37:46".
//             Append a `Z` when parsing with `new Date(...)`;
export const CREATE_RUNTIME_EVENTS_TABLE = `
  CREATE TABLE "_zero.runtimeEvents" (
    event TEXT PRIMARY KEY ON CONFLICT REPLACE,
    timestamp TEXT NOT NULL DEFAULT (current_timestamp)
  );
`;

const CREATE_REPLICATION_STATE_SCHEMA =
  // replicaVersion   : A value identifying the version at which the initial sync happened, i.e.
  //                    the version at which all rows were copied, and to `_0_version` was set.
  //                    This value is used to distinguish data from other replicas (e.g. if a
  //                    replica is reset or if there are ever multiple replicas).
  // publications     : JSON stringified array of publication names
  // lock             : Auto-magic column for enforcing single-row semantics.
  `
  CREATE TABLE "_zero.replicationConfig" (
    replicaVersion TEXT NOT NULL,
    publications TEXT NOT NULL,
    lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
  );
  ` +
  // stateVersion     : The latest version replicated from upstream, starting with the initial
  //                    `replicaVersion` and moving forward to each subsequent commit watermark
  //                    (e.g. corresponding to a Postgres LSN). Versions are represented as
  //                    lexicographically sortable watermarks (e.g. LexiVersions).
  //
  `
  CREATE TABLE "_zero.replicationState" (
    stateVersion TEXT NOT NULL,
    lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
  );
  ` +
  CREATE_CHANGELOG_SCHEMA +
  CREATE_RUNTIME_EVENTS_TABLE +
  CREATE_COLUMN_METADATA_TABLE;

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

export type SubscriptionState = v.Infer<typeof subscriptionStateSchema>;

const replicationStateSchema = v.object({
  stateVersion: v.string(),
});

export type ReplicationState = v.Infer<typeof replicationStateSchema>;

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
    INSERT INTO "_zero.replicationState" (stateVersion) VALUES (?)
    `,
  ).run(watermark);
  recordEvent(db, 'sync');
}

export function recordEvent(db: Database, event: RuntimeEvent) {
  db.prepare(
    `
    INSERT INTO "_zero.runtimeEvents" (event) VALUES (?) 
    `,
  ).run(event);
}

export function getAscendingEvents(db: Database) {
  const result = db
    .prepare(
      `SELECT event, timestamp FROM "_zero.runtimeEvents" 
         ORDER BY timestamp ASC
    `,
    )
    .all<{event: string; timestamp: string}>();
  return result.map(({event, timestamp}) => ({
    event,
    timestamp: new Date(timestamp + 'Z'),
  }));
}

export function getSubscriptionState(db: StatementRunner): SubscriptionState {
  const result = db.get(
    `
      SELECT c.replicaVersion, c.publications, s.stateVersion as watermark
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
  db.run(`UPDATE "_zero.replicationState" SET stateVersion=?`, watermark);
}

export function getReplicationState(db: StatementRunner): ReplicationState {
  const result = db.get(`SELECT stateVersion FROM "_zero.replicationState"`);
  return v.parse(result, replicationStateSchema);
}
