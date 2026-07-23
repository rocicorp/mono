import {assert} from '../../../../../shared/src/asserts.ts';
import type {Database} from '../../../../../zqlite/src/db.ts';

export const CHANGE_LOG_STREAM_TABLE = '_zero.changeLogStream';
export const CHANGE_LOG_STREAM_WRITE_TIME_INDEX =
  '_zero.changeLogStream_writeTimeMs';

export const CREATE_CHANGE_LOG_STREAM_TABLE = /*sql*/ `
  CREATE TABLE "${CHANGE_LOG_STREAM_TABLE}" (
    "watermark"   TEXT NOT NULL,
    "pos"         INTEGER NOT NULL,
    "change"      TEXT NOT NULL,
    "precommit"   TEXT,
    "writeTimeMs" INTEGER,
    PRIMARY KEY ("watermark", "pos")
  );
`;

export const CREATE_CHANGE_LOG_STREAM_INDEX = /*sql*/ `
  CREATE INDEX "${CHANGE_LOG_STREAM_WRITE_TIME_INDEX}"
    ON "${CHANGE_LOG_STREAM_TABLE}" ("writeTimeMs", "watermark")
    WHERE "writeTimeMs" IS NOT NULL;
`;

export const CREATE_CHANGE_LOG_STREAM_SCHEMA =
  CREATE_CHANGE_LOG_STREAM_TABLE + CREATE_CHANGE_LOG_STREAM_INDEX;

type ReplicationState = {
  stateVersion: string;
  writeTimeMs: number | null;
};

/**
 * Seeds a valid synthetic transaction at the replica's current watermark.
 *
 * The insert is idempotent because migrateData can run again after a rollback
 * followed by a roll-forward. Both rows are inserted by one statement so a
 * caller outside a larger transaction cannot leave a partial seed.
 */
export function seedChangeLogStream(db: Database): void {
  const state = db
    .prepare(/*sql*/ `
      SELECT "stateVersion", "writeTimeMs"
        FROM "_zero.replicationState"
    `)
    .get<ReplicationState | undefined>();
  assert(state !== undefined, 'replication state must be initialized');
  assert(
    state.writeTimeMs !== null,
    'replication state writeTimeMs must be initialized',
  );

  db.prepare(/*sql*/ `
    INSERT INTO "${CHANGE_LOG_STREAM_TABLE}"
      ("watermark", "pos", "change", "precommit", "writeTimeMs")
    VALUES
      (@stateVersion, 0, '{"tag":"begin"}', NULL, NULL),
      (@stateVersion, 1, '{"tag":"commit"}', @stateVersion, @writeTimeMs)
    ON CONFLICT ("watermark", "pos") DO NOTHING
  `).run(state);
}
