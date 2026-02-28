import type {LogContext} from '@rocicorp/logger';
import {Database} from '../../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../../db/statements.ts';
import type {ShardID} from '../../../types/shards.ts';
import {appSchema, upstreamSchema} from '../../../types/shards.ts';
import {Subscription} from '../../../types/subscription.ts';
import {
  createReplicationStateTables,
  getSubscriptionState,
  initReplicationState,
  type SubscriptionState,
} from '../../replicator/schema/replication-state.ts';
import {schemaVersionMigrationMap} from '../common/replica-schema.ts';
import type {ChangeSource, ChangeStream} from '../change-source.ts';
import type {ChangeStreamMessage} from '../protocol/current.ts';

class NoopChangeSource implements ChangeSource {
  startStream(): Promise<ChangeStream> {
    // A subscription that never emits changes (blocks forever).
    const changes = Subscription.create<ChangeStreamMessage>();
    // An acks sink that silently discards pushes.
    const acks = {push() {}};
    return Promise.resolve({changes, acks});
  }
}

/**
 * Ensures the `_zero.*` replication tables exist, creating them with
 * dummy data if the DB has none (e.g. a plain SQLite file).
 */
function ensureReplicationTables(db: Database): void {
  const exists = db
    .prepare(
      `SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_zero.replicationConfig'`,
    )
    .get();
  if (exists) {
    return;
  }

  createReplicationStateTables(db);
  initReplicationState(db, [], '00', {}, false /* tables already created */);

  // Create versionHistory so upgradeReplica() sees an up-to-date DB.
  const currentVersion = Math.max(
    ...Object.keys(schemaVersionMigrationMap).map(Number),
  );
  db.prepare(
    `
    CREATE TABLE IF NOT EXISTS "_zero.versionHistory" (
      dataVersion INTEGER NOT NULL,
      schemaVersion INTEGER NOT NULL,
      minSafeVersion INTEGER NOT NULL,
      lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
    );
  `,
  ).run();
  db.prepare(
    `
    INSERT INTO "_zero.versionHistory" (dataVersion, schemaVersion, minSafeVersion, lock)
    VALUES (?, ?, ?, 1)
  `,
  ).run(currentVersion, currentVersion, currentVersion);
}

function ensureShardMetadataTables(db: Database, shard: ShardID): void {
  const us = upstreamSchema(shard);
  const app = appSchema(shard);

  db.exec(`
    CREATE TABLE IF NOT EXISTS "${us}.clients" (
      "clientGroupID"  TEXT,
      "clientID"       TEXT,
      "lastMutationID" INTEGER,
      "userID"         TEXT,
      _0_version       TEXT NOT NULL,
      PRIMARY KEY ("clientGroupID", "clientID")
    );

    CREATE TABLE IF NOT EXISTS "${us}.mutations" (
      "clientGroupID"  TEXT,
      "clientID"       TEXT,
      "mutationID"     INTEGER,
      "result"         TEXT,
      _0_version       TEXT NOT NULL,
      PRIMARY KEY ("clientGroupID", "clientID", "mutationID")
    );

    CREATE TABLE IF NOT EXISTS "${app}.permissions" (
      "lock"        INTEGER PRIMARY KEY,
      "permissions" TEXT,
      "hash"        TEXT,
      _0_version    TEXT NOT NULL
    );
  `);
}

export function initializeNoopChangeSource(
  lc: LogContext,
  replicaDbFile: string,
  shard: ShardID,
): {subscriptionState: SubscriptionState; changeSource: ChangeSource} {
  lc.info?.(`initializing noop change source for replica ${replicaDbFile}`);

  const db = new Database(lc, replicaDbFile);
  let subscriptionState: SubscriptionState;
  try {
    ensureReplicationTables(db);
    ensureShardMetadataTables(db, shard);
    const state = getSubscriptionState(new StatementRunner(db));
    // Override watermark to equal replicaVersion so that
    // ensureReplicationConfig in the change-streamer doesn't throw
    // AutoResetSignal. In noop mode there's no upstream to resume from,
    // so the watermark distinction is meaningless.
    subscriptionState = {
      ...state,
      watermark: state.replicaVersion,
    };
  } finally {
    db.close();
  }

  lc.info?.(
    `noop change source initialized. replicaVersion=${subscriptionState.replicaVersion}, watermark=${subscriptionState.watermark}`,
  );

  return {
    subscriptionState,
    changeSource: new NoopChangeSource(),
  };
}
