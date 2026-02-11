import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../../../shared/src/asserts.ts';
import * as v from '../../../../../../shared/src/valita.ts';
import {
  getVersionHistory,
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../../db/migration.ts';
import type {PostgresDB} from '../../../../types/pg.ts';
import {upstreamSchema, type ShardConfig} from '../../../../types/shards.ts';
import {id} from '../../../../types/sql.ts';
import {AutoResetSignal} from '../../../change-streamer/schema/tables.ts';
import {decommissionShard} from '../decommission.ts';
import {publishedSchema} from './published.ts';
import {
  getMutationsTableDefinition,
  legacyReplicationSlot,
  metadataPublicationName,
  setupTablesAndReplication,
  setupTriggers,
} from './shard.ts';

/**
 * Ensures that a shard is set up for initial sync.
 */
export async function ensureShardSchema(
  lc: LogContext,
  db: PostgresDB,
  shard: ShardConfig,
): Promise<void> {
  const initialSetup: Migration = {
    migrateSchema: (lc, tx) => setupTablesAndReplication(lc, tx, shard),
    minSafeVersion: 1,
  };
  await runSchemaMigrations(
    lc,
    `upstream-shard-${shard.appID}`,
    upstreamSchema(shard),
    db,
    initialSetup,
    // The incremental migration of any existing replicas will be replaced by
    // the incoming replica being synced, so the replicaVersion here is
    // unnecessary.
    getIncrementalMigrations(shard, 'obsolete'),
  );
}

/**
 * Updates the schema for an existing shard.
 */
export async function updateShardSchema(
  lc: LogContext,
  db: PostgresDB,
  shard: ShardConfig,
  replicaVersion: string,
): Promise<void> {
  await runSchemaMigrations(
    lc,
    `upstream-shard-${shard.appID}`,
    upstreamSchema(shard),
    db,
    {
      // If the expected existing shard is absent, throw an
      // AutoResetSignal to backtrack and initial sync.
      migrateSchema: () => {
        throw new AutoResetSignal(
          `upstream shard ${upstreamSchema(shard)} is not initialized`,
        );
      },
    },
    getIncrementalMigrations(shard, replicaVersion),
  );

  // The decommission check is run in updateShardSchema so that it happens
  // after initial sync, and not when the shard schema is initially set up.
  await decommissionLegacyShard(lc, db, shard);
}

function getIncrementalMigrations(
  shard: ShardConfig,
  replicaVersion?: string,
): IncrementalMigrationMap {
  const shardConfigTable = `${upstreamSchema(shard)}.shardConfig`;

  return {
    4: {
      migrateSchema: () => {
        throw new AutoResetSignal('resetting to upgrade shard schema');
      },
      minSafeVersion: 3,
    },

    // v5 changes the upstream schema organization from "zero_{SHARD_ID}" to
    // the "{APP_ID}_0". An incremental migration indicates that the previous
    // SHARD_ID was "0" and the new APP_ID is "zero" (i.e. the default values
    // for those options). In this case, the upstream format is identical, and
    // no migration is necessary. However, the version is bumped to v5 to
    // indicate that it was created with the {APP_ID} configuration and should
    // not be decommissioned as a legacy shard.
    5: {},

    6: {
      migrateSchema: async (lc, sql) => {
        assert(
          replicaVersion,
          `replicaVersion is always passed for incremental migrations`,
        );
        await Promise.all([
          sql`
          ALTER TABLE ${sql(shardConfigTable)} ADD "replicaVersion" TEXT`,
          sql`
          UPDATE ${sql(shardConfigTable)} SET ${sql({replicaVersion})}`,
        ]);
        lc.info?.(
          `Recorded replicaVersion ${replicaVersion} in upstream shardConfig`,
        );
      },
    },

    // Updates the DDL event trigger protocol to v2, and adds support for
    // ALTER SCHEMA x RENAME TO y
    7: {
      migrateSchema: async (lc, sql) => {
        const [{publications}] = await sql<{publications: string[]}[]>`
          SELECT publications FROM ${sql(shardConfigTable)}`;
        await setupTriggers(lc, sql, {...shard, publications});
        lc.info?.(`Upgraded to v2 event triggers`);
      },
    },

    // Adds support for non-disruptive resyncs, which tracks multiple
    // replicas with different slot names.
    8: {
      migrateSchema: async (lc, sql) => {
        const legacyShardConfigSchema = v.object({
          replicaVersion: v.string().nullable(),
          initialSchema: publishedSchema.nullable(),
        });
        const result = await sql`
          SELECT "replicaVersion", "initialSchema" FROM ${sql(shardConfigTable)}`;
        assert(
          result.length === 1,
          () => `Expected exactly one shardConfig row, got ${result.length}`,
        );
        const {replicaVersion, initialSchema} = v.parse(
          result[0],
          legacyShardConfigSchema,
          'passthrough',
        );

        await Promise.all([
          sql`
          CREATE TABLE ${sql(upstreamSchema(shard))}.replicas (
            "slot"          TEXT PRIMARY KEY,
            "version"       TEXT NOT NULL,
            "initialSchema" JSON NOT NULL
          );
          `,
          sql`
          INSERT INTO ${sql(upstreamSchema(shard))}.replicas ${sql({
            slot: legacyReplicationSlot(shard),
            version: replicaVersion,
            initialSchema,
          })}
          `,
          sql`
          ALTER TABLE ${sql(shardConfigTable)} DROP "replicaVersion", DROP "initialSchema"
          `,
        ]);
        lc.info?.(`Upgraded schema to support non-disruptive resyncs`);
      },
    },

    // Fixes field ordering of compound indexes. This incremental migration
    // only fixes indexes resulting from new schema changes. A full resync is
    // required to fix existing indexes.
    //
    // The migration has been subsumed by the identical logic for migrating
    // to v12 (i.e. a trigger upgrade).
    9: {},

    // Adds the `mutations` table used to track mutation results.
    10: {
      migrateSchema: async (lc, sql) => {
        await sql.unsafe(/*sql*/ `
          ${getMutationsTableDefinition(upstreamSchema(shard))}
          ALTER PUBLICATION ${id(metadataPublicationName(shard.appID, shard.shardNum))} ADD TABLE ${id(upstreamSchema(shard))}."mutations";
        `);
        lc.info?.('Upgraded schema with new mutations table');
      },
    },

    // Formerly dropped the schemaVersions table, but restored in the v13
    // migration for rollback safety.
    11: {},

    // Upgrade DDL trigger to query schemaOID, needed information for auto-backfill.
    // (subsumed by v14)
    12: {},

    // Recreates the legacy schemaVersions table that was prematurely dropped
    // in the (former) v11 migration. It needs to remain present for at least one
    // release in order to be rollback safe.
    //
    // TODO: Drop the table once a release that no longer reads the table has
    // been rolled out.
    13: {
      migrateSchema: async (_, sql) => {
        await sql`
          CREATE TABLE IF NOT EXISTS ${sql(upstreamSchema(shard))}."schemaVersions" (
            "minSupportedVersion" INT4,
            "maxSupportedVersion" INT4,
            "lock" BOOL PRIMARY KEY DEFAULT true CHECK (lock)
        );`;
        await sql`
          INSERT INTO ${sql(upstreamSchema(shard))}."schemaVersions" 
            ("lock", "minSupportedVersion", "maxSupportedVersion")
            VALUES (true, 1, 1)
            ON CONFLICT DO NOTHING;
        `;
      },
    },

    // Upgrade DDL trigger to log more info to PG logs.
    14: {
      migrateSchema: async (lc, sql) => {
        const [{publications}] = await sql<{publications: string[]}[]>`
          SELECT publications FROM ${sql(shardConfigTable)}`;
        await setupTriggers(lc, sql, {...shard, publications});
        lc.info?.(`Upgraded DDL event triggers`);
      },
    },
  };
}

export async function decommissionLegacyShard(
  lc: LogContext,
  db: PostgresDB,
  shard: ShardConfig,
) {
  if (shard.appID !== 'zero') {
    // When migration from non-default shard ids, e.g. "zero_prod" => "prod_0",
    // clean up the old "zero_prod" shard if it is pre-v5. Note that the v5
    // check is important to guard against cleaning up a **new** "zero_0" app
    // that coexists with the current App (with app-id === "0").
    const versionHistory = await getVersionHistory(db, `zero_${shard.appID}`);
    if (versionHistory !== null && versionHistory.schemaVersion < 5) {
      await decommissionShard(lc, db, 'zero', shard.appID);
    }
  }
}
