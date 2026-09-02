import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../../../shared/src/asserts.ts';
import * as v from '../../../../../../shared/src/valita.ts';
import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../../db/migration.ts';
import type {PostgresDB} from '../../../../types/pg.ts';
import {upstreamSchema, type ShardConfig} from '../../../../types/shards.ts';
import {id} from '../../../../types/sql.ts';
import {AutoResetSignal} from '../../../change-streamer/schema/tables.ts';
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
    getIncrementalMigrations(shard),
  );
}

function getIncrementalMigrations(shard: ShardConfig): IncrementalMigrationMap {
  const shardConfigTable = `${upstreamSchema(shard)}.shardConfig`;

  return {
    6: {
      migrateSchema: () => {
        throw new AutoResetSignal('resetting to upgrade shard schema');
      },
    },

    // v7: Updates the DDL event trigger protocol to v2, and adds support for
    //     ALTER SCHEMA x RENAME TO y
    //     (migration logic subsumbed by v23)

    // v8: Adds support for non-disruptive resyncs, which tracks multiple
    //     replicas with different slot names.
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

    // v9: Fixes field ordering of compound indexes. This incremental migration
    // only fixes indexes resulting from new schema changes. A full resync is
    // required to fix existing indexes.
    //
    // The migration has been subsumed by the identical logic for migrating
    // to v12 (i.e. a trigger upgrade).

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

    // v11: Formerly dropped the schemaVersions table, but restored in the v13
    // migration for rollback safety.

    // v12: Upgrade DDL trigger to query schemaOID, needed information for auto-backfill.
    // (subsumed by v14)

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

    // v14: Upgrade DDL trigger to log more info to PG logs.
    // (subsumed by v16)

    // Add initialSyncContext column to replicas table.
    15: {
      migrateSchema: async (_, sql) => {
        await sql`
          ALTER TABLE ${sql(upstreamSchema(shard))}.replicas
            ADD COLUMN "initialSyncContext" JSON,
            ADD COLUMN "subscriberContext" JSON
        `;
      },
    },

    // v16: Upgrade DDL trigger to fire on all ALTER TABLE statements
    // to catch the *removal* of a table from the published set.
    // v17 (1.0.0): Upgrade DDL triggers to support the COMMENT ON PUBLICATION hook for
    // working around the lack of event trigger support for PUBLICATION
    // changes in supabase.
    // This also adds forwards-compatible support for hierarchical logical
    // message prefixes and unknown ddl event types.
    // v18: Pure refactoring of event trigger code.
    // v19 (1.4.0): Correctly handle concurrently issued DDL statements.
    // v20 (1.4.0): Handle nested DDL triggers
    // v21 (1.5.0): Handle cross-transaction DDL operations (i.e. concurrent
    // index operations), and support manual invocation of update_schemas().

    22: {
      migrateSchema: async (_, sql) => {
        await sql`
          ALTER TABLE ${sql(upstreamSchema(shard))}.replicas 
            ALTER "initialSchema" DROP NOT NULL;
        `;
        await sql`
          ALTER TABLE ${sql(upstreamSchema(shard))}.replicas 
            DROP CONSTRAINT replicas_pkey;
        `;
        await sql`
          ALTER TABLE ${sql(upstreamSchema(shard))}.replicas 
            ADD COLUMN id TEXT PRIMARY KEY DEFAULT replace(gen_random_uuid()::text, '-', '');
        `;
        await sql`
          ALTER TABLE ${sql(upstreamSchema(shard))}.replicas 
            ADD COLUMN rank BIGSERIAL;
        `;
      },
    },

    // v23 (1.6.0): Event triggers: Handle the case where current_query()
    // returns NULL. Add more logging to debug unexpected messages
    23: {
      migrateSchema: async (lc, sql) => {
        const [{publications}] = await sql<{publications: string[]}[]>`
          SELECT publications FROM ${sql(shardConfigTable)}`;
        await setupTriggers(lc, sql, {...shard, publications});
        lc.info?.(`Upgraded DDL event triggers`);
      },
    },

    // v24 (1.10.0):
    // - adds the "backupPath" column for per-replica backups.
    // - adds the "generation" column to replace "version".
    24: {
      migrateSchema: async (_, sql) => {
        await sql`
          ALTER TABLE ${sql(upstreamSchema(shard))}.replicas 
            ADD COLUMN "backupPath" TEXT;
        `;
        await sql`
          ALTER TABLE ${sql(upstreamSchema(shard))}.replicas 
            ADD COLUMN "generation" TEXT;
        `;
      },

      migrateData: async (_, sql) => {
        // Backfill the "generation" column to eventually replace "version"
        await sql`
          UPDATE ${sql(upstreamSchema(shard))}.replicas SET "generation" = "version";
        `;
      },
    },

    // v25 (1.10.0):
    // - adds the "backupV5" column to signal the current semantics
    //   of the replication slot's confirmed_flush_lsn with respect to what
    //   has been backed up.
    25: {
      migrateSchema: async (_, sql) => {
        await sql`
          ALTER TABLE ${sql(upstreamSchema(shard))}.replicas 
            ADD COLUMN "backupV5" BOOL DEFAULT false;
        `;
      },
    },
  };
}

// Referenced in tests.
export const CURRENT_SCHEMA_VERSION = Object.keys(
  getIncrementalMigrations({
    appID: 'unused',
    shardNum: 0,
    publications: ['foo'],
  }),
).reduce((prev, curr) => Math.max(prev, parseInt(curr)), 0);
