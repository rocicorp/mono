import type {LogContext} from '@rocicorp/logger';
import type {PendingQuery, Row} from 'postgres';
import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../db/migration.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {cvrSchema, type ShardID} from '../../../types/shards.ts';
import {createRowsVersionTable, setupCVRTables} from './cvr.ts';

export async function initViewSyncerSchema(
  log: LogContext,
  db: PostgresDB,
  shard: ShardID,
): Promise<void> {
  const schema = cvrSchema(shard);

  const setupMigration: Migration = {
    migrateSchema: (lc, tx) => setupCVRTables(lc, tx, shard),
    minSafeVersion: 1,
  };

  const migrateV1toV2: Migration = {
    migrateSchema: async (_, tx) => {
      await tx`ALTER TABLE ${tx(schema)}.instances ADD "replicaVersion" TEXT`;
    },
  };

  const migrateV2ToV3: Migration = {
    migrateSchema: async (_, tx) => {
      await tx.unsafe(createRowsVersionTable(shard));
    },

    /** Populates the cvr.rowsVersion table with versions from cvr.instances. */
    migrateData: async (lc, tx) => {
      const pending: PendingQuery<Row[]>[] = [];
      for await (const versions of tx<
        {clientGroupID: string; version: string}[]
      >`
      SELECT "clientGroupID", "version" FROM ${tx(schema)}.instances`.cursor(
        5000,
      )) {
        for (const version of versions) {
          pending.push(
            tx`INSERT INTO ${tx(schema)}."rowsVersion" ${tx(version)} 
               ON CONFLICT ("clientGroupID")
               DO UPDATE SET ${tx(version)}`.execute(),
          );
        }
      }
      lc.info?.(`initializing rowsVersion for ${pending.length} cvrs`);
      await Promise.all(pending);
    },
  };

  const migrateV3ToV4: Migration = {
    migrateSchema: async (_, tx) => {
      await tx`ALTER TABLE ${tx(schema)}.instances ADD "owner" TEXT`;
      await tx`ALTER TABLE ${tx(schema)}.instances ADD "grantedAt" TIMESTAMPTZ`;
    },
  };

  const migrateV5ToV6: Migration = {
    migrateSchema: async (_, tx) => {
      await tx`
      ALTER TABLE ${tx(schema)}."rows"
        DROP CONSTRAINT fk_rows_client_group`;
      await tx`
      ALTER TABLE ${tx(schema)}."rowsVersion"
        DROP CONSTRAINT fk_rows_version_client_group`;
    },
  };

  const migrateV6ToV7: Migration = {
    migrateSchema: async (_, tx) => {
      await tx`ALTER TABLE ${tx(schema)}.desires ADD "expiresAt" TIMESTAMPTZ`;
      await tx`ALTER TABLE ${tx(
        schema,
      )}.desires ADD "inactivatedAt" TIMESTAMPTZ`;
      await tx`ALTER TABLE ${tx(schema)}.desires ADD "ttl" INTERVAL`;

      await tx`CREATE INDEX desires_expires_at ON ${tx(
        schema,
      )}.desires ("expiresAt")`;
      await tx`CREATE INDEX desires_inactivated_at ON ${tx(
        schema,
      )}.desires ("inactivatedAt")`;
    },
  };

  const migrateV7ToV8: Migration = {
    migrateSchema: async (_, tx) => {
      await tx`ALTER TABLE ${tx(
        schema,
      )}."desires" DROP CONSTRAINT fk_desires_client`;
    },
  };

  const migrateV8ToV9: Migration = {
    migrateSchema: async (_, tx) => {
      await tx`ALTER TABLE ${tx(schema)}.instances ADD "clientSchema" JSONB`;
    },
  };

  const migrateV9ToV10: Migration = {
    migrateSchema: async (_, tx) => {
      await tx`ALTER TABLE ${tx(schema)}.queries ADD "queryName" TEXT`;
      await tx`ALTER TABLE ${tx(schema)}.queries ADD "queryArgs" JSONB`;
      await tx`ALTER TABLE ${tx(schema)}.queries ALTER COLUMN "clientAST" DROP NOT NULL`;
    },
  };

  const migrateV10ToV11: Migration = {
    migrateSchema: async (_, tx) => {
      await tx`DROP INDEX IF EXISTS ${tx(schema)}.desires_expires_at`;
      await tx`ALTER TABLE ${tx(schema)}.queries DROP COLUMN "expiresAt"`;
      await tx`DROP INDEX IF EXISTS ${tx(schema)}.client_patch_version`;
      await tx`ALTER TABLE ${tx(schema)}.clients DROP COLUMN "patchVersion"`;
      await tx`ALTER TABLE ${tx(schema)}.clients DROP COLUMN "deleted"`;
    },
  };

  const schemaVersionMigrationMap: IncrementalMigrationMap = {
    2: migrateV1toV2,
    3: migrateV2ToV3,
    4: migrateV3ToV4,
    // v5 enables asynchronous row-record flushing, and thus relies on
    // the logic that updates and checks the rowsVersion table in v3.
    5: {minSafeVersion: 3},
    6: migrateV5ToV6,
    7: migrateV6ToV7,
    8: migrateV7ToV8,
    9: migrateV8ToV9,
    // v10 adds queryName and queryArgs to the queries table to support
    // custom queries. clientAST is now optional to support migrating
    // off client queries.
    10: migrateV9ToV10,
    // V11 removes the deprecated queries."expiresAt", clients."patchVersion",
    // clients."deleted" columns.
    11: migrateV10ToV11,
  };

  await runSchemaMigrations(
    log,
    'view-syncer',
    cvrSchema(shard),
    db,
    setupMigration,
    schemaVersionMigrationMap,
  );
}
