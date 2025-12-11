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
      await tx`ALTER TABLE ${tx(schema)}.desires DROP COLUMN "expiresAt"`;
      await tx`DROP INDEX IF EXISTS ${tx(schema)}.client_patch_version`;
      await tx`ALTER TABLE ${tx(schema)}.clients DROP COLUMN "patchVersion"`;
      await tx`ALTER TABLE ${tx(schema)}.clients DROP COLUMN "deleted"`;
    },
  };

  const migratedV11ToV12: Migration = {
    migrateSchema: async (_, tx) => {
      await tx`ALTER TABLE ${tx(schema)}.queries ALTER COLUMN "queryArgs" TYPE JSON USING "queryArgs"::JSON`;
    },
  };

  const migratedV12ToV13: Migration = {
    migrateSchema: async (_, tx) => {
      await tx`ALTER TABLE ${tx(schema)}.instances ADD COLUMN "ttlClock" DOUBLE PRECISION NOT NULL DEFAULT 0`;
    },
  };

  const migratedV13ToV14: Migration = {
    migrateSchema: async (_, sql) => {
      await sql`
        CREATE INDEX instances_last_active ON ${sql(schema)}.instances ("lastActive");
      `;

      // Update / add foreign key constraints to cascade deletes.
      for (const [table, reference] of [
        ['clients', 'instances'],
        ['queries', 'instances'],
        ['rows', 'rowsVersion'],
      ] as [string, string][]) {
        const constraint = sql(`fk_${table}_client_group`);
        await sql`
          ALTER TABLE ${sql(schema)}.${sql(table)} DROP CONSTRAINT IF EXISTS ${constraint}`;
        await sql`
          ALTER TABLE ${sql(schema)}.${sql(table)} ADD CONSTRAINT ${constraint}
            FOREIGN KEY("clientGroupID")
            REFERENCES ${sql(schema)}.${sql(reference)} ("clientGroupID")
            ON DELETE CASCADE;
        `;
      }
    },
  };

  const migratedV14ToV15: Migration = {
    migrateSchema: async (_, sql) => {
      // Add new columns for storing inactivatedAt and ttl in milliseconds.
      // This avoids postgres.js type conversion issues with TIMESTAMPTZ and INTERVAL.
      await sql`ALTER TABLE ${sql(schema)}.desires 
        ADD COLUMN "inactivatedAtMs" DOUBLE PRECISION`;
      await sql`ALTER TABLE ${sql(schema)}.desires 
        ADD COLUMN "ttlMs" DOUBLE PRECISION`;
    },
    // Migrate existing data: convert TIMESTAMPTZ to milliseconds for inactivatedAt
    // and INTERVAL to milliseconds for ttl
    // Note: EXTRACT(EPOCH FROM NULL) returns NULL, so NULL values are preserved
    migrateData: async (lc, sql) => {
      lc.info?.(
        'Migrating desires.inactivatedAt to inactivatedAtMs and ttl to ttlMs',
      );
      await sql`
        UPDATE ${sql(schema)}.desires
        SET "inactivatedAtMs" = EXTRACT(EPOCH FROM "inactivatedAt") * 1000,
            "ttlMs" = EXTRACT(EPOCH FROM "ttl") * 1000
      `;
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
    12: migratedV11ToV12,
    // V13 adds instances."ttlClock"
    13: migratedV12ToV13,
    // V14 adds an index on instances."lastActive" and a FK constraint
    // from rows."clientGroupID" to rowsVersion."clientGroupID" for
    // garbage collection
    14: migratedV13ToV14,
    // V15 adds desires."inactivatedAtTTLClock" to store TTLClock values
    // directly as DOUBLE PRECISION, avoiding postgres.js TIMESTAMPTZ
    // type conversion issues
    15: migratedV14ToV15,
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
