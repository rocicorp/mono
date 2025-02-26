import {PG_INSUFFICIENT_PRIVILEGE} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import {literal} from 'pg-format';
import postgres from 'postgres';
import {assert} from '../../../../../../shared/src/asserts.ts';
import * as v from '../../../../../../shared/src/valita.ts';
import {Default} from '../../../../db/postgres-replica-identity-enum.ts';
import type {PostgresDB, PostgresTransaction} from '../../../../types/pg.ts';
import {id} from '../../../../types/sql.ts';
import type {ShardConfig} from '../shard-config.ts';
import {createEventTriggerStatements} from './ddl.ts';
import {
  getPublicationInfo,
  publishedSchema,
  type PublicationInfo,
  type PublishedSchema,
} from './published.ts';
import {ALLOWED_APP_ID_CHARACTERS, validate} from './validation.ts';

// Creates a function that appends `_SHARD_ID` to the input.
export function append(shardID: string) {
  return (name: string) => id(name + '_' + shardID);
}

export function schemaFor(appID: string, shardID: string) {
  return append(shardID)(appID);
}

export function unescapedSchema(appID: string, shardID: string) {
  return `${appID}_${shardID}`;
}

export function internalPublicationPrefix(appID: string) {
  return `_${appID}_`;
}

function defaultPublicationName(appID: string, shardID: string) {
  return `_${appID}_public_${shardID}`;
}

function metadataPublicationName(appID: string, shardID: string) {
  return `_${appID}_metadata_${shardID}`;
}

// The GLOBAL_SETUP must be idempotent as it can be run multiple times for different shards.
// Exported for testing.
export function globalSetup(appID: string): string {
  // Sanity check; should already be validated upstream.
  assert(ALLOWED_APP_ID_CHARACTERS.test(appID), `Invalid App ID ${appID}`);

  return `
  CREATE SCHEMA IF NOT EXISTS ${appID};

  CREATE TABLE IF NOT EXISTS ${appID}."schemaVersions" (
    "minSupportedVersion" INT4,
    "maxSupportedVersion" INT4,

    -- Ensure that there is only a single row in the table.
    -- Application code can be agnostic to this column, and
    -- simply invoke UPDATE statements on the version columns.
    "lock" BOOL PRIMARY KEY DEFAULT true CHECK (lock)
  );

  INSERT INTO ${appID}."schemaVersions" ("lock", "minSupportedVersion", "maxSupportedVersion")
    VALUES (true, 1, 1) ON CONFLICT DO NOTHING;

  CREATE TABLE IF NOT EXISTS ${appID}.permissions (
    "permissions" JSONB,
    "hash"        TEXT,

    -- Ensure that there is only a single row in the table.
    -- Application code can be agnostic to this column, and
    -- simply invoke UPDATE statements on the version columns.
    "lock" BOOL PRIMARY KEY DEFAULT true CHECK (lock)
  );

  CREATE OR REPLACE FUNCTION ${appID}.set_permissions_hash()
  RETURNS TRIGGER AS $$
  BEGIN
      NEW.hash = md5(NEW.permissions::text);
      RETURN NEW;
  END;
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE TRIGGER on_set_permissions 
    BEFORE INSERT OR UPDATE ON ${appID}.permissions
    FOR EACH ROW
    EXECUTE FUNCTION ${appID}.set_permissions_hash();

  INSERT INTO ${appID}.permissions (permissions) VALUES (NULL) ON CONFLICT DO NOTHING;
`;
}

export async function ensureGlobalTables(db: PostgresDB, appID: string) {
  await db.unsafe(globalSetup(appID));
}

export function getClientsTableDefinition(schema: string) {
  return `CREATE TABLE ${schema}."clients" (
    "clientGroupID"  TEXT NOT NULL,
    "clientID"       TEXT NOT NULL,
    "lastMutationID" BIGINT NOT NULL,
    "userID"         TEXT,
    PRIMARY KEY("clientGroupID", "clientID")
  );`;
}

export function shardSetup(
  appID: string,
  shardID: string,
  publications: string[],
): string {
  assert(ALLOWED_APP_ID_CHARACTERS.test(appID), `Invalid App ID ${appID}`);

  const schema = schemaFor(appID, shardID);

  const metadataPublication = metadataPublicationName(appID, shardID);

  publications.push(metadataPublication);
  publications.sort();

  return `
  CREATE SCHEMA IF NOT EXISTS ${schema};

  ${getClientsTableDefinition(schema)}

  CREATE PUBLICATION ${id(metadataPublication)}
    FOR TABLE ${appID}."schemaVersions", ${appID}."permissions", TABLE ${schema}."clients";

  CREATE TABLE ${schema}."shardConfig" (
    "publications"  TEXT[] NOT NULL,
    "ddlDetection"  BOOL NOT NULL,
    "initialSchema" JSON,

    -- Ensure that there is only a single row in the table.
    "lock" BOOL PRIMARY KEY DEFAULT true CHECK (lock)
  );

  INSERT INTO ${schema}."shardConfig" 
    ("lock", "publications", "ddlDetection", "initialSchema")
    VALUES (true, 
      ARRAY[${literal(publications)}], 
      false,  -- set in SAVEPOINT with triggerSetup() statements
      null    -- set in initial-sync at consistent_point LSN.
    );
  `;
}

export function dropShard(appID: string, shardID: string): string {
  const schema = schemaFor(appID, shardID);
  const metadataPublication = metadataPublicationName(appID, shardID);
  const defaultPublication = defaultPublicationName(appID, shardID);

  // DROP SCHEMA ... CASCADE does not drop dependent PUBLICATIONS,
  // so PUBLICATIONs must be dropped explicitly.
  return `
    DROP PUBLICATION IF EXISTS ${id(defaultPublication)};
    DROP PUBLICATION IF EXISTS ${id(metadataPublication)};
    DROP SCHEMA IF EXISTS ${schema} CASCADE;
  `;
}

const internalShardConfigSchema = v.object({
  publications: v.array(v.string()),
  ddlDetection: v.boolean(),
  initialSchema: publishedSchema.nullable(),
});

export type InternalShardConfig = v.Infer<typeof internalShardConfigSchema>;

// triggerSetup is run separately in a sub-transaction (i.e. SAVEPOINT) so
// that a failure (e.g. due to lack of superuser permissions) can be handled
// by continuing in a degraded mode (ddlDetection = false).
function triggerSetup(
  appID: string,
  shardID: string,
  publications: string[],
): string {
  const schema = schemaFor(appID, shardID);
  return (
    createEventTriggerStatements(appID, shardID, publications) +
    `UPDATE ${schema}."shardConfig" SET "ddlDetection" = true;`
  );
}

// Called in initial-sync to store the exact schema that was initially synced.
export async function setInitialSchema(
  db: PostgresDB,
  appID: string,
  shardID: string,
  {tables, indexes}: PublishedSchema,
) {
  const schema = unescapedSchema(appID, shardID);
  const synced: PublishedSchema = {tables, indexes};
  await db`UPDATE ${db(schema)}."shardConfig" SET "initialSchema" = ${synced}`;
}

export async function getInternalShardConfig(
  db: PostgresDB,
  appID: string,
  shardID: string,
): Promise<InternalShardConfig> {
  const result = await db`
    SELECT "publications", "ddlDetection", "initialSchema" 
      FROM ${db(unescapedSchema(appID, shardID))}."shardConfig";
  `;
  assert(result.length === 1);
  return v.parse(result[0], internalShardConfigSchema, 'passthrough');
}

/**
 * Sets up and returns all publications (including internal ones) for
 * the given shard.
 */
export async function setupTablesAndReplication(
  lc: LogContext,
  tx: PostgresTransaction,
  appID: string,
  {id, publications}: ShardConfig,
) {
  // Validate requested publications.
  for (const pub of publications) {
    if (pub.startsWith('_')) {
      throw new Error(
        `Publication names starting with "_" are reserved for internal use.\n` +
          `Please use a different name for publication "${pub}".`,
      );
    }
  }
  const allPublications: string[] = [];

  // Setup application publications.
  if (publications.length) {
    const results = await tx<{pubname: string}[]>`
    SELECT pubname from pg_publication WHERE pubname IN ${tx(
      publications,
    )}`.values();

    if (results.length !== publications.length) {
      throw new Error(
        `Unknown or invalid publications. Specified: [${publications}]. Found: [${results.flat()}]`,
      );
    }
    allPublications.push(...publications);
  } else {
    const defaultPublication = defaultPublicationName(appID, id);
    // Note: For re-syncing, this publication is dropped in dropShard(), so an existence
    //       check is unnecessary.
    await tx`
      CREATE PUBLICATION ${tx(defaultPublication)} 
        FOR TABLES IN SCHEMA public
        WITH (publish_via_partition_root = true)`;
    allPublications.push(defaultPublication);
  }

  // Setup the global tables and shard tables / publications.
  await tx.unsafe(globalSetup(appID) + shardSetup(appID, id, allPublications));

  const pubs = await getPublicationInfo(tx, allPublications);
  await replicaIdentitiesForTablesWithoutPrimaryKeys(pubs)?.apply(lc, tx);

  try {
    await tx.savepoint(sub =>
      sub.unsafe(triggerSetup(appID, id, allPublications)),
    );
  } catch (e) {
    if (
      !(
        e instanceof postgres.PostgresError &&
        e.code === PG_INSUFFICIENT_PRIVILEGE
      )
    ) {
      throw e;
    }
    // If triggerSetup() fails, replication continues in ddlDetection=false mode.
    lc.warn?.(
      `Unable to create event triggers for schema change detection:\n\n` +
        `"${e.hint ?? e.message}"\n\n` +
        `Proceeding in degraded mode: schema changes will halt replication,\n` +
        `requiring the replica to be reset (manually or with --auto-reset).`,
    );
  }
}

export function validatePublications(
  lc: LogContext,
  published: PublicationInfo,
) {
  // Verify that all publications export the proper events.
  published.publications.forEach(pub => {
    if (
      !pub.pubinsert ||
      !pub.pubtruncate ||
      !pub.pubdelete ||
      !pub.pubtruncate
    ) {
      // TODO: Make APIError?
      throw new Error(
        `PUBLICATION ${pub.pubname} must publish insert, update, delete, and truncate`,
      );
    }
  });

  published.tables.forEach(table => validate(lc, table));
}

type ReplicaIdentities = {
  apply(lc: LogContext, db: PostgresDB): Promise<void>;
};

export function replicaIdentitiesForTablesWithoutPrimaryKeys(
  pubs: PublishedSchema,
): ReplicaIdentities | undefined {
  const replicaIdentities: {
    schema: string;
    tableName: string;
    indexName: string;
  }[] = [];
  for (const table of pubs.tables) {
    if (!table.primaryKey?.length && table.replicaIdentity === Default) {
      // Look for an index that can serve as the REPLICA IDENTITY USING INDEX. It must be:
      // - UNIQUE
      // - NOT NULL columns
      // - not deferrable (i.e. isImmediate)
      // - not partial (are already filtered out)
      //
      // https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY
      const {schema, name: tableName} = table;
      for (const {columns, name: indexName} of pubs.indexes.filter(
        idx =>
          idx.schema === schema &&
          idx.tableName === tableName &&
          idx.unique &&
          idx.isImmediate,
      )) {
        if (Object.keys(columns).some(col => !table.columns[col].notNull)) {
          continue; // Only indexes with all NOT NULL columns are suitable.
        }
        replicaIdentities.push({schema, tableName, indexName});
        break;
      }
    }
  }

  if (replicaIdentities.length === 0) {
    return undefined;
  }
  return {
    apply: async (lc: LogContext, db: PostgresDB) => {
      for (const {schema, tableName, indexName} of replicaIdentities) {
        lc.info?.(
          `setting "${indexName}" as the REPLICA IDENTITY for "${tableName}"`,
        );
        await db`ALTER TABLE ${db(schema)}.${db(
          tableName,
        )} REPLICA IDENTITY USING INDEX ${db(indexName)}`;
      }
    },
  };
}
