import type {LogContext} from '@rocicorp/logger';
import {literal} from 'pg-format';
import {assert} from '../../../../../../shared/src/asserts.js';
import type {PostgresDB, PostgresTransaction} from '../../../../types/pg.js';
import {id} from '../../../../types/sql.js';
import type {ShardConfig} from '../shard-config.js';
import {createEventTriggerStatements} from './ddl.js';
import {getPublicationInfo, type PublicationInfo} from './published.js';
import {validate} from './validation.js';

// Creates a function that appends `_SHARD_ID` to the input.
export function append(shardID: string) {
  return (name: string) => id(name + '_' + shardID);
}

export function schemaFor(shardID: string) {
  return append(shardID)('zero');
}

export function unescapedSchema(shardID: string) {
  return `zero_${shardID}`;
}

export const APP_PUBLICATION_PREFIX = 'zero_';
export const INTERNAL_PUBLICATION_PREFIX = '_zero_';

const DEFAULT_APP_PUBLICATION = APP_PUBLICATION_PREFIX + 'public';
const METADATA_PUBLICATION_PREFIX = INTERNAL_PUBLICATION_PREFIX + 'metadata_';

// The GLOBAL_SETUP must be idempotent as it can be run multiple times for different shards.
const GLOBAL_SETUP = `
  CREATE SCHEMA IF NOT EXISTS zero;

  CREATE TABLE IF NOT EXISTS zero."schemaVersions" (
    "minSupportedVersion" INT4,
    "maxSupportedVersion" INT4,

    -- Ensure that there is only a single row in the table.
    -- Application code can be agnostic to this column, and
    -- simply invoke UPDATE statements on the version columns.
    "lock" BOOL PRIMARY KEY DEFAULT true,
    CONSTRAINT zero_schema_versions_single_row_constraint CHECK (lock)
  );

  INSERT INTO zero."schemaVersions" ("lock", "minSupportedVersion", "maxSupportedVersion")
    VALUES (true, 1, 1) ON CONFLICT DO NOTHING;
`;

function shardSetup(shardID: string, publications: string[]): string {
  const sharded = append(shardID);
  const schema = schemaFor(shardID);

  const metadataPublication = METADATA_PUBLICATION_PREFIX + shardID;

  publications.push(metadataPublication);
  publications.sort();

  return (
    `
  CREATE SCHEMA IF NOT EXISTS ${schema};

  CREATE TABLE ${schema}."clients" (
    "clientGroupID"  TEXT NOT NULL,
    "clientID"       TEXT NOT NULL,
    "lastMutationID" BIGINT NOT NULL,
    "userID"         TEXT,
    PRIMARY KEY("clientGroupID", "clientID")
  );

  CREATE PUBLICATION ${id(metadataPublication)}
    FOR TABLE zero."schemaVersions", TABLE ${schema}."clients";

  CREATE TABLE ${schema}."shardConfig" (
    "publications"  TEXT[] NOT NULL,

    -- Ensure that there is only a single row in the table.
    "lock" BOOL PRIMARY KEY DEFAULT true,
    CONSTRAINT ${sharded('single_row_shard_config')} CHECK (lock)
  );

  INSERT INTO ${schema}."shardConfig" (lock, publications)
    VALUES (true, ARRAY[${literal(publications)}]);
  ` + createEventTriggerStatements(shardID, publications)
  );
}

export async function getShardConfig(
  db: PostgresDB,
  shardID: string,
): Promise<{publications: string[]}> {
  const result = await db<{publications: string[]}[]>`
    SELECT publications FROM ${db(unescapedSchema(shardID))}."shardConfig";
  `;
  assert(result.length === 1);
  return result[0];
}

/**
 * Sets up and returns all publications (including internal ones) for
 * the given shard.
 */
export async function setupTablesAndReplication(
  lc: LogContext,
  tx: PostgresTransaction,
  {id, publications}: ShardConfig,
) {
  // Validate requested publications.
  for (const pub of publications) {
    // TODO: We can consider relaxing this now that we use per-shard
    // triggers rather than global prefix-based triggers. We should
    // probably still disallow the INTERNAL_PUBLICATION_PREFIX though.
    if (!pub.startsWith(APP_PUBLICATION_PREFIX)) {
      throw new Error(
        `Publication ${pub} does not start with ${APP_PUBLICATION_PREFIX}`,
      );
    }
  }

  const allPublications = [];

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
    const defaultPub = await tx`
    SELECT 1 FROM pg_publication WHERE pubname = ${DEFAULT_APP_PUBLICATION}`;
    if (defaultPub.length === 0) {
      await tx`
      CREATE PUBLICATION ${tx(
        DEFAULT_APP_PUBLICATION,
      )} FOR TABLES IN SCHEMA public`;
    }
    allPublications.push(DEFAULT_APP_PUBLICATION);
  }

  // Setup the global tables and shard tables / publications.
  await tx.unsafe(GLOBAL_SETUP + shardSetup(id, allPublications));

  const pubInfo = await getPublicationInfo(tx, allPublications);
  validatePublications(lc, id, pubInfo);
}

export function validatePublications(
  lc: LogContext,
  shardID: string,
  published: PublicationInfo,
) {
  const shardSchema = unescapedSchema(shardID);
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

  published.tables.forEach(table => validate(lc, shardSchema, table));
}
