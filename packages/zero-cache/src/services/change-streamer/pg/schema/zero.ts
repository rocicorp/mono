import {ident, literal} from 'pg-format';
import type {PostgresTransaction} from '../../../../types/pg.js';
import type {ShardConfig} from '../shard-config.js';
import {createEventTriggerStatements} from './ddl.js';
import {getPublicationInfo, type PublicationInfo} from './published.js';

export const APP_PUBLICATION_PREFIX = 'zero_';
export const INTERNAL_PUBLICATION_PREFIX = '_zero_';

const DEFAULT_APP_PUBLICATION = APP_PUBLICATION_PREFIX + 'public';
const SCHEMA_VERSIONS_PUBLICATION =
  INTERNAL_PUBLICATION_PREFIX + 'schema_versions';

const GLOBAL_SETUP = `
  CREATE SCHEMA IF NOT EXISTS zero;

  CREATE TABLE zero.clients (
    "shardID"        TEXT NOT NULL,
    "clientGroupID"  TEXT NOT NULL,
    "clientID"       TEXT NOT NULL,
    "lastMutationID" BIGINT NOT NULL,
    "userID"         TEXT,
    PRIMARY KEY("shardID", "clientGroupID", "clientID")
  );

  -- Note: this must be kept in sync with init.sql in zbugs.
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

  CREATE PUBLICATION ${SCHEMA_VERSIONS_PUBLICATION} FOR TABLE zero."schemaVersions";
`;

/**
 * Sets up and returns all publications (including internal ones) for
 * the given shard.
 */
export async function setupTablesAndReplication(
  tx: PostgresTransaction,
  {id, publications}: ShardConfig,
): Promise<PublicationInfo> {
  // Validate requested publications.
  for (const pub of publications) {
    // TODO: We can consider relaxing this now that we use per-shard
    // triggers rather than global prefix-basd triggers. We should
    // probably still disallow the INTERNAL_PUBLICATION_PREFIX though.
    if (!pub.startsWith(APP_PUBLICATION_PREFIX)) {
      throw new Error(
        `Publication ${pub} does not start with ${APP_PUBLICATION_PREFIX}`,
      );
    }
  }

  const allPublications = [];

  // Setup the global tables and publication if not present.
  const globalPub = await tx`
  SELECT 1 FROM pg_publication WHERE pubname = ${SCHEMA_VERSIONS_PUBLICATION}`;
  if (globalPub.length === 0) {
    await tx.unsafe(GLOBAL_SETUP);
  }
  allPublications.push(SCHEMA_VERSIONS_PUBLICATION);

  // Setup the zero.clients publication for rows for this shardID.
  const clientsPublication = INTERNAL_PUBLICATION_PREFIX + id + '_clients';
  const shardPub = await tx`
    SELECT 1 FROM pg_publication WHERE pubname = ${clientsPublication}`;
  if (shardPub.length === 0) {
    await tx.unsafe(`
      CREATE PUBLICATION ${ident(clientsPublication)}
        FOR TABLE zero.clients WHERE ("shardID" = ${literal(id)})`);
  }
  allPublications.push(clientsPublication);

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

  // Setup DDL trigger events.
  await tx.unsafe(createEventTriggerStatements(id, allPublications));

  return getPublicationInfo(tx, allPublications);
}
