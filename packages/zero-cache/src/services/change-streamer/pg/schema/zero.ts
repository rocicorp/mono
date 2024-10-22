import type {LogContext} from '@rocicorp/logger';
import {ident, literal} from 'pg-format';
import {warnIfDataTypeSupported} from '../../../../db/pg-to-lite.js';
import type {PostgresTransaction} from '../../../../types/pg.js';
import {ZERO_VERSION_COLUMN_NAME} from '../../../replicator/schema/replication-state.js';
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
  lc: LogContext,
  tx: PostgresTransaction,
  {id, publications}: ShardConfig,
): Promise<PublicationInfo> {
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

  const pubInfo = await getPublicationInfo(tx, allPublications);
  validatePublications(lc, pubInfo);
  return pubInfo;
}

const ALLOWED_IDENTIFIER_CHARS = /^[A-Za-z_-]+$/;

function validatePublications(lc: LogContext, published: PublicationInfo) {
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

  published.tables.forEach(table => {
    if (!['public', 'zero'].includes(table.schema)) {
      // This may be relaxed in the future. We would need a plan for support in the AST first.
      throw new Error('Only the default "public" schema is supported.');
    }
    if (ZERO_VERSION_COLUMN_NAME in table.columns) {
      throw new Error(
        `Table "${table.name}" uses reserved column name "${ZERO_VERSION_COLUMN_NAME}"`,
      );
    }
    if (table.primaryKey.length === 0) {
      throw new Error(`Table "${table.name}" does not have a PRIMARY KEY`);
    }
    if (!ALLOWED_IDENTIFIER_CHARS.test(table.schema)) {
      throw new Error(`Schema "${table.schema}" has invalid characters.`);
    }
    if (!ALLOWED_IDENTIFIER_CHARS.test(table.name)) {
      throw new Error(`Table "${table.name}" has invalid characters.`);
    }
    for (const [col, spec] of Object.entries(table.columns)) {
      if (!ALLOWED_IDENTIFIER_CHARS.test(col)) {
        throw new Error(
          `Column "${col}" in table "${table.name}" has invalid characters.`,
        );
      }
      warnIfDataTypeSupported(lc, spec.dataType, table.name, col);
    }
  });
}
