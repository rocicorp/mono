import type {LogContext} from '@rocicorp/logger';
import postgres from 'postgres';
import * as v from 'shared/src/valita.js';
import {createTableStatement} from './tables/create.js';
import {getPublishedTables} from './tables/published.js';
import type {ColumnSpec, TableSpec} from './tables/specs.js';

const PUB_PREFIX = 'zero_';

const publicationSchema = v.object({
  pubname: v.string(),
  pubinsert: v.boolean(),
  pubupdate: v.boolean(),
  pubdelete: v.boolean(),
  pubtruncate: v.boolean(),
});

const publicationsResultSchema = v.array(publicationSchema);

const ZERO_VERSION_COLUMN_NAME = '_0_version';
const ZERO_VERSION_COLUMN_SPEC: ColumnSpec = {
  characterMaximumLength: 38,
  columnDefault: "'00'::text",
  dataType: 'character varying',
};

type Published = {
  publications: string[];
  tables: Record<string, TableSpec>;
};

/**
 * Starts Postgres logical replication from the upstream DB to the Sync Replica.
 * Specifically, we rely on Postgres to perform the "initial data synchronization" phase
 * of a logical replication subscription as described in
 * https://www.postgresql.org/docs/current/logical-replication-subscription.html
 *
 * This results in the Postgres sync replica copying a snapshot of published tables/columns
 * from upstream (using snapshot reservation and per-thread worker processes). After this
 * completes, incremental logical replication kicks in, which is where the Replicator takes over.
 */
export async function startPostgresReplication(
  lc: LogContext,
  tx: postgres.TransactionSql,
  upstreamUri: string,
  slotName = 'zero_slot',
) {
  lc.info?.(`Starting initial data synchronization from ${upstreamUri}`);
  const published = await setupUpstream(lc, upstreamUri, slotName);

  lc.info?.(`Upstream is setup for publishing`, published);

  // Create the corresponding schemas and tables in the Sync Replica, with the
  // additional _0_version column to track row versions.
  const schemas = new Set<string>();
  const tablesStmts = Object.values(published.tables).map(table => {
    if (ZERO_VERSION_COLUMN_NAME in table.columns) {
      throw new Error(
        `Table ${table.name} uses reserved name column name ${ZERO_VERSION_COLUMN_NAME}`,
      );
    }
    schemas.add(table.schema);
    // Add the _0_version column with a default value of "00".
    table.columns[ZERO_VERSION_COLUMN_NAME] = ZERO_VERSION_COLUMN_SPEC;
    return createTableStatement(table);
  });

  const schemaStmts = [...schemas].map(
    schema => `CREATE SCHEMA IF NOT EXISTS ${schema};`,
  );

  const stmts = [
    ...schemaStmts,
    ...tablesStmts,
    `
    CREATE SUBSCRIPTION zero_sync
      CONNECTION '${upstreamUri}'
      PUBLICATION ${published.publications.join(',')}
      WITH (slot_name='${slotName}', create_slot=false);`,
  ];

  // Execute all statements in a single batch.
  await tx.unsafe(stmts.join('\n'));
}

async function setupUpstream(
  lc: LogContext,
  upstreamUri: string,
  slotName: string,
): Promise<Published> {
  const upstreamDB = postgres(upstreamUri, {
    transform: postgres.camel,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    fetch_types: false,
  });
  const [_, publishedTables] = await Promise.all([
    // Ensure that the replication slot exists. This must be done in its own
    // transaction, or else Postgres will complain with:
    //
    // PostgresError: cannot create logical replication slot in transaction that has performed writes
    ensureReplicationSlot(lc, upstreamDB, slotName),

    // In parallel, ensure that the schema and publications are setup.
    // Note that both transactions must succeed for the migration to continue.
    ensurePublishedTables(lc, upstreamDB),
  ]);
  return publishedTables;
}

function ensureReplicationSlot(
  lc: LogContext,
  upstreamDB: postgres.Sql,
  slotName: string,
) {
  return upstreamDB.begin(async tx => {
    const slots = await tx`
    SELECT slot_name FROM pg_replication_slots WHERE slot_name = ${slotName}`;

    if (slots.count > 0) {
      lc.info?.(`Replication slot "${slotName}" already exists`);
      return;
    }

    lc.info?.(`Creating replication slot "${slotName}"`);
    await tx`
    SELECT * FROM pg_create_logical_replication_slot(${slotName}, 'pgoutput');`;
  });
}

function ensurePublishedTables(
  _: LogContext,
  upstreamDB: postgres.Sql,
): Promise<Published> {
  return upstreamDB.begin(async tx => {
    const pubInfo = v.parse(
      await tx`
    SELECT ${tx(Object.keys(publicationSchema.shape))} FROM pg_publication
    WHERE STARTS_WITH(pubname, ${PUB_PREFIX})
    `,
      publicationsResultSchema,
    );

    const tables = await getPublishedTables(tx, PUB_PREFIX);
    if ('zero.clients' in tables) {
      // upstream is already set up for replication.
      return {
        publications: pubInfo.map(p => p.pubname),
        tables,
      };
    }

    // Verify that any manually configured publications export the proper events.
    pubInfo.forEach(pub => {
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
      if (pub.pubname === `${PUB_PREFIX}metadata`) {
        throw new Error(
          `PUBLICATION name ${PUB_PREFIX}metadata is reserved for internal use`,
        );
      }
    });

    const publications = pubInfo.map(p => p.pubname);

    let dataPublication = '';
    if (pubInfo.length === 0) {
      // If there are no custom zero_* publications, set one up to publish all tables.
      const pubName = `${PUB_PREFIX}data`;
      dataPublication = `CREATE PUBLICATION ${pubName} FOR ALL TABLES;`;
      publications.push(pubName);
    }

    // Send everything as a single batch.
    await tx.unsafe(
      `
    CREATE SCHEMA zero;
    CREATE TABLE zero.clients (
      client_id TEXT PRIMARY KEY,
      last_mutation_id BIGINT
    );
    CREATE PUBLICATION ${PUB_PREFIX + 'metadata'} FOR TABLES IN SCHEMA zero;
    ${dataPublication}
    `,
    );

    publications.push(`${PUB_PREFIX}metadata`);

    return {
      publications,
      tables: await getPublishedTables(tx, PUB_PREFIX),
    };
  });
}
