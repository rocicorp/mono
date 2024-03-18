import type {LogContext} from '@rocicorp/logger';
import postgres from 'postgres';
import {sleep} from 'shared/src/sleep.js';
import * as v from 'shared/src/valita.js';
import {CREATE_REPLICATION_TABLES} from './incremental-sync.js';
import {createTableStatement} from './tables/create.js';
import {getPublishedTables} from './tables/published.js';
import type {ColumnSpec, TableSpec} from './tables/specs.js';

const PUB_PREFIX = 'zero_';
const SLOT_PREFIX = `zero_slot_`;

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
  replicaID: string,
  tx: postgres.TransactionSql,
  upstreamUri: string,
  subName = 'zero_sync',
) {
  lc.info?.(`Starting initial data synchronization from ${upstreamUri}`);
  const slotName = `${SLOT_PREFIX}${replicaID}`;
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
    CREATE SUBSCRIPTION ${subName}
      CONNECTION '${upstreamUri}'
      PUBLICATION ${published.publications.join(',')}
      WITH (slot_name='${slotName}', create_slot=false);`,
  ];

  // Execute all statements in a single batch.
  await tx.unsafe(stmts.join('\n'));

  lc.info?.(`Started initial data synchronization from ${upstreamUri}`);
}

type SubscribedTable = {
  subname: string;
  schema: string;
  table: string;
  state: string;
};

const MAX_POLLING_INTERVAL = 30000;

/**
 * Waits for the initial data synchronization, started by the {@link startPostgresReplication}
 * migration, to complete. This is determined by polling the `pg_subscription_rel` table:
 * https://www.postgresql.org/docs/current/catalog-pg-subscription-rel.html
 *
 * Once tables are synchronized, this migration step is considered complete, to be followed up
 * with the {@link handoffPostgresReplication} step. Note that although the waiting and
 * handoff can technically be done in a single step, holding a transaction for a long time
 * and then attempting to modify a global table (`pg_subscription`) tends to cause deadlocks
 * in the testing environment.
 */
export async function waitForInitialDataSynchronization(
  // export async function handoffPostgresReplication(
  lc: LogContext,
  _replicaID: string,
  tx: postgres.TransactionSql,
  upstreamUri: string,
  subName = 'zero_sync',
) {
  lc.info?.(`Awaiting initial data synchronization from ${upstreamUri}`);
  for (
    let interval = 100; // Exponential backoff, up to 30 seconds between polls.
    ;
    interval = Math.min(interval * 2, MAX_POLLING_INTERVAL)
  ) {
    const subscribed = await tx<SubscribedTable[]>`
    SELECT p.subname, n.nspname as schema, c.relname as table, r.srsubstate as state 
      FROM pg_subscription p
      JOIN pg_subscription_rel r ON p.oid = r.srsubid
      JOIN pg_class c ON c.oid = r.srrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE p.subname = ${subName};`;

    if (subscribed.length === 0) {
      // This indicates that something is wrong.
      // At minimum there should be the zero.clients table.
      throw new Error('No subscribed tables');
    }

    const syncing = subscribed.filter(table => table.state !== 'r');
    if (syncing.length === 0) {
      lc.info?.(`Finished syncing ${subscribed.length} tables`, subscribed);
      return;
    }

    if (interval >= MAX_POLLING_INTERVAL) {
      const postInitialize = subscribed.filter(table => table.state !== 'i');
      if (postInitialize.length === 0) {
        // Something is wrong here, as Postgres should be able to transition
        // at least one table from the 'i' (initialize) state to 'd' (data copy)
        // or later. Manual inspection is warranted. For instance, it's possible
        // that the Postgres instance needs to be configured with more
        // max_logical_replication_workers.
        throw new Error(
          'Subscribed tables have failed to pass the "initialize" state',
        );
      }
    }
    lc.info?.(
      `Still syncing ${syncing.length} tables (${syncing.map(
        t => t.table,
      )}). Polling in ${interval}ms.`,
      subscribed,
    );
    await sleep(interval);
  }
}

/**
 * Following up after {@link waitForInitialDataSynchronization}, this migration step detaches
 * and drops the subscription so that the replication slot can be used by the replicator.
 * The replication tables are also set up in this step.
 */
export async function handoffPostgresReplication(
  lc: LogContext,
  _replicaID: string,
  tx: postgres.TransactionSql,
  upstreamUri: string,
  subName = 'zero_sync',
) {
  lc.info?.(`Taking over subscription from ${upstreamUri}`);
  await tx.unsafe(
    // Disable and detach the subscription from the replication slot so that the slot
    // can be handed off to the Replicator logic. See the "Notes" section in
    // https://www.postgresql.org/docs/current/sql-dropsubscription.html
    `
    ALTER SUBSCRIPTION ${subName} DISABLE;
    ALTER SUBSCRIPTION ${subName} SET(slot_name=NONE);
    DROP SUBSCRIPTION IF EXISTS ${subName};
  ` +
      // Create the Replication tables in the same transaction.
      CREATE_REPLICATION_TABLES,
  );
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

/**
 * Ensures that the replication slot is created on upstream.
 *
 * Note that this performed in its own transaction because:
 *
 * * pg_create_logical_replication_slot() cannot be called in a transaction with other
 *   writes. This is presumably because it modifies a top-level table of the Postgres instance.
 *
 * * Creating the slot explicitly on upstream, rather than implicitly from the `CREATE SUBSCRIPTION`
 *   command on the replica, allows the latter to be performed transactionally with the rest of
 *   the replica setup. Postgres will otherwise fail with the error:
 *   ```
 *   PostgresError: CREATE SUBSCRIPTION ... WITH (create_slot = true) cannot run inside a transaction block
 *   ```
 */
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
