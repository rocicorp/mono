import type {LogContext} from '@rocicorp/logger';
import postgres from 'postgres';
import {sleep} from 'shared/src/sleep.js';
import {id, idList} from '../../types/sql.js';
import {createTableStatement} from './tables/create.js';
import {PublicationInfo, getPublicationInfo} from './tables/published.js';
import type {ColumnSpec} from './tables/specs.js';

export const PUB_PREFIX = 'zero_';

export const ZERO_VERSION_COLUMN_NAME = '_0_version';
const ZERO_VERSION_COLUMN_SPEC: ColumnSpec = {
  characterMaximumLength: 38,
  columnDefault: "'00'::text",
  dataType: 'character varying',
  notNull: false,
};

export function replicationSlot(replicaID: string): string {
  return `zero_slot_${replicaID}`;
}

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
  const slotName = replicationSlot(replicaID);
  const published = await setupUpstream(lc, upstreamUri, slotName);

  lc.info?.(`Upstream is setup for publishing`, published);

  // Create the corresponding schemas and tables in the Sync Replica, with the
  // additional _0_version column to track row versions.
  const schemas = new Set<string>();
  const tablesStmts = Object.values(published.tables).map(table => {
    if (table.schema === '_zero') {
      throw new Error(`Schema _zero is reserved for internal use`);
    }
    if (ZERO_VERSION_COLUMN_NAME in table.columns) {
      throw new Error(
        `Table ${table.name} uses reserved column name ${ZERO_VERSION_COLUMN_NAME}`,
      );
    }
    if (table.primaryKey.length === 0) {
      throw new Error(`Table ${table.name} does not have a PRIMARY KEY`);
    }
    schemas.add(table.schema);
    // Add the _0_version column with a default value of "00".
    table.columns[ZERO_VERSION_COLUMN_NAME] = ZERO_VERSION_COLUMN_SPEC;
    return createTableStatement(table);
  });

  const schemaStmts = [...schemas].map(
    schema => `CREATE SCHEMA IF NOT EXISTS ${id(schema)};`,
  );

  // Emulate all of the upstream zero_* PUBLICATIONS to cover all of the
  // replicated tables (simplifying with FOR TABLES IN SCHEMA). This serves two
  // purposes:
  //  1. It serves as a reference for which PUBLICATIONS to subscribe to during
  //     incremental replication.
  //  2. It facilitates replicated table selection logic used to initialize the
  //     incremental replication process, as the destination tables and their
  //     structure must be known.
  //
  // By using PUBLICATIONS for this, the same `getPublicationInfo()` logic can
  // be used on both the upstream and replica.
  const publications = published.publications.map(p => p.pubname);
  const publicationStmts = publications.map(pub =>
    // The publication that we manage, "zero_meta", is used to track all of the
    // replicated schemas. This is the only publication that would need to be
    // altered if, for example, a new schema is encountered from upstream.
    pub === PUB_PREFIX + 'meta'
      ? `CREATE PUBLICATION ${id(pub)} FOR TABLES IN SCHEMA ${idList(schemas)};`
      : // All of the other publications are created simply to indicate that they should be
        // subscribed to on upstream.
        `CREATE PUBLICATION ${id(pub)};`,
  );

  const stmts = [
    ...schemaStmts,
    ...tablesStmts,
    ...publicationStmts,
    `
    CREATE SUBSCRIPTION ${id(subName)}
      CONNECTION '${upstreamUri}'
      PUBLICATION ${idList(publications)}
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
  lc: LogContext,
  _replicaID: string,
  sql: postgres.Sql,
  upstreamUri: string,
  subName = 'zero_sync',
) {
  lc.info?.(`Awaiting initial data synchronization from ${upstreamUri}`);
  for (
    let interval = 100; // Exponential backoff, up to 30 seconds between polls.
    ;
    interval = Math.min(interval * 2, MAX_POLLING_INTERVAL)
  ) {
    const subscribed = await sql<SubscribedTable[]>`
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
    ALTER SUBSCRIPTION ${id(subName)} DISABLE;
    ALTER SUBSCRIPTION ${id(subName)} SET(slot_name=NONE);
    DROP SUBSCRIPTION IF EXISTS ${id(subName)};
  `,
  );
}

// Exported for testing
export async function setupUpstream(
  lc: LogContext,
  upstreamUri: string,
  slotName: string,
): Promise<PublicationInfo> {
  const upstreamDB = postgres(upstreamUri, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    fetch_types: false,
  });
  const [_, published] = await Promise.all([
    // Ensure that the replication slot exists. This must be done in its own
    // transaction, or else Postgres will complain with:
    //
    // PostgresError: cannot create logical replication slot in transaction that has performed writes
    ensureReplicationSlot(lc, upstreamDB, slotName),

    // In parallel, ensure that the schema and publications are setup.
    // Note that both transactions must succeed for the migration to continue.
    ensurePublishedTables(lc, upstreamDB),
  ]);
  return published;
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
): Promise<PublicationInfo> {
  return upstreamDB.begin(async tx => {
    const published = await getPublicationInfo(tx, PUB_PREFIX);
    if ('zero.clients' in published.tables) {
      // upstream is already set up for replication.
      return published;
    }

    // Verify that any manually configured publications export the proper events.
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
      if (pub.pubname === `${PUB_PREFIX}metadata`) {
        throw new Error(
          `PUBLICATION name ${PUB_PREFIX}metadata is reserved for internal use`,
        );
      }
    });

    let dataPublication = '';
    if (published.publications.length === 0) {
      // If there are no custom zero_* publications, set one up to publish all tables.
      dataPublication = `CREATE PUBLICATION ${PUB_PREFIX}data FOR ALL TABLES;`;
    }

    // Send everything as a single batch.
    await tx.unsafe(
      `
    CREATE SCHEMA zero;
    CREATE TABLE zero.clients (
      "clientID" TEXT PRIMARY KEY,
      "lastMutationID" BIGINT
    );
    CREATE PUBLICATION "${PUB_PREFIX}meta" FOR TABLES IN SCHEMA zero;
    ${dataPublication}
    `,
    );

    return getPublicationInfo(tx, PUB_PREFIX);
  });
}
