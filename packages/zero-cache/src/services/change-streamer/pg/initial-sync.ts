import type {LogContext} from '@rocicorp/logger';
import {ident} from 'pg-format';
import postgres from 'postgres';
import {
  importSnapshot,
  Mode,
  TransactionPool,
} from 'zero-cache/src/db/transaction-pool.js';
import {
  liteValues,
  mapPostgresToLiteDataType,
} from 'zero-cache/src/types/lite.js';
import {liteTableName} from 'zero-cache/src/types/names.js';
import {PostgresDB, postgresTypeConfig} from 'zero-cache/src/types/pg.js';
import type {
  ColumnSpec,
  FilteredTableSpec,
  IndexSpec,
} from 'zero-cache/src/types/specs.js';
import {Database} from 'zqlite/src/db.js';
import {initChangeLog} from '../../replicator/schema/change-log.js';
import {
  initReplicationState,
  ZERO_VERSION_COLUMN_NAME,
} from '../../replicator/schema/replication-state.js';
import {createTableStatement} from './tables/create.js';
import {
  getPublicationInfo,
  PublicationInfo,
  ZERO_PUB_PREFIX,
} from './tables/published.js';

const ZERO_VERSION_COLUMN_SPEC: ColumnSpec = {
  characterMaximumLength: null,
  dataType: 'TEXT',
  notNull: true,
};

export function replicationSlot(replicaID: string): string {
  return `zero_slot_${replicaID}`;
}

const ALLOWED_IDENTIFIER_CHARS = /^[A-Za-z_-]+$/;

export async function initialSync(
  lc: LogContext,
  replicaID: string,
  tx: Database,
  upstreamURI: string,
) {
  const upstreamDB = postgres(upstreamURI, {
    ...postgresTypeConfig(),
    max: MAX_WORKERS,
  });
  const replicationSession = postgres(upstreamURI, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    fetch_types: false, // Necessary for the streaming protocol
    connection: {replication: 'database'}, // https://www.postgresql.org/docs/current/protocol-replication.html
  });
  try {
    await checkUpstreamConfig(upstreamDB);
    const {publications, tables, indices} = await ensurePublishedTables(
      lc,
      upstreamDB,
    );
    const pubNames = publications.map(p => p.pubname);
    lc.info?.(`Upstream is setup with publications [${pubNames}]`);

    createLiteTables(tx, tables);
    createLiteIndices(tx, indices);

    const {database, host} = upstreamDB.options;
    lc.info?.(`opening replication session to ${database}@${host}`);
    const {snapshot_name: snapshot, consistent_point: lsn} =
      await createReplicationSlot(lc, replicaID, replicationSession);

    // Run up to MAX_WORKERS to copy of tables at the replication slot's snapshot.
    const copiers = startTableCopyWorkers(
      lc,
      upstreamDB,
      tables.length,
      snapshot,
    );
    await Promise.all(
      tables.map(table =>
        copiers.processReadTask(db => copy(lc, table, db, tx)),
      ),
    );
    copiers.setDone();

    initReplicationState(tx, pubNames, lsn);
    initChangeLog(tx);
    lc.info?.(`Synced initial data from ${pubNames} up to ${lsn}`);

    await copiers.done();
  } finally {
    // Close the upstream connections.
    await replicationSession.end();
    await upstreamDB.end();
  }
}

async function checkUpstreamConfig(upstreamDB: PostgresDB) {
  // Check upstream wal_level
  const {wal_level: walLevel} = (await upstreamDB`SHOW wal_level`)[0];
  if (walLevel !== 'logical') {
    throw new Error(
      `Postgres must be configured with "wal_level = logical" (currently: "${walLevel})`,
    );
  }
}

function ensurePublishedTables(
  lc: LogContext,
  upstreamDB: postgres.Sql,
  restrictToLiteDataTypes = true, // TODO: Remove this option
): Promise<PublicationInfo> {
  const {database, host} = upstreamDB.options;
  lc.info?.(`Ensuring upstream PUBLICATION on ${database}@${host}`);

  return upstreamDB.begin(async tx => {
    const published = await getPublicationInfo(tx, ZERO_PUB_PREFIX);
    if (
      published.tables.find(
        table => table.schema === 'zero' && table.name === 'clients',
      )
    ) {
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
      if (pub.pubname === `${ZERO_PUB_PREFIX}metadata`) {
        throw new Error(
          `PUBLICATION name ${ZERO_PUB_PREFIX}metadata is reserved for internal use`,
        );
      }
    });

    let dataPublication = '';
    if (published.publications.length === 0) {
      // If there are no custom zero_* publications, set one up to publish all tables.
      dataPublication = `CREATE PUBLICATION ${ZERO_PUB_PREFIX}data FOR TABLES IN SCHEMA zero, public;`;
    }

    // Send everything as a single batch.
    await tx.unsafe(
      `
    CREATE SCHEMA IF NOT EXISTS zero;
    CREATE TABLE zero.clients (
      "clientGroupID"  TEXT   NOT NULL,
      "clientID"       TEXT   NOT NULL,
      "lastMutationID" BIGINT,
      "userID"         TEXT,
      PRIMARY KEY("clientGroupID", "clientID")
    );
    CREATE PUBLICATION "${ZERO_PUB_PREFIX}meta" FOR TABLES IN SCHEMA zero;
    ${dataPublication}
    `,
    );

    const newPublished = await getPublicationInfo(tx, ZERO_PUB_PREFIX);
    newPublished.tables.forEach(table => {
      if (table.schema === '_zero') {
        throw new Error(`Schema "_zero" is reserved for internal use`);
      }
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
        if (restrictToLiteDataTypes) {
          mapPostgresToLiteDataType(spec.dataType); // Throws on unsupported datatypes
        }
      }
    });

    return newPublished;
  });
}

/* eslint-disable @typescript-eslint/naming-convention */
// Row returned by `CREATE_REPLICATION_SLOT`
type ReplicationSlot = {
  slot_name: string;
  consistent_point: string;
  snapshot_name: string;
  output_plugin: string;
};
/* eslint-enable @typescript-eslint/naming-convention */

async function createReplicationSlot(
  lc: LogContext,
  replicaID: string,
  session: postgres.Sql,
): Promise<ReplicationSlot> {
  // Note: The replication connection does not support the extended query protocol,
  //       so all commands must be sent using sql.unsafe(). This is technically safe
  //       because all placeholder values are under our control (i.e. "slotName").
  const slotName = replicationSlot(replicaID);
  const slots = await session.unsafe(
    `SELECT * FROM pg_replication_slots WHERE slot_name = '${slotName}'`,
  );

  // Because a snapshot created by CREATE_REPLICATION_SLOT only lasts for the lifetime
  // of the replication session, if there is an existing slot, it must be deleted so that
  // the slot (and corresponding snapshot) can be created anew.
  //
  // This means that in order for initial data sync to succeed, it must fully complete
  // within the lifetime of a replication session. Note that this is same requirement
  // (and behavior) for Postgres-to-Postgres initial sync:
  // https://github.com/postgres/postgres/blob/5304fec4d8a141abe6f8f6f2a6862822ec1f3598/src/backend/replication/logical/tablesync.c#L1358
  if (slots.length > 0) {
    lc.info?.(`Dropping existing replication slot ${slotName}`);
    await session.unsafe(`DROP_REPLICATION_SLOT ${slotName} WAIT`);
  }
  const slot = (
    await session.unsafe<ReplicationSlot[]>(
      `CREATE_REPLICATION_SLOT ${slotName} LOGICAL pgoutput`,
    )
  )[0];
  lc.info?.(`Created replication slot ${slotName}`, slot);
  return slot;
}

// TODO: Consider parameterizing these.
const MAX_WORKERS = 5;
const BATCH_SIZE = 100_000;

function startTableCopyWorkers(
  lc: LogContext,
  db: PostgresDB,
  numTables: number,
  snapshot: string,
): TransactionPool {
  const {init} = importSnapshot(snapshot);
  const numWorkers = Math.min(numTables, MAX_WORKERS);
  const tableCopiers = new TransactionPool(
    lc,
    Mode.READONLY,
    init,
    undefined,
    numWorkers,
  );
  void tableCopiers.run(db);

  lc.info?.(`Started ${numWorkers} workers to copy ${numTables} tables`);
  return tableCopiers;
}

function createLiteTables(tx: Database, tables: FilteredTableSpec[]) {
  for (const t of tables) {
    const liteTable = {
      ...t,
      schema: '', // SQLite does not support schemas
      name: liteTableName(t),
      columns: {
        ...Object.fromEntries(
          Object.entries(t.columns).map(([col, spec]) => [
            col,
            {
              dataType: mapPostgresToLiteDataType(spec.dataType),
              characterMaximumLength: null,
              // Omit constraints from upstream columns, as they may change without our knowledge.
              // Instead, simply rely on upstream enforcing all column constraints.
              notNull: false,
            },
          ]),
        ),
        [ZERO_VERSION_COLUMN_NAME]: ZERO_VERSION_COLUMN_SPEC,
      },
    };
    tx.exec(createTableStatement(liteTable));
  }
}

function createLiteIndices(tx: Database, indices: IndexSpec[]) {
  for (const index of indices) {
    const tableName = liteTableName({
      schema: index.schemaName,
      name: index.tableName,
    });
    const columns = index.columns.map(c => ident(c)).join(',');
    const unique = index.unique ? 'UNIQUE ' : '';
    tx.exec(
      `CREATE ${unique} INDEX ${ident(index.name)} ON ${ident(
        tableName,
      )} (${columns})`,
    );
  }
}

async function copy(
  lc: LogContext,
  table: FilteredTableSpec,
  from: PostgresDB,
  to: Database,
) {
  let totalRows = 0;
  const tableName = liteTableName(table);
  const selectColumns = Object.keys(table.columns)
    .map(c => ident(c))
    .join(',');
  const insertColumns = [
    ...Object.keys(table.columns),
    ZERO_VERSION_COLUMN_NAME,
  ];
  const insertColumnList = insertColumns.map(c => ident(c)).join(',');
  const insertStmt = to.prepare(
    `INSERT INTO "${tableName}" (${insertColumnList}) VALUES (${new Array(
      insertColumns.length,
    )
      .fill('?')
      .join(',')})`,
  );
  const selectStmt =
    `SELECT ${selectColumns} FROM ${ident(table.schema)}.${ident(table.name)}` +
    (table.filterConditions.length === 0
      ? ''
      : ` WHERE ${table.filterConditions.join(' OR ')}`);

  const cursor = from.unsafe(selectStmt).cursor(BATCH_SIZE);
  for await (const rows of cursor) {
    for (const row of rows) {
      insertStmt.run([
        ...liteValues(row),
        '00', // initial _0_version
      ]);
    }
    totalRows += rows.length;
    lc.debug?.(`Copied ${totalRows} rows from ${table.schema}.${table.name}`);
  }
  lc.info?.(`Finished copying ${totalRows} rows into ${tableName}`);
}
