import type {LogContext} from '@rocicorp/logger';
import {Database} from 'better-sqlite3';
import {ident} from 'pg-format';
import postgres from 'postgres';
import {
  importSnapshot,
  Mode,
  TransactionPool,
} from 'zero-cache/src/db/transaction-pool.js';
import {PostgresDB} from 'zero-cache/src/types/pg.js';
import {
  initReplicationState,
  ZERO_VERSION_COLUMN_NAME,
} from './schema/replication.js';
import {createTableStatement} from './tables/create.js';
import {liteTableName} from './tables/names.js';
import {
  getPublicationInfo,
  PublicationInfo,
  ZERO_PUB_PREFIX,
} from './tables/published.js';
import type {ColumnSpec, FilteredTableSpec} from './tables/specs.js';

const ZERO_VERSION_COLUMN_SPEC: ColumnSpec = {
  characterMaximumLength: null,
  dataType: 'TEXT',
  notNull: true,
};

export function replicationSlot(replicaID: string): string {
  return `zero_slot_${replicaID}`;
}

const ALLOWED_IDENTIFIER_CHARS = /^[A-Za-z_-]+$/;

// Exported for testing
// TODO: Delete
export async function setupUpstream(
  lc: LogContext,
  upstreamDB: postgres.Sql,
  slotName: string,
): Promise<PublicationInfo> {
  const [_, published] = await Promise.all([
    // Ensure that the replication slot exists. This must be done in its own
    // transaction, or else Postgres will complain with:
    //
    // PostgresError: cannot create logical replication slot in transaction that has performed writes
    ensureReplicationSlot(lc, upstreamDB, slotName),

    // In parallel, ensure that the schema and publications are setup.
    // Note that both transactions must succeed for the migration to continue.
    ensurePublishedTables(lc, upstreamDB, false),
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
// TODO: Delete
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

/* eslint-disable @typescript-eslint/naming-convention */
// Row returned by `CREATE_REPLICATION_SLOT`
type ReplicationSlot = {
  slot_name: string;
  consistent_point: string;
  snapshot_name: string;
  output_plugin: string;
};
/* eslint-enable @typescript-eslint/naming-convention */

export async function initialSync(
  lc: LogContext,
  replicaID: string,
  tx: Database,
  upstreamDB: PostgresDB,
  upstreamURI: string,
) {
  await checkUpstreamConfig(upstreamDB);
  const {publications, tables} = await ensurePublishedTables(lc, upstreamDB);
  const pubNames = publications.map(p => p.pubname);
  lc.info?.(`Upstream is setup with publications [${pubNames}]`);

  createLiteTables(tx, tables);

  const {database, host} = upstreamDB.options;
  lc.info?.(`opening replication session to ${database}@${host}`);
  const repl = postgres(upstreamURI, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    fetch_types: false, // Necessary for the streaming protocol
    connection: {replication: 'database'}, // https://www.postgresql.org/docs/current/protocol-replication.html
  });
  try {
    // Note: The replication connection does not support the extended query protocol,
    //       so all commands must be sent using sql.unsafe(). This is technically safe
    //       because all placeholder values are under our control (i.e. "slotName").
    const slotName = replicationSlot(replicaID);
    const slots = await repl.unsafe(
      `SELECT * FROM pg_replication_slots WHERE slot_name = '${slotName}'`,
    );

    // Because a snapshot created by CREATE_REPLICATION_SLOT only lasts for the lifetime
    // of the replication session, if there is an existing slot, it must be deleted so that
    // the slot (and corresponding snapshot) can be created anew.
    //
    // This means that in order for initial data sync to succeed, it must fully complete
    // within the lifetime of a replication session.
    if (slots.length > 0) {
      lc.info?.(`Dropping existing replication slot ${slotName}`);
      await repl.unsafe(`DROP_REPLICATION_SLOT ${slotName}`);
    }
    const slot = (
      await repl.unsafe<ReplicationSlot[]>(
        `CREATE_REPLICATION_SLOT ${slotName} LOGICAL pgoutput`,
      )
    )[0];
    lc.info?.(`Created replication slot ${slotName}`, slot);
    const {snapshot_name: snapshot, consistent_point: lsn} = slot;

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
    lc.info?.(`Synced initial data from ${pubNames} up to ${lsn}`);

    await copiers.done();
  } finally {
    await repl.end(); // Close the replication session.
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
          mapToLiteDataType(spec.dataType); // Throws on unsupported datatypes
        }
      }
    });

    return newPublished;
  });
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

function mapToLiteDataType(pgDataType: string): string {
  switch (pgDataType) {
    case 'smallint':
    case 'integer':
    case 'int':
    case 'int2':
    case 'int4':
    case 'int8':
    case 'bigint':
    case 'smallserial':
    case 'serial':
    case 'serial2':
    case 'serial4':
    case 'serial8':
    case 'bigserial':
    case 'boolean':
      return 'INTEGER';
    case 'decimal':
    case 'numeric':
    case 'real':
    case 'double precision':
    case 'float':
    case 'float4':
    case 'float8':
      return 'REAL';
    case 'bytea':
      return 'BLOB';
    case 'character':
    case 'character varying':
    case 'text':
      return 'TEXT';
    // case 'date':
    // case 'time':
    // case 'timestamp':
    // case 'timestamp with time zone':
    // case 'timestamp without time zone':
    // case 'time with time zone':
    // case 'time without time zone':
    //   return 'INTEGER';
    default:
      if (pgDataType.endsWith('[]')) {
        throw new Error(`Array types are not supported: ${pgDataType}`);
      }
      throw new Error(`The "${pgDataType}" data type is not supported`);
  }
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
              dataType: mapToLiteDataType(spec.dataType),
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
        ...Object.values(row),
        '00', // initial _0_version
      ]);
    }
    totalRows += rows.length;
    lc.debug?.(`Copied ${totalRows} rows from ${table.schema}.${table.name}`);
  }
  lc.info?.(`Finished copying ${totalRows} rows into ${tableName}`);
}
