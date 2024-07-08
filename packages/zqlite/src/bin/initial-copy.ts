import postgres from 'postgres';
import Database from 'better-sqlite3';
import {argv} from 'process';

const BATCH_SIZE = 100_000;

function mapDataType(pgDataType: string): string {
  switch (pgDataType) {
    case 'smallint':
    case 'integer':
    case 'bigint':
    case 'smallserial':
    case 'serial':
    case 'bigserial':
    case 'boolean':
      return 'INTEGER';
    case 'decimal':
    case 'numeric':
    case 'real':
    case 'double precision':
      return 'REAL';
    case 'bytea':
      return 'BLOB';
    case 'character':
    case 'character varying':
    case 'text':
      return 'TEXT';
    case 'date':
    case 'time':
    case 'timestamp':
    case 'timestamp with time zone':
    case 'timestamp without time zone':
    case 'time with time zone':
    case 'time without time zone':
      return 'INTEGER';
    default:
      return 'ANY';
  }
}

async function getTablePrimaryKeys(
  sql: postgres.Sql<Record<string, unknown>>,
  tableName: string,
): Promise<string[]> {
  const primaryKeys = await sql`
      SELECT column_name
      FROM information_schema.key_column_usage
      WHERE constraint_name = (
          SELECT constraint_name
          FROM information_schema.table_constraints
          WHERE table_name = ${tableName} AND constraint_type = 'PRIMARY KEY'
      )
      ORDER BY ordinal_position ASC
  `;
  return primaryKeys.map(key => key.column_name);
}

async function getPostgresTables(
  sql: postgres.Sql<Record<string, unknown>>,
): Promise<string[]> {
  const tables = await sql`
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
    `;
  return tables.map(table => table.table_name);
}

async function copySchemaToSQLite(
  sql: postgres.Sql<Record<string, unknown>>,
  sqliteDb: Database.Database,
) {
  const tables = await getPostgresTables(sql);

  for (const table of tables) {
    const [columns, primaryKeys] = await Promise.all([
      sql`
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = ${table}
            ORDER BY ordinal_position ASC
        `,
      getTablePrimaryKeys(sql, table),
    ]);

    let createTableQuery = `CREATE TABLE ${table} (`;
    createTableQuery += columns
      .map(col => `${col.column_name} ${mapDataType(col.data_type)}`)
      .join(', ');
    createTableQuery += ", _0_version TEXT DEFAULT '00'";
    if (primaryKeys.length > 0) {
      createTableQuery += `, PRIMARY KEY (${primaryKeys
        .map(pk => `"${pk}"`)
        .join(', ')})`;
    }
    createTableQuery += ')';

    sqliteDb.exec(createTableQuery);
  }
}

async function copyDataToSQLite(
  sql: postgres.Sql<Record<string, unknown>>,
  sqliteDb: Database.Database,
) {
  const tables = await getPostgresTables(sql);

  for (const table of tables) {
    console.log('COPYING: ', table);
    const columnNames = await sql`
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = ${table}
        `;
    const columnsForInsert = columnNames
      .map(col => `"${col.column_name}"`)
      .join(', ');
    const columnsForSelect = columnNames
      .map(col => {
        if (
          col.data_type.startsWith('time') ||
          col.data_type.startsWith('date')
        ) {
          return `EXTRACT(EPOCH FROM "${col.column_name}") AS "${col.column_name}"`;
        }
        return `"${col.column_name}"`;
      })
      .join(', ');

    const sqliteStmt = sqliteDb.prepare(
      `INSERT INTO "${table}" (${columnsForInsert}) VALUES (${new Array(
        columnNames.length,
      )
        .fill('?')
        .join(',')})`,
    );

    const tx = sqliteDb.transaction((rows: postgres.Row[]) => {
      for (const row of rows) {
        sqliteStmt.run(Object.values(row));
      }
    });
    const cursor = sql`SELECT ${sql.unsafe(columnsForSelect)} FROM ${sql(
      table,
    )}`.cursor(BATCH_SIZE);
    for await (const rows of cursor) {
      tx(rows);
    }
  }
}

if (argv.length !== 4) {
  // TODO (mlaw):
  // Add an option to also create the replication slot and
  // output the WAL LSN to use for replication.
  console.error(
    'Usage: ts-node script.ts <postgres_connection_string> <sqlite_db_path>',
  );
  process.exit(1);
}

const postgresConnString = argv[2];
const sqliteDbPath = argv[3];

const sql = postgres(postgresConnString);
const sqliteDb = new Database(sqliteDbPath);
sqliteDb.pragma('foreign_keys = OFF');
sqliteDb.pragma('journal_mode = WAL');
// For initial import we'll wait for the disk flush on close.
sqliteDb.pragma('synchronous = OFF');

await copySchemaToSQLite(sql, sqliteDb);
console.log('Schema copied');

await copyDataToSQLite(sql, sqliteDb);

await sql.end();

console.log(
  'Data migration completed successfully. VACUUMING and ANALYZING DB',
);
sqliteDb.pragma('synchronous = NORMAL');
sqliteDb.exec('VACUUM');
console.log('VACUUM completed');
sqliteDb.exec('ANALYZE main');
console.log('ANALYZE completed');
sqliteDb.close();
