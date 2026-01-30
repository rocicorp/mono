import '../../../packages/shared/src/dotenv.ts';

import * as fs from 'fs';
import * as readline from 'readline';
import {dirname, join} from 'path';
import postgres from 'postgres';
import {pipeline} from 'stream/promises';
import {fileURLToPath} from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const TABLES_IN_SEED_ORDER = [
  'user',
  'project',
  'issue',
  'comment',
  'label',
  'issueLabel',
] as const;
const TABLE_CSV_FILE_REGEX = `^(${TABLES_IN_SEED_ORDER.join('|')})(_.*)?.csv$`;

// Types for dynamically discovered schema objects
interface IndexInfo {
  indexName: string;
  indexDdl: string;
}

interface ForeignKeyInfo {
  tableName: string;
  constraintName: string;
  fkDefinition: string;
}

interface TriggerInfo {
  tableName: string;
  triggerName: string;
}

/**
 * Discover all non-primary-key indexes for the given tables.
 */
async function discoverIndexes(
  sql: postgres.Sql,
  tables: readonly string[],
): Promise<IndexInfo[]> {
  const results: IndexInfo[] = [];
  for (const table of tables) {
    const rows = await sql<{indexname: string; index_ddl: string}[]>`
      SELECT
        pg_indexes.indexname,
        pg_get_indexdef(pg_class.oid) as index_ddl
      FROM pg_indexes
      JOIN pg_namespace ON pg_indexes.schemaname = pg_namespace.nspname
      JOIN pg_class ON pg_class.relname = pg_indexes.indexname
        AND pg_class.relnamespace = pg_namespace.oid
      JOIN pg_index ON pg_index.indexrelid = pg_class.oid
      WHERE pg_indexes.tablename = ${table}
        AND pg_indexes.schemaname = 'public'
        AND pg_index.indisprimary = FALSE
    `;
    for (const row of rows) {
      results.push({
        indexName: row.indexname,
        indexDdl: row.index_ddl,
      });
    }
  }
  return results;
}

/**
 * Discover all foreign key constraints in the public schema.
 */
async function discoverForeignKeys(
  sql: postgres.Sql,
): Promise<ForeignKeyInfo[]> {
  const rows = await sql<
    {constraint_name: string; table_name: string; fk_definition: string}[]
  >`
    SELECT
      c.conname as constraint_name,
      table_class.relname as table_name,
      pg_get_constraintdef(c.oid) as fk_definition
    FROM pg_constraint c
    JOIN pg_class table_class ON c.conrelid = table_class.oid
    JOIN pg_namespace table_ns ON table_class.relnamespace = table_ns.oid
    WHERE c.contype = 'f'
      AND table_ns.nspname = 'public'
  `;
  return rows.map(row => ({
    tableName: row.table_name,
    constraintName: row.constraint_name,
    fkDefinition: row.fk_definition,
  }));
}

/**
 * Discover all user-defined triggers for the given tables.
 */
async function discoverTriggers(
  sql: postgres.Sql,
  tables: readonly string[],
): Promise<TriggerInfo[]> {
  const results: TriggerInfo[] = [];
  for (const table of tables) {
    const rows = await sql<{trigger_name: string; table_name: string}[]>`
      SELECT
        tgname as trigger_name,
        relname as table_name
      FROM pg_trigger
      JOIN pg_class ON pg_trigger.tgrelid = pg_class.oid
      JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
      WHERE NOT tgisinternal
        AND pg_namespace.nspname = 'public'
        AND relname = ${table}
    `;
    for (const row of rows) {
      results.push({
        tableName: row.table_name,
        triggerName: row.trigger_name,
      });
    }
  }
  return results;
}

async function seed() {
  const dataDir =
    process.env.ZERO_SEED_DATA_DIR ??
    join(__dirname, '../db/seed-data/github/');

  const forceSeed =
    process.env.ZERO_SEED_FORCE !== undefined &&
    ['t', 'true', '1', ''].indexOf(
      process.env.ZERO_SEED_FORCE.toLocaleLowerCase().trim(),
    ) !== -1;

  const appendMode =
    process.env.ZERO_SEED_APPEND !== undefined &&
    ['t', 'true', '1', ''].indexOf(
      process.env.ZERO_SEED_APPEND.toLocaleLowerCase().trim(),
    ) !== -1;

  // oxlint-disable-next-line no-console
  console.log(process.env.ZERO_UPSTREAM_DB);

  const sql = postgres(process.env.ZERO_UPSTREAM_DB as string, {
    // Increase timeouts since operations can run long for large datasets
    idle_timeout: 0,
    connect_timeout: 60,
    max_lifetime: null,
  });

  try {
    const files = fs
      .readdirSync(dataDir)
      .filter(f => f.match(TABLE_CSV_FILE_REGEX))
      // apply in sorted order
      .sort();

    if (files.length === 0) {
      // oxlint-disable-next-line no-console
      console.log(
        `No ${TABLE_CSV_FILE_REGEX} files found to seed in ${dataDir}.`,
      );
      process.exit(0);
    }

    // Check if already seeded (skip in append mode)
    if (!forceSeed && !appendMode) {
      const result = await sql`select 1 from "user" limit 1`;
      if (result.length === 1) {
        // oxlint-disable-next-line no-console
        console.log('Database already seeded.');
        process.exit(0);
      }
    }

    // If forcing (not appending), truncate existing data
    if (forceSeed && !appendMode) {
      // oxlint-disable-next-line no-console
      console.log('Force mode: truncating existing data...');
      // Truncate in reverse order to respect foreign key dependencies
      for (const tableName of [...TABLES_IN_SEED_ORDER].reverse()) {
        await sql`TRUNCATE ${sql(tableName)} CASCADE`;
      }
    }

    if (appendMode) {
      // oxlint-disable-next-line no-console
      console.log('Append mode: adding data to existing tables...');
    }

    // Discover all tables in public schema for comprehensive discovery
    const allTablesResult = await sql<{tablename: string}[]>`
      SELECT tablename FROM pg_tables WHERE schemaname = 'public'
    `;
    const allTables = allTablesResult.map(r => r.tablename);

    // Discover schema objects
    // oxlint-disable-next-line no-console
    console.log('Discovering schema objects...');
    const [indexes, foreignKeys, triggers] = await Promise.all([
      discoverIndexes(sql, allTables),
      discoverForeignKeys(sql),
      discoverTriggers(sql, allTables),
    ]);
    // oxlint-disable-next-line no-console
    console.log(
      `  Found ${indexes.length} indexes, ${foreignKeys.length} FKs, ${triggers.length} triggers`,
    );

    // Set memory parameters for faster index rebuilds
    // oxlint-disable-next-line no-console
    console.log('Setting memory parameters...');
    await sql`SET maintenance_work_mem = '2GB'`;
    await sql`SET work_mem = '256MB'`;

    // Disable all triggers
    // oxlint-disable-next-line no-console
    console.log('Disabling triggers...');
    for (const {tableName, triggerName} of triggers) {
      try {
        await sql`ALTER TABLE ${sql(tableName)} DISABLE TRIGGER ${sql.unsafe('"' + triggerName + '"')}`;
      } catch (e) {
        // oxlint-disable-next-line no-console
        console.log(
          `  Warning: could not disable trigger ${triggerName} on ${tableName}: ${e}`,
        );
      }
    }

    // Drop foreign key constraints
    // oxlint-disable-next-line no-console
    console.log('Dropping foreign key constraints...');
    for (const {tableName, constraintName} of foreignKeys) {
      try {
        await sql.unsafe(
          `ALTER TABLE "${tableName}" DROP CONSTRAINT IF EXISTS "${constraintName}"`,
        );
      } catch (e) {
        // oxlint-disable-next-line no-console
        console.log(`  Warning: could not drop FK ${constraintName}: ${e}`);
      }
    }

    // Drop non-PK indexes
    // oxlint-disable-next-line no-console
    console.log('Dropping indexes...');
    for (const {indexName} of indexes) {
      try {
        await sql.unsafe(`DROP INDEX IF EXISTS "${indexName}"`);
      } catch (e) {
        // oxlint-disable-next-line no-console
        console.log(`  Warning: could not drop index ${indexName}: ${e}`);
      }
    }

    // COPY data (no transaction - each file is its own implicit transaction)
    // oxlint-disable-next-line no-console
    console.log('Loading data via COPY...');
    for (const tableName of TABLES_IN_SEED_ORDER) {
      const tableFiles = files.filter(
        f => f.startsWith(`${tableName}.`) || f.startsWith(`${tableName}_`),
      );
      if (tableFiles.length === 0) continue;

      // oxlint-disable-next-line no-console
      console.log(`  Loading ${tableName} (${tableFiles.length} files)...`);
      let tableRowCount = 0;

      for (const file of tableFiles) {
        const filePath = join(dataDir, file);

        const headerLine = await readFirstLine(filePath);
        if (!headerLine) {
          // oxlint-disable-next-line no-console
          console.warn(`  Skipping empty file: ${filePath}`);
          continue;
        }

        const columns = headerLine
          .split(',')
          .map(c => c.trim())
          .map(c => c.replace(/^"|"$/g, ''));

        const fileStream = fs.createReadStream(filePath, {encoding: 'utf8'});
        const query =
          await sql`COPY ${sql(tableName)} (${sql(columns)}) FROM STDIN DELIMITER ',' CSV HEADER`.writable();
        await pipeline(fileStream, query);
        tableRowCount++;
      }

      // oxlint-disable-next-line no-console
      console.log(`  ${tableName}: loaded ${tableRowCount} files`);
    }

    // Recreate indexes
    // oxlint-disable-next-line no-console
    console.log('Recreating indexes (this may take a while)...');
    for (const {indexName, indexDdl} of indexes) {
      // oxlint-disable-next-line no-console
      console.log(`  Creating index ${indexName}...`);
      try {
        await sql.unsafe(indexDdl);
      } catch (e) {
        // oxlint-disable-next-line no-console
        console.log(`  Warning: could not create index ${indexName}: ${e}`);
      }
    }

    // Recreate foreign key constraints
    // oxlint-disable-next-line no-console
    console.log('Recreating foreign key constraints...');
    for (const {tableName, constraintName, fkDefinition} of foreignKeys) {
      // oxlint-disable-next-line no-console
      console.log(`  Creating FK ${constraintName}...`);
      try {
        await sql.unsafe(
          `ALTER TABLE "${tableName}" ADD CONSTRAINT "${constraintName}" ${fkDefinition}`,
        );
      } catch (e) {
        // oxlint-disable-next-line no-console
        console.log(`  Warning: could not create FK ${constraintName}: ${e}`);
      }
    }

    // Re-enable triggers
    // oxlint-disable-next-line no-console
    console.log('Re-enabling triggers...');
    for (const {tableName, triggerName} of triggers) {
      try {
        await sql`ALTER TABLE ${sql(tableName)} ENABLE TRIGGER ${sql.unsafe('"' + triggerName + '"')}`;
      } catch (e) {
        // oxlint-disable-next-line no-console
        console.log(
          `  Warning: could not enable trigger ${triggerName} on ${tableName}: ${e}`,
        );
      }
    }

    // Analyze tables for query planner
    // oxlint-disable-next-line no-console
    console.log('Running ANALYZE...');
    for (const tableName of TABLES_IN_SEED_ORDER) {
      await sql`ANALYZE ${sql(tableName)}`;
    }

    // oxlint-disable-next-line no-console
    console.log('Seeding complete.');
    process.exit(0);
  } catch (err) {
    // oxlint-disable-next-line no-console
    console.error('Seeding failed:', err);
    process.exit(1);
  }
}

async function readFirstLine(filePath: string): Promise<string | null> {
  const readStream = fs.createReadStream(filePath, {encoding: 'utf8'});
  const rl = readline.createInterface({input: readStream, crlfDelay: Infinity});

  for await (const line of rl) {
    rl.close(); // Close the reader as soon as we have the first line
    readStream.destroy(); // Manually destroy the stream to free up resources
    return line;
  }

  return null; // Return null if the file is empty
}

await seed();
