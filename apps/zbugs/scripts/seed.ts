import '../../../packages/shared/src/dotenv.ts';

import * as fs from 'fs';
import {dirname, join} from 'path';
import postgres from 'postgres';
import * as readline from 'readline';
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
  tableName: string;
  indexName: string;
  indexDdl: string;
  constraintName: string | null;
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

// --- Progress tracking helpers ---

async function ensureProgressTable(sql: postgres.Sql): Promise<void> {
  await sql`
    CREATE TABLE IF NOT EXISTS _seed_progress (
      key TEXT PRIMARY KEY,
      value TEXT,
      created_at TIMESTAMPTZ DEFAULT now()
    )
  `;
}

async function hasProgress(sql: postgres.Sql, key: string): Promise<boolean> {
  const rows = await sql`
    SELECT 1 FROM _seed_progress WHERE key = ${key}
  `;
  return rows.length > 0;
}

async function getProgress(
  sql: postgres.Sql,
  key: string,
): Promise<string | undefined> {
  const rows = await sql<{value: string}[]>`
    SELECT value FROM _seed_progress WHERE key = ${key}
  `;
  return rows.length > 0 ? rows[0].value : undefined;
}

async function setProgress(
  sql: postgres.Sql,
  key: string,
  value: string,
): Promise<void> {
  await sql`
    INSERT INTO _seed_progress (key, value)
    VALUES (${key}, ${value})
    ON CONFLICT (key) DO UPDATE SET value = ${value}
  `;
}

async function clearProgress(sql: postgres.Sql): Promise<void> {
  await sql`DELETE FROM _seed_progress`;
}

async function dropProgressTable(sql: postgres.Sql): Promise<void> {
  await sql`DROP TABLE IF EXISTS _seed_progress`;
}

async function hasAnyProgress(sql: postgres.Sql): Promise<boolean> {
  const rows = await sql`SELECT 1 FROM _seed_progress LIMIT 1`;
  return rows.length > 0;
}

// --- Schema discovery ---

/**
 * Discover all non-primary-key indexes for the given tables.
 */
async function discoverIndexes(
  sql: postgres.Sql,
  tables: readonly string[],
): Promise<IndexInfo[]> {
  const results: IndexInfo[] = [];
  for (const table of tables) {
    const rows = await sql<
      {
        indexname: string;
        index_ddl: string;
        constraint_name: string | null;
      }[]
    >`
      SELECT
        pg_indexes.indexname,
        pg_get_indexdef(pg_class.oid) as index_ddl,
        pg_constraint.conname as constraint_name
      FROM pg_indexes
      JOIN pg_namespace ON pg_indexes.schemaname = pg_namespace.nspname
      JOIN pg_class ON pg_class.relname = pg_indexes.indexname
        AND pg_class.relnamespace = pg_namespace.oid
      JOIN pg_index ON pg_index.indexrelid = pg_class.oid
      LEFT JOIN pg_constraint ON pg_constraint.conindid = pg_class.oid
        AND pg_constraint.contype IN ('u', 'p')
      WHERE pg_indexes.tablename = ${table}
        AND pg_indexes.schemaname = 'public'
        AND pg_index.indisprimary = FALSE
    `;
    for (const row of rows) {
      results.push({
        tableName: table,
        indexName: row.indexname,
        indexDdl: row.index_ddl,
        constraintName: row.constraint_name,
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

function parseBoolEnv(value: string | undefined): boolean {
  return (
    value !== undefined &&
    ['t', 'true', '1', ''].indexOf(value.toLocaleLowerCase().trim()) !== -1
  );
}

async function seed() {
  const dataDir =
    process.env.ZERO_SEED_DATA_DIR ??
    join(__dirname, '../db/seed-data/github/');

  const forceSeed = parseBoolEnv(process.env.ZERO_SEED_FORCE);
  const resetSeed = parseBoolEnv(process.env.ZERO_SEED_RESET);

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

    // Ensure progress table exists
    await ensureProgressTable(sql);

    // Handle ZERO_SEED_RESET: clear progress and start fresh
    if (resetSeed) {
      // oxlint-disable-next-line no-console
      console.log('Reset mode: clearing progress and starting fresh...');
      await clearProgress(sql);
    }

    // Detect mode: resume vs fresh
    const resuming = await hasAnyProgress(sql);
    if (resuming) {
      // oxlint-disable-next-line no-console
      console.log('Resuming previous seed run...');
    }

    // Check if already seeded (only for fresh, non-force, non-append runs)
    if (!resuming && !forceSeed && !appendMode) {
      const result = await sql`select 1 from "user" limit 1`;
      if (result.length === 1) {
        // oxlint-disable-next-line no-console
        console.log('Database already seeded.');
        await dropProgressTable(sql);
        process.exit(0);
      }
    }

    // --- FRESH: truncate and discover schema ---
    if (!resuming) {
      // If forcing (not appending), truncate existing data
      if (forceSeed && !appendMode) {
        // oxlint-disable-next-line no-console
        console.log('Force mode: truncating existing data...');
        for (const tableName of [...TABLES_IN_SEED_ORDER].reverse()) {
          const exists = await sql`
            SELECT 1 FROM pg_tables
            WHERE schemaname = 'public' AND tablename = ${tableName}
          `;
          if (exists.length > 0) {
            await sql`TRUNCATE ${sql(tableName)} CASCADE`;
          }
        }
      }
      await setProgress(sql, 'phase:truncated', 'done');

      // Discover all tables in public schema for comprehensive discovery
      const allTablesResult = await sql<{tablename: string}[]>`
        SELECT tablename FROM pg_tables WHERE schemaname = 'public'
      `;
      const allTables = allTablesResult
        .map(r => r.tablename)
        .filter(t => t !== '_seed_progress');

      // Discover schema objects and persist to progress table
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

      await setProgress(sql, 'schema:indexes', JSON.stringify(indexes));
      await setProgress(sql, 'schema:foreignKeys', JSON.stringify(foreignKeys));
      await setProgress(sql, 'schema:triggers', JSON.stringify(triggers));
    }

    if (appendMode) {
      // oxlint-disable-next-line no-console
      console.log('Append mode: adding data to existing tables...');
    }

    // --- Load schema objects (from progress table for both fresh and resume) ---
    const indexes: IndexInfo[] = JSON.parse(
      (await getProgress(sql, 'schema:indexes')) ?? '[]',
    );
    const foreignKeys: ForeignKeyInfo[] = JSON.parse(
      (await getProgress(sql, 'schema:foreignKeys')) ?? '[]',
    );
    const triggers: TriggerInfo[] = JSON.parse(
      (await getProgress(sql, 'schema:triggers')) ?? '[]',
    );

    if (resuming) {
      // oxlint-disable-next-line no-console
      console.log(
        `  Loaded ${indexes.length} indexes, ${foreignKeys.length} FKs, ${triggers.length} triggers from progress`,
      );
    }

    // --- Drop schema objects (skip if already done) ---
    if (!(await hasProgress(sql, 'phase:schema_dropped'))) {
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

      // Drop non-PK indexes (constraint-backed indexes must be dropped via ALTER TABLE)
      // oxlint-disable-next-line no-console
      console.log('Dropping indexes...');
      for (const {tableName, indexName, constraintName} of indexes) {
        try {
          if (constraintName) {
            await sql.unsafe(
              `ALTER TABLE "${tableName}" DROP CONSTRAINT IF EXISTS "${constraintName}"`,
            );
          } else {
            await sql.unsafe(`DROP INDEX IF EXISTS "${indexName}"`);
          }
        } catch (e) {
          // oxlint-disable-next-line no-console
          console.log(`  Warning: could not drop index ${indexName}: ${e}`);
        }
      }

      await setProgress(sql, 'phase:schema_dropped', 'done');
    } else {
      // oxlint-disable-next-line no-console
      console.log('Schema already dropped, skipping...');
    }

    // Set memory parameters for faster index rebuilds (session-level, always set)
    // oxlint-disable-next-line no-console
    console.log('Setting memory parameters...');
    await sql`SET maintenance_work_mem = '2GB'`;
    await sql`SET work_mem = '256MB'`;

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
        const progressKey = `file:${file}`;
        if (await hasProgress(sql, progressKey)) {
          // oxlint-disable-next-line no-console
          console.log(`  Skipping ${file} (already loaded)`);
          tableRowCount++;
          continue;
        }

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
        await setProgress(sql, progressKey, 'done');
        tableRowCount++;
      }

      // oxlint-disable-next-line no-console
      console.log(`  ${tableName}: loaded ${tableRowCount} files`);
    }

    // --- Restore schema objects (skip if already done) ---
    if (!(await hasProgress(sql, 'phase:schema_restored'))) {
      // Recreate indexes (constraint-backed indexes are recreated via ALTER TABLE)
      // oxlint-disable-next-line no-console
      console.log('Recreating indexes (this may take a while)...');
      for (const {tableName, indexName, indexDdl, constraintName} of indexes) {
        // oxlint-disable-next-line no-console
        console.log(`  Creating index ${indexName}...`);
        try {
          if (constraintName) {
            // The DDL from pg_get_indexdef is a CREATE UNIQUE INDEX statement.
            // First create the index, then add it as a constraint.
            await sql.unsafe(indexDdl);
            await sql.unsafe(
              `ALTER TABLE "${tableName}" ADD CONSTRAINT "${constraintName}" UNIQUE USING INDEX "${indexName}"`,
            );
          } else {
            await sql.unsafe(indexDdl);
          }
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

      await setProgress(sql, 'phase:schema_restored', 'done');
    } else {
      // oxlint-disable-next-line no-console
      console.log('Schema already restored, skipping...');
    }

    // Analyze tables for query planner (skip if already done)
    if (!(await hasProgress(sql, 'phase:analyzed'))) {
      // oxlint-disable-next-line no-console
      console.log('Running ANALYZE...');
      for (const tableName of TABLES_IN_SEED_ORDER) {
        await sql`ANALYZE ${sql(tableName)}`;
      }
      await setProgress(sql, 'phase:analyzed', 'done');
    } else {
      // oxlint-disable-next-line no-console
      console.log('ANALYZE already done, skipping...');
    }

    // Clean exit: drop progress table
    await dropProgressTable(sql);

    // oxlint-disable-next-line no-console
    console.log('Seeding complete.');
    process.exit(0);
  } catch (err) {
    // oxlint-disable-next-line no-console
    console.error('Seeding failed:', err);
    // oxlint-disable-next-line no-console
    console.error('Re-run the script to resume from where it left off.');
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
