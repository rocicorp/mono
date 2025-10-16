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

async function seed() {
  const dataDir =
    process.env.ZERO_SEED_DATA_DIR ??
    join(__dirname, '../db/seed-data/github/');

  const forceSeed =
    process.env.ZERO_SEED_FORCE !== undefined &&
    ['t', 'true', '1', ''].indexOf(
      process.env.ZERO_SEED_FORCE.toLocaleLowerCase().trim(),
    ) !== -1;

  // oxlint-disable-next-line no-console
  console.log(process.env.ZERO_UPSTREAM_DB);

  const sql = postgres(process.env.ZERO_UPSTREAM_DB as string);

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

    // Use a single transaction for atomicity
    await sql.begin(async sql => {
      let checkedIfAlreadySeeded = forceSeed;
      await sql`ALTER TABLE issue DISABLE TRIGGER issue_set_last_modified;`;
      await sql`ALTER TABLE issue DISABLE TRIGGER issue_set_created_on_insert_trigger;`;
      await sql`ALTER TABLE comment DISABLE TRIGGER update_issue_modified_time_on_comment;`;
      await sql`ALTER TABLE comment DISABLE TRIGGER comment_set_created_on_insert_trigger;`;
      for (const tableName of TABLES_IN_SEED_ORDER) {
        for (const file of files) {
          if (
            !file.startsWith(`${tableName}.`) &&
            !file.startsWith(`${tableName}_`)
          ) {
            continue;
          }
          const filePath = join(dataDir, file);

          if (!checkedIfAlreadySeeded) {
            const result = await sql`select 1 from ${sql(tableName)} limit 1`;
            if (result.length === 1) {
              // oxlint-disable-next-line no-console
              console.log('Database already seeded.');
              return;
            }
            checkedIfAlreadySeeded = true;
          }

          const headerLine = await readFirstLine(filePath);
          if (!headerLine) {
            // eslint-disable-next-line no-console
            console.warn(`Skipping empty file: ${filePath}`);
            continue;
          }

          let columns = headerLine
            .split(',')
            .map(c => c.trim())
            .map(c => c.replace(/^"|"$/g, ''));
          // oxlint-disable-next-line no-console
          console.log(
            `Seeding table ${tableName} (${columns.join(', ')}) with rows from ${filePath}.`,
          );
          const fileStream = fs.createReadStream(filePath, {
            encoding: 'utf8',
          });
          const query =
            await sql`COPY ${sql(tableName)} (${sql(columns)}) FROM STDIN DELIMITER ',' CSV HEADER`.writable();
          await pipeline(fileStream, query);
        }
      }
      await sql`ALTER TABLE issue ENABLE TRIGGER issue_set_last_modified;`;
      await sql`ALTER TABLE issue ENABLE TRIGGER issue_set_created_on_insert_trigger;`;
      await sql`ALTER TABLE comment ENABLE TRIGGER update_issue_modified_time_on_comment;`;
      await sql`ALTER TABLE comment ENABLE TRIGGER comment_set_created_on_insert_trigger;`;
    });

    // oxlint-disable-next-line no-console
    console.log('✅ Seeding complete.');
    process.exit(0);
  } catch (err) {
    // oxlint-disable-next-line no-console
    console.error('❌ Seeding failed:', err);
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
