import {fileURLToPath} from 'url';
import {dirname, join} from 'path';
import * as fs from 'fs';
import postgres from 'postgres';
import {pipeline} from 'stream/promises';
import '../../../packages/shared/src/dotenv.ts';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const CSV_FILE_PATTERN = /^\d+_(?<tableName>.*)\.csv$/;

async function seed() {
  const dataDir =
    process.env.ZERO_SEED_DATA_DIR ??
    join(__dirname, '../db/seed-data/github/');

  console.log(process.env.ZERO_UPSTREAM_DB);

  const sql = postgres(process.env.ZERO_UPSTREAM_DB as string);

  try {
    const files = fs
      .readdirSync(dataDir)
      .filter(f => f.match(CSV_FILE_PATTERN))
      // apply in sorted order
      .sort();

    if (files.length === 0) {
      console.log(`No ${CSV_FILE_PATTERN} files found to seed.`);
      process.exit(0);
    }

    // Use a single transaction for atomicity
    await sql.begin(async sql => {
      let checkedIfAlreadySeeded = false;
      for (const file of files) {
        const filePath = join(dataDir, file);

        const match = CSV_FILE_PATTERN.exec(file);
        if (!match?.groups) {
          continue;
        }
        const tableName = match.groups.tableName;

        if (!checkedIfAlreadySeeded) {
          const result = await sql`select 1 from ${sql(tableName)} limit 1`;
          if (result.length === 1) {
            console.log('Database already seeded.');
            return;
          }
          checkedIfAlreadySeeded = true;
        }
        console.log(`Seeding table ${tableName} with rows from ${filePath}.`);
        const fileStream = fs.createReadStream(filePath, {
          encoding: 'utf8',
        });
        const query =
          await sql`COPY ${sql(tableName)} FROM STDIN DELIMITER ',' CSV HEADER`.writable();
        await pipeline(fileStream, query);
      }
    });

    console.log('✅ Seeding complete.');
    process.exit(0);
  } catch (err) {
    console.error('❌ Seeding failed:', err);
    process.exit(1);
  }
}

await seed();
