import {fileURLToPath} from 'url';
import {dirname, join} from 'path';
import * as fs from 'fs';
import {db} from '../db/db.ts';
import {sql} from 'drizzle-orm';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

async function seed() {
  const dataDir = join(__dirname, '../db/seed-data/github/');

  try {
    if (
      (await (
        await db.execute(sql.raw('select 1 from issue limit 1'))
      ).rowCount) === 1
    ) {
      console.log('Database already seeded.');
    } else {
      const files = fs
        .readdirSync(dataDir)
        .filter(file => file.endsWith('.sql'))
        // apply in sorted order
        .sort();

      if (files.length === 0) {
        console.log('No *.sql files found to seed.');
        process.exit(0);
      }

      // Use a single transaction for atomicity
      await db.transaction(async tx => {
        for (const file of files) {
          const filePath = join(dataDir, file);
          const sqlContent = fs.readFileSync(filePath, 'utf-8');
          await tx.execute(sql.raw(sqlContent));
        }
      });

      console.log('✅ Seeding complete.');
    }
    process.exit(0);
  } catch (err) {
    console.error('❌ Seeding failed:', err);
    process.exit(1);
  }
}

await seed();
