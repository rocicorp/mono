// https://supabase.com/docs/guides/cli/seeding-your-database
import {promises as fs} from 'node:fs';
import path from 'node:path';
import {dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {parse} from 'csv-parse';

const __dirname = dirname(fileURLToPath(import.meta.url));

const tables = ['user', 'issue', 'comment', 'label', 'issueLabel'];
const outputFile = 'seed.sql';

async function convertCsvToSql(tableName) {
  const csvFilePath = path.join(__dirname, 'seed-data', `${tableName}.csv`);

  try {
    const csvContent = await fs.readFile(csvFilePath, 'utf-8');
    const records = await new Promise((resolve, reject) => {
      parse(
        csvContent,
        {columns: true, skip_empty_lines: true},
        (err, records) => {
          if (err) reject(err);
          else resolve(records);
        },
      );
    });

    let sqlStatements = `\n-- Inserts for ${tableName} table\n`;
    for (const record of records) {
      const columns = Object.keys(record)
        .map(col => `"${col}"`)
        .join(', ');
      const values = Object.values(record)
        .map(val => {
          if (val === '') return 'NULL';
          if (val === 'true') return 'TRUE';
          if (val === 'false') return 'FALSE';
          if (!isNaN(val)) return val;
          return `'${val.replace(/'/g, "''")}'`;
        })
        .join(', ');

      sqlStatements += `INSERT INTO "${tableName}" (${columns}) VALUES (${values});\n`;
    }

    return sqlStatements;
  } catch (error) {
    console.error(`Error processing ${tableName}: ${error.message}`);
    return '';
  }
}

async function main() {
  const inserts = (
    await Promise.all(
      tables.map(async table => await convertCsvToSql(`${table}`)),
    )
  ).join('\n');

  const contents = /*sql*/ `BEGIN;
${inserts}
COMMIT;

SELECT
    *
FROM
    pg_create_logical_replication_slot('zero_slot_r1', 'pgoutput');`;

  const outputPath = path.join(__dirname, outputFile);
  await fs.writeFile(outputPath, contents);
  console.log(`All SQL INSERT statements have been written to ${outputPath}`);
}

main().catch(console.error);
