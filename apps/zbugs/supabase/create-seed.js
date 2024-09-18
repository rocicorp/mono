// https://supabase.com/docs/guides/cli/seeding-your-database
import * as fs from 'node:fs/promises';
import {parse} from 'csv-parse';

const tables = ['user', 'issue', 'comment', 'label', 'issueLabel'];
const outputFile = 'seed.sql';

async function convertCsvToSql(tableName) {
  try {
    const csvContent = await fs.readFile(
      new URL(`./seed-data/${tableName}.csv`, import.meta.url),
      'utf-8',
    );
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
    await Promise.all(tables.map(table => convertCsvToSql(`${table}`)))
  ).join('\n');

  const contents = /*sql*/ `BEGIN;
${inserts}

-- We have to manually update the "labelIDs" column in the issue table because
-- COPY doesn't run triggers.
UPDATE
    issue
SET
    "labelIDs" = (
        SELECT
            STRING_AGG("labelID", ',')
        FROM
            "issueLabel"
        WHERE
            "issueID" = issue.id
    );
COMMIT;`;

  await fs.writeFile(new URL(`./${outputFile}`, import.meta.url), contents);
  console.log(`All SQL INSERT statements have been written to ${outputFile}`);
}

await main();
