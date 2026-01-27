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

const isBulkMode =
  process.env.ZERO_SEED_BULK !== undefined &&
  ['t', 'true', '1', ''].indexOf(
    process.env.ZERO_SEED_BULK.toLocaleLowerCase().trim(),
  ) !== -1;

// Indexes to drop/recreate in bulk mode (extracted from migrations)
const BULK_DROP_INDEXES = [
  'comment_issueid_idx',
  'emoji_created_idx',
  'emoji_subject_id_idx',
  'issue_created_idx',
  'issue_modified_idx',
  'issue_open_modified_idx',
  'issue_project_idx',
  'issue_shortID_idx',
  'issue_projectID_open_assigneeID_modified_idx',
  'issue_projectID_open_creatorID_modified_idx',
  'issue_projectID_open_modified_idx',
  'issue_projectID_assigneeID_modified_idx',
  'issue_projectID_creatorID_modified_idx',
  'issue_projectID_modified_idx',
  'issue_creatorID_idx',
  'issue_assigneeID_idx',
  'issuelabel_issueid_idx',
  'user_githubid_idx',
  'user_login_idx',
  'label_project_idx',
  'label_name_idx',
  'project_lower_case_name_idx',
];

// Foreign keys to drop/recreate in bulk mode
const BULK_FK_CONSTRAINTS = [
  {table: 'comment', name: 'comment_issueID_fkey'},
  {table: 'comment', name: 'comment_creatorID_fkey'},
  {table: 'emoji', name: 'emoji_creatorID_fkey'},
  {table: 'issue', name: 'issue_creatorID_fkey'},
  {table: 'issue', name: 'issue_assigneeID_fkey'},
  {table: 'issue', name: 'issue_projectID_fkey'},
  {table: 'issueLabel', name: 'issueLabel_labelID_projectID_fkey'},
  {table: 'issueLabel', name: 'issueLabel_issueID_projectID_fkey'},
  {table: 'issueNotifications', name: 'issueNotifications_userID_fkey'},
  {table: 'issueNotifications', name: 'issueNotifications_issueID_fkey'},
  {table: 'userPref', name: 'userPref_userID_fkey'},
  {table: 'viewState', name: 'viewState_userID_fkey'},
  {table: 'viewState', name: 'viewState_issueID_fkey'},
  {table: 'label', name: 'label_projectID_fkey'},
];

// FK recreation DDL (must match migration definitions)
const BULK_FK_RECREATE_DDL = [
  `ALTER TABLE "comment" ADD CONSTRAINT "comment_issueID_fkey" FOREIGN KEY ("issueID") REFERENCES "public"."issue"("id") ON DELETE cascade ON UPDATE no action`,
  `ALTER TABLE "comment" ADD CONSTRAINT "comment_creatorID_fkey" FOREIGN KEY ("creatorID") REFERENCES "public"."user"("id") ON DELETE no action ON UPDATE no action`,
  `ALTER TABLE "emoji" ADD CONSTRAINT "emoji_creatorID_fkey" FOREIGN KEY ("creatorID") REFERENCES "public"."user"("id") ON DELETE cascade ON UPDATE no action`,
  `ALTER TABLE "issue" ADD CONSTRAINT "issue_creatorID_fkey" FOREIGN KEY ("creatorID") REFERENCES "public"."user"("id") ON DELETE no action ON UPDATE no action`,
  `ALTER TABLE "issue" ADD CONSTRAINT "issue_assigneeID_fkey" FOREIGN KEY ("assigneeID") REFERENCES "public"."user"("id") ON DELETE no action ON UPDATE no action`,
  `ALTER TABLE "issue" ADD CONSTRAINT "issue_projectID_fkey" FOREIGN KEY ("projectID") REFERENCES "public"."project"("id") ON DELETE no action ON UPDATE no action`,
  `ALTER TABLE "issueLabel" ADD CONSTRAINT "issueLabel_labelID_projectID_fkey" FOREIGN KEY ("labelID", "projectID") REFERENCES "public"."label"("id", "projectID") ON DELETE no action ON UPDATE no action`,
  `ALTER TABLE "issueLabel" ADD CONSTRAINT "issueLabel_issueID_projectID_fkey" FOREIGN KEY ("issueID", "projectID") REFERENCES "public"."issue"("id", "projectID") ON DELETE cascade ON UPDATE no action`,
  `ALTER TABLE "issueNotifications" ADD CONSTRAINT "issueNotifications_userID_fkey" FOREIGN KEY ("userID") REFERENCES "public"."user"("id") ON DELETE cascade ON UPDATE no action`,
  `ALTER TABLE "issueNotifications" ADD CONSTRAINT "issueNotifications_issueID_fkey" FOREIGN KEY ("issueID") REFERENCES "public"."issue"("id") ON DELETE cascade ON UPDATE no action`,
  `ALTER TABLE "userPref" ADD CONSTRAINT "userPref_userID_fkey" FOREIGN KEY ("userID") REFERENCES "public"."user"("id") ON DELETE cascade ON UPDATE no action`,
  `ALTER TABLE "viewState" ADD CONSTRAINT "viewState_userID_fkey" FOREIGN KEY ("userID") REFERENCES "public"."user"("id") ON DELETE cascade ON UPDATE no action`,
  `ALTER TABLE "viewState" ADD CONSTRAINT "viewState_issueID_fkey" FOREIGN KEY ("issueID") REFERENCES "public"."issue"("id") ON DELETE cascade ON UPDATE no action`,
  `ALTER TABLE "label" ADD CONSTRAINT "label_projectID_fkey" FOREIGN KEY ("projectID") REFERENCES "public"."project"("id") ON DELETE no action ON UPDATE no action`,
];

// Index recreation DDL
const BULK_INDEX_RECREATE_DDL = [
  `CREATE INDEX "comment_issueid_idx" ON "comment" USING btree ("issueID")`,
  `CREATE INDEX "emoji_created_idx" ON "emoji" USING btree ("created")`,
  `CREATE INDEX "emoji_subject_id_idx" ON "emoji" USING btree ("subjectID")`,
  `CREATE INDEX "issue_created_idx" ON "issue" USING btree ("created")`,
  `CREATE INDEX "issue_modified_idx" ON "issue" USING btree ("modified")`,
  `CREATE INDEX "issue_open_modified_idx" ON "issue" USING btree ("open","modified")`,
  `CREATE UNIQUE INDEX "issue_project_idx" ON "issue" USING btree ("id","projectID")`,
  `CREATE INDEX "issue_shortID_idx" ON "issue" USING btree ("shortID")`,
  `CREATE INDEX "issue_projectID_open_assigneeID_modified_idx" ON "issue" USING btree ("projectID","open","assigneeID","modified","id")`,
  `CREATE INDEX "issue_projectID_open_creatorID_modified_idx" ON "issue" USING btree ("projectID","open","creatorID","modified","id")`,
  `CREATE INDEX "issue_projectID_open_modified_idx" ON "issue" USING btree ("projectID","open","modified","id")`,
  `CREATE INDEX "issue_projectID_assigneeID_modified_idx" ON "issue" USING btree ("projectID","assigneeID","modified","id")`,
  `CREATE INDEX "issue_projectID_creatorID_modified_idx" ON "issue" USING btree ("projectID","creatorID","modified","id")`,
  `CREATE INDEX "issue_projectID_modified_idx" ON "issue" USING btree ("projectID","modified","id")`,
  `CREATE INDEX "issue_creatorID_idx" ON "issue" USING btree ("creatorID","id")`,
  `CREATE INDEX "issue_assigneeID_idx" ON "issue" USING btree ("assigneeID","id")`,
  `CREATE INDEX "issuelabel_issueid_idx" ON "issueLabel" USING btree ("issueID")`,
  `CREATE UNIQUE INDEX "user_githubid_idx" ON "user" USING btree ("githubID")`,
  `CREATE UNIQUE INDEX "user_login_idx" ON "user" USING btree ("login")`,
  `CREATE UNIQUE INDEX "label_project_idx" ON "label" USING btree ("id","projectID")`,
  `CREATE INDEX "label_name_idx" ON "label" USING btree ("name")`,
  `CREATE UNIQUE INDEX "project_lower_case_name_idx" ON "project" USING btree ("lowerCaseName")`,
];

// All triggers to disable in bulk mode
const BULK_DISABLE_TRIGGERS: Array<{table: string; trigger: string}> = [
  {table: 'issue', trigger: 'issue_set_last_modified'},
  {table: 'issue', trigger: 'issue_set_created_on_insert_trigger'},
  {table: 'comment', trigger: 'update_issue_modified_time_on_comment'},
  {table: 'comment', trigger: 'comment_set_created_on_insert_trigger'},
  {table: 'comment', trigger: 'check_comment_body_length'},
  {table: 'emoji', trigger: 'emoji_check_subject_id_update_trigger'},
  {table: 'emoji', trigger: 'emoji_set_created_on_insert_trigger'},
  {table: 'issue', trigger: 'delete_emoji_on_issue_delete_trigger'},
  {table: 'comment', trigger: 'delete_emoji_on_comment_delete_trigger'},
  {
    table: 'project',
    trigger: 'project_set_lowercase_name_on_insert_or_update_trigger',
  },
];

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

  const sql = postgres(process.env.ZERO_UPSTREAM_DB as string, {
    // For bulk mode, increase timeouts since operations can run very long
    ...(isBulkMode
      ? {
          idle_timeout: 0,
          connect_timeout: 60,
          max_lifetime: null,
        }
      : {}),
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

    if (isBulkMode) {
      await seedBulk(sql, dataDir, files, forceSeed);
    } else {
      await seedNormal(sql, dataDir, files, forceSeed);
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

async function seedNormal(
  sql: postgres.Sql,
  dataDir: string,
  files: string[],
  forceSeed: boolean,
) {
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

        const columns = headerLine
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
}

async function seedBulk(
  sql: postgres.Sql,
  dataDir: string,
  files: string[],
  forceSeed: boolean,
) {
  // oxlint-disable-next-line no-console
  console.log('Bulk mode enabled: optimizing for large data loads...');

  // Check if already seeded (outside transaction for bulk)
  if (!forceSeed) {
    const result = await sql`select 1 from "user" limit 1`;
    if (result.length === 1) {
      // oxlint-disable-next-line no-console
      console.log('Database already seeded.');
      return;
    }
  }

  // Step 1: Set memory parameters for faster index rebuilds
  // oxlint-disable-next-line no-console
  console.log('Setting memory parameters...');
  await sql`SET maintenance_work_mem = '2GB'`;
  await sql`SET work_mem = '256MB'`;

  // Step 2: Disable all triggers
  // oxlint-disable-next-line no-console
  console.log('Disabling triggers...');
  for (const {table, trigger} of BULK_DISABLE_TRIGGERS) {
    try {
      await sql`ALTER TABLE ${sql(table)} DISABLE TRIGGER ${sql.unsafe('"' + trigger + '"')}`;
    } catch (e) {
      // oxlint-disable-next-line no-console
      console.log(
        `  Warning: could not disable trigger ${trigger} on ${table}: ${e}`,
      );
    }
  }

  // Step 3: Drop foreign key constraints
  // oxlint-disable-next-line no-console
  console.log('Dropping foreign key constraints...');
  for (const {table, name} of BULK_FK_CONSTRAINTS) {
    try {
      await sql.unsafe(
        `ALTER TABLE "${table}" DROP CONSTRAINT IF EXISTS "${name}"`,
      );
    } catch (e) {
      // oxlint-disable-next-line no-console
      console.log(`  Warning: could not drop FK ${name}: ${e}`);
    }
  }

  // Step 4: Drop non-PK indexes
  // oxlint-disable-next-line no-console
  console.log('Dropping indexes...');
  for (const idx of BULK_DROP_INDEXES) {
    try {
      await sql.unsafe(`DROP INDEX IF EXISTS "${idx}"`);
    } catch (e) {
      // oxlint-disable-next-line no-console
      console.log(`  Warning: could not drop index ${idx}: ${e}`);
    }
  }

  // Step 5: COPY data (no transaction - each file is its own implicit transaction)
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

  // Step 6: Recreate indexes
  // oxlint-disable-next-line no-console
  console.log('Recreating indexes (this may take a while)...');
  for (const ddl of BULK_INDEX_RECREATE_DDL) {
    const indexName = ddl.match(/"([^"]+)"/)?.[1] ?? 'unknown';
    // oxlint-disable-next-line no-console
    console.log(`  Creating index ${indexName}...`);
    try {
      await sql.unsafe(ddl);
    } catch (e) {
      // oxlint-disable-next-line no-console
      console.log(`  Warning: could not create index ${indexName}: ${e}`);
    }
  }

  // Step 7: Recreate foreign key constraints
  // oxlint-disable-next-line no-console
  console.log('Recreating foreign key constraints...');
  for (const ddl of BULK_FK_RECREATE_DDL) {
    const fkName = ddl.match(/CONSTRAINT "([^"]+)"/)?.[1] ?? 'unknown';
    // oxlint-disable-next-line no-console
    console.log(`  Creating FK ${fkName}...`);
    try {
      await sql.unsafe(ddl);
    } catch (e) {
      // oxlint-disable-next-line no-console
      console.log(`  Warning: could not create FK ${fkName}: ${e}`);
    }
  }

  // Step 8: Re-enable triggers
  // oxlint-disable-next-line no-console
  console.log('Re-enabling triggers...');
  for (const {table, trigger} of BULK_DISABLE_TRIGGERS) {
    try {
      await sql`ALTER TABLE ${sql(table)} ENABLE TRIGGER ${sql.unsafe('"' + trigger + '"')}`;
    } catch (e) {
      // oxlint-disable-next-line no-console
      console.log(
        `  Warning: could not enable trigger ${trigger} on ${table}: ${e}`,
      );
    }
  }

  // Step 9: Analyze tables for query planner
  // oxlint-disable-next-line no-console
  console.log('Running ANALYZE...');
  for (const tableName of TABLES_IN_SEED_ORDER) {
    await sql`ANALYZE ${sql(tableName)}`;
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
