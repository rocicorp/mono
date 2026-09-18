import {dirname, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {drizzle} from 'drizzle-orm/postgres-js';
import {migrate} from 'drizzle-orm/postgres-js/migrator';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from '../../../packages/zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../../packages/zero-cache/src/types/pg.ts';

const migrationsFolder = resolve(
  dirname(fileURLToPath(import.meta.url)),
  'migrations',
);

describe('update_issue_modified_on_emoji_change', () => {
  let db: PostgresDB;
  let notices: string[];

  beforeEach(async () => {
    notices = [];
    db = await testDBs.create('zbugs_migrations_test', {
      onNotice: n => notices.push(n.message),
    });
    await migrate(drizzle(db), {migrationsFolder});

    // Enough rows that the planner prefers indexes over sequential scans.
    // Every issue i1..i500 gets four comments; "i-no-comments" gets none.
    await db.unsafe(`
      INSERT INTO "user" (id, login, "githubID") VALUES ('u1', 'u1', 1);
      INSERT INTO issue (id, title, open, "creatorID")
        SELECT 'i' || g, 'issue ' || g, true, 'u1'
        FROM generate_series(1, 500) g;
      INSERT INTO issue (id, title, open, "creatorID")
        VALUES ('i-no-comments', 'no comments', true, 'u1');
      INSERT INTO comment (id, "issueID", body, "creatorID")
        SELECT 'c' || g, 'i' || (1 + g % 500), 'body', 'u1'
        FROM generate_series(1, 2000) g;
      ANALYZE issue;
      ANALYZE comment;

      -- Baseline every issue at modified = 0. The BEFORE UPDATE trigger on
      -- issue would otherwise overwrite it with the current time.
      ALTER TABLE issue DISABLE TRIGGER issue_set_last_modified;
      UPDATE issue SET modified = 0;
      ALTER TABLE issue ENABLE TRIGGER issue_set_last_modified;
    `);
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  async function modifiedIssues(): Promise<string[]> {
    const rows = await db<{id: string}[]>`
      SELECT id FROM issue WHERE modified <> 0 ORDER BY id`;
    return rows.map(r => r.id);
  }

  test('reacting to an issue bumps its modified time, even with no comments', async () => {
    await db`
      INSERT INTO emoji (id, value, "subjectID", "creatorID")
      VALUES ('e1', '👍', 'i-no-comments', 'u1')`;
    expect(await modifiedIssues()).toEqual(['i-no-comments']);
  });

  test('reacting to a comment bumps the modified time of its issue', async () => {
    // c7 belongs to i8.
    await db`
      INSERT INTO emoji (id, value, "subjectID", "creatorID")
      VALUES ('e1', '👍', 'c7', 'u1')`;
    expect(await modifiedIssues()).toEqual(['i8']);
  });

  test('the issue update does not scan the comment table', async () => {
    // Have Postgres report the plan of every statement the trigger runs.
    await db.unsafe(`
      LOAD 'auto_explain';
      SET auto_explain.log_min_duration = 0;
      SET auto_explain.log_nested_statements = on;
      SET auto_explain.log_level = 'NOTICE';
    `);

    await db`
      INSERT INTO emoji (id, value, "subjectID", "creatorID")
      VALUES ('e1', '👍', 'i-no-comments', 'u1')`;
    await db`
      INSERT INTO emoji (id, value, "subjectID", "creatorID")
      VALUES ('e2', '👍', 'c7', 'u1')`;

    const plans = notices.filter(n => n.includes('UPDATE issue'));
    expect(plans).toHaveLength(2);
    for (const plan of plans) {
      expect(plan).not.toContain('Seq Scan');
    }
  });
});
