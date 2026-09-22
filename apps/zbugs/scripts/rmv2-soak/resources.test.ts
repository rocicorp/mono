import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {afterAll, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../packages/shared/src/logging-test-utils.ts';
import {Database} from '../../../../packages/zqlite/src/db.ts';
import {sqliteFootprint, sqlitePageUsage} from './resources.ts';

const dir = mkdtempSync(join(tmpdir(), 'rmv2-soak-resources-'));
const lc = createSilentLogContext();

afterAll(() => rmSync(dir, {recursive: true, force: true}));

test('distinguishes live pages from the physical SQLite footprint', async () => {
  const file = join(dir, 'change-log.db');
  using db = new Database(lc, file);
  db.exec('CREATE TABLE entries (id INTEGER PRIMARY KEY, value TEXT)');
  const insert = db.prepare('INSERT INTO entries (value) VALUES (?)');
  db.exec('BEGIN');
  for (let i = 0; i < 1_000; i++) {
    insert.run('x'.repeat(1_000));
  }
  db.exec('COMMIT');

  const footprintBefore = await sqliteFootprint(file);
  const usageBefore = sqlitePageUsage(lc, file);
  db.exec('DELETE FROM entries');
  const footprintAfter = await sqliteFootprint(file);
  const usageAfter = sqlitePageUsage(lc, file);

  expect(usageBefore).toBeDefined();
  expect(usageAfter).toBeDefined();
  expect(usageAfter?.liveBytes).toBeLessThan(usageBefore?.liveBytes ?? 0);
  expect(usageAfter?.freeBytes).toBeGreaterThan(usageBefore?.freeBytes ?? 0);
  expect(footprintAfter).toBe(footprintBefore);
});

test('returns undefined when the database does not exist', () => {
  expect(sqlitePageUsage(lc, join(dir, 'missing.db'))).toBeUndefined();
});
