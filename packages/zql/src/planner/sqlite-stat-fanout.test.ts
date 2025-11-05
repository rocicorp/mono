import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {Database} from '../../../zqlite/src/db.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {SQLiteStatFanout} from './sqlite-stat-fanout.ts';

describe('SQLiteStatFanout', () => {
  let db: Database;
  let fanoutCalc: SQLiteStatFanout;

  beforeEach(() => {
    db = new Database(createSilentLogContext(), ':memory:');
    fanoutCalc = new SQLiteStatFanout(db);
  });

  afterEach(() => {
    db.close();
  });

  describe('stat4 histogram (accurate, excludes NULLs)', () => {
    test('sparse foreign key with many NULLs', () => {
      // Setup: 5 projects, 100 tasks (20 with project_id, 80 NULL)
      db.exec(`
        CREATE TABLE project (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE task (
          id INTEGER PRIMARY KEY,
          project_id INTEGER,
          title TEXT,
          FOREIGN KEY (project_id) REFERENCES project(id)
        );
        CREATE INDEX idx_project_id ON task(project_id);
      `);

      for (let i = 1; i <= 5; i++) {
        db.prepare('INSERT INTO project (id, name) VALUES (?, ?)').run(
          i,
          `Project ${i}`,
        );
      }

      for (let i = 1; i <= 100; i++) {
        const projectId = i <= 20 ? ((i - 1) % 5) + 1 : null;
        db.prepare(
          'INSERT INTO task (id, project_id, title) VALUES (?, ?, ?)',
        ).run(i, projectId, `Task ${i}`);
      }

      db.exec('ANALYZE');

      const result = fanoutCalc.getFanout('task', 'project_id');

      expect(result.source).toBe('stat4');
      expect(result.fanout).toBe(4); // 20 tasks / 5 distinct project_ids
      expect(result.nullCount).toBe(80); // NULL samples tracked
    });

    test('evenly distributed one-to-many', () => {
      // Setup: 3 departments, 30 employees evenly distributed
      db.exec(`
        CREATE TABLE department (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE employee (
          id INTEGER PRIMARY KEY,
          dept_id INTEGER NOT NULL,
          name TEXT,
          FOREIGN KEY (dept_id) REFERENCES department(id)
        );
        CREATE INDEX idx_dept_id ON employee(dept_id);
      `);

      for (let i = 1; i <= 3; i++) {
        db.prepare('INSERT INTO department (id, name) VALUES (?, ?)').run(
          i,
          `Dept ${i}`,
        );
      }

      for (let i = 1; i <= 30; i++) {
        const deptId = ((i - 1) % 3) + 1;
        db.prepare(
          'INSERT INTO employee (id, dept_id, name) VALUES (?, ?, ?)',
        ).run(i, deptId, `Employee ${i}`);
      }

      db.exec('ANALYZE');

      const result = fanoutCalc.getFanout('employee', 'dept_id');

      expect(result.source).toBe('stat4');
      expect(result.fanout).toBe(10); // 30 employees / 3 departments
      expect(result.nullCount).toBe(0); // No NULLs
    });

    test('highly sparse index (many distinct values)', () => {
      // Setup: 1000 rows with 900 distinct values
      db.exec(`
        CREATE TABLE sparse (
          id INTEGER PRIMARY KEY,
          rare_value INTEGER
        );
        CREATE INDEX idx_rare ON sparse(rare_value);
      `);

      for (let i = 1; i <= 1000; i++) {
        // First 900 are unique, then some duplicates
        const rareValue = i <= 900 ? i : i % 100;
        db.prepare('INSERT INTO sparse (id, rare_value) VALUES (?, ?)').run(
          i,
          rareValue,
        );
      }

      db.exec('ANALYZE');

      const result = fanoutCalc.getFanout('sparse', 'rare_value');

      expect(result.source).toBe('stat4');
      // Median of samples should be low (most values appear 1-2 times)
      expect(result.fanout).toBeGreaterThanOrEqual(1);
      expect(result.fanout).toBeLessThanOrEqual(3);
    });

    test('skewed distribution (hot and cold values)', () => {
      // Setup: 10 customers, customer 1 has 500 orders, others have ~55 each
      db.exec(`
        CREATE TABLE customer (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE orders (
          id INTEGER PRIMARY KEY,
          customer_id INTEGER NOT NULL,
          total REAL,
          FOREIGN KEY (customer_id) REFERENCES customer(id)
        );
        CREATE INDEX idx_customer_id ON orders(customer_id);
      `);

      for (let i = 1; i <= 10; i++) {
        db.prepare('INSERT INTO customer (id, name) VALUES (?, ?)').run(
          i,
          `Customer ${i}`,
        );
      }

      for (let i = 1; i <= 1000; i++) {
        const customerId = i <= 500 ? 1 : ((i - 501) % 9) + 2;
        db.prepare(
          'INSERT INTO orders (id, customer_id, total) VALUES (?, ?, ?)',
        ).run(i, customerId, 100 + i);
      }

      db.exec('ANALYZE');

      const result = fanoutCalc.getFanout('orders', 'customer_id');

      expect(result.source).toBe('stat4');
      // Median should be close to ~55, not the average of 100
      // (stat4 samples distribution, so median is more robust)
      expect(result.fanout).toBeGreaterThanOrEqual(50);
      expect(result.fanout).toBeLessThanOrEqual(60);
    });

    test('composite index - leftmost column', () => {
      // Setup: Composite index on (status, priority)
      db.exec(`
        CREATE TABLE ticket (
          id INTEGER PRIMARY KEY,
          status TEXT,
          priority INTEGER
        );
        CREATE INDEX idx_status_priority ON ticket(status, priority);
      `);

      const statuses = ['open', 'closed', 'pending'];
      for (let i = 1; i <= 90; i++) {
        db.prepare(
          'INSERT INTO ticket (id, status, priority) VALUES (?, ?, ?)',
        ).run(i, statuses[i % 3], (i % 3) + 1);
      }

      db.exec('ANALYZE');

      const result = fanoutCalc.getFanout('ticket', 'status');

      expect(result.source).toBe('stat4');
      expect(result.fanout).toBe(30); // 90 tickets / 3 statuses
    });
  });

  describe('stat1 fallback (includes NULLs)', () => {
    test('uses stat1 when stat4 unavailable', () => {
      // Note: In practice, stat4 is usually available if ANALYZE is run
      // This test would require a SQLite build without ENABLE_STAT4,
      // which is uncommon. We'll test the code path indirectly.

      db.exec(`
        CREATE TABLE simple (id INTEGER PRIMARY KEY, value INTEGER);
        CREATE INDEX idx_value ON simple(value);
      `);

      for (let i = 1; i <= 100; i++) {
        db.prepare('INSERT INTO simple (id, value) VALUES (?, ?)').run(
          i,
          i % 10,
        );
      }

      db.exec('ANALYZE');

      const result = fanoutCalc.getFanout('simple', 'value');

      // Should get result from either stat4 or stat1
      expect(['stat4', 'stat1']).toContain(result.source);
      expect(result.fanout).toBeGreaterThan(0);
    });
  });

  describe('default fallback', () => {
    test('uses default when no index exists', () => {
      db.exec(`
        CREATE TABLE no_index (id INTEGER PRIMARY KEY, value INTEGER);
      `);

      for (let i = 1; i <= 100; i++) {
        db.prepare('INSERT INTO no_index (id, value) VALUES (?, ?)').run(
          i,
          i % 10,
        );
      }

      db.exec('ANALYZE');

      const result = fanoutCalc.getFanout('no_index', 'value');

      expect(result.source).toBe('default');
      expect(result.fanout).toBe(3); // Default value
    });

    test('uses default when ANALYZE not run', () => {
      db.exec(`
        CREATE TABLE not_analyzed (id INTEGER PRIMARY KEY, value INTEGER);
        CREATE INDEX idx_value ON not_analyzed(value);
      `);

      for (let i = 1; i <= 100; i++) {
        db.prepare('INSERT INTO not_analyzed (id, value) VALUES (?, ?)').run(
          i,
          i % 10,
        );
      }

      // Don't run ANALYZE

      const result = fanoutCalc.getFanout('not_analyzed', 'value');

      expect(result.source).toBe('default');
      expect(result.fanout).toBe(3);
    });

    test('respects custom default fanout', () => {
      const customCalc = new SQLiteStatFanout(db, 10);

      db.exec(`
        CREATE TABLE no_stats (id INTEGER PRIMARY KEY, value INTEGER);
      `);

      const result = customCalc.getFanout('no_stats', 'value');

      expect(result.source).toBe('default');
      expect(result.fanout).toBe(10);
    });
  });

  describe('caching', () => {
    test('caches results for repeated queries', () => {
      db.exec(`
        CREATE TABLE cached (id INTEGER PRIMARY KEY, value INTEGER);
        CREATE INDEX idx_value ON cached(value);
      `);

      for (let i = 1; i <= 100; i++) {
        db.prepare('INSERT INTO cached (id, value) VALUES (?, ?)').run(
          i,
          i % 10,
        );
      }

      db.exec('ANALYZE');

      const result1 = fanoutCalc.getFanout('cached', 'value');
      const result2 = fanoutCalc.getFanout('cached', 'value');

      expect(result1).toBe(result2); // Same object reference (cached)
    });

    test('clearCache() invalidates cached results', () => {
      db.exec(`
        CREATE TABLE clearable (id INTEGER PRIMARY KEY, value INTEGER);
        CREATE INDEX idx_value ON clearable(value);
      `);

      for (let i = 1; i <= 100; i++) {
        db.prepare('INSERT INTO clearable (id, value) VALUES (?, ?)').run(
          i,
          i % 10,
        );
      }

      db.exec('ANALYZE');

      const result1 = fanoutCalc.getFanout('clearable', 'value');

      // Insert more data and re-analyze
      for (let i = 101; i <= 200; i++) {
        db.prepare('INSERT INTO clearable (id, value) VALUES (?, ?)').run(
          i,
          i % 10,
        );
      }

      db.exec('ANALYZE');

      // Without clearing cache, would get stale result
      fanoutCalc.clearCache();

      const result2 = fanoutCalc.getFanout('clearable', 'value');

      expect(result2.fanout).toBeGreaterThanOrEqual(result1.fanout);
    });
  });

  describe('edge cases', () => {
    test('table with no rows', () => {
      db.exec(`
        CREATE TABLE empty (id INTEGER PRIMARY KEY, value INTEGER);
        CREATE INDEX idx_value ON empty(value);
      `);

      db.exec('ANALYZE');

      const result = fanoutCalc.getFanout('empty', 'value');

      // Should fallback to default (no stats for empty table)
      expect(result.source).toBe('default');
      expect(result.fanout).toBe(3);
    });

    test('all NULL values', () => {
      db.exec(`
        CREATE TABLE all_null (id INTEGER PRIMARY KEY, value INTEGER);
        CREATE INDEX idx_value ON all_null(value);
      `);

      for (let i = 1; i <= 100; i++) {
        db.prepare('INSERT INTO all_null (id, value) VALUES (?, ?)').run(
          i,
          null,
        );
      }

      db.exec('ANALYZE');

      const result = fanoutCalc.getFanout('all_null', 'value');

      // When all values are NULL:
      // - stat4 will have only NULL samples, so we fallback
      // - stat1 may still report stats (100 rows, but no distinct non-NULL values)
      // Either stat1 or default is acceptable
      expect(['stat1', 'default']).toContain(result.source);
      expect(result.fanout).toBeGreaterThan(0);
    });

    test('case insensitive column names', () => {
      db.exec(`
        CREATE TABLE case_test (id INTEGER PRIMARY KEY, "MixedCase" INTEGER);
        CREATE INDEX idx_mixed ON case_test("MixedCase");
      `);

      for (let i = 1; i <= 30; i++) {
        db.prepare('INSERT INTO case_test (id, "MixedCase") VALUES (?, ?)').run(
          i,
          i % 3,
        );
      }

      db.exec('ANALYZE');

      // Should work with different casing
      const result1 = fanoutCalc.getFanout('case_test', 'MixedCase');
      const result2 = fanoutCalc.getFanout('case_test', 'mixedcase');

      expect(result1.source).not.toBe('default');
      expect(result2.source).not.toBe('default');
    });
  });

  describe('comparison with stat1', () => {
    test('stat4 excludes NULLs, stat1 includes them', () => {
      db.exec(`
        CREATE TABLE compare (id INTEGER PRIMARY KEY, fk INTEGER);
        CREATE INDEX idx_fk ON compare(fk);
      `);

      // 10 non-NULL (2 per distinct value), 90 NULL
      for (let i = 1; i <= 100; i++) {
        const fk = i <= 10 ? ((i - 1) % 5) + 1 : null;
        db.prepare('INSERT INTO compare (id, fk) VALUES (?, ?)').run(i, fk);
      }

      db.exec('ANALYZE');

      // Get stat4 result
      const stat4Result = fanoutCalc.getFanout('compare', 'fk');

      // Get stat1 result directly
      const stat1Row = db
        .prepare(
          "SELECT stat FROM sqlite_stat1 WHERE tbl='compare' AND idx='idx_fk'",
        )
        .get() as {stat: string} | undefined;

      expect(stat1Row).toBeDefined();
      const stat1Fanout = parseInt(
        (stat1Row as {stat: string}).stat.split(' ')[1],
        10,
      );

      expect(stat4Result.source).toBe('stat4');
      expect(stat4Result.fanout).toBe(2); // 10 rows / 5 distinct values
      expect(stat1Fanout).toBeGreaterThan(10); // 100 rows / 5 distinct = 20

      // stat1 overestimates by 10x!
      expect(stat1Fanout / stat4Result.fanout).toBeGreaterThanOrEqual(5);
    });
  });
});
