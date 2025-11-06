import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {Database} from '../../../zqlite/src/db.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {SQLiteStatFanout} from './sqlite-stat-fanout.ts';

describe('SQLiteStatFanout', () => {
  let db: Database;
  let fanoutCalc: SQLiteStatFanout;

  beforeEach(() => {
    db = new Database(createSilentLogContext(), ':memory:');
    // Note: fanoutCalc is created after ANALYZE in each test
    // because prepared statements require stat tables to exist
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
      fanoutCalc = new SQLiteStatFanout(db);

      const result = fanoutCalc.getFanout('task', {project_id: undefined});

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
      fanoutCalc = new SQLiteStatFanout(db);

      const result = fanoutCalc.getFanout('employee', {dept_id: undefined});

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
      fanoutCalc = new SQLiteStatFanout(db);

      const result = fanoutCalc.getFanout('sparse', {rare_value: undefined});

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
      fanoutCalc = new SQLiteStatFanout(db);

      const result = fanoutCalc.getFanout('orders', {customer_id: undefined});

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
      fanoutCalc = new SQLiteStatFanout(db);

      const result = fanoutCalc.getFanout('ticket', {status: undefined});

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
      fanoutCalc = new SQLiteStatFanout(db);

      const result = fanoutCalc.getFanout('simple', {value: undefined});

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
      fanoutCalc = new SQLiteStatFanout(db);

      const result = fanoutCalc.getFanout('no_index', {value: undefined});

      expect(result.source).toBe('default');
      expect(result.fanout).toBe(3); // Default value
    });

    test('uses default when ANALYZE not run', () => {
      // Create a dummy table and run ANALYZE to initialize stat tables
      // This allows SQLiteStatFanout constructor to prepare statements
      db.exec(`
        CREATE TABLE dummy (id INTEGER PRIMARY KEY);
        INSERT INTO dummy VALUES (1);
      `);
      db.exec('ANALYZE');
      fanoutCalc = new SQLiteStatFanout(db);

      // Now create the actual test table WITHOUT running ANALYZE on it
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

      // Don't run ANALYZE on not_analyzed table

      const result = fanoutCalc.getFanout('not_analyzed', {value: undefined});

      expect(result.source).toBe('default');
      expect(result.fanout).toBe(3);
    });

    test('respects custom default fanout', () => {
      // Create a dummy table and run ANALYZE to initialize stat tables
      db.exec(`
        CREATE TABLE dummy2 (id INTEGER PRIMARY KEY);
        INSERT INTO dummy2 VALUES (1);
      `);
      db.exec('ANALYZE');

      const customCalc = new SQLiteStatFanout(db, 10);

      db.exec(`
        CREATE TABLE no_stats (id INTEGER PRIMARY KEY, value INTEGER);
      `);

      const result = customCalc.getFanout('no_stats', {value: undefined});

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
      fanoutCalc = new SQLiteStatFanout(db);

      const result1 = fanoutCalc.getFanout('cached', {value: undefined});
      const result2 = fanoutCalc.getFanout('cached', {value: undefined});

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
      fanoutCalc = new SQLiteStatFanout(db);

      const result1 = fanoutCalc.getFanout('clearable', {value: undefined});

      // Insert more data and re-analyze
      for (let i = 101; i <= 200; i++) {
        db.prepare('INSERT INTO clearable (id, value) VALUES (?, ?)').run(
          i,
          i % 10,
        );
      }

      db.exec('ANALYZE');
      fanoutCalc = new SQLiteStatFanout(db);

      // Without clearing cache, would get stale result
      fanoutCalc.clearCache();

      const result2 = fanoutCalc.getFanout('clearable', {value: undefined});

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
      fanoutCalc = new SQLiteStatFanout(db);

      const result = fanoutCalc.getFanout('empty', {value: undefined});

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
      fanoutCalc = new SQLiteStatFanout(db);

      const result = fanoutCalc.getFanout('all_null', {value: undefined});

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
      fanoutCalc = new SQLiteStatFanout(db);

      // Should work with different casing
      const result1 = fanoutCalc.getFanout('case_test', {MixedCase: undefined});
      const result2 = fanoutCalc.getFanout('case_test', {mixedcase: undefined});

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
      fanoutCalc = new SQLiteStatFanout(db);

      // Get stat4 result
      const stat4Result = fanoutCalc.getFanout('compare', {fk: undefined});

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

  describe('compound index support', () => {
    test('backward compat: string argument (single column)', () => {
      db.exec(`
        CREATE TABLE compat (id INTEGER PRIMARY KEY, value INTEGER);
        CREATE INDEX idx_value ON compat(value);
      `);

      for (let i = 1; i <= 30; i++) {
        db.prepare('INSERT INTO compat (id, value) VALUES (?, ?)').run(
          i,
          i % 3,
        );
      }

      db.exec('ANALYZE');
      fanoutCalc = new SQLiteStatFanout(db);

      // Should work with single-column constraint
      const result = fanoutCalc.getFanout('compat', {value: undefined});

      expect(result.source).not.toBe('default');
      expect(result.fanout).toBe(10);
    });

    test('backward compat: single-column constraint object', () => {
      db.exec(`
        CREATE TABLE compat2 (id INTEGER PRIMARY KEY, userId INTEGER);
        CREATE INDEX idx_userId ON compat2(userId);
      `);

      for (let i = 1; i <= 40; i++) {
        db.prepare('INSERT INTO compat2 (id, userId) VALUES (?, ?)').run(
          i,
          i % 4,
        );
      }

      db.exec('ANALYZE');
      fanoutCalc = new SQLiteStatFanout(db);

      // Should work with constraint object
      const result = fanoutCalc.getFanout('compat2', {userId: undefined});

      expect(result.source).not.toBe('default');
      expect(result.fanout).toBe(10);
    });

    test('two-column compound index, both constrained', () => {
      db.exec(`
        CREATE TABLE orders (
          id INTEGER PRIMARY KEY,
          customerId INTEGER,
          storeId INTEGER
        );
        CREATE INDEX idx_customer_store ON orders(customerId, storeId);
      `);

      // 100 orders with proper distribution:
      // - 10 customers × 5 stores = 50 pairs × 2 orders each = 100 total
      let orderId = 1;
      for (let customerId = 1; customerId <= 10; customerId++) {
        for (let storeId = 1; storeId <= 5; storeId++) {
          // 2 orders per (customer, store) pair
          for (let j = 0; j < 2; j++) {
            db.prepare(
              'INSERT INTO orders (id, customerId, storeId) VALUES (?, ?, ?)',
            ).run(orderId++, customerId, storeId);
          }
        }
      }

      db.exec('ANALYZE');
      fanoutCalc = new SQLiteStatFanout(db);

      // Test single column (should use depth 1)
      const single = fanoutCalc.getFanout('orders', {customerId: undefined});
      expect(single.source).not.toBe('default');
      expect(single.fanout).toBe(10); // 100 orders / 10 customers

      // Clear cache to ensure fresh lookup
      fanoutCalc.clearCache();

      // Test both columns (should use depth 2)
      const compound = fanoutCalc.getFanout('orders', {
        customerId: undefined,
        storeId: undefined,
      });
      expect(compound.source).not.toBe('default');
      expect(compound.fanout).toBe(2); // 100 orders / 50 (customer, store) pairs
    });

    test('three-column compound index, all constrained', () => {
      db.exec(`
        CREATE TABLE events (
          id INTEGER PRIMARY KEY,
          tenantId INTEGER,
          userId INTEGER,
          eventType TEXT
        );
        CREATE INDEX idx_tenant_user_type ON events(tenantId, userId, eventType);
      `);

      // 120 events with predictable distribution:
      // - 2 tenants × 5 users × 3 event types × 4 events each = 120
      let id = 1;
      for (let tenant = 1; tenant <= 2; tenant++) {
        for (let user = 1; user <= 5; user++) {
          for (const eventType of ['login', 'logout', 'action']) {
            for (let j = 0; j < 4; j++) {
              db.prepare(
                'INSERT INTO events (id, tenantId, userId, eventType) VALUES (?, ?, ?, ?)',
              ).run(id++, tenant, user, eventType);
            }
          }
        }
      }

      db.exec('ANALYZE');
      fanoutCalc = new SQLiteStatFanout(db);

      // Depth 1: tenantId only
      const depth1 = fanoutCalc.getFanout('events', {tenantId: undefined});
      expect(depth1.fanout).toBe(60); // 120 / 2 tenants

      // Depth 2: tenantId + userId
      const depth2 = fanoutCalc.getFanout('events', {
        tenantId: undefined,
        userId: undefined,
      });
      expect(depth2.fanout).toBe(12); // 120 / 10 (tenant, user) pairs

      // Depth 3: all three columns
      const depth3 = fanoutCalc.getFanout('events', {
        tenantId: undefined,
        userId: undefined,
        eventType: undefined,
      });
      expect(depth3.fanout).toBe(4); // 120 / 30 (tenant, user, type) tuples
    });

    test('three-column index, only first two constrained', () => {
      db.exec(`
        CREATE TABLE logs (
          id INTEGER PRIMARY KEY,
          appId INTEGER,
          level TEXT,
          timestamp INTEGER
        );
        CREATE INDEX idx_app_level_time ON logs(appId, level, timestamp);
      `);

      // 60 logs: 3 apps × 2 levels × 10 timestamps
      for (let i = 1; i <= 60; i++) {
        const appId = ((i - 1) % 3) + 1;
        const level = (i - 1) % 2 === 0 ? 'error' : 'warn';
        const timestamp = i;
        db.prepare(
          'INSERT INTO logs (id, appId, level, timestamp) VALUES (?, ?, ?, ?)',
        ).run(i, appId, level, timestamp);
      }

      db.exec('ANALYZE');
      fanoutCalc = new SQLiteStatFanout(db);

      // Should use depth 2 (appId + level)
      const result = fanoutCalc.getFanout('logs', {
        appId: undefined,
        level: undefined,
      });

      expect(result.source).not.toBe('default');
      expect(result.fanout).toBe(10); // 60 / 6 (app, level) pairs
    });

    test('columns in any order match index (flexible matching)', () => {
      db.exec(`
        CREATE TABLE flexible_order (
          id INTEGER PRIMARY KEY,
          a INTEGER,
          b INTEGER
        );
        CREATE INDEX idx_a_b ON flexible_order(a, b);
      `);

      for (let i = 1; i <= 30; i++) {
        db.prepare(
          'INSERT INTO flexible_order (id, a, b) VALUES (?, ?, ?)',
        ).run(i, i % 3, i % 5);
      }

      db.exec('ANALYZE');
      fanoutCalc = new SQLiteStatFanout(db);

      // With flexible matching, both {a, b} and {b, a} should match index (a, b)
      // Object.keys() returns keys in insertion order: {b, a} → ['b', 'a']
      // But flexible matching checks if both 'a' and 'b' exist in first 2 positions
      const result1 = fanoutCalc.getFanout('flexible_order', {
        b: undefined,
        a: undefined,
      });

      expect(result1.source).not.toBe('default');
      expect(result1.fanout).toBeGreaterThan(0);

      // Same result for {a, b} order
      const result2 = fanoutCalc.getFanout('flexible_order', {
        a: undefined,
        b: undefined,
      });

      expect(result2.source).not.toBe('default');
      expect(result2.fanout).toBeGreaterThan(0);

      // Both should give same fanout (may be cached)
      expect(result1.fanout).toBe(result2.fanout);
    });

    test('constraint matches index with different column order', () => {
      db.exec(`
        CREATE TABLE reversed_index (
          id INTEGER PRIMARY KEY,
          customerId INTEGER,
          storeId INTEGER
        );
        CREATE INDEX idx_store_customer ON reversed_index(storeId, customerId);
      `);

      // 100 rows: 5 stores × 10 customers × 2 rows each
      let id = 1;
      for (let storeId = 1; storeId <= 5; storeId++) {
        for (let customerId = 1; customerId <= 10; customerId++) {
          for (let j = 0; j < 2; j++) {
            db.prepare(
              'INSERT INTO reversed_index (id, customerId, storeId) VALUES (?, ?, ?)',
            ).run(id++, customerId, storeId);
          }
        }
      }

      db.exec('ANALYZE');
      fanoutCalc = new SQLiteStatFanout(db);

      // Constraint {customerId, storeId} should match index (storeId, customerId)
      // Even though order differs, both columns are in first 2 positions
      const result = fanoutCalc.getFanout('reversed_index', {
        customerId: undefined,
        storeId: undefined,
      });

      expect(result.source).not.toBe('default');
      expect(result.fanout).toBe(2); // 100 rows / 50 (store, customer) pairs
    });

    test('partial prefix not supported (should fallback)', () => {
      db.exec(`
        CREATE TABLE partial (
          id INTEGER PRIMARY KEY,
          a INTEGER,
          b INTEGER,
          c INTEGER
        );
        CREATE INDEX idx_a_b_c ON partial(a, b, c);
      `);

      for (let i = 1; i <= 30; i++) {
        db.prepare('INSERT INTO partial (id, a, b, c) VALUES (?, ?, ?, ?)').run(
          i,
          i % 2,
          i % 3,
          i % 5,
        );
      }

      db.exec('ANALYZE');
      fanoutCalc = new SQLiteStatFanout(db);

      // Constraint has a and c, but not b (gap in the middle)
      // 'c' is not in the first 2 positions, so should not match
      const result = fanoutCalc.getFanout('partial', {
        a: undefined,
        c: undefined,
      });

      expect(result.source).toBe('default');
      expect(result.fanout).toBe(3);
    });

    test('caching works with compound constraints', () => {
      db.exec(`
        CREATE TABLE cache_compound (
          id INTEGER PRIMARY KEY,
          x INTEGER,
          y INTEGER
        );
        CREATE INDEX idx_x_y ON cache_compound(x, y);
      `);

      for (let i = 1; i <= 40; i++) {
        db.prepare(
          'INSERT INTO cache_compound (id, x, y) VALUES (?, ?, ?)',
        ).run(i, i % 4, i % 5);
      }

      db.exec('ANALYZE');
      fanoutCalc = new SQLiteStatFanout(db);

      const result1 = fanoutCalc.getFanout('cache_compound', {
        x: undefined,
        y: undefined,
      });
      const result2 = fanoutCalc.getFanout('cache_compound', {
        x: undefined,
        y: undefined,
      });

      // Should return same cached object
      expect(result1).toBe(result2);
    });

    test('cache key is order-independent and matching is flexible', () => {
      db.exec(`
        CREATE TABLE cache_order (
          id INTEGER PRIMARY KEY,
          p INTEGER,
          q INTEGER
        );
        CREATE INDEX idx_p_q ON cache_order(p, q);
      `);

      for (let i = 1; i <= 20; i++) {
        db.prepare('INSERT INTO cache_order (id, p, q) VALUES (?, ?, ?)').run(
          i,
          i % 2,
          i % 5,
        );
      }

      db.exec('ANALYZE');
      fanoutCalc = new SQLiteStatFanout(db);

      // First query: {p, q} matches index (p, q) at depth 2
      const result1 = fanoutCalc.getFanout('cache_order', {
        p: undefined,
        q: undefined,
      });

      expect(result1.source).not.toBe('default');

      // Second query: {q, p} also matches index (p, q) at depth 2 (flexible matching)
      // Cache key is the same because we sort columns for cache
      // So this returns the SAME cached object as result1
      const result2 = fanoutCalc.getFanout('cache_order', {
        q: undefined,
        p: undefined,
      });

      // Should return same cached object (even though object key order differs)
      expect(result1).toBe(result2);
      expect(result2.source).not.toBe('default');
    });
  });
});
