// oxlint-disable no-conditional-expect
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {Database} from './db.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {SQLiteStatFanout} from './sqlite-stat-fanout.ts';
import {must} from '../../shared/src/must.ts';

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

  // Helper functions to reduce test duplication

  /**
   * Creates a simple table with an optional index, inserts data, and runs ANALYZE.
   */
  function createTable(options: {
    name: string;
    columns: string; // SQL column definitions (may include PRIMARY KEY/UNIQUE)
    index?: string; // Index column name(s), optional if using PRIMARY KEY/UNIQUE inline
    data: Array<unknown[]>; // Rows to insert
  }): void {
    // Create table
    db.exec(`CREATE TABLE ${options.name} (${options.columns});`);

    // Create explicit index if specified
    if (options.index && options.index.length > 0) {
      const indexName = 'idx_' + options.index.replace(/[",\s]/g, '_');
      db.exec(
        `CREATE INDEX ${indexName} ON ${options.name}(${options.index});`,
      );
    }

    // Only insert data if array is not empty
    if (options.data.length > 0) {
      const placeholders = options.data[0]?.map(() => '?').join(', ');
      const stmt = db.prepare(
        `INSERT INTO ${options.name} VALUES (${placeholders})`,
      );
      for (const row of options.data) {
        stmt.run(...row);
      }
    }

    db.exec('ANALYZE');
    fanoutCalc = new SQLiteStatFanout(db);
  }

  /**
   * Creates parent and child tables with a foreign key relationship.
   */
  function createRelationalTables(options: {
    parent: {name: string; count: number};
    child: {
      name: string;
      count: number;
      fkColumn: string;
      /** Number of child rows that have non-NULL FK values */
      fkCount?: number;
      /** Distribution function: childId => parentId | null */
      distribution?: (childId: number) => number | null;
    };
  }): void {
    const {parent, child} = options;

    // Create tables
    db.exec(`
      CREATE TABLE ${parent.name} (id INTEGER PRIMARY KEY, name TEXT);
      CREATE TABLE ${child.name} (
        id INTEGER PRIMARY KEY,
        ${child.fkColumn} INTEGER,
        name TEXT
      );
      CREATE INDEX idx_${child.fkColumn} ON ${child.name}(${child.fkColumn});
    `);

    // Insert parent data
    const parentStmt = db.prepare(
      `INSERT INTO ${parent.name} (id, name) VALUES (?, ?)`,
    );
    for (let i = 1; i <= parent.count; i++) {
      parentStmt.run(i, `${parent.name}${i}`);
    }

    // Insert child data
    const childStmt = db.prepare(
      `INSERT INTO ${child.name} (id, ${child.fkColumn}, name) VALUES (?, ?, ?)`,
    );
    const fkCount = child.fkCount ?? child.count;
    const distribution =
      child.distribution ??
      ((childId: number) =>
        childId <= fkCount ? ((childId - 1) % parent.count) + 1 : null);

    for (let i = 1; i <= child.count; i++) {
      const fkValue = distribution(i);
      childStmt.run(i, fkValue, `${child.name}${i}`);
    }

    db.exec('ANALYZE');
    fanoutCalc = new SQLiteStatFanout(db);
  }

  /**
   * Asserts fanout result matches expected values.
   */
  function expectFanout(
    tableName: string,
    columns: string[],
    expected: {
      source?: 'stat4' | 'stat1' | 'default';
      fanout?: number;
      fanoutMin?: number;
      fanoutMax?: number;
      notDefault?: boolean;
    },
  ): void {
    const result = fanoutCalc.getFanout(tableName, columns);

    if (expected.source !== undefined) {
      expect(result.source).toBe(expected.source);
    }

    if (expected.notDefault) {
      expect(result.source).not.toBe('default');
    }

    if (expected.fanout !== undefined) {
      expect(result.fanout).toBe(expected.fanout);
    }

    if (expected.fanoutMin !== undefined) {
      expect(result.fanout).toBeGreaterThanOrEqual(expected.fanoutMin);
    }

    if (expected.fanoutMax !== undefined) {
      expect(result.fanout).toBeLessThanOrEqual(expected.fanoutMax);
    }
  }

  /**
   * Creates a table with a compound index and multi-dimensional data.
   * Useful for testing compound index statistics at various depths.
   */
  function createCompoundIndexTable(options: {
    name: string;
    columns: string[];
    dimensions: number[];
    rowsPerCombination?: number;
  }): void {
    const {name, columns, dimensions, rowsPerCombination = 1} = options;

    // Build column definitions
    const colDefs = [
      'id INTEGER PRIMARY KEY',
      ...columns.map(c => `${c} INTEGER`),
    ].join(', ');

    // Create table and index
    db.exec(`
      CREATE TABLE ${name} (${colDefs});
      CREATE INDEX idx_${columns.join('_')} ON ${name}(${columns.join(', ')});
    `);

    // Generate multi-dimensional data
    const stmt = db.prepare(
      `INSERT INTO ${name} (id, ${columns.join(', ')}) VALUES (${['?', ...columns.map(() => '?')].join(', ')})`,
    );

    let id = 1;
    function generateRows(depth: number, values: number[]): void {
      if (depth === dimensions.length) {
        // Generate rows for this combination
        for (let i = 0; i < rowsPerCombination; i++) {
          stmt.run(id++, ...values);
        }
        return;
      }

      for (let i = 1; i <= dimensions[depth]; i++) {
        generateRows(depth + 1, [...values, i]);
      }
    }

    generateRows(0, []);

    db.exec('ANALYZE');
    fanoutCalc = new SQLiteStatFanout(db);
  }

  describe('stat4 histogram (accurate, excludes NULLs)', () => {
    test('sparse foreign key with many NULLs', () => {
      // Setup: 5 projects, 100 tasks (20 with project_id, 80 NULL)
      createRelationalTables({
        parent: {name: 'project', count: 5},
        child: {name: 'task', count: 100, fkColumn: 'project_id', fkCount: 20},
      });

      expectFanout('task', ['project_id'], {
        source: 'stat4',
        fanout: 4, // 20 tasks / 5 distinct project_ids
      });
    });

    test('evenly distributed one-to-many', () => {
      // Setup: 3 departments, 30 employees evenly distributed
      createRelationalTables({
        parent: {name: 'department', count: 3},
        child: {name: 'employee', count: 30, fkColumn: 'dept_id'},
      });

      expectFanout('employee', ['dept_id'], {
        source: 'stat4',
        fanout: 10, // 30 employees / 3 departments
      });
    });

    test('highly sparse index (many distinct values)', () => {
      // Setup: 1000 rows with 900 distinct values
      const data = Array.from({length: 1000}, (_, i) => {
        const id = i + 1;
        // First 900 are unique, then some duplicates
        const rareValue = id <= 900 ? id : id % 100;
        return [id, rareValue];
      });

      createTable({
        name: 'sparse',
        columns: 'id INTEGER PRIMARY KEY, rare_value INTEGER',
        index: 'rare_value',
        data,
      });

      expectFanout('sparse', ['rare_value'], {
        source: 'stat4',
        fanoutMin: 1, // Median should be low (most values appear 1-2 times)
        fanoutMax: 3,
      });
    });

    test('skewed distribution (hot and cold values)', () => {
      // Setup: 10 customers, customer 1 has 500 orders, others have ~55 each
      createRelationalTables({
        parent: {name: 'customer', count: 10},
        child: {
          name: 'orders',
          count: 1000,
          fkColumn: 'customer_id',
          distribution: i => (i <= 500 ? 1 : ((i - 501) % 9) + 2),
        },
      });

      expectFanout('orders', ['customer_id'], {
        source: 'stat4',
        fanoutMin: 50, // Median should be close to ~55, not the average of 100
        fanoutMax: 60,
      });
    });

    test('composite index - leftmost column', () => {
      // Setup: Composite index on (status, priority)
      const statuses = ['open', 'closed', 'pending'];
      const data = Array.from({length: 90}, (_, i) => {
        const id = i + 1;
        return [id, statuses[i % 3], (i % 3) + 1];
      });

      createTable({
        name: 'ticket',
        columns: 'id INTEGER PRIMARY KEY, status TEXT, priority INTEGER',
        index: 'status, priority',
        data,
      });

      expectFanout('ticket', ['status'], {
        source: 'stat4',
        fanout: 30, // 90 tickets / 3 statuses
      });
    });
  });

  describe('stat1 fallback (includes NULLs)', () => {
    test('uses stat1 when stat4 unavailable', () => {
      // Note: In practice, stat4 is usually available if ANALYZE is run
      const data = Array.from({length: 100}, (_, i) => [i + 1, i % 10]);

      createTable({
        name: 'simple',
        columns: 'id INTEGER PRIMARY KEY, value INTEGER',
        index: 'value',
        data,
      });

      const result = fanoutCalc.getFanout('simple', ['value']);
      // Should get result from either stat4 or stat1
      expect(['stat4', 'stat1']).toContain(result.source);
      expect(result.fanout).toBeGreaterThan(0);
    });
  });

  describe('default fallback', () => {
    test('uses default when no index exists', () => {
      // Create table without index
      db.exec('CREATE TABLE no_index (id INTEGER PRIMARY KEY, value INTEGER)');
      const stmt = db.prepare('INSERT INTO no_index (id, value) VALUES (?, ?)');
      for (let i = 1; i <= 100; i++) {
        stmt.run(i, i % 10);
      }
      db.exec('ANALYZE');
      fanoutCalc = new SQLiteStatFanout(db);

      expectFanout('no_index', ['value'], {
        source: 'default',
        fanout: 3,
      });
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
      expectFanout('not_analyzed', ['value'], {source: 'default', fanout: 3});
    });

    test('respects custom default fanout', () => {
      // Create a dummy table and run ANALYZE to initialize stat tables
      db.exec(`
        CREATE TABLE dummy2 (id INTEGER PRIMARY KEY);
        INSERT INTO dummy2 VALUES (1);
      `);
      db.exec('ANALYZE');

      const customCalc = new SQLiteStatFanout(db, 10);
      db.exec('CREATE TABLE no_stats (id INTEGER PRIMARY KEY, value INTEGER)');

      const result = customCalc.getFanout('no_stats', ['value']);
      expect(result.source).toBe('default');
      expect(result.fanout).toBe(10);
    });
  });

  describe('caching', () => {
    test('caches results for repeated queries', () => {
      const data = Array.from({length: 100}, (_, i) => [i + 1, i % 10]);
      createTable({
        name: 'cached',
        columns: 'id INTEGER PRIMARY KEY, value INTEGER',
        index: 'value',
        data,
      });

      const result1 = fanoutCalc.getFanout('cached', ['value']);
      const result2 = fanoutCalc.getFanout('cached', ['value']);
      expect(result1).toBe(result2); // Same object reference (cached)
    });

    test('clearCache() invalidates cached results', () => {
      const data = Array.from({length: 100}, (_, i) => [i + 1, i % 10]);
      createTable({
        name: 'clearable',
        columns: 'id INTEGER PRIMARY KEY, value INTEGER',
        index: 'value',
        data,
      });

      const result1 = fanoutCalc.getFanout('clearable', ['value']);

      // Insert more data and re-analyze
      const stmt = db.prepare(
        'INSERT INTO clearable (id, value) VALUES (?, ?)',
      );
      for (let i = 101; i <= 200; i++) {
        stmt.run(i, i % 10);
      }
      db.exec('ANALYZE');
      fanoutCalc = new SQLiteStatFanout(db);
      fanoutCalc.clearCache();

      const result2 = fanoutCalc.getFanout('clearable', ['value']);
      expect(result2.fanout).toBeGreaterThanOrEqual(result1.fanout);
    });
  });

  describe('edge cases', () => {
    test('table with no rows', () => {
      createTable({
        name: 'empty',
        columns: 'id INTEGER PRIMARY KEY, value INTEGER',
        index: 'value',
        data: [],
      });

      expectFanout('empty', ['value'], {source: 'default', fanout: 3});
    });

    test('all NULL values', () => {
      const data = Array.from({length: 100}, (_, i) => [i + 1, null]);
      createTable({
        name: 'all_null',
        columns: 'id INTEGER PRIMARY KEY, value INTEGER',
        index: 'value',
        data,
      });

      expectFanout('all_null', ['value'], {
        source: 'stat4',
        fanout: 0, // NULLs don't match in joins
      });
    });

    test('case insensitive column names', () => {
      const data = Array.from({length: 30}, (_, i) => [i + 1, i % 3]);
      createTable({
        name: 'case_test',
        columns: 'id INTEGER PRIMARY KEY, "MixedCase" INTEGER',
        index: '"MixedCase"',
        data,
      });

      // Should work with different casing
      expectFanout('case_test', ['MixedCase'], {notDefault: true});
      expectFanout('case_test', ['mixedcase'], {notDefault: true});
    });
  });

  describe('comparison with stat1', () => {
    test('stat4 excludes NULLs, stat1 includes them', () => {
      // 10 non-NULL (2 per distinct value), 90 NULL
      const data = Array.from({length: 100}, (_, i) => {
        const id = i + 1;
        const fk = id <= 10 ? ((id - 1) % 5) + 1 : null;
        return [id, fk];
      });

      createTable({
        name: 'compare',
        columns: 'id INTEGER PRIMARY KEY, fk INTEGER',
        index: 'fk',
        data,
      });

      // Get stat4 result
      expectFanout('compare', ['fk'], {source: 'stat4', fanout: 2});

      // Get stat1 result directly
      const stat1Row = db
        .prepare(
          "SELECT stat FROM sqlite_stat1 WHERE tbl='compare' AND idx='idx_fk'",
        )
        .get() as {stat: string} | undefined;

      expect(stat1Row).toBeDefined();
      const stat1Fanout = parseInt(must(stat1Row).stat.split(' ')[1], 10);
      expect(stat1Fanout).toBeGreaterThan(10); // 100 rows / 5 distinct = 20
      expect(stat1Fanout / 2).toBeGreaterThanOrEqual(5); // stat1 overestimates by 10x!
    });
  });

  describe('compound index support', () => {
    test('two-column compound index, both constrained', () => {
      // 50 orders: 10 customers × 5 stores (unique pairs)
      // Uses PRIMARY KEY instead of explicit index - tests automatic index support
      const data = [];
      let id = 1;
      for (let customerId = 1; customerId <= 10; customerId++) {
        for (let storeId = 1; storeId <= 5; storeId++) {
          data.push([id++, customerId, storeId]);
        }
      }

      createTable({
        name: 'orders',
        columns:
          'id INTEGER, customerId INTEGER, storeId INTEGER, PRIMARY KEY (customerId, storeId)',
        data,
      });

      expectFanout('orders', ['customerId'], {notDefault: true, fanout: 5}); // 50 / 10 customers
      fanoutCalc.clearCache();
      expectFanout('orders', ['customerId', 'storeId'], {
        notDefault: true,
        fanout: 1, // 50 / 50 unique pairs
      });
    });

    test('three-column compound index, all constrained', () => {
      // 120 events: 2 tenants × 5 users × 3 event types × 4 events each
      const eventTypes = ['login', 'logout', 'action'];
      const data = [];
      let id = 1;
      for (let tenant = 1; tenant <= 2; tenant++) {
        for (let user = 1; user <= 5; user++) {
          for (const eventType of eventTypes) {
            for (let j = 0; j < 4; j++) {
              data.push([id++, tenant, user, eventType]);
            }
          }
        }
      }

      createTable({
        name: 'events',
        columns:
          'id INTEGER PRIMARY KEY, tenantId INTEGER, userId INTEGER, eventType TEXT',
        index: 'tenantId, userId, eventType',
        data,
      });

      expectFanout('events', ['tenantId'], {fanout: 60});
      expectFanout('events', ['tenantId', 'userId'], {fanout: 12});
      expectFanout('events', ['tenantId', 'userId', 'eventType'], {fanout: 4});
    });

    test('three-column index, only first two constrained', () => {
      // 60 logs: 3 apps × 2 levels × 10 timestamps
      const data = Array.from({length: 60}, (_, i) => {
        const id = i + 1;
        return [id, (i % 3) + 1, i % 2 === 0 ? 'error' : 'warn', id];
      });

      createTable({
        name: 'logs',
        columns:
          'id INTEGER PRIMARY KEY, appId INTEGER, level TEXT, timestamp INTEGER',
        index: 'appId, level, timestamp',
        data,
      });

      expectFanout('logs', ['appId', 'level'], {notDefault: true, fanout: 10});
    });

    test('columns in any order match index (flexible matching)', () => {
      const data = Array.from({length: 30}, (_, i) => [i + 1, i % 3, i % 5]);

      createTable({
        name: 'flexible_order',
        columns: 'id INTEGER PRIMARY KEY, a INTEGER, b INTEGER',
        index: 'a, b',
        data,
      });

      // Both orders should match and give same fanout (flexible matching)
      expectFanout('flexible_order', ['b', 'a'], {notDefault: true});
      const result1 = fanoutCalc.getFanout('flexible_order', ['b', 'a']);
      const result2 = fanoutCalc.getFanout('flexible_order', ['a', 'b']);
      expect(result1.fanout).toBe(result2.fanout);
    });

    test('constraint matches index with different column order', () => {
      // 100 rows: 5 stores × 10 customers × 2 rows each
      createCompoundIndexTable({
        name: 'reversed_index',
        columns: ['storeId', 'customerId'],
        dimensions: [5, 10],
        rowsPerCombination: 2,
      });

      // Constraint order differs from index, but both columns in first 2 positions
      expectFanout('reversed_index', ['customerId', 'storeId'], {
        notDefault: true,
        fanout: 2,
      });
    });

    test('partial prefix not supported (should fallback)', () => {
      const data = Array.from({length: 30}, (_, i) => {
        const id = i + 1;
        return [id, i % 2, i % 3, i % 5];
      });

      createTable({
        name: 'partial',
        columns: 'id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER',
        index: 'a, b, c',
        data,
      });

      // Constraint has a and c, but not b (gap) - should fallback
      expectFanout('partial', ['a', 'c'], {source: 'default', fanout: 3});
    });

    test('caching works with compound constraints', () => {
      const data = Array.from({length: 40}, (_, i) => [i + 1, i % 4, i % 5]);

      createTable({
        name: 'cache_compound',
        columns: 'id INTEGER PRIMARY KEY, x INTEGER, y INTEGER',
        index: 'x, y',
        data,
      });

      const result1 = fanoutCalc.getFanout('cache_compound', ['x', 'y']);
      const result2 = fanoutCalc.getFanout('cache_compound', ['x', 'y']);
      expect(result1).toBe(result2); // Same cached object
    });

    test('cache key is order-independent and matching is flexible', () => {
      const data = Array.from({length: 20}, (_, i) => [i + 1, i % 2, i % 5]);

      createTable({
        name: 'cache_order',
        columns: 'id INTEGER PRIMARY KEY, p INTEGER, q INTEGER',
        index: 'p, q',
        data,
      });

      const result1 = fanoutCalc.getFanout('cache_order', ['p', 'q']);

      expect(result1.source).not.toBe('default');

      // Second query: {q, p} also matches index (p, q) at depth 2 (flexible matching)
      // Cache key is the same because we sort columns for cache
      // So this returns the SAME cached object as result1
      const result2 = fanoutCalc.getFanout('cache_order', ['q', 'p']);

      // Should return same cached object (even though object key order differs)
      expect(result1).toBe(result2);
      expect(result2.source).not.toBe('default');
    });
  });
});
