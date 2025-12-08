import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Database} from './db.ts';
import {btreeCost, createSQLiteCostModel} from './sqlite-cost-model.ts';
import {computeZqlSpecs} from '../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../zero-cache/src/db/specs.ts';

describe('SQLite cost model', () => {
  let db: Database;
  let costModel: ReturnType<typeof createSQLiteCostModel>;

  beforeEach(() => {
    const lc = createSilentLogContext();
    db = new Database(lc, ':memory:');

    // CREATE TABLE foo (a, b, c) with proper types
    // Note: SQLite needs explicit types for computeZqlSpecs to work properly
    // Need both PRIMARY KEY (for NOT NULL constraint) and UNIQUE INDEX (for computeZqlSpecs)
    db.exec(`
      CREATE TABLE foo (a INTEGER PRIMARY KEY, b INTEGER, c INTEGER);
      CREATE UNIQUE INDEX foo_a_unique ON foo(a);
    `);

    // Insert 2,000 rows
    const stmt = db.prepare('INSERT INTO foo (a, b, c) VALUES (?, ?, ?)');
    for (let i = 0; i < 2_000; i++) {
      stmt.run(i * 3 + 1, i * 3 + 2, i * 3 + 3);
    }

    // Run ANALYZE to populate statistics
    db.exec('ANALYZE');

    // Get table specs using computeZqlSpecs
    const tableSpecs = new Map<string, LiteAndZqlSpec>();
    computeZqlSpecs(lc, db, tableSpecs);

    // Create the cost model
    costModel = createSQLiteCostModel(db, tableSpecs);
  });

  test('table scan ordered by primary key requires no sort', () => {
    // SELECT * FROM foo ORDER BY a
    // Ordered by primary key, so no sort needed - expected cost is just the table scan (~2000 rows)
    const {rows, startupCost} = costModel(
      'foo',
      [['a', 'asc']],
      undefined,
      undefined,
    );
    // Expected: (SQLite estimate) = 1920
    expect(rows).toBe(1920);
    expect(startupCost).toBe(0);
  });

  test('table scan ordered by non-indexed column includes sort cost', () => {
    // SELECT * FROM foo ORDER BY b
    const {startupCost, rows} = costModel(
      'foo',
      [['b', 'asc']],
      undefined,
      undefined,
    );
    expect(rows).toBe(1920);
    expect(startupCost).toBeCloseTo(btreeCost(rows), 3); // Allow some variance in sort cost estimate
  });

  test('primary key lookup via condition', () => {
    const {rows, startupCost} = costModel(
      'foo',
      [['a', 'asc']],
      {
        type: 'simple',
        left: {type: 'column', name: 'a'},
        op: '=',
        right: {type: 'literal', value: 4},
      },
      undefined,
    );
    expect(rows).toBe(1);
    expect(startupCost).toBe(0);
  });

  test('primary key lookup via constraint', () => {
    const {rows, startupCost} = costModel('foo', [['a', 'asc']], undefined, {
      a: undefined,
    });
    expect(rows).toBe(1);
    expect(startupCost).toBe(0);
  });

  test('range check on primary key', () => {
    // SELECT * FROM foo WHERE a > 1 ORDER BY a
    // Should use primary key index for range scan
    const {rows, startupCost} = costModel(
      'foo',
      [['a', 'asc']],
      {
        type: 'simple',
        left: {type: 'column', name: 'a'},
        op: '>',
        right: {type: 'literal', value: 1},
      },
      undefined,
    );
    // With primary key index, range scan should be efficient
    expect(rows).toBe(480);
    expect(startupCost).toBe(0);
  });

  test('range check on non-indexed column', () => {
    // SELECT * FROM foo WHERE b > 2 ORDER BY a
    // Requires full table scan since b is not indexed
    const {rows, startupCost} = costModel(
      'foo',
      [['a', 'asc']],
      {
        type: 'simple',
        left: {type: 'column', name: 'b'},
        op: '>',
        right: {type: 'literal', value: 200},
      },
      undefined,
    );
    // Full table scan with some filtering selectivity factored in
    expect(rows).toBe(1792);
    expect(startupCost).toBe(0);
  });

  test('equality check on non-indexed column', () => {
    // SELECT * FROM foo WHERE b = 2 ORDER BY a
    // Requires full table scan since b is not indexed
    const {rows, startupCost} = costModel(
      'foo',
      [['a', 'asc']],
      {
        type: 'simple',
        left: {type: 'column', name: 'b'},
        op: '=',
        right: {type: 'literal', value: 2},
      },
      undefined,
    );
    // Full table scan with some filtering selectivity factored in
    // much higher cost the PK lookup which is what we expect.
    // not quite as high as a full table scan. Why?
    expect(rows).toBe(480);
    expect(startupCost).toBe(0);
  });

  test('startup cost for index scan is zero', () => {
    // SELECT * FROM foo ORDER BY a
    // Uses primary key index - no sort needed, so startup cost should be 0
    const {startupCost, rows} = costModel(
      'foo',
      [['a', 'asc']],
      undefined,
      undefined,
    );
    expect(startupCost).toBe(0);
    expect(rows).toBe(1920);
  });

  test('inline values work with string literals', () => {
    // This test verifies that string values are properly inlined and escaped
    const {rows} = costModel(
      'foo',
      [['a', 'asc']],
      {
        type: 'simple',
        left: {type: 'column', name: 'b'},
        op: '=',
        right: {type: 'literal', value: 'test string'},
      },
      undefined,
    );
    // SQLite can estimate selectivity based on actual value
    expect(rows).toBe(480);
  });

  test('inline values work with boolean literals', () => {
    // This test verifies that boolean values are properly inlined as 1/0
    const {rows} = costModel(
      'foo',
      [['a', 'asc']],
      {
        type: 'simple',
        left: {type: 'column', name: 'b'},
        op: '=',
        right: {type: 'literal', value: true},
      },
      undefined,
    );
    // SQLite estimates based on the inlined value (1)
    expect(rows).toBe(960);
  });

  test('inline values work with null literals', () => {
    // This test verifies that null values are properly inlined as NULL
    const {rows} = costModel(
      'foo',
      [['a', 'asc']],
      {
        type: 'simple',
        left: {type: 'column', name: 'b'},
        op: 'IS',
        right: {type: 'literal', value: null},
      },
      undefined,
    );
    // SQLite estimates based on statistics (none inserted in our test data, but estimates conservatively)
    expect(rows).toBe(1792);
  });

  test('inline values work with array literals in IN clauses', () => {
    // This test verifies that array values are properly inlined as JSON
    const {rows} = costModel(
      'foo',
      [['a', 'asc']],
      {
        type: 'simple',
        left: {type: 'column', name: 'a'},
        op: 'IN',
        right: {type: 'literal', value: [1, 4, 7, 10]},
      },
      undefined,
    );
    // SQLite can estimate based on the array size
    expect(rows).toBeGreaterThan(0);
    expect(rows).toBeLessThan(1920);
  });
});

describe('SQLite cost model with skewed data (STAT4 verification)', () => {
  let db: Database;
  let costModel: ReturnType<typeof createSQLiteCostModel>;

  beforeEach(() => {
    const lc = createSilentLogContext();
    db = new Database(lc, ':memory:');

    // Create table with proper types for computeZqlSpecs
    db.exec(`
      CREATE TABLE skewed (id INTEGER PRIMARY KEY, value INTEGER);
      CREATE UNIQUE INDEX skewed_id_unique ON skewed(id);
      CREATE INDEX idx_skewed_value ON skewed(value);
    `);

    // Insert SKEWED data:
    // - 1900 rows with value=1 (common)
    // - 100 rows with value=999 (rare)
    const stmt = db.prepare('INSERT INTO skewed (id, value) VALUES (?, ?)');
    for (let i = 0; i < 1900; i++) {
      stmt.run(i, 1);
    }
    for (let i = 1900; i < 2000; i++) {
      stmt.run(i, 999);
    }

    // Run ANALYZE to populate STAT4 statistics
    db.exec('ANALYZE');

    // Get table specs
    const tableSpecs = new Map<string, LiteAndZqlSpec>();
    computeZqlSpecs(lc, db, tableSpecs);

    // Create the cost model
    costModel = createSQLiteCostModel(db, tableSpecs);
  });

  test('value inlining leverages STAT4 for accurate estimates on rare values', () => {
    // Query for the RARE value (999) - only 100 out of 2000 rows
    const rareEstimate = costModel(
      'skewed',
      [['id', 'asc']],
      {
        type: 'simple',
        left: {type: 'column', name: 'value'},
        op: '=',
        right: {type: 'literal', value: 999},
      },
      undefined,
    );

    // With inlined values and STAT4, SQLite should recognize 999 is rare
    // and estimate close to the actual 100 rows
    expect(rareEstimate.rows).toBeLessThan(500); // Much less than average (1000)
    expect(rareEstimate.rows).toBeGreaterThan(50); // But not zero
  });

  test('value inlining leverages STAT4 for accurate estimates on common values', () => {
    // Query for the COMMON value (1) - 1900 out of 2000 rows
    const commonEstimate = costModel(
      'skewed',
      [['id', 'asc']],
      {
        type: 'simple',
        left: {type: 'column', name: 'value'},
        op: '=',
        right: {type: 'literal', value: 1},
      },
      undefined,
    );

    // Should recognize 1 is common and estimate much higher
    expect(commonEstimate.rows).toBeGreaterThan(1500);
    expect(commonEstimate.rows).toBeLessThan(2000);
  });
});
