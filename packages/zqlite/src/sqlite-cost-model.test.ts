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
    // SQLite estimates 480 rows (25% selectivity)
    // With 50x correction for unindexed equality: 480 / 50 = 9.6 → 9 rows
    // This matches PostgreSQL's 0.5% selectivity assumption
    expect(rows).toBe(9);
    expect(startupCost).toBe(0);
  });

  test('multiple equality checks on non-indexed columns', () => {
    // SELECT * FROM foo WHERE b = 2 AND c = 3 ORDER BY a
    // Both b and c are unindexed - compound correction should apply
    const {rows, startupCost} = costModel(
      'foo',
      [['a', 'asc']],
      {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            left: {type: 'column', name: 'b'},
            op: '=',
            right: {type: 'literal', value: 2},
          },
          {
            type: 'simple',
            left: {type: 'column', name: 'c'},
            op: '=',
            right: {type: 'literal', value: 3},
          },
        ],
      },
      undefined,
    );
    // SQLite estimates 120 rows (25% * 25% selectivity)
    // With 50^2 correction for 2 unindexed equalities: 120 / 2500 = 0.048 → 1 row
    // This matches PostgreSQL's compound selectivity (0.5% * 0.5% = 0.0025%)
    expect(rows).toBe(1);
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
});
