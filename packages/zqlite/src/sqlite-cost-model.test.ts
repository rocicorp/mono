import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Database} from './db.ts';
import {createSQLiteCostModel} from './sqlite-cost-model.ts';
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
    const {baseCardinality} = costModel(
      'foo',
      [['a', 'asc']],
      undefined,
      undefined,
    );
    // Expected: (SQLite estimate) = 1920
    expect(baseCardinality).toBe(1920);
  });

  test('table scan ordered by non-indexed column includes sort cost', () => {
    // SELECT * FROM foo ORDER BY b
    // Table scan (~2000 rows) + sort loop (~2000 rows) - expected cost is 3840
    const {startupCost, baseCardinality} = costModel(
      'foo',
      [['b', 'asc']],
      undefined,
      undefined,
    );
    expect(startupCost + baseCardinality).toBe(3840);
  });

  test('primary key lookup via condition', () => {
    const {baseCardinality} = costModel(
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
    expect(baseCardinality).toBe(1);
  });

  test('primary key lookup via constraint', () => {
    const {baseCardinality} = costModel('foo', [['a', 'asc']], undefined, {
      a: undefined,
    });
    expect(baseCardinality).toBe(1);
  });

  test('range check on primary key', () => {
    // SELECT * FROM foo WHERE a > 1 ORDER BY a
    // Should use primary key index for range scan
    const {baseCardinality} = costModel(
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
    expect(baseCardinality).toBe(480);
  });

  test('range check on non-indexed column', () => {
    // SELECT * FROM foo WHERE b > 2 ORDER BY a
    // Requires full table scan since b is not indexed
    const {baseCardinality} = costModel(
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
    expect(baseCardinality).toBe(1792);
  });

  test('equality check on non-indexed column', () => {
    // SELECT * FROM foo WHERE b = 2 ORDER BY a
    // Requires full table scan since b is not indexed
    const {baseCardinality} = costModel(
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
    expect(baseCardinality).toBe(480);
  });

  test('startup cost for index scan is zero', () => {
    // SELECT * FROM foo ORDER BY a
    // Uses primary key index - no sort needed, so startup cost should be 0
    const {startupCost, baseCardinality} = costModel(
      'foo',
      [['a', 'asc']],
      undefined,
      undefined,
    );
    expect(startupCost).toBe(0);
    expect(baseCardinality).toBe(1920);
  });

  test('startup cost for sort is non-zero', () => {
    // SELECT * FROM foo ORDER BY b
    // Requires sort - startup cost should be non-zero
    const {startupCost, baseCardinality} = costModel(
      'foo',
      [['b', 'asc']],
      undefined,
      undefined,
    );
    // Startup cost is the sort operation
    expect(startupCost).toBe(1920);
    // Base cardinality is the scan cost
    expect(baseCardinality).toBe(1920);
  });

  test('total cost with sort matches old behavior', () => {
    // Verify that startupCost + baseCardinality equals the old total cost
    const {startupCost, baseCardinality} = costModel(
      'foo',
      [['b', 'asc']],
      undefined,
      undefined,
    );
    // Old behavior: total cost was 3840
    expect(startupCost + baseCardinality).toBe(3840);
  });

  test('small table (2 rows) - constrained vs unconstrained', () => {
    // Create a new database with a small table
    const lc = createSilentLogContext();
    const smallDb = new Database(lc, ':memory:');

    smallDb.exec(`
      CREATE TABLE small (id INTEGER PRIMARY KEY, name TEXT, _0_version INTEGER);
      CREATE UNIQUE INDEX small_id_unique ON small(id);
    `);

    // Insert only 2 rows (like the project table in zbugs)
    smallDb.exec(`INSERT INTO small (id, name, _0_version) VALUES (1, 'foo', 1), (2, 'bar', 1)`);

    // Run ANALYZE to populate statistics
    smallDb.exec('ANALYZE');

    // Get table specs
    const tableSpecs = new Map<string, LiteAndZqlSpec>();
    computeZqlSpecs(lc, smallDb, tableSpecs);

    // Create cost model for small table
    const smallCostModel = createSQLiteCostModel(smallDb, tableSpecs);

    // Test costs (using id for sort order like other tests)
    const constrainedCost = smallCostModel('small', [['id', 'asc']], undefined, {id: undefined});
    const unconstrainedCost = smallCostModel('small', [['id', 'asc']], undefined, undefined);

    console.log('Small table constrained (id):', constrainedCost);
    console.log('Small table unconstrained ():', unconstrainedCost);

    // The unconstrained scan should cost more than constrained scan
    // This is the bug: both return 1.0, making the planner think they're equal
    expect(unconstrainedCost.baseCardinality).toBeGreaterThan(constrainedCost.baseCardinality);
  });
});
