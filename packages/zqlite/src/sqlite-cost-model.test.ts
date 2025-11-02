import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Database} from './db.ts';
import {
  btreeCost,
  createSQLiteCostModel,
  getStat1Fanout,
} from './sqlite-cost-model.ts';
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
      a: {},
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
});

describe('getStat1Fanout', () => {
  let db: Database;

  beforeEach(() => {
    const lc = createSilentLogContext();
    db = new Database(lc, ':memory:');
  });

  test('returns fanout from single-column index', () => {
    // Create parent and child tables with 1:5 relationship
    db.exec(`
      CREATE TABLE parent (id INTEGER PRIMARY KEY);
      CREATE TABLE child (id INTEGER PRIMARY KEY, parentId INTEGER);
      CREATE INDEX child_parentId ON child(parentId);
    `);

    // Insert 10 parent rows, each with 5 children
    const parentStmt = db.prepare('INSERT INTO parent (id) VALUES (?)');
    const childStmt = db.prepare(
      'INSERT INTO child (id, parentId) VALUES (?, ?)',
    );

    for (let i = 0; i < 10; i++) {
      parentStmt.run(i);
      for (let j = 0; j < 5; j++) {
        childStmt.run(i * 5 + j, i);
      }
    }

    // Run ANALYZE to populate statistics
    db.exec('ANALYZE');

    // Query fanout for parentId constraint
    const fanout = getStat1Fanout(db, 'child', {parentId: {}});

    // Should be 5 (average 5 children per parent)
    expect(fanout).toBe(5);
  });

  test('returns fanout from composite index', () => {
    // Create table with composite foreign key
    db.exec(`
      CREATE TABLE orders (
        id INTEGER PRIMARY KEY,
        userId INTEGER,
        productId INTEGER
      );
      CREATE INDEX orders_user_product ON orders(userId, productId);
    `);

    // Insert data: 5 users, each with 2 products, 3 orders per (user, product)
    const stmt = db.prepare(
      'INSERT INTO orders (id, userId, productId) VALUES (?, ?, ?)',
    );

    let id = 0;
    for (let userId = 0; userId < 5; userId++) {
      for (let productId = 0; productId < 2; productId++) {
        for (let orderNum = 0; orderNum < 3; orderNum++) {
          stmt.run(id++, userId, productId);
        }
      }
    }

    db.exec('ANALYZE');

    // Query fanout for userId constraint (should be ~6 orders per user)
    const userFanout = getStat1Fanout(db, 'orders', {userId: {}});
    expect(userFanout).toBe(6);

    // Query fanout for userId+productId constraint (should be 3 orders per combo)
    const comboFanout = getStat1Fanout(db, 'orders', {
      userId: {},
      productId: {},
    });
    expect(comboFanout).toBe(3);
  });

  test('uses best matching index for partial constraint', () => {
    db.exec(`
      CREATE TABLE items (
        id INTEGER PRIMARY KEY,
        categoryId INTEGER,
        subcategoryId INTEGER,
        name TEXT
      );
      CREATE INDEX items_category_subcategory ON items(categoryId, subcategoryId);
    `);

    // Insert 3 categories, each with 2 subcategories, 4 items per subcategory
    const stmt = db.prepare(
      'INSERT INTO items (id, categoryId, subcategoryId, name) VALUES (?, ?, ?, ?)',
    );

    let id = 0;
    for (let cat = 0; cat < 3; cat++) {
      for (let subcat = 0; subcat < 2; subcat++) {
        for (let item = 0; item < 4; item++) {
          stmt.run(id++, cat, subcat, `item-${id}`);
        }
      }
    }

    db.exec('ANALYZE');

    // Query fanout for just categoryId (should use first column of composite index)
    const categoryFanout = getStat1Fanout(db, 'items', {categoryId: {}});
    expect(categoryFanout).toBe(8); // 8 items per category
  });

  test('returns undefined when no suitable index exists', () => {
    db.exec(`
      CREATE TABLE unindexed (id INTEGER PRIMARY KEY, value INTEGER);
    `);

    db.exec('INSERT INTO unindexed (id, value) VALUES (1, 100), (2, 200)');
    db.exec('ANALYZE');

    // No index on 'value' column
    const fanout = getStat1Fanout(db, 'unindexed', {value: {}});
    expect(fanout).toBeUndefined();
  });

  test('returns undefined when no statistics available', () => {
    db.exec(`
      CREATE TABLE nostats (id INTEGER PRIMARY KEY, parentId INTEGER);
      CREATE INDEX nostats_parentId ON nostats(parentId);
    `);

    db.exec('INSERT INTO nostats (id, parentId) VALUES (1, 100)');
    // Don't run ANALYZE - no statistics

    const fanout = getStat1Fanout(db, 'nostats', {parentId: {}});
    expect(fanout).toBeUndefined();
  });

  test('returns undefined for empty constraint', () => {
    db.exec(`
      CREATE TABLE empty (id INTEGER PRIMARY KEY);
      CREATE INDEX empty_id ON empty(id);
    `);

    const fanout = getStat1Fanout(db, 'empty', {});
    expect(fanout).toBeUndefined();
  });

  test('handles index column order mismatch', () => {
    db.exec(`
      CREATE TABLE ordered (
        id INTEGER PRIMARY KEY,
        a INTEGER,
        b INTEGER,
        c INTEGER
      );
      CREATE INDEX ordered_abc ON ordered(a, b, c);
    `);

    // Insert data with known fanout
    const stmt = db.prepare(
      'INSERT INTO ordered (id, a, b, c) VALUES (?, ?, ?, ?)',
    );
    for (let i = 0; i < 20; i++) {
      stmt.run(i, i % 2, i % 4, i % 5); // 2 values for a, 4 for (a,b), 5 for (a,b,c)
    }

    db.exec('ANALYZE');

    // Constraint with columns in different order than index
    // Index is (a, b, c) but constraint is {b, a}
    // Should NOT match because order doesn't match index prefix
    const fanout = getStat1Fanout(db, 'ordered', {b: {}, a: {}});

    // The function relies on Object.keys() order which in practice follows
    // insertion order, so {b, a} keys would be ['b', 'a'] which doesn't
    // match index prefix ['a', 'b']
    expect(fanout).toBeUndefined();
  });

  test('fanout is returned in cost model result', () => {
    const lc = createSilentLogContext();
    db = new Database(lc, ':memory:');

    // Create parent and child tables
    // Note: computeZqlSpecs requires UNIQUE INDEX on primary key
    db.exec(`
      CREATE TABLE parent (id INTEGER PRIMARY KEY);
      CREATE UNIQUE INDEX parent_id ON parent(id);
      CREATE TABLE child (id INTEGER PRIMARY KEY, parentId INTEGER);
      CREATE UNIQUE INDEX child_id ON child(id);
      CREATE INDEX child_parentId ON child(parentId);
    `);

    // Insert data with 1:5 relationship
    for (let i = 0; i < 10; i++) {
      db.prepare('INSERT INTO parent (id) VALUES (?)').run(i);
      for (let j = 0; j < 5; j++) {
        db.prepare('INSERT INTO child (id, parentId) VALUES (?, ?)').run(
          i * 5 + j,
          i,
        );
      }
    }

    db.exec('ANALYZE');

    const tableSpecs = new Map<string, LiteAndZqlSpec>();
    computeZqlSpecs(lc, db, tableSpecs);

    const costModel = createSQLiteCostModel(db, tableSpecs);

    // Query with constraint should return fanout
    const result = costModel('child', [['id', 'asc']], undefined, {
      parentId: {},
    });

    expect(result.fanOut).toBe(5);
  });

  test('fanout is undefined when no constraint provided', () => {
    const lc = createSilentLogContext();
    db = new Database(lc, ':memory:');

    db.exec(`
      CREATE TABLE simple (id INTEGER PRIMARY KEY);
      CREATE UNIQUE INDEX simple_id ON simple(id);
    `);

    db.exec('INSERT INTO simple (id) VALUES (1), (2), (3)');
    db.exec('ANALYZE');

    const tableSpecs = new Map<string, LiteAndZqlSpec>();
    computeZqlSpecs(lc, db, tableSpecs);

    const costModel = createSQLiteCostModel(db, tableSpecs);

    // Query without constraint should not return fanout
    const result = costModel('simple', [['id', 'asc']], undefined, undefined);

    expect(result.fanOut).toBeUndefined();
  });
});
