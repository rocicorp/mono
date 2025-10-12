import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Database} from './db.ts';
import {createSQLiteCostModel} from './sqlite-cost-model.ts';

describe('SQLite cost model', () => {
  test('estimates costs based on query complexity and indexes', () => {
    const db = new Database(createSilentLogContext(), ':memory:');

    // Create schema with indexes
    db.exec(/* sql */ `
      CREATE TABLE issue (
        id TEXT PRIMARY KEY,
        creatorId TEXT,
        assigneeId TEXT,
        title TEXT,
        created NUMBER,
        modified NUMBER
      );

      CREATE INDEX issue_creator_modified ON issue (creatorId, modified);
      CREATE INDEX issue_assignee_modified ON issue (assigneeId, modified);
      CREATE INDEX modified ON issue (modified);
    `);

    // Insert ~1000 rows of test data
    const insertStmt = db.prepare(/* sql */ `
      INSERT INTO issue (id, creatorId, assigneeId, title, created, modified)
      VALUES (?, ?, ?, ?, ?, ?)
    `);

    const creators = ['user1', 'user2', 'user3', 'user4', 'user5'];
    const assignees = ['user6', 'user7', 'user8', 'user9', 'user10'];

    for (let i = 0; i < 1000; i++) {
      const creatorId = creators[i % creators.length];
      const assigneeId = assignees[i % assignees.length];
      const created = Date.now() - i * 1000 * 60 * 60; // Spread over time
      const modified = created + Math.random() * 1000 * 60 * 60 * 24; // Modified within 24h

      insertStmt.run(
        `issue-${i}`,
        creatorId,
        assigneeId,
        `Issue ${i}`,
        created,
        modified,
      );
    }

    // Run ANALYZE to update statistics
    db.exec('ANALYZE');

    // Create cost model
    const columns = [
      'id',
      'creatorId',
      'assigneeId',
      'title',
      'created',
      'modified',
    ];
    const costModel = createSQLiteCostModel(db, 'issue', columns);

    // Test 1: Query with no constraints - should be very expensive (full table scan)
    const cost1 = costModel(
      [['modified', 'desc']], // Uses modified index
      undefined,
      undefined,
    );

    // Test 2: Query with creatorId constraint - should be relatively cheap (uses index)
    const cost2 = costModel([['modified', 'desc']], undefined, {
      creatorId: 'user1',
    });

    // Test 3: Query with creatorId AND modified constraint - should be cheapest
    // (uses composite index issue_creator_modified optimally)
    const cost3 = costModel(
      [['modified', 'desc']],
      {
        type: 'simple',
        left: {type: 'column', name: 'modified'},
        op: '>',
        right: {type: 'literal', value: Date.now() - 1000 * 60 * 60 * 24 * 30},
      },
      {creatorId: 'user1'},
    );

    // Test 4: Query with non-indexed constraint - should be very expensive (full scan)
    const cost4 = costModel(
      [['modified', 'desc']],
      {
        type: 'simple',
        left: {type: 'column', name: 'title'},
        op: '=',
        right: {type: 'literal', value: 'Issue 42'},
      },
      undefined,
    );

    // Test 5: Query with creatorId constraint but non-indexed ORDER BY
    // Should be more expensive than cost2 due to temp b-tree for sorting
    const cost5 = costModel(
      [['created', 'desc']], // created is not in the index with creatorId
      undefined,
      {creatorId: 'user1'},
    );

    // Verify relative costs
    console.log('Cost analysis:');
    console.log(`  No constraints (full scan): ${cost1}`);
    console.log(`  With creatorId constraint: ${cost2}`);
    console.log(`  With creatorId + modified filter: ${cost3}`);
    console.log(`  With non-indexed title filter: ${cost4}`);
    console.log(`  With creatorId constraint, ORDER BY created: ${cost5}`);

    // Cost assertions - verify relative ordering
    // Full scan should be expensive
    expect(cost1).toBeGreaterThan(100);

    // Non-indexed filter should be very expensive (close to full scan)
    expect(cost4).toBeGreaterThan(100);

    // Indexed queries should be much cheaper than full scan
    expect(cost2).toBeLessThan(cost1 / 2);
    expect(cost3).toBeLessThan(cost1 / 2);

    // Double constraint should be cheaper than single constraint
    expect(cost3).toBeLessThan(cost2);

    // Non-indexed ORDER BY should be at least as expensive as indexed ORDER BY
    // This is because it may require a temp b-tree (additional loop)
    // In practice, SQLite may optimize this away if the result set is small
    expect(cost5).toBeGreaterThanOrEqual(cost2);

    db.close();
  });

  test('handles queries without scanstatus data gracefully', () => {
    const db = new Database(createSilentLogContext(), ':memory:');

    // Create simple table without data or indexes
    db.exec(/* sql */ `
      CREATE TABLE simple (
        id TEXT PRIMARY KEY,
        value TEXT
      );
    `);

    const columns = ['id', 'value'];
    const costModel = createSQLiteCostModel(db, 'simple', columns);

    // Even without ANALYZE or data, should return fallback costs
    const costNoConstraint = costModel([['id', 'asc']], undefined, undefined);
    const costWithConstraint = costModel([['id', 'asc']], undefined, {
      id: 'test',
    });

    // Should get default fallback costs
    expect(costNoConstraint).toBeGreaterThan(0);
    expect(costWithConstraint).toBeGreaterThan(0);

    // With constraint should be cheaper than without
    expect(costWithConstraint).toBeLessThan(costNoConstraint);

    db.close();
  });

  test('handles complex filter conditions', () => {
    const db = new Database(createSilentLogContext(), ':memory:');

    db.exec(/* sql */ `
      CREATE TABLE issue (
        id TEXT PRIMARY KEY,
        creatorId TEXT,
        status TEXT,
        priority NUMBER
      );

      CREATE INDEX issue_creator_status ON issue (creatorId, status);
    `);

    // Insert test data
    const insertStmt = db.prepare(/* sql */ `
      INSERT INTO issue (id, creatorId, status, priority) VALUES (?, ?, ?, ?)
    `);

    for (let i = 0; i < 500; i++) {
      insertStmt.run(
        `issue-${i}`,
        `user${i % 5}`,
        ['open', 'closed', 'pending'][i % 3],
        Math.floor(Math.random() * 10),
      );
    }

    db.exec('ANALYZE');

    const columns = ['id', 'creatorId', 'status', 'priority'];
    const costModel = createSQLiteCostModel(db, 'issue', columns);

    // Test with AND condition
    const costAnd = costModel(
      [['id', 'asc']],
      {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            left: {type: 'column', name: 'status'},
            op: '=',
            right: {type: 'literal', value: 'open'},
          },
          {
            type: 'simple',
            left: {type: 'column', name: 'priority'},
            op: '>',
            right: {type: 'literal', value: 5},
          },
        ],
      },
      {creatorId: 'user1'},
    );

    // Test with OR condition (typically more expensive)
    const costOr = costModel(
      [['id', 'asc']],
      {
        type: 'or',
        conditions: [
          {
            type: 'simple',
            left: {type: 'column', name: 'status'},
            op: '=',
            right: {type: 'literal', value: 'open'},
          },
          {
            type: 'simple',
            left: {type: 'column', name: 'status'},
            op: '=',
            right: {type: 'literal', value: 'closed'},
          },
        ],
      },
      {creatorId: 'user1'},
    );

    console.log('Complex filter costs:');
    console.log(`  AND condition: ${costAnd}`);
    console.log(`  OR condition: ${costOr}`);

    // Both should return reasonable costs
    expect(costAnd).toBeGreaterThan(0);
    expect(costOr).toBeGreaterThan(0);

    db.close();
  });

  test('different orderings affect cost', () => {
    const db = new Database(createSilentLogContext(), ':memory:');

    db.exec(/* sql */ `
      CREATE TABLE task (
        id TEXT PRIMARY KEY,
        projectId TEXT,
        status TEXT,
        priority NUMBER,
        created NUMBER
      );

      CREATE INDEX task_project_status ON task (projectId, status);
      CREATE INDEX task_project_created ON task (projectId, created);
    `);

    // Insert test data
    const insertStmt = db.prepare(/* sql */ `
      INSERT INTO task (id, projectId, status, priority, created) VALUES (?, ?, ?, ?, ?)
    `);

    for (let i = 0; i < 500; i++) {
      insertStmt.run(
        `task-${i}`,
        `proj${i % 10}`,
        ['todo', 'doing', 'done'][i % 3],
        i % 5,
        Date.now() - i * 1000,
      );
    }

    db.exec('ANALYZE');

    const columns = ['id', 'projectId', 'status', 'priority', 'created'];
    const costModel = createSQLiteCostModel(db, 'task', columns);

    // Order by indexed column (projectId, status) - should be cheap
    const costIndexedOrder = costModel(
      [
        ['projectId', 'asc'],
        ['status', 'asc'],
      ],
      undefined,
      {projectId: 'proj1'},
    );

    // Order by indexed column (projectId, created) - should be cheap
    const costIndexedOrder2 = costModel(
      [
        ['projectId', 'asc'],
        ['created', 'desc'],
      ],
      undefined,
      {projectId: 'proj1'},
    );

    // Order by non-indexed column - should be more expensive (requires sort)
    const costNonIndexedOrder = costModel([['priority', 'asc']], undefined, {
      projectId: 'proj1',
    });

    console.log('Ordering costs:');
    console.log(`  Indexed (projectId, status): ${costIndexedOrder}`);
    console.log(`  Indexed (projectId, created): ${costIndexedOrder2}`);
    console.log(`  Non-indexed (priority): ${costNonIndexedOrder}`);

    // All should return valid costs
    expect(costIndexedOrder).toBeGreaterThan(0);
    expect(costIndexedOrder2).toBeGreaterThan(0);
    expect(costNonIndexedOrder).toBeGreaterThan(0);

    // Non-indexed ORDER BY typically requires temp b-tree, making it more expensive
    expect(costNonIndexedOrder).toBeGreaterThanOrEqual(costIndexedOrder);
    expect(costNonIndexedOrder).toBeGreaterThanOrEqual(costIndexedOrder2);

    db.close();
  });
});
