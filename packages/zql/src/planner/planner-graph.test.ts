import {expect, suite, test} from 'vitest';
import {PlannerGraph, UnflippableJoinError} from './planner-graph.ts';
import {PlannerJoin} from './planner-join.ts';
import {PlannerTerminus} from './planner-terminus.ts';
import {simpleCostModel} from './test/helpers.ts';

suite('PlannerGraph', () => {
  test('can add and retrieve sources', () => {
    const graph = new PlannerGraph();

    const source = graph.addSource('users', simpleCostModel);
    expect(source).toBeDefined();

    const retrieved = graph.getSource('users');
    expect(retrieved).toBe(source);
  });

  test('throws when adding duplicate source', () => {
    const graph = new PlannerGraph();
    graph.addSource('users', simpleCostModel);

    expect(() => graph.addSource('users', simpleCostModel)).toThrow(
      'Source users already exists in the graph',
    );
  });

  test('throws when getting non-existent source', () => {
    const graph = new PlannerGraph();

    expect(() => graph.getSource('users')).toThrow(
      'Source users not found in the graph',
    );
  });

  test('can set terminus', () => {
    const graph = new PlannerGraph();
    const source = graph.addSource('users', simpleCostModel);
    const connection = source.connect([['id', 'asc']], undefined);
    graph.connections.push(connection);

    const terminus = new PlannerTerminus(connection);
    connection.setOutput(terminus); // Wire the connection to terminus

    // Should be able to set terminus without error
    expect(() => graph.setTerminus(terminus)).not.toThrow();

    // Should be able to propagate constraints (connection not pinned, so no error)
    expect(() => graph.propagateConstraints()).not.toThrow();
  });

  test('getUnpinnedConnections() returns only unpinned connections', () => {
    const graph = new PlannerGraph();
    const source = graph.addSource('users', simpleCostModel);

    const conn1 = source.connect([['id', 'asc']], undefined);
    const conn2 = source.connect([['name', 'asc']], undefined);
    const conn3 = source.connect([['email', 'asc']], undefined);

    graph.connections.push(conn1, conn2, conn3);

    // Pin conn2
    conn2.pinned = true;

    const unpinned = graph.getUnpinnedConnections();
    expect(unpinned).toHaveLength(2);
    expect(unpinned).toContain(conn1);
    expect(unpinned).toContain(conn3);
    expect(unpinned).not.toContain(conn2);
  });

  test('estimateCosts() returns connections sorted by cost', () => {
    const graph = new PlannerGraph();
    const source = graph.addSource('users', simpleCostModel);

    const conn1 = source.connect([['id', 'asc']], undefined);
    const conn2 = source.connect([['name', 'asc']], undefined);
    const conn3 = source.connect([['email', 'asc']], undefined);

    graph.connections.push(conn1, conn2, conn3);

    // Add different constraints to create different costs
    conn1.propagateConstraints([0], {a: 'string', b: 'string'}, 'unpinned'); // 80
    conn2.propagateConstraints([0], {a: 'string'}, 'unpinned'); // 90
    conn3.propagateConstraints([0], undefined, 'unpinned'); // 100

    const costs = graph.estimateCosts();
    expect(costs).toHaveLength(3);
    expect(costs[0].connection).toBe(conn1); // Lowest cost (80)
    expect(costs[0].cost).toBe(80);
    expect(costs[1].connection).toBe(conn2); // Middle cost (90)
    expect(costs[1].cost).toBe(90);
    expect(costs[2].connection).toBe(conn3); // Highest cost (100)
    expect(costs[2].cost).toBe(100);
  });

  test('hasPlan() returns false when not all connections pinned', () => {
    const graph = new PlannerGraph();
    const source = graph.addSource('users', simpleCostModel);

    const conn1 = source.connect([['id', 'asc']], undefined);
    const conn2 = source.connect([['name', 'asc']], undefined);

    graph.connections.push(conn1, conn2);

    conn1.pinned = true;

    expect(graph.hasPlan()).toBe(false);
  });

  test('hasPlan() returns true when all connections pinned', () => {
    const graph = new PlannerGraph();
    const source = graph.addSource('users', simpleCostModel);

    const conn1 = source.connect([['id', 'asc']], undefined);
    const conn2 = source.connect([['name', 'asc']], undefined);

    graph.connections.push(conn1, conn2);

    conn1.pinned = true;
    conn2.pinned = true;

    expect(graph.hasPlan()).toBe(true);
  });

  test('getPlanSummary() returns correct counts', () => {
    const graph = new PlannerGraph();
    const source = graph.addSource('users', simpleCostModel);

    const conn1 = source.connect([['id', 'asc']], undefined);
    const conn2 = source.connect([['name', 'asc']], undefined);

    graph.connections.push(conn1, conn2);
    conn1.pinned = true;

    const join = new PlannerJoin(
      conn1,
      conn2,
      {userId: 'string'},
      {id: 'string'},
      true,
    );
    graph.joins.push(join);
    join.flip(); // Flip BEFORE pinning
    join.pin();

    const summary = graph.getPlanSummary();
    expect(summary.totalConnections).toBe(2);
    expect(summary.pinnedConnections).toBe(1);
    expect(summary.unpinnedConnections).toBe(1);
    expect(summary.totalJoins).toBe(1);
    expect(summary.pinnedJoins).toBe(1);
    expect(summary.flippedJoins).toBe(1);
  });

  test('getTotalCost() multiplies all connection costs', () => {
    const graph = new PlannerGraph();
    const source = graph.addSource('users', simpleCostModel);

    const conn1 = source.connect([['id', 'asc']], undefined);
    const conn2 = source.connect([['name', 'asc']], undefined);

    graph.connections.push(conn1, conn2);

    conn1.propagateConstraints([0], {a: 'string'}, 'unpinned'); // 90
    conn2.propagateConstraints([0], {a: 'string', b: 'string'}, 'unpinned'); // 80

    const totalCost = graph.getTotalCost();
    expect(totalCost).toBeCloseTo(7200, 0); // 90 × 80 = 7200
  });

  test('resetPlanningState() resets all nodes to initial state', () => {
    const graph = new PlannerGraph();
    const source = graph.addSource('users', simpleCostModel);

    const conn = source.connect([['id', 'asc']], undefined);
    graph.connections.push(conn);

    const join = new PlannerJoin(
      conn,
      conn,
      {userId: 'string'},
      {id: 'string'},
      true,
    );
    graph.joins.push(join);

    // Modify state
    conn.pinned = true;
    join.flip();
    join.pin();

    // Reset
    graph.resetPlanningState();

    // Verify reset
    expect(conn.pinned).toBe(false);
    expect(join.type).toBe('left');
    expect(join.pinned).toBe(false);
  });

  test('capturePlanningSnapshot() and restorePlanningSnapshot() preserve state', () => {
    const graph = new PlannerGraph();
    const source = graph.addSource('users', simpleCostModel);

    const conn = source.connect([['id', 'asc']], undefined);
    graph.connections.push(conn);

    const join = new PlannerJoin(
      conn,
      conn,
      {userId: 'string'},
      {id: 'string'},
      true,
    );
    graph.joins.push(join);

    // Set initial state
    conn.pinned = true;
    join.flip();
    join.pin();

    // Save
    const saved = graph.capturePlanningSnapshot();

    // Modify state
    graph.resetPlanningState();
    expect(conn.pinned).toBe(false);
    expect(join.type).toBe('left');

    // Restore
    graph.restorePlanningSnapshot(saved);
    expect(conn.pinned).toBe(true);
    expect(join.type).toBe('flipped');
    expect(join.pinned).toBe(true);
  });
});

suite('UnflippableJoinError', () => {
  test('is an instance of Error', () => {
    const error = new UnflippableJoinError('test message');
    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe('UnflippableJoinError');
    expect(error.message).toBe('test message');
  });
});
