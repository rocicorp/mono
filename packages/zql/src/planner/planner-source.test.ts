import {expect, suite, test} from 'vitest';
import {PlannerSource} from './planner-source.ts';
import {simpleCostModel} from './test/helpers.ts';

suite('PlannerSource', () => {
  test('creates source with table name', () => {
    const source = new PlannerSource('users', simpleCostModel);
    expect(source).toBeDefined();
  });

  test('connect() returns PlannerConnection', () => {
    const source = new PlannerSource('users', simpleCostModel);
    const connection = source.connect([['id', 'asc']], undefined);

    expect(connection.kind).toBe('connection');
  });

  test('connect() with simple condition', () => {
    const source = new PlannerSource('users', simpleCostModel);
    const condition = {
      type: 'simple' as const,
      op: '=' as const,
      left: {type: 'column' as const, name: 'id'},
      right: {type: 'literal' as const, value: '123'},
    };

    const connection = source.connect([['id', 'asc']], condition);
    expect(connection.kind).toBe('connection');
  });

  test('multiple connect() calls create independent connections', () => {
    const source = new PlannerSource('users', simpleCostModel);

    const conn1 = source.connect([['id', 'asc']], undefined);
    const conn2 = source.connect([['name', 'asc']], undefined);

    expect(conn1).not.toBe(conn2);
    expect(conn1.kind).toBe('connection');
    expect(conn2.kind).toBe('connection');
  });
});
