/* oxlint-disable require-await */
import {describe, expect, expectTypeOf, test, vi} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import type {RunnableQuery} from '../../../zql/src/query/runnable-query.ts';
import type {QueryDelegate} from '../../../zql/src/query/query-delegate.ts';
import {QueryImpl} from '../../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';

describe('RunnableQuery', () => {
  // Simple test schema
  const issueTable = table('issue')
    .columns({
      id: string(),
      title: string(),
      status: string(),
      priority: number(),
    })
    .primaryKey('id');

  const schema = createSchema({
    tables: [issueTable],
  });

  // Mock delegate
  const createMockDelegate = (): QueryDelegate => ({
    run: vi.fn(async () => []),
    materialize: vi.fn(),
    preload: vi.fn(() => ({
      cleanup: () => {},
      complete: Promise.resolve(),
    })),
    batchViewUpdates: vi.fn(fn => fn()),
    createStorage: vi.fn(),
    decorateSourceInput: vi.fn((input, _queryID) => input),
    decorateInput: vi.fn((input, _name) => input),
    decorateFilterInput: vi.fn((input, _name) => input),
    addEdge: vi.fn(),
    addMetric: vi.fn(),
    addServerQuery: vi.fn(() => () => {}),
    addCustomQuery: vi.fn(() => () => {}),
    updateServerQuery: vi.fn(),
    updateCustomQuery: vi.fn(),
    flushQueryChanges: vi.fn(),
    onTransactionCommit: vi.fn(() => () => {}),
    assertValidRunOptions: vi.fn(),
    defaultQueryComplete: false,
    getSource: vi.fn(),
  });

  test('query created with delegate has run method', async () => {
    const delegate = createMockDelegate();
    const query = new QueryImpl(delegate, schema, 'issue');

    // Type check: query should be assignable to RunnableQuery
    expectTypeOf(query).toMatchTypeOf<RunnableQuery<typeof schema, 'issue'>>();

    // Test that run method exists and calls delegate
    await query.run();
    expect(delegate.run).toHaveBeenCalledWith(query, undefined);
  });

  test('query created without delegate throws error on run', async () => {
    const query = new QueryImpl(undefined, schema, 'issue');

    await expect(query.run()).rejects.toThrow(
      /Cannot call run\(\) on a query that is not attached to a Zero instance/,
    );
  });

  test('run method passes options to delegate', async () => {
    const delegate = createMockDelegate();
    const query = new QueryImpl(delegate, schema, 'issue');
    const options = {type: 'complete' as const};

    await query.run(options);
    expect(delegate.run).toHaveBeenCalledWith(query, options);
  });

  test('chained query methods preserve runnable type', async () => {
    const delegate = createMockDelegate();
    const query = new QueryImpl(delegate, schema, 'issue');

    // Chain multiple methods
    const chainedQuery = query
      .where('status', 'open')
      .where('priority', '>', 1)
      .limit(10)
      .orderBy('priority', 'desc');

    // Type check: chained query should still have run method
    expectTypeOf(chainedQuery).toHaveProperty('run');

    // Should be able to call run
    await chainedQuery.run();
    expect(delegate.run).toHaveBeenCalled();

    // Verify AST was built correctly
    const internals = asQueryInternals(chainedQuery);
    const ast = internals.ast;
    expect(ast.where).toBeDefined();
    expect(ast.limit).toBe(10);
    expect(ast.orderBy).toBeDefined();
  });

  test('one() method preserves runnable type', async () => {
    const delegate = createMockDelegate();
    const query = new QueryImpl(delegate, schema, 'issue');

    const oneQuery = query.where('status', 'open').one();

    // Type check
    expectTypeOf(oneQuery).toHaveProperty('run');

    // Should be able to call run
    await oneQuery.run();
    expect(delegate.run).toHaveBeenCalled();

    // Verify AST has limit: 1
    const internals = asQueryInternals(oneQuery);
    const ast = internals.ast;
    expect(ast.limit).toBe(1);
  });

  // TODO: Add whereExists test with relationships

  test('delegate is passed through chained queries', async () => {
    const delegate = createMockDelegate();
    const query = new QueryImpl(delegate, schema, 'issue');

    // Create a chain
    const q1 = query.where('status', 'open');
    const q2 = q1.limit(10);
    const q3 = q2.orderBy('priority', 'desc');

    // All queries in the chain should be runnable
    await q1.run();
    await q2.run();
    await q3.run();

    expect(delegate.run).toHaveBeenCalledTimes(3);
  });

  test('run returns results from delegate', async () => {
    const mockResults = [
      {id: '1', title: 'Issue 1', status: 'open', priority: 1},
      {id: '2', title: 'Issue 2', status: 'open', priority: 2},
    ];

    const delegate = createMockDelegate();
    (delegate.run as ReturnType<typeof vi.fn>).mockResolvedValue(mockResults);

    const query = new QueryImpl(delegate, schema, 'issue');
    const results = await query.run();

    expect(results).toEqual(mockResults);
  });

  test('run propagates errors from delegate', async () => {
    const error = new Error('Query failed');
    const delegate = createMockDelegate();
    (delegate.run as ReturnType<typeof vi.fn>).mockRejectedValue(error);

    const query = new QueryImpl(delegate, schema, 'issue');

    await expect(query.run()).rejects.toThrow('Query failed');
  });
});
