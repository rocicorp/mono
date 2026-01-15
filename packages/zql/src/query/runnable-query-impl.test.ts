import {describe, expect, test} from 'vitest';
import {newRunnableQuery} from './runnable-query-impl.ts';
import {QueryDelegateImpl} from './test/query-delegate.ts';
import {schema} from './test/test-schemas.ts';

describe('RunnableQueryImpl run/preload/materialize', () => {
  test('run() works on runnable query', async () => {
    const queryDelegate = new QueryDelegateImpl({callGot: true});
    const issueQuery = newRunnableQuery(queryDelegate, schema, 'issue');
    const result = await issueQuery.run();
    expect(result).toEqual([]);
  });

  test('preload() works on runnable query', () => {
    const queryDelegate = new QueryDelegateImpl();
    const issueQuery = newRunnableQuery(queryDelegate, schema, 'issue');
    const {cleanup, complete} = issueQuery.preload();
    expect(typeof cleanup).toBe('function');
    expect(complete).toBeInstanceOf(Promise);
    cleanup();
  });

  test('materialize() works on runnable query', () => {
    const queryDelegate = new QueryDelegateImpl();
    const issueQuery = newRunnableQuery(queryDelegate, schema, 'issue');
    const view = issueQuery.materialize();
    expect(view).toBeDefined();
    expect(view.data).toEqual([]);
    view.destroy();
  });

  test('run() works on chained runnable query', async () => {
    const queryDelegate = new QueryDelegateImpl({callGot: true});
    const issueQuery = newRunnableQuery(queryDelegate, schema, 'issue')
      .where('id', '0001')
      .related('owner')
      .orderBy('id', 'asc')
      .limit(10);
    const result = await issueQuery.run();
    expect(result).toEqual([]);
  });

  test('preload() works on chained runnable query', () => {
    const queryDelegate = new QueryDelegateImpl();
    const issueQuery = newRunnableQuery(queryDelegate, schema, 'issue')
      .where('id', '0001')
      .related('owner')
      .orderBy('id', 'asc')
      .limit(10);
    const {cleanup, complete} = issueQuery.preload();
    expect(typeof cleanup).toBe('function');
    expect(complete).toBeInstanceOf(Promise);
    cleanup();
  });

  test('materialize() works on chained runnable query', () => {
    const queryDelegate = new QueryDelegateImpl();
    const issueQuery = newRunnableQuery(queryDelegate, schema, 'issue')
      .where('id', '0001')
      .related('owner')
      .orderBy('id', 'asc')
      .limit(10);
    const view = issueQuery.materialize();
    expect(view).toBeDefined();
    view.destroy();
  });

  test('one() on runnable query works with run()', async () => {
    const queryDelegate = new QueryDelegateImpl({callGot: true});
    const issueQuery = newRunnableQuery(queryDelegate, schema, 'issue').one();
    const result = await issueQuery.run();
    expect(result).toBeUndefined();
  });

  test('materialize() with custom TTL', () => {
    const queryDelegate = new QueryDelegateImpl();
    const issueQuery = newRunnableQuery(queryDelegate, schema, 'issue');
    const view = issueQuery.materialize(5000);
    expect(view).toBeDefined();
    view.destroy();
  });
});
