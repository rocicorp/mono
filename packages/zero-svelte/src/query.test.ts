import {afterEach, describe, expect, test, vi} from 'vitest';
import {MemorySource} from '../../zql/src/ivm/memory-source.ts';
import {makeSourceChangeAdd} from '../../zql/src/ivm/source.ts';
import {QueryDelegateImpl} from '../../zql/src/query/test/query-delegate.ts';
import {
  asQueryInternals,
  consume,
  must,
  newQuery,
  type QueryDelegate,
} from './bindings.ts';
import {Query as SvelteQuery} from './query.svelte.ts';
import {
  createSchema,
  number,
  string,
  table,
  type MaterializeOptions,
  type Query,
  type Schema,
} from './zero.ts';
import {ViewStore} from './zero.svelte.ts';
import type {Z} from './zero.svelte.ts';

function setupTestEnvironment() {
  const schema = createSchema({
    tables: [
      table('table')
        .columns({
          a: number(),
          b: string(),
        })
        .primaryKey('a'),
    ],
  });
  const ms = new MemorySource(
    schema.tables.table.name,
    schema.tables.table.columns,
    schema.tables.table.primaryKey,
  );
  consume(ms.push(makeSourceChangeAdd({a: 1, b: 'a'})));
  consume(ms.push(makeSourceChangeAdd({a: 2, b: 'b'})));

  const queryDelegate = new QueryDelegateImpl({sources: {table: ms}});
  const tableQuery = newQuery(schema, 'table');

  return {ms, tableQuery, queryDelegate, schema};
}

afterEach(() => vi.resetAllMocks());

type C = unknown;

function newMockZero(clientID: string, queryDelegate: QueryDelegate) {
  return {
    clientID,
    context: undefined as C,
    materialize: (query: unknown, options?: MaterializeOptions) =>
      queryDelegate.materialize(
        query as Query<string, Schema, unknown>,
        undefined,
        options,
      ),
  } as Parameters<ViewStore['getView']>[0];
}

describe('ViewStore + ViewWrapper', () => {
  test('basic query returns initial data', () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const viewStore = new ViewStore();
    const mockZero = newMockZero('test-client', queryDelegate);

    const wrapper = viewStore.getView(mockZero, tableQuery, true, '1m');

    expect(wrapper.data).toEqual([
      {a: 1, b: 'a'},
      {a: 2, b: 'b'},
    ]);
    expect(wrapper.details).toEqual({type: 'unknown'});
  });

  test('view updates when data changes', async () => {
    const {ms, tableQuery, queryDelegate} = setupTestEnvironment();
    const viewStore = new ViewStore();
    const mockZero = newMockZero('test-client', queryDelegate);

    const wrapper = viewStore.getView(mockZero, tableQuery, true, '1m');

    must(queryDelegate.gotCallbacks[0])(true);
    await Promise.resolve();

    consume(ms.push(makeSourceChangeAdd({a: 3, b: 'c'})));
    queryDelegate.commit();

    expect(wrapper.data).toEqual([
      {a: 1, b: 'a'},
      {a: 2, b: 'b'},
      {a: 3, b: 'c'},
    ]);
    expect(wrapper.details).toEqual({type: 'complete'});
  });

  test('query complete callback updates details', async () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const viewStore = new ViewStore();
    const mockZero = newMockZero('test-client', queryDelegate);

    const wrapper = viewStore.getView(mockZero, tableQuery, true, '1m');

    expect(wrapper.details).toEqual({type: 'unknown'});

    must(queryDelegate.gotCallbacks[0])(true);
    await Promise.resolve();

    expect(wrapper.details).toEqual({type: 'complete'});
  });

  test('error callback updates details with error info', async () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const viewStore = new ViewStore();
    const mockZero = newMockZero('test-client', queryDelegate);

    const wrapper = viewStore.getView(mockZero, tableQuery, true, '1m');

    must(queryDelegate.gotCallbacks[0])(true, {
      id: 'q1',
      message: 'Something went wrong',
      details: {something: 'went wrong'},
      error: 'app',
      name: 'TestQuery',
    });
    await new Promise(r => setTimeout(r, 0));

    const details = wrapper.details;
    expect(details.type).toBe('error');
    if (details.type === 'error') {
      expect(details.error.message).toBe('Something went wrong');
      expect(details.error.type).toBe('app');
      expect(details.error.details).toEqual({something: 'went wrong'});
    }
  });

  test('retry/refetch on error recreates view', async () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const viewStore = new ViewStore();
    const mockZero = newMockZero('test-client', queryDelegate);

    const wrapper = viewStore.getView(mockZero, tableQuery, true, '1m');

    must(queryDelegate.gotCallbacks[0])(true, {
      id: 'q1',
      message: 'Something went wrong',
      error: 'app',
      name: 'TestQuery',
    });
    await new Promise(r => setTimeout(r, 0));

    const details = wrapper.details;
    expect(details.type).toBe('error');
    if (details.type === 'error') {
      details.refetch();
    }

    expect(wrapper.details).toEqual({type: 'unknown'});
    expect(wrapper.data).toEqual([
      {a: 1, b: 'a'},
      {a: 2, b: 'b'},
    ]);
  });

  test('disabled query returns empty array for plural', () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const viewStore = new ViewStore();
    const mockZero = newMockZero('test-client', queryDelegate);

    const wrapper = viewStore.getView(mockZero, tableQuery, false, '1m');

    expect(wrapper.data).toEqual([]);
    expect(wrapper.details).toEqual({type: 'unknown'});
  });

  test('falsy query returns undefined data', () => {
    const {queryDelegate} = setupTestEnvironment();
    const viewStore = new ViewStore();
    const mockZero = newMockZero('test-client', queryDelegate);

    const wrapper = viewStore.getView(mockZero, undefined, true, '1m');

    expect(wrapper.data).toBeUndefined();
    expect(wrapper.details).toEqual({type: 'unknown'});
  });

  test('same hash reuses existing view', () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const viewStore = new ViewStore();
    const mockZero = newMockZero('test-client', queryDelegate);

    const wrapper1 = viewStore.getView(mockZero, tableQuery, true, '1m');
    const wrapper2 = viewStore.getView(mockZero, tableQuery, true, '1m');

    expect(wrapper1).toBe(wrapper2);
  });

  test('different hash creates different view', () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const viewStore = new ViewStore();
    const mockZero = newMockZero('test-client', queryDelegate);

    const query1 = tableQuery.where('a', 1);
    const query2 = tableQuery.where('a', 2);

    const wrapper1 = viewStore.getView(mockZero, query1, true, '1m');
    const wrapper2 = viewStore.getView(mockZero, query2, true, '1m');

    expect(wrapper1).not.toBe(wrapper2);
  });

  test('different clientID creates different view', () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const viewStore = new ViewStore();
    const mockZero1 = newMockZero('client-a', queryDelegate);
    const mockZero2 = newMockZero('client-b', queryDelegate);

    const wrapper1 = viewStore.getView(mockZero1, tableQuery, true, '1m');
    const wrapper2 = viewStore.getView(mockZero2, tableQuery, true, '1m');

    expect(wrapper1).not.toBe(wrapper2);
  });

  test('singular vs plural create different views', () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const viewStore = new ViewStore();
    const mockZero = newMockZero('test-client', queryDelegate);

    const pluralQuery = tableQuery.where('a', 1).limit(1);
    const singularQuery = tableQuery.where('a', 1).one();

    expect(asQueryInternals(pluralQuery).hash()).toBe(
      asQueryInternals(singularQuery).hash(),
    );

    const wrapper1 = viewStore.getView(mockZero, pluralQuery, true, '1m');
    const wrapper2 = viewStore.getView(mockZero, singularQuery, true, '1m');

    expect(wrapper1).not.toBe(wrapper2);
  });

  test('destroy removes view from store', () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const viewStore = new ViewStore();
    const mockZero = newMockZero('test-client', queryDelegate);

    const wrapper1 = viewStore.getView(mockZero, tableQuery, true, '1m');
    wrapper1.destroy();

    const wrapper2 = viewStore.getView(mockZero, tableQuery, true, '1m');
    expect(wrapper1).not.toBe(wrapper2);
  });

  test('updateTTL propagates to underlying view', () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const viewStore = new ViewStore();
    const mockZero = newMockZero('test-client', queryDelegate);

    const updateServerQuerySpy = vi.spyOn(queryDelegate, 'updateServerQuery');

    viewStore.getView(mockZero, tableQuery, true, '1m');
    viewStore.getView(mockZero, tableQuery, true, '10m');

    expect(updateServerQuerySpy).toHaveBeenCalledExactlyOnceWith(
      {table: 'table'},
      '10m',
    );
  });

  test('result returns tuple of [data, details]', () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const viewStore = new ViewStore();
    const mockZero = newMockZero('test-client', queryDelegate);

    const wrapper = viewStore.getView(mockZero, tableQuery, true, '1m');
    const result = wrapper.result;

    expect(result[0]).toEqual([
      {a: 1, b: 'a'},
      {a: 2, b: 'b'},
    ]);
    expect(result[1]).toEqual({type: 'unknown'});
  });

  test('view disposed when destroyed', () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const viewStore = new ViewStore();
    const mockZero = newMockZero('test-client', queryDelegate);
    const materializeSpy = vi.spyOn(queryDelegate, 'materialize');

    const wrapper = viewStore.getView(mockZero, tableQuery, true, '1m');

    expect(materializeSpy).toHaveBeenCalledTimes(1);

    const view = materializeSpy.mock.results[0].value;
    const destroySpy = vi.spyOn(view, 'destroy');

    expect(destroySpy).toHaveBeenCalledTimes(0);

    wrapper.destroy();

    expect(destroySpy).toHaveBeenCalledTimes(1);
  });
});

describe('Query class', () => {
  function createMockZ(queryDelegate: QueryDelegate): Z {
    return {
      clientID: 'test-client',
      context: undefined,
      materialize: (query: unknown, options?: MaterializeOptions) =>
        queryDelegate.materialize(
          query as Query<'table', Schema, unknown>,
          undefined,
          options,
        ),
      viewStore: new ViewStore(),
    } as unknown as Z;
  }

  test('returns data from view', () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const mockZ = createMockZ(queryDelegate);

    const q = new SvelteQuery(mockZ, tableQuery);

    expect(q.data).toEqual([
      {a: 1, b: 'a'},
      {a: 2, b: 'b'},
    ]);
    expect(q.details).toEqual({type: 'unknown'});
  });

  test('shared view stays alive until all consumers destroy it', () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const mockZ = createMockZ(queryDelegate);
    const materializeSpy = vi.spyOn(queryDelegate, 'materialize');

    const q1 = new SvelteQuery(mockZ, tableQuery);
    const q2 = new SvelteQuery(mockZ, tableQuery);
    const view = materializeSpy.mock.results[0].value;
    const destroySpy = vi.spyOn(view, 'destroy');

    expect(materializeSpy).toHaveBeenCalledTimes(1);
    expect(q1.data).toEqual([
      {a: 1, b: 'a'},
      {a: 2, b: 'b'},
    ]);
    expect(q2.data).toEqual(q1.data);

    q1.destroy();

    expect(q1.data).toBeUndefined();
    expect(q2.data).toEqual([
      {a: 1, b: 'a'},
      {a: 2, b: 'b'},
    ]);
    expect(destroySpy).toHaveBeenCalledTimes(0);

    q2.destroy();

    expect(destroySpy).toHaveBeenCalledTimes(1);
  });

  test('updateQuery switches to new query', () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const mockZ = createMockZ(queryDelegate);

    const q = new SvelteQuery(mockZ, tableQuery);

    expect(q.data).toEqual([
      {a: 1, b: 'a'},
      {a: 2, b: 'b'},
    ]);

    q.updateQuery(tableQuery.where('a', 1));

    expect(q.data).toEqual([{a: 1, b: 'a'}]);
  });

  test('updateQuery releases old view', () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const mockZ = createMockZ(queryDelegate);
    const materializeSpy = vi.spyOn(queryDelegate, 'materialize');

    const q = new SvelteQuery(mockZ, tableQuery.where('a', 1));
    const view = materializeSpy.mock.results[0].value;
    const destroySpy = vi.spyOn(view, 'destroy');

    q.updateQuery(tableQuery.where('a', 2));

    expect(q.data).toEqual([{a: 2, b: 'b'}]);
    expect(destroySpy).toHaveBeenCalledTimes(1);
  });

  test('disabled query returns empty array for plural', () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const mockZ = createMockZ(queryDelegate);

    const q = new SvelteQuery(mockZ, tableQuery, false);

    expect(q.data).toEqual([]);
    expect(q.details).toEqual({type: 'unknown'});
  });

  test('enabled option as object', () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const mockZ = createMockZ(queryDelegate);

    const q = new SvelteQuery(mockZ, tableQuery, {enabled: false});

    expect(q.data).toEqual([]);

    q.updateQuery(tableQuery, {enabled: true});

    expect(q.data).toEqual([
      {a: 1, b: 'a'},
      {a: 2, b: 'b'},
    ]);
  });

  test('falsy query returns undefined', () => {
    const {queryDelegate} = setupTestEnvironment();
    const mockZ = createMockZ(queryDelegate);

    const q = new SvelteQuery(mockZ, null);

    expect(q.data).toBeUndefined();
    expect(q.details).toEqual({type: 'unknown'});
  });

  test('destroy cleans up', () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const mockZ = createMockZ(queryDelegate);

    const q = new SvelteQuery(mockZ, tableQuery);

    expect(q.data).toBeDefined();

    q.destroy();

    expect(q.data).toBeUndefined();
    expect(q.details).toEqual({type: 'unknown'});
  });

  test('current is alias for data', () => {
    const {tableQuery, queryDelegate} = setupTestEnvironment();
    const mockZ = createMockZ(queryDelegate);

    const q = new SvelteQuery(mockZ, tableQuery);

    expect(q.current).toEqual(q.data);
  });
});
