import {renderHook, testEffect} from '@solidjs/testing-library';
import {createEffect, createSignal} from 'solid-js';
import {expect, test, vi} from 'vitest';
import {must} from '../../shared/src/must.ts';
import {
  createSchema,
  number,
  string,
  table,
  type TTL,
} from '../../zero/src/zero.ts';
import {MemorySource} from '../../zql/src/ivm/memory-source.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import {QueryDelegateImpl} from '../../zql/src/query/test/query-delegate.ts';
import {solidViewFactory} from './solid-view.ts';
import {useQuery} from './use-query.ts';

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
  ms.push({row: {a: 1, b: 'a'}, type: 'add'});
  ms.push({row: {a: 2, b: 'b'}, type: 'add'});

  const queryDelegate = new QueryDelegateImpl({table: ms});
  const tableQuery = newQuery(queryDelegate, schema, 'table');

  return {ms, tableQuery, queryDelegate};
}

test('useQuery', async () => {
  const {ms, tableQuery, queryDelegate} = setupTestEnvironment();

  const [rows, resultType] = useQuery(() => tableQuery);
  expect(rows()).toEqual([
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
  ]);
  expect(resultType()).toEqual({type: 'unknown'});

  must(queryDelegate.gotCallbacks[0])(true);
  await 1;

  ms.push({row: {a: 3, b: 'c'}, type: 'add'});
  queryDelegate.commit();

  expect(rows()).toEqual([
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
    {a: 3, b: 'c'},
  ]);
  expect(resultType()).toEqual({type: 'complete'});
});

test('useQuery with ttl', () => {
  const {tableQuery, queryDelegate} = setupTestEnvironment();
  const [ttl, setTTL] = createSignal<TTL>('1m');

  const materializeSpy = vi.spyOn(tableQuery, 'materialize');
  const addServerQuerySpy = vi.spyOn(queryDelegate, 'addServerQuery');
  const updateServerQuerySpy = vi.spyOn(queryDelegate, 'updateServerQuery');

  const querySignal = vi.fn(() => tableQuery);

  renderHook(useQuery, {
    initialProps: [querySignal, () => ({ttl: ttl()})],
  });

  expect(querySignal).toHaveBeenCalledTimes(1);
  expect(addServerQuerySpy).toHaveBeenCalledTimes(1);
  expect(updateServerQuerySpy).toHaveBeenCalledTimes(0);
  expect(materializeSpy).toHaveBeenCalledExactlyOnceWith(
    solidViewFactory,
    '1m',
  );
  addServerQuerySpy.mockClear();
  materializeSpy.mockClear();

  setTTL('10m');
  expect(addServerQuerySpy).toHaveBeenCalledTimes(0);
  expect(updateServerQuerySpy).toHaveBeenCalledExactlyOnceWith(
    {
      orderBy: [['a', 'asc']],
      table: 'table',
    },
    '10m',
  );
  expect(materializeSpy).toHaveBeenCalledTimes(0);
});

test('useQuery deps change', async () => {
  const {tableQuery, queryDelegate} = setupTestEnvironment();

  const [a, setA] = createSignal(1);

  const [rows, resultDetails] = useQuery(() => tableQuery.where('a', a()));

  const rowLog: unknown[] = [];
  const resultDetailsLog: unknown[] = [];
  const resetLogs = () => {
    rowLog.length = 0;
    resultDetailsLog.length = 0;
  };

  createEffect(() => {
    rowLog.push(rows());
  });

  createEffect(() => {
    resultDetailsLog.push(resultDetails());
  });

  expect(rowLog).toEqual([[{a: 1, b: 'a'}]]);
  expect(resultDetailsLog).toEqual([{type: 'unknown'}]);
  resetLogs();

  queryDelegate.gotCallbacks.forEach(cb => cb?.(true));
  await 1;

  expect(rowLog).toEqual([]);
  expect(resultDetailsLog).toEqual([{type: 'complete'}]);
  resetLogs();

  setA(2);
  expect(rowLog).toEqual([[{a: 2, b: 'b'}]]);
  expect(resultDetailsLog).toEqual([{type: 'unknown'}]);
  resetLogs();

  queryDelegate.gotCallbacks.forEach(cb => cb?.(true));
  await 1;

  expect(rowLog).toEqual([]);
  expect(resultDetailsLog).toEqual([{type: 'complete'}]);
});

test('useQuery deps change testEffect', () => {
  const {ms, tableQuery, queryDelegate} = setupTestEnvironment();
  const [a, setA] = createSignal(1);
  const [rows] = useQuery(() => tableQuery.where('a', a()));
  return testEffect(done =>
    createEffect((run: number = 0) => {
      if (run === 0) {
        expect(rows()).toEqual([{a: 1, b: 'a'}]);
        ms.push({type: 'edit', oldRow: {a: 1, b: 'a'}, row: {a: 1, b: 'a2'}});
        queryDelegate.commit();
      } else if (run === 1) {
        expect(rows()).toEqual([{a: 1, b: 'a2'}]);
        setA(2);
      } else if (run === 2) {
        expect(rows()).toEqual([{a: 2, b: 'b'}]);
        done();
      }
      return run + 1;
    }),
  );
});
