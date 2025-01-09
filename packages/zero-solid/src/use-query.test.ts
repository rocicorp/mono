import {testEffect} from '@solidjs/testing-library';
import {createEffect, createSignal} from 'solid-js';
import {expect, test} from 'vitest';
import {must} from '../../shared/src/must.js';
import {MemorySource} from '../../zql/src/ivm/memory-source.js';
import {newQuery} from '../../zql/src/query/query-impl.js';
import {QueryDelegateImpl} from '../../zql/src/query/test/query-delegate.js';
import {useQuery} from './use-query.js';

function setupTestEnvironment() {
  const tableSchema = {
    tableName: 'table',
    columns: {
      a: {type: 'number'},
      b: {type: 'string'},
    },
    primaryKey: ['a'],
    relationships: {},
  } as const;
  const ms = new MemorySource(
    tableSchema.tableName,
    tableSchema.columns,
    tableSchema.primaryKey,
  );
  ms.push({row: {a: 1, b: 'a'}, type: 'add'});
  ms.push({row: {a: 2, b: 'b'}, type: 'add'});

  const queryDelegate = new QueryDelegateImpl({table: ms});
  const tableQuery = newQuery(queryDelegate, tableSchema);

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

  expect(rows()).toEqual([
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
    {a: 3, b: 'c'},
  ]);
  expect(resultType()).toEqual({type: 'complete'});
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
  const {ms, tableQuery} = setupTestEnvironment();
  const [a, setA] = createSignal(1);
  const [rows] = useQuery(() => tableQuery.where('a', a()));
  return testEffect(done =>
    createEffect((run: number = 0) => {
      if (run === 0) {
        expect(rows()).toEqual([{a: 1, b: 'a'}]);
        ms.push({type: 'edit', oldRow: {a: 1, b: 'a'}, row: {a: 1, b: 'a2'}});
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
