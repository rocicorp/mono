import {expect, test, vi} from 'vitest';
import {QueryManager} from './query-manager.js';
import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import type {ChangeDesiredQueriesMessage} from 'zero-protocol';
import {
  type ScanOptions,
  type ReadTransaction,
  type ScanIndexOptions,
  makeScanResult,
  ReadonlyJSONValue,
  ScanResult,
  IndexKey,
  ScanNoIndexOptions,
  DeepReadonly,
} from 'replicache';

test('add', () => {
  const send = vi.fn<[ChangeDesiredQueriesMessage], void>();
  const queryManager = new QueryManager('client1', send);
  const ast: AST = {
    table: 'issues',
    select: [
      ['id', 'id'],
      ['name', 'name'],
    ],
    orderBy: [['id'], 'asc'],
  };
  queryManager.add(ast);
  expect(send).toBeCalledTimes(1);
  expect(send).toBeCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '3m39m3xhe8uxg',
          ast: {
            table: 'issues',
            alias: undefined,
            select: [
              ['id', 'id'],
              ['name', 'name'],
            ],
            aggregate: undefined,
            where: undefined,
            joins: undefined,
            groupBy: undefined,
            orderBy: [['id'], 'asc'],
            limit: undefined,
          },
        },
      ],
    },
  ]);

  queryManager.add(ast);
  expect(send).toBeCalledTimes(1);
});

test('remove', () => {
  const send = vi.fn<[ChangeDesiredQueriesMessage], void>();
  const queryManager = new QueryManager('client1', send);
  const ast: AST = {
    table: 'issues',
    select: [
      ['id', 'id'],
      ['name', 'name'],
    ],
    orderBy: [['id'], 'asc'],
  };

  expect(queryManager.remove(ast)).toBe(false);

  queryManager.add(ast);
  expect(send).toBeCalledTimes(1);
  expect(send).toBeCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '3m39m3xhe8uxg',
          ast: {
            table: 'issues',
            alias: undefined,
            select: [
              ['id', 'id'],
              ['name', 'name'],
            ],
            aggregate: undefined,
            where: undefined,
            joins: undefined,
            groupBy: undefined,
            orderBy: [['id'], 'asc'],
            limit: undefined,
          },
        },
      ],
    },
  ]);

  queryManager.add(ast);
  expect(send).toBeCalledTimes(1);

  expect(queryManager.remove(ast)).toBe(true);
  expect(send).toBeCalledTimes(1);
  expect(queryManager.remove(ast)).toBe(true);
  expect(send).toBeCalledTimes(2);
  expect(send).toBeCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'del',
          hash: '3m39m3xhe8uxg',
        },
      ],
    },
  ]);

  expect(queryManager.remove(ast)).toBe(false);
});

function getTestScanAsyncIterator(
  entries: (readonly [key: string, value: ReadonlyJSONValue])[],
) {
  return async function* (fromKey: string) {
    for (const [key, value] of entries) {
      if (key >= fromKey) {
        yield [key, value] as const;
      }
    }
  };
}

class TestTransaction implements ReadTransaction {
  readonly clientID = 'client1';
  readonly environment = 'client';
  readonly location = 'client';
  scanEntries: (readonly [key: string, value: ReadonlyJSONValue])[] = [];
  scanCalls: ScanOptions[] = [];

  get(_key: string): Promise<ReadonlyJSONValue | undefined> {
    throw new Error('unexpected call to get');
  }
  has(_key: string): Promise<boolean> {
    throw new Error('unexpected call to has');
  }
  isEmpty(): Promise<boolean> {
    throw new Error('unexpected call to isEmpty');
  }
  scan(options: ScanIndexOptions): ScanResult<IndexKey, ReadonlyJSONValue>;
  scan(options?: ScanNoIndexOptions): ScanResult<string, ReadonlyJSONValue>;
  scan(options?: ScanOptions): ScanResult<IndexKey | string, ReadonlyJSONValue>;

  scan<V extends ReadonlyJSONValue>(
    options: ScanIndexOptions,
  ): ScanResult<IndexKey, DeepReadonly<V>>;
  scan<V extends ReadonlyJSONValue>(
    options?: ScanNoIndexOptions,
  ): ScanResult<string, DeepReadonly<V>>;
  scan<V extends ReadonlyJSONValue>(
    options?: ScanOptions,
  ): ScanResult<IndexKey | string, DeepReadonly<V>>;

  scan(
    options: ScanOptions = {},
  ): ScanResult<IndexKey | string, ReadonlyJSONValue> {
    this.scanCalls.push(options);
    return makeScanResult(options, getTestScanAsyncIterator(this.scanEntries));
  }
}

test('getQueriesPatch', async () => {
  const send = vi.fn<[ChangeDesiredQueriesMessage], void>();
  const queryManager = new QueryManager('client1', send);
  // hash: 3m39m3xhe8uxg
  const ast1: AST = {
    table: 'issues',
    select: [
      ['id', 'id'],
      ['name', 'name'],
    ],
    orderBy: [['id'], 'asc'],
  };
  queryManager.add(ast1);
  // hash 1wpmhwzkyaqrd
  const ast2: AST = {
    table: 'issues',
    select: [
      ['id', 'id'],
      ['name', 'name'],
    ],
    orderBy: [['id'], 'desc'],
  };
  queryManager.add(ast2);

  const testReadTransaction = new TestTransaction();
  testReadTransaction.scanEntries = [
    ['d/client1/3m39m3xhe8uxg', 'unused'],
    ['d/client1/shouldBeDeleted', 'unused'],
  ];

  const patch = await queryManager.getQueriesPatch(testReadTransaction);
  expect(patch).toEqual([
    {
      op: 'del',
      hash: 'shouldBeDeleted',
    },
    {
      op: 'put',
      hash: '1wpmhwzkyaqrd',
      ast: {
        table: 'issues',
        alias: undefined,
        select: [
          ['id', 'id'],
          ['name', 'name'],
        ],
        aggregate: undefined,
        where: undefined,
        joins: undefined,
        groupBy: undefined,
        orderBy: [['id'], 'desc'],
        limit: undefined,
      },
    },
  ]);
  expect(testReadTransaction.scanCalls).toEqual([{prefix: 'd/client1/'}]);
});
