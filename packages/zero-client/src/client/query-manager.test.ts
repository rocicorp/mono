import {
  DeepReadonly,
  IndexKey,
  ReadonlyJSONValue,
  ScanNoIndexOptions,
  ScanResult,
  makeScanResult,
  type ReadTransaction,
  type ScanIndexOptions,
  type ScanOptions,
} from 'replicache';
import {expect, test, vi} from 'vitest';
import type {ChangeDesiredQueriesMessage} from 'zero-protocol';
import {QueryManager} from './query-manager.js';
import {AST} from 'zql/src/zql/ast/ast.js';

test('add', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const queryManager = new QueryManager('client1', send);
  const ast: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
  };
  queryManager.add(ast);
  expect(send).toBeCalledTimes(1);
  expect(send).toBeCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '1m2bs2hhq3g1e',
          ast: {
            table: 'issues',
            where: undefined,
            orderBy: [['id', 'asc']],
          } satisfies AST,
        },
      ],
    },
  ]);

  queryManager.add(ast);
  expect(send).toBeCalledTimes(1);
});

test('remove', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const queryManager = new QueryManager('client1', send);
  const ast: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
  };

  const remove1 = queryManager.add(ast);
  expect(send).toBeCalledTimes(1);
  expect(send).toBeCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '1m2bs2hhq3g1e',
          ast: {
            table: 'issues',
            where: undefined,
            orderBy: [['id', 'asc']],
          } satisfies AST,
        },
      ],
    },
  ]);

  const remove2 = queryManager.add(ast);
  expect(send).toBeCalledTimes(1);

  remove1();
  expect(send).toBeCalledTimes(1);
  remove2();
  expect(send).toBeCalledTimes(2);
  expect(send).toBeCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'del',
          hash: '1m2bs2hhq3g1e',
        },
      ],
    },
  ]);

  remove2();
  expect(send).toBeCalledTimes(2);
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
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const queryManager = new QueryManager('client1', send);
  // hash: 1m2bs2hhq3g1e
  const ast1: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
  };
  queryManager.add(ast1);
  // hash 1m2bs2hhq3g1e
  const ast2: AST = {
    table: 'issues',
    orderBy: [['id', 'desc']],
  };
  queryManager.add(ast2);

  const testReadTransaction = new TestTransaction();
  testReadTransaction.scanEntries = [
    ['d/client1/1m2bs2hhq3g1e', 'unused'],
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
      hash: '51ea5ipsgcxi',
      ast: {
        table: 'issues',
        orderBy: [['id', 'desc']],
      } satisfies AST,
    },
  ]);
  expect(testReadTransaction.scanCalls).toEqual([{prefix: 'd/client1/'}]);
});
