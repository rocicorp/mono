import {
  makeScanResult,
  type DeepReadonly,
  type IndexKey,
  type ReadonlyJSONValue,
  type ReadTransaction,
  type ScanIndexOptions,
  type ScanNoIndexOptions,
  type ScanOptions,
  type ScanResult,
} from '../../../replicache/src/mod.js';
import {expect, test, vi} from 'vitest';
import type {ChangeDesiredQueriesMessage} from '../../../zero-protocol/src/mod.js';
import type {AST} from '../../../zql/src/zql/ast/ast.js';
import {QueryManager} from './query-manager.js';
import {toGotQueriesKey} from './keys.js';

function createExperimentalWatchMock() {
  return vi.fn();
}

test('add', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const queryManager = new QueryManager('client1', send, () => () => {});
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
  const queryManager = new QueryManager('client1', send, () => () => {});
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
  const queryManager = new QueryManager('client1', send, () => () => {});
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

test('gotCallback, query already got', async () => {
  const queryHash = '1m2bs2hhq3g1e';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
  const queryManager = new QueryManager('client1', send, experimentalWatch);
  expect(experimentalWatch).toBeCalledTimes(1);
  const watchCallback = experimentalWatch.mock.calls[0][0];
  watchCallback([
    {
      op: 'add',
      key: toGotQueriesKey(queryHash) as string & IndexKey,
      newValue: 'unused',
    },
  ]);

  const ast: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
  };

  const gotCalback1 = vi.fn<(got: boolean) => void>();
  queryManager.add(ast, gotCalback1);
  expect(send).toBeCalledTimes(1);
  expect(send).toBeCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: queryHash,
          ast: {
            table: 'issues',
            alias: undefined,
            where: undefined,
            related: undefined,
            start: undefined,
            orderBy: [['id', 'asc']],
            limit: undefined,
            schema: undefined,
          } satisfies AST,
        },
      ],
    },
  ]);

  expect(gotCalback1).toBeCalledTimes(0);

  await new Promise(resolve => setTimeout(resolve, 0));

  expect(gotCalback1).nthCalledWith(1, true);

  const gotCalback2 = vi.fn<(got: boolean) => void>();
  queryManager.add(ast, gotCalback2);
  expect(send).toBeCalledTimes(1);

  expect(gotCalback2).toBeCalledTimes(0);
  await new Promise(resolve => setTimeout(resolve, 0));

  expect(gotCalback2).nthCalledWith(1, true);
  expect(gotCalback1).toBeCalledTimes(1);
});

test('gotCallback, query got after add', async () => {
  const queryHash = '1m2bs2hhq3g1e';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
  const queryManager = new QueryManager('client1', send, experimentalWatch);
  expect(experimentalWatch).toBeCalledTimes(1);
  const watchCallback = experimentalWatch.mock.calls[0][0];

  const ast: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
  };

  const gotCalback1 = vi.fn<(got: boolean) => void>();
  queryManager.add(ast, gotCalback1);
  expect(send).toBeCalledTimes(1);
  expect(send).toBeCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: queryHash,
          ast: {
            table: 'issues',
            alias: undefined,
            where: undefined,
            related: undefined,
            start: undefined,
            orderBy: [['id', 'asc']],
            limit: undefined,
            schema: undefined,
          } satisfies AST,
        },
      ],
    },
  ]);

  expect(gotCalback1).toBeCalledTimes(0);

  await new Promise(resolve => setTimeout(resolve, 0));

  expect(gotCalback1).nthCalledWith(1, false);

  watchCallback([
    {
      op: 'add',
      key: toGotQueriesKey(queryHash) as string & IndexKey,
      newValue: 'unused',
    },
  ]);

  expect(gotCalback1).nthCalledWith(2, true);
});

test('gotCallback, query got after add then removed', async () => {
  const queryHash = '1m2bs2hhq3g1e';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
  const queryManager = new QueryManager('client1', send, experimentalWatch);
  expect(experimentalWatch).toBeCalledTimes(1);
  const watchCallback = experimentalWatch.mock.calls[0][0];

  const ast: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
  };

  const gotCalback1 = vi.fn<(got: boolean) => void>();
  queryManager.add(ast, gotCalback1);
  expect(send).toBeCalledTimes(1);
  expect(send).toBeCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: queryHash,
          ast: {
            table: 'issues',
            alias: undefined,
            where: undefined,
            related: undefined,
            start: undefined,
            orderBy: [['id', 'asc']],
            limit: undefined,
            schema: undefined,
          } satisfies AST,
        },
      ],
    },
  ]);

  expect(gotCalback1).toBeCalledTimes(0);

  await new Promise(resolve => setTimeout(resolve, 0));

  expect(gotCalback1).nthCalledWith(1, false);

  watchCallback([
    {
      op: 'add',
      key: toGotQueriesKey(queryHash) as string & IndexKey,
      newValue: 'unused',
    },
  ]);

  expect(gotCalback1).nthCalledWith(2, true);

  watchCallback([
    {
      op: 'del',
      key: toGotQueriesKey(queryHash) as string & IndexKey,
      oldValue: 'unused',
    },
  ]);

  expect(gotCalback1).nthCalledWith(3, false);
});

test('gotCallback, query got after subscription removed', async () => {
  const queryHash = '1m2bs2hhq3g1e';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(q: ChangeDesiredQueriesMessage) => void>();
  const queryManager = new QueryManager('client1', send, experimentalWatch);
  expect(experimentalWatch).toBeCalledTimes(1);
  const watchCallback = experimentalWatch.mock.calls[0][0];

  const ast: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
  };

  const gotCalback1 = vi.fn<(got: boolean) => void>();
  const remove = queryManager.add(ast, gotCalback1);
  expect(send).toBeCalledTimes(1);
  expect(send).toBeCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: queryHash,
          ast: {
            table: 'issues',
            alias: undefined,
            where: undefined,
            related: undefined,
            start: undefined,
            orderBy: [['id', 'asc']],
            limit: undefined,
            schema: undefined,
          } satisfies AST,
        },
      ],
    },
  ]);

  expect(gotCalback1).toBeCalledTimes(0);

  await new Promise(resolve => setTimeout(resolve, 0));

  expect(gotCalback1).nthCalledWith(1, false);

  remove();

  expect(gotCalback1).toBeCalledTimes(1);
  watchCallback([
    {
      op: 'add',
      key: toGotQueriesKey(queryHash) as string & IndexKey,
      newValue: 'unused',
    },
  ]);

  expect(gotCalback1).toBeCalledTimes(1);
});
