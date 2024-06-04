import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
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
import type {ReplicacheImpl} from 'replicache/src/replicache-impl.js';
import {expect, test, vi} from 'vitest';
import type {ChangeDesiredQueriesMessage} from 'zero-protocol';
import {toGotQueriesKey} from './keys.js';
import {QueryManager} from './query-manager.js';

function createExperimentalWatchMock() {
  return vi.fn<
    Parameters<InstanceType<typeof ReplicacheImpl>['experimentalWatch']>,
    ReturnType<InstanceType<typeof ReplicacheImpl>['experimentalWatch']>
  >();
}

test('add', () => {
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<[ChangeDesiredQueriesMessage], void>();
  const queryManager = new QueryManager('client1', send, experimentalWatch);
  const ast: AST = {
    table: 'issues',
    select: [
      [['issues', 'id'], 'id'],
      [['issues', 'name'], 'name'],
    ],
    orderBy: [[['issues', 'id'], 'asc']],
  };
  queryManager.add(ast);
  expect(send).toBeCalledTimes(1);
  expect(send).toBeCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: 'vgoxbdhr8m7c',
          ast: {
            table: 'issues',
            alias: undefined,
            select: [
              [['issues', 'id'], 'id'],
              [['issues', 'name'], 'name'],
            ],
            aggregate: undefined,
            where: undefined,
            joins: undefined,
            groupBy: undefined,
            orderBy: [[['issues', 'id'], 'asc']],
            limit: undefined,
            schema: undefined,
          } satisfies AST,
        },
      ],
    },
  ]);

  queryManager.add(ast);
  expect(send).toBeCalledTimes(1);
});

test('remove', () => {
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<[ChangeDesiredQueriesMessage], void>();
  const queryManager = new QueryManager('client1', send, experimentalWatch);
  const ast: AST = {
    table: 'issues',
    select: [
      [['issues', 'id'], 'id'],
      [['issues', 'name'], 'name'],
    ],
    orderBy: [[['issues', 'id'], 'asc']],
  };

  const remove1 = queryManager.add(ast);
  expect(send).toBeCalledTimes(1);
  expect(send).toBeCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: 'vgoxbdhr8m7c',
          ast: {
            table: 'issues',
            alias: undefined,
            select: [
              [['issues', 'id'], 'id'],
              [['issues', 'name'], 'name'],
            ],
            aggregate: undefined,
            where: undefined,
            joins: undefined,
            groupBy: undefined,
            schema: undefined,
            orderBy: [[['issues', 'id'], 'asc']],
            limit: undefined,
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
          hash: 'vgoxbdhr8m7c',
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
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<[ChangeDesiredQueriesMessage], void>();
  const queryManager = new QueryManager('client1', send, experimentalWatch);
  // hash: 3m39m3xhe8uxg
  const ast1: AST = {
    table: 'issues',
    select: [
      [['issues', 'id'], 'id'],
      [['issues', 'name'], 'name'],
    ],
    orderBy: [[['issues', 'id'], 'asc']],
  };
  queryManager.add(ast1);
  // hash 1wpmhwzkyaqrd
  const ast2: AST = {
    table: 'issues',
    select: [
      [['issues', 'id'], 'id'],
      [['issues', 'name'], 'name'],
    ],
    orderBy: [[['issues', 'id'], 'desc']],
  };
  queryManager.add(ast2);

  const testReadTransaction = new TestTransaction();
  testReadTransaction.scanEntries = [
    ['d/client1/vgoxbdhr8m7c', 'unused'],
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
      hash: '34gh23e9vauns',
      ast: {
        table: 'issues',
        alias: undefined,
        select: [
          [['issues', 'id'], 'id'],
          [['issues', 'name'], 'name'],
        ],
        aggregate: undefined,
        where: undefined,
        joins: undefined,
        groupBy: undefined,
        orderBy: [[['issues', 'id'], 'desc']],
        limit: undefined,
        schema: undefined,
      } satisfies AST,
    },
  ]);
  expect(testReadTransaction.scanCalls).toEqual([{prefix: 'd/client1/'}]);
});

test('gotCallback, query already got', async () => {
  const queryHash = 'vgoxbdhr8m7c';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<[ChangeDesiredQueriesMessage], void>();
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
    select: [
      [['issues', 'id'], 'id'],
      [['issues', 'name'], 'name'],
    ],
    orderBy: [[['issues', 'id'], 'asc']],
  };

  const gotCalback1 = vi.fn<[boolean], void>();
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
            select: [
              [['issues', 'id'], 'id'],
              [['issues', 'name'], 'name'],
            ],
            aggregate: undefined,
            where: undefined,
            joins: undefined,
            groupBy: undefined,
            orderBy: [[['issues', 'id'], 'asc']],
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

  const gotCalback2 = vi.fn<[boolean], void>();
  queryManager.add(ast, gotCalback2);
  expect(send).toBeCalledTimes(1);

  expect(gotCalback2).toBeCalledTimes(0);
  await new Promise(resolve => setTimeout(resolve, 0));

  expect(gotCalback2).nthCalledWith(1, true);
  expect(gotCalback1).toBeCalledTimes(1);
});

test('gotCallback, query got after add', async () => {
  const queryHash = 'vgoxbdhr8m7c';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<[ChangeDesiredQueriesMessage], void>();
  const queryManager = new QueryManager('client1', send, experimentalWatch);
  expect(experimentalWatch).toBeCalledTimes(1);
  const watchCallback = experimentalWatch.mock.calls[0][0];

  const ast: AST = {
    table: 'issues',
    select: [
      [['issues', 'id'], 'id'],
      [['issues', 'name'], 'name'],
    ],
    orderBy: [[['issues', 'id'], 'asc']],
  };

  const gotCalback1 = vi.fn<[boolean], void>();
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
            select: [
              [['issues', 'id'], 'id'],
              [['issues', 'name'], 'name'],
            ],
            aggregate: undefined,
            where: undefined,
            joins: undefined,
            groupBy: undefined,
            orderBy: [[['issues', 'id'], 'asc']],
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
  const queryHash = 'vgoxbdhr8m7c';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<[ChangeDesiredQueriesMessage], void>();
  const queryManager = new QueryManager('client1', send, experimentalWatch);
  expect(experimentalWatch).toBeCalledTimes(1);
  const watchCallback = experimentalWatch.mock.calls[0][0];

  const ast: AST = {
    table: 'issues',
    select: [
      [['issues', 'id'], 'id'],
      [['issues', 'name'], 'name'],
    ],
    orderBy: [[['issues', 'id'], 'asc']],
  };

  const gotCalback1 = vi.fn<[boolean], void>();
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
            select: [
              [['issues', 'id'], 'id'],
              [['issues', 'name'], 'name'],
            ],
            aggregate: undefined,
            where: undefined,
            joins: undefined,
            groupBy: undefined,
            orderBy: [[['issues', 'id'], 'asc']],
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
  const queryHash = 'vgoxbdhr8m7c';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<[ChangeDesiredQueriesMessage], void>();
  const queryManager = new QueryManager('client1', send, experimentalWatch);
  expect(experimentalWatch).toBeCalledTimes(1);
  const watchCallback = experimentalWatch.mock.calls[0][0];

  const ast: AST = {
    table: 'issues',
    select: [
      [['issues', 'id'], 'id'],
      [['issues', 'name'], 'name'],
    ],
    orderBy: [[['issues', 'id'], 'asc']],
  };

  const gotCalback1 = vi.fn<[boolean], void>();
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
            select: [
              [['issues', 'id'], 'id'],
              [['issues', 'name'], 'name'],
            ],
            aggregate: undefined,
            where: undefined,
            joins: undefined,
            groupBy: undefined,
            orderBy: [[['issues', 'id'], 'asc']],
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
