import {describe, expect, test, vi} from 'vitest';
import type {IndexKey} from '../../../replicache/src/db/index.ts';
import {
  makeScanResult,
  type ScanResult,
} from '../../../replicache/src/scan-iterator.ts';
import type {
  ScanIndexOptions,
  ScanNoIndexOptions,
  ScanOptions,
} from '../../../replicache/src/scan-options.ts';
import {
  type DeepReadonly,
  type ReadTransaction,
} from '../../../replicache/src/transactions.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {ChangeDesiredQueriesMessage} from '../../../zero-protocol/src/change-desired-queries.ts';
import {toGotQueriesKey} from './keys.ts';
import {QueryManager} from './query-manager.ts';

function createExperimentalWatchMock() {
  return vi.fn();
}

test('add', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const queryManager = new QueryManager(
    'client1',
    send,
    () => () => {},
    maxRecentQueriesSize,
  );
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
          hash: '3w07tflzjgrd9',
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

test('remove, recent queries max size 0', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const queryManager = new QueryManager(
    'client1',
    send,
    () => () => {},
    maxRecentQueriesSize,
  );
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
          hash: '3w07tflzjgrd9',
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
          hash: '3w07tflzjgrd9',
        },
      ],
    },
  ]);

  remove2();
  expect(send).toBeCalledTimes(2);
});

test('remove, max recent queries size 2', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 2;
  const queryManager = new QueryManager(
    'client1',
    send,
    () => () => {},
    maxRecentQueriesSize,
  );
  const ast1: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
  };

  const ast2: AST = {
    table: 'issues',
    orderBy: [['id', 'desc']],
  };

  const ast3: AST = {
    table: 'users',
    orderBy: [['id', 'asc']],
  };

  const ast4: AST = {
    table: 'users',
    orderBy: [['id', 'desc']],
  };

  const remove1Ast1 = queryManager.add(ast1);
  expect(send).toBeCalledTimes(1);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '3w07tflzjgrd9',
          ast: {
            table: 'issues',
            where: undefined,
            orderBy: [['id', 'asc']],
          } satisfies AST,
        },
      ],
    },
  ]);

  const remove2Ast1 = queryManager.add(ast1);
  expect(send).toBeCalledTimes(1);

  const removeAst2 = queryManager.add(ast2);
  expect(send).toBeCalledTimes(2);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '23xyk6p9abj0c',
          ast: {
            table: 'issues',
            where: undefined,
            orderBy: [['id', 'desc']],
          } satisfies AST,
        },
      ],
    },
  ]);

  const removeAst3 = queryManager.add(ast3);
  expect(send).toBeCalledTimes(3);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '2rltuh73615pi',
          ast: {
            table: 'users',
            where: undefined,
            orderBy: [['id', 'asc']],
          } satisfies AST,
        },
      ],
    },
  ]);

  const removeAst4 = queryManager.add(ast4);
  expect(send).toBeCalledTimes(4);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: 'aaaqpuy7h6qi',
          ast: {
            table: 'users',
            where: undefined,
            orderBy: [['id', 'desc']],
          } satisfies AST,
        },
      ],
    },
  ]);

  remove1Ast1();
  expect(send).toBeCalledTimes(4);
  remove2Ast1();
  expect(send).toBeCalledTimes(4);

  removeAst2();
  expect(send).toBeCalledTimes(4);

  removeAst3();
  expect(send).toBeCalledTimes(5);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'del',
          hash: '3w07tflzjgrd9',
        },
      ],
    },
  ]);

  removeAst4();
  expect(send).toBeCalledTimes(6);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'del',
          hash: '23xyk6p9abj0c',
        },
      ],
    },
  ]);
});

test('test add/remove/add/remove changes lru order max recent queries size 2', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 2;
  const queryManager = new QueryManager(
    'client1',
    send,
    () => () => {},
    maxRecentQueriesSize,
  );
  const ast1: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
  };

  const ast2: AST = {
    table: 'issues',
    orderBy: [['id', 'desc']],
  };

  const ast3: AST = {
    table: 'users',
    orderBy: [['id', 'asc']],
  };

  const ast4: AST = {
    table: 'users',
    orderBy: [['id', 'desc']],
  };

  const remove1Ast1 = queryManager.add(ast1);
  expect(send).toBeCalledTimes(1);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '3w07tflzjgrd9',
          ast: {
            table: 'issues',
            where: undefined,
            orderBy: [['id', 'asc']],
          } satisfies AST,
        },
      ],
    },
  ]);

  const removeAst2 = queryManager.add(ast2);
  expect(send).toBeCalledTimes(2);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '23xyk6p9abj0c',
          ast: {
            table: 'issues',
            where: undefined,
            orderBy: [['id', 'desc']],
          } satisfies AST,
        },
      ],
    },
  ]);

  const removeAst3 = queryManager.add(ast3);
  expect(send).toBeCalledTimes(3);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '2rltuh73615pi',
          ast: {
            table: 'users',
            where: undefined,
            orderBy: [['id', 'asc']],
          } satisfies AST,
        },
      ],
    },
  ]);

  const removeAst4 = queryManager.add(ast4);
  expect(send).toBeCalledTimes(4);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: 'aaaqpuy7h6qi',
          ast: {
            table: 'users',
            where: undefined,
            orderBy: [['id', 'desc']],
          } satisfies AST,
        },
      ],
    },
  ]);

  remove1Ast1();
  expect(send).toBeCalledTimes(4);

  const remove2Ast1 = queryManager.add(ast1);
  expect(send).toBeCalledTimes(4);

  removeAst2();
  expect(send).toBeCalledTimes(4);

  remove2Ast1();
  expect(send).toBeCalledTimes(4);

  removeAst3();

  expect(send).toBeCalledTimes(5);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'del',
          hash: '23xyk6p9abj0c',
        },
      ],
    },
  ]);

  removeAst4();
  expect(send).toBeCalledTimes(6);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'del',
          hash: '3w07tflzjgrd9',
        },
      ],
    },
  ]);
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
  const maxRecentQueriesSize = 0;
  const queryManager = new QueryManager(
    'client1',
    send,
    () => () => {},
    maxRecentQueriesSize,
  );
  // hash: 3w07tflzjgrd9
  const ast1: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
  };
  queryManager.add(ast1);
  // hash 23xyk6p9abj0c
  const ast2: AST = {
    table: 'issues',
    orderBy: [['id', 'desc']],
  };
  queryManager.add(ast2);

  const testReadTransaction = new TestTransaction();
  testReadTransaction.scanEntries = [
    ['d/client1/3w07tflzjgrd9', 'unused'],
    ['d/client1/shouldBeDeleted', 'unused'],
  ];

  const patch = await queryManager.getQueriesPatch(testReadTransaction);
  expect(patch).toEqual(
    new Map(
      [
        {
          op: 'del',
          hash: 'shouldBeDeleted',
        },
        {
          op: 'put',
          hash: '23xyk6p9abj0c',
          ast: {
            table: 'issues',
            orderBy: [['id', 'desc']],
          } satisfies AST,
        },
      ].map(x => [x.hash, x] as const),
    ),
  );
  expect(testReadTransaction.scanCalls).toEqual([{prefix: 'd/client1/'}]);
});

test('getQueriesPatch includes recent queries in desired', async () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 2;
  const queryManager = new QueryManager(
    'client1',
    send,
    () => () => {},
    maxRecentQueriesSize,
  );
  const ast1: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
  };
  const remove1 = queryManager.add(ast1);
  const ast2: AST = {
    table: 'issues',
    orderBy: [['id', 'desc']],
  };
  const remove2 = queryManager.add(ast2);
  const ast3: AST = {
    table: 'users',
    orderBy: [['id', 'asc']],
  };
  const remove3 = queryManager.add(ast3);
  const ast4: AST = {
    table: 'users',
    orderBy: [['id', 'desc']],
  };
  const remove4 = queryManager.add(ast4);
  remove1();
  remove2();
  remove3();
  remove4();

  // ast1 and ast2 are actually removed since maxRecentQueriesSize is 2

  const testReadTransaction = new TestTransaction();
  testReadTransaction.scanEntries = [
    ['d/client1/3w07tflzjgrd9', 'unused'],
    ['d/client1/shouldBeDeleted', 'unused'],
  ];

  const patch = await queryManager.getQueriesPatch(testReadTransaction);
  expect(patch).toMatchInlineSnapshot(`
    Map {
      "3w07tflzjgrd9" => {
        "hash": "3w07tflzjgrd9",
        "op": "del",
      },
      "shouldBeDeleted" => {
        "hash": "shouldBeDeleted",
        "op": "del",
      },
      "2rltuh73615pi" => {
        "ast": {
          "alias": undefined,
          "limit": undefined,
          "orderBy": [
            [
              "id",
              "asc",
            ],
          ],
          "related": undefined,
          "schema": undefined,
          "start": undefined,
          "table": "users",
          "where": undefined,
        },
        "hash": "2rltuh73615pi",
        "op": "put",
      },
      "aaaqpuy7h6qi" => {
        "ast": {
          "alias": undefined,
          "limit": undefined,
          "orderBy": [
            [
              "id",
              "desc",
            ],
          ],
          "related": undefined,
          "schema": undefined,
          "start": undefined,
          "table": "users",
          "where": undefined,
        },
        "hash": "aaaqpuy7h6qi",
        "op": "put",
      },
    }
  `);
  expect(testReadTransaction.scanCalls).toEqual([{prefix: 'd/client1/'}]);
});

test('gotCallback, query already got', () => {
  const queryHash = '3w07tflzjgrd9';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();

  const maxRecentQueriesSize = 0;
  const queryManager = new QueryManager(
    'client1',
    send,
    experimentalWatch,
    maxRecentQueriesSize,
  );
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

  expect(gotCalback1).nthCalledWith(1, true);

  const gotCalback2 = vi.fn<(got: boolean) => void>();
  queryManager.add(ast, gotCalback2);
  expect(send).toBeCalledTimes(1);

  expect(gotCalback2).nthCalledWith(1, true);
  expect(gotCalback1).toBeCalledTimes(1);
});

test('gotCallback, query got after add', () => {
  const queryHash = '3w07tflzjgrd9';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const queryManager = new QueryManager(
    'client1',
    send,
    experimentalWatch,
    maxRecentQueriesSize,
  );
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

test('gotCallback, query got after add then removed', () => {
  const queryHash = '3w07tflzjgrd9';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const queryManager = new QueryManager(
    'client1',
    send,
    experimentalWatch,
    maxRecentQueriesSize,
  );
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

test('gotCallback, query got after subscription removed', () => {
  const queryHash = '3w07tflzjgrd9';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(q: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const queryManager = new QueryManager(
    'client1',
    send,
    experimentalWatch,
    maxRecentQueriesSize,
  );
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

const normalizingFields = {
  alias: undefined,
  limit: undefined,
  related: undefined,
  schema: undefined,
  start: undefined,
  where: undefined,
} as const;

describe('queriesPatch with lastPatch', () => {
  test('returns the normal set if no lastPatch is provided', async () => {
    const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => boolean>(
      () => false,
    );
    const maxRecentQueriesSize = 0;
    const queryManager = new QueryManager(
      'client1',
      send,
      () => () => {},
      maxRecentQueriesSize,
    );

    queryManager.add({
      table: 'issue',
      orderBy: [['id', 'asc']],
    });
    const testReadTransaction = new TestTransaction();
    const patch = await queryManager.getQueriesPatch(testReadTransaction);
    expect([...patch.values()]).toEqual([
      {
        ast: {
          orderBy: [['id', 'asc']],
          table: 'issue',
          ...normalizingFields,
        },
        hash: '12hwg3ihkijhm',
        op: 'put',
      },
    ]);
  });

  test('removes entries from the patch that are in lastPatch', async () => {
    const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => boolean>(
      () => false,
    );
    const queryManager = new QueryManager('client1', send, () => () => {}, 0);

    const clean = queryManager.add({
      table: 'issue',
      orderBy: [['id', 'asc']],
    });
    const testReadTransaction = new TestTransaction();

    // patch and lastPatch are the same
    const patch1 = await queryManager.getQueriesPatch(
      testReadTransaction,
      new Map([
        [
          '12hwg3ihkijhm',
          {
            ast: {
              orderBy: [['id', 'asc']],
              table: 'issue',
            },
            hash: '12hwg3ihkijhm',
            op: 'put',
          },
        ],
      ]),
    );
    expect([...patch1.values()]).toEqual([]);

    // patch has a `del` event that is not in lastPatch
    clean();
    const patch2 = await queryManager.getQueriesPatch(
      testReadTransaction,
      new Map([
        [
          '12hwg3ihkijhm',
          {
            ast: {
              orderBy: [['id', 'asc']],
              table: 'issue',
            },
            hash: '12hwg3ihkijhm',
            op: 'put',
          },
        ],
      ]),
    );
    expect([...patch2.values()]).toEqual([
      {
        hash: '12hwg3ihkijhm',
        op: 'del',
      },
    ]);
  });
});
