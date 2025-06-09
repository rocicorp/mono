import {beforeEach, describe, expect, test, vi, type Mock} from 'vitest';
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
import * as v from '../../../shared/src/valita.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {ChangeDesiredQueriesMessage} from '../../../zero-protocol/src/change-desired-queries.ts';
import {upPutOpSchema} from '../../../zero-protocol/src/queries-patch.ts';
import {schema} from '../../../zql/src/query/test/test-schemas.ts';
import type {TTL} from '../../../zql/src/query/ttl.ts';
import {toGotQueriesKey} from './keys.ts';
import {QueryManager} from './query-manager.ts';
import {MutationTracker} from './mutation-tracker.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';

function createExperimentalWatchMock() {
  return vi.fn();
}

const lc = createSilentLogContext();
test('add', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc);
  const queryManager = new QueryManager(
    mutationTracker,
    'client1',
    schema.tables,
    send,
    () => () => {},
    maxRecentQueriesSize,
  );
  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
  };
  queryManager.addLegacy(ast, 'forever');
  expect(send).toBeCalledTimes(1);
  expect(send).toBeCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '12hwg3ihkijhm',
          ast: {
            table: 'issues',
            where: undefined,
            orderBy: [['id', 'asc']],
          } satisfies AST,
          ttl: -1,
        },
      ],
    },
  ]);

  queryManager.addLegacy(ast, 'forever');
  expect(send).toBeCalledTimes(1);
});

test('add and remove a custom query', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc);
  const queryManager = new QueryManager(
    mutationTracker,
    'client1',
    schema.tables,
    send,
    () => () => {},
    maxRecentQueriesSize,
  );
  const rm1 = queryManager.addCustom('customQuery', [1], '1m');
  expect(send).toBeCalledTimes(1);
  expect(send).toBeCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '2l1ig6e3tnu0a',
          name: 'customQuery',
          args: [1],
          ttl: 60000,
        },
      ],
    },
  ]);

  const rm2 = queryManager.addCustom('customQuery', [1], '1m');
  expect(send).toBeCalledTimes(1);

  rm2();
  const rm3 = queryManager.addCustom('customQuery', [1], '1m');
  expect(send).toBeCalledTimes(1);
  rm1();
  rm3();
  queryManager.addCustom('customQuery', [1], '1m');
  // once for del, another for put
  expect(send).toBeCalledTimes(3);

  send.mockClear();

  // now update the custom query
  queryManager.updateCustom('customQuery', [1], '2m');
  // update event sent
  expect(send).toBeCalledTimes(1);
  expect(send).toBeCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '2l1ig6e3tnu0a',
          name: 'customQuery',
          args: [1],
          ttl: 120000,
        },
      ],
    },
  ]);

  queryManager.updateCustom('customQuery', [1], '1m');
  // send not called with lower ttl
  expect(send).toBeCalledTimes(1);
});

test('add renamed fields', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc);
  const queryManager = new QueryManager(
    mutationTracker,
    'client1',
    schema.tables,
    send,
    () => () => {},
    maxRecentQueriesSize,
  );
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {
            type: 'column',
            name: 'ownerId',
          },
          op: 'IS NOT',
          right: {
            type: 'literal',
            value: 'null',
          },
        },
        {
          type: 'correlatedSubquery',
          related: {
            correlation: {
              parentField: ['id'],
              childField: ['issueId'],
            },
            subquery: {
              table: 'comment',
            },
          },
          op: 'EXISTS',
        },
      ],
    },
    related: [
      {
        correlation: {
          parentField: ['ownerId'],
          childField: ['id'],
        },
        subquery: {
          table: 'user',
        },
      },
    ],
    orderBy: [
      ['ownerId', 'desc'],
      ['id', 'asc'],
    ],
    start: {
      row: {id: '123', ownerId: 'foobar'},
      exclusive: false,
    },
  };

  queryManager.addLegacy(ast, 'forever');
  expect(send).toBeCalledTimes(1);
  expect(send.mock.calls[0][0]).toMatchInlineSnapshot(`
    [
      "changeDesiredQueries",
      {
        "desiredQueriesPatch": [
          {
            "args": undefined,
            "ast": {
              "alias": undefined,
              "limit": undefined,
              "orderBy": [
                [
                  "owner_id",
                  "desc",
                ],
                [
                  "id",
                  "asc",
                ],
              ],
              "related": [
                {
                  "correlation": {
                    "childField": [
                      "id",
                    ],
                    "parentField": [
                      "owner_id",
                    ],
                  },
                  "hidden": undefined,
                  "subquery": {
                    "alias": undefined,
                    "limit": undefined,
                    "orderBy": undefined,
                    "related": undefined,
                    "schema": undefined,
                    "start": undefined,
                    "table": "users",
                    "where": undefined,
                  },
                  "system": undefined,
                },
              ],
              "schema": undefined,
              "start": {
                "exclusive": false,
                "row": {
                  "id": "123",
                  "owner_id": "foobar",
                },
              },
              "table": "issues",
              "where": {
                "conditions": [
                  {
                    "left": {
                      "name": "owner_id",
                      "type": "column",
                    },
                    "op": "IS NOT",
                    "right": {
                      "type": "literal",
                      "value": "null",
                    },
                    "type": "simple",
                  },
                  {
                    "op": "EXISTS",
                    "related": {
                      "correlation": {
                        "childField": [
                          "issue_id",
                        ],
                        "parentField": [
                          "id",
                        ],
                      },
                      "subquery": {
                        "alias": undefined,
                        "limit": undefined,
                        "orderBy": undefined,
                        "related": undefined,
                        "schema": undefined,
                        "start": undefined,
                        "table": "comments",
                        "where": undefined,
                      },
                    },
                    "type": "correlatedSubquery",
                  },
                ],
                "type": "and",
              },
            },
            "hash": "2courpv3kf7et",
            "name": undefined,
            "op": "put",
            "ttl": -1,
          },
        ],
      },
    ]
  `);
});

test('remove, recent queries max size 0', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc);
  const queryManager = new QueryManager(
    mutationTracker,
    'client1',
    schema.tables,
    send,
    () => () => {},
    maxRecentQueriesSize,
  );
  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
  };

  const remove1 = queryManager.addLegacy(ast, 'forever');
  expect(send).toBeCalledTimes(1);
  expect(send).toBeCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '12hwg3ihkijhm',
          ast: {
            table: 'issues',
            where: undefined,
            orderBy: [['id', 'asc']],
          } satisfies AST,
          ttl: -1,
        },
      ],
    },
  ]);

  const remove2 = queryManager.addLegacy(ast, 'forever');
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
          hash: '12hwg3ihkijhm',
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
  const mutationTracker = new MutationTracker(lc);
  const queryManager = new QueryManager(
    mutationTracker,
    'client1',
    schema.tables,
    send,
    () => () => {},
    maxRecentQueriesSize,
  );
  const ast1: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
  };

  const ast2: AST = {
    table: 'issue',
    orderBy: [['id', 'desc']],
  };

  const ast3: AST = {
    table: 'user',
    orderBy: [['id', 'asc']],
  };

  const ast4: AST = {
    table: 'user',
    orderBy: [['id', 'desc']],
  };

  const remove1Ast1 = queryManager.addLegacy(ast1, 'forever');
  expect(send).toBeCalledTimes(1);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '12hwg3ihkijhm',
          ast: {
            table: 'issues',
            where: undefined,
            orderBy: [['id', 'asc']],
          } satisfies AST,
          ttl: -1,
        },
      ],
    },
  ]);

  const remove2Ast1 = queryManager.addLegacy(ast1, 'forever');
  expect(send).toBeCalledTimes(1);

  const removeAst2 = queryManager.addLegacy(ast2, 'forever');
  expect(send).toBeCalledTimes(2);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '1hydj1t7t5yv4',
          ast: {
            table: 'issues',
            where: undefined,
            orderBy: [['id', 'desc']],
          } satisfies AST,
          ttl: -1,
        },
      ],
    },
  ]);

  const removeAst3 = queryManager.addLegacy(ast3, 'forever');
  expect(send).toBeCalledTimes(3);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '3c5d3uiyypuxu',
          ast: {
            table: 'users',
            where: undefined,
            orderBy: [['id', 'asc']],
          } satisfies AST,
          ttl: -1,
        },
      ],
    },
  ]);

  const removeAst4 = queryManager.addLegacy(ast4, 'forever');
  expect(send).toBeCalledTimes(4);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '2q7cds8pild5w',
          ast: {
            table: 'users',
            where: undefined,
            orderBy: [['id', 'desc']],
          } satisfies AST,
          ttl: -1,
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
          hash: '12hwg3ihkijhm',
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
          hash: '1hydj1t7t5yv4',
        },
      ],
    },
  ]);
});

test('test add/remove/add/remove changes lru order max recent queries size 2', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 2;
  const mutationTracker = new MutationTracker(lc);
  const queryManager = new QueryManager(
    mutationTracker,
    'client1',
    schema.tables,
    send,
    () => () => {},
    maxRecentQueriesSize,
  );
  const ast1: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
  };

  const ast2: AST = {
    table: 'issue',
    orderBy: [['id', 'desc']],
  };

  const ast3: AST = {
    table: 'user',
    orderBy: [['id', 'asc']],
  };

  const ast4: AST = {
    table: 'user',
    orderBy: [['id', 'desc']],
  };

  const remove1Ast1 = queryManager.addLegacy(ast1, 'forever');
  expect(send).toBeCalledTimes(1);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '12hwg3ihkijhm',
          ast: {
            table: 'issues',
            where: undefined,
            orderBy: [['id', 'asc']],
          } satisfies AST,
          ttl: -1,
        },
      ],
    },
  ]);

  const removeAst2 = queryManager.addLegacy(ast2, 'forever');
  expect(send).toBeCalledTimes(2);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '1hydj1t7t5yv4',
          ast: {
            table: 'issues',
            where: undefined,
            orderBy: [['id', 'desc']],
          } satisfies AST,
          ttl: -1,
        },
      ],
    },
  ]);

  const removeAst3 = queryManager.addLegacy(ast3, 'forever');
  expect(send).toBeCalledTimes(3);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '3c5d3uiyypuxu',
          ast: {
            table: 'users',
            where: undefined,
            orderBy: [['id', 'asc']],
          } satisfies AST,
          ttl: -1,
        },
      ],
    },
  ]);

  const removeAst4 = queryManager.addLegacy(ast4, 'forever');
  expect(send).toBeCalledTimes(4);
  expect(send).toHaveBeenLastCalledWith([
    'changeDesiredQueries',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: '2q7cds8pild5w',
          ast: {
            table: 'users',
            where: undefined,
            orderBy: [['id', 'desc']],
          } satisfies AST,
          ttl: -1,
        },
      ],
    },
  ]);

  remove1Ast1();
  expect(send).toBeCalledTimes(4);

  const remove2Ast1 = queryManager.addLegacy(ast1, 'forever');
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
          hash: '1hydj1t7t5yv4',
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
          hash: '12hwg3ihkijhm',
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

describe('getQueriesPatch', () => {
  test('basics', async () => {
    const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
    const maxRecentQueriesSize = 0;
    const mutationTracker = new MutationTracker(lc);
    const queryManager = new QueryManager(
      mutationTracker,
      'client1',
      schema.tables,
      send,
      () => () => {},
      maxRecentQueriesSize,
    );
    // hash: 12hwg3ihkijhm
    const ast1: AST = {
      table: 'issue',
      orderBy: [['id', 'asc']],
    };
    queryManager.addLegacy(ast1, 'forever');
    // hash 1hydj1t7t5yv4
    const ast2: AST = {
      table: 'issue',
      orderBy: [['id', 'desc']],
    };
    queryManager.addLegacy(ast2, 'forever');

    const testReadTransaction = new TestTransaction();
    testReadTransaction.scanEntries = [
      ['d/client1/12hwg3ihkijhm', 'unused'],
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
            hash: '1hydj1t7t5yv4',
            ast: {
              table: 'issues',
              orderBy: [['id', 'desc']],
            } satisfies AST,
            ttl: -1,
          },
        ].map(x => [x.hash, x] as const),
      ),
    );
    expect(testReadTransaction.scanCalls).toEqual([{prefix: 'd/client1/'}]);
  });

  describe('add a second query with same hash', () => {
    let send: Mock<(arg: ChangeDesiredQueriesMessage) => void>;
    let queryManager: QueryManager;

    beforeEach(() => {
      send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
      const maxRecentQueriesSize = 0;
      const mutationTracker = new MutationTracker(lc);
      queryManager = new QueryManager(
        mutationTracker,
        'client1',
        schema.tables,
        send,
        () => () => {},
        maxRecentQueriesSize,
      );
    });

    async function add(ttl: TTL): Promise<number | undefined> {
      // hash 1hydj1t7t5yv4
      const ast: AST = {
        table: 'issue',
        orderBy: [['id', 'desc']],
      };
      queryManager.addLegacy(ast, ttl);

      const testReadTransaction = new TestTransaction();
      testReadTransaction.scanEntries = [];
      const patch = await queryManager.getQueriesPatch(testReadTransaction);
      expect(testReadTransaction.scanCalls).toEqual([{prefix: 'd/client1/'}]);
      const op = patch.get('1hydj1t7t5yv4');
      v.assert(op, upPutOpSchema);
      return op.ttl;
    }

    test('with first having a ttl', async () => {
      expect(await add(1000)).toBe(1000);
      expect(send).toBeCalledTimes(1);
      expect(send.mock.calls[0]).toMatchInlineSnapshot(`
        [
          [
            "changeDesiredQueries",
            {
              "desiredQueriesPatch": [
                {
                  "args": undefined,
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
                    "table": "issues",
                    "where": undefined,
                  },
                  "hash": "1hydj1t7t5yv4",
                  "name": undefined,
                  "op": "put",
                  "ttl": 1000,
                },
              ],
            },
          ],
        ]
      `);

      send.mockClear();
      expect(await add(2000)).toBe(2000);
      expect(send).toBeCalledTimes(1);
      expect(send.mock.calls[0]).toMatchInlineSnapshot(`
        [
          [
            "changeDesiredQueries",
            {
              "desiredQueriesPatch": [
                {
                  "args": undefined,
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
                    "table": "issues",
                    "where": undefined,
                  },
                  "hash": "1hydj1t7t5yv4",
                  "name": undefined,
                  "op": "put",
                  "ttl": 2000,
                },
              ],
            },
          ],
        ]
      `);

      send.mockClear();
      expect(await add(500)).toBe(2000);
      expect(send).toBeCalledTimes(0);

      send.mockClear();
      expect(await add('forever')).toBe(-1);
      expect(send).toBeCalledTimes(1);
    });

    test('with first NOT having a ttl', async () => {
      expect(await add('none')).toBe(0);
      expect(send).toBeCalledTimes(1);
      expect(send.mock.calls[0]).toMatchInlineSnapshot(`
      [
        [
          "changeDesiredQueries",
          {
            "desiredQueriesPatch": [
              {
                "args": undefined,
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
                  "table": "issues",
                  "where": undefined,
                },
                "hash": "1hydj1t7t5yv4",
                "name": undefined,
                "op": "put",
                "ttl": 0,
              },
            ],
          },
        ],
      ]
    `);

      send.mockClear();
      expect(await add('none')).toBe(0);
      expect(send).toBeCalledTimes(0);

      send.mockClear();
      expect(await add(1000)).toBe(1000);
      expect(send).toBeCalledTimes(1);
      expect(send.mock.calls[0]).toMatchInlineSnapshot(`
        [
          [
            "changeDesiredQueries",
            {
              "desiredQueriesPatch": [
                {
                  "args": undefined,
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
                    "table": "issues",
                    "where": undefined,
                  },
                  "hash": "1hydj1t7t5yv4",
                  "name": undefined,
                  "op": "put",
                  "ttl": 1000,
                },
              ],
            },
          ],
        ]
      `);

      send.mockClear();
      expect(await add('forever')).toBe(-1);
      expect(send).toBeCalledTimes(1);
    });
  });

  test('getQueriesPatch includes recent queries in desired', async () => {
    const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
    const maxRecentQueriesSize = 2;
    const mutationTracker = new MutationTracker(lc);
    const queryManager = new QueryManager(
      mutationTracker,
      'client1',
      schema.tables,
      send,
      () => () => {},
      maxRecentQueriesSize,
    );
    const ast1: AST = {
      table: 'issue',
      orderBy: [['id', 'asc']],
    };
    const remove1 = queryManager.addLegacy(ast1, 'forever');
    const ast2: AST = {
      table: 'issue',
      orderBy: [['id', 'desc']],
    };
    const remove2 = queryManager.addLegacy(ast2, 'forever');
    const ast3: AST = {
      table: 'user',
      orderBy: [['id', 'asc']],
    };
    const remove3 = queryManager.addLegacy(ast3, 'forever');
    const ast4: AST = {
      table: 'user',
      orderBy: [['id', 'desc']],
    };
    const remove4 = queryManager.addLegacy(ast4, 'forever');
    remove1();
    remove2();
    remove3();
    remove4();

    // ast1 and ast2 are actually removed since maxRecentQueriesSize is 2

    const testReadTransaction = new TestTransaction();
    testReadTransaction.scanEntries = [
      ['d/client1/12hwg3ihkijhm', 'unused'],
      ['d/client1/shouldBeDeleted', 'unused'],
    ];

    const patch = await queryManager.getQueriesPatch(testReadTransaction);
    expect(patch).toMatchInlineSnapshot(`
      Map {
        "12hwg3ihkijhm" => {
          "hash": "12hwg3ihkijhm",
          "op": "del",
        },
        "shouldBeDeleted" => {
          "hash": "shouldBeDeleted",
          "op": "del",
        },
        "3c5d3uiyypuxu" => {
          "args": undefined,
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
          "hash": "3c5d3uiyypuxu",
          "name": undefined,
          "op": "put",
          "ttl": -1,
        },
        "2q7cds8pild5w" => {
          "args": undefined,
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
          "hash": "2q7cds8pild5w",
          "name": undefined,
          "op": "put",
          "ttl": -1,
        },
      }
    `);
    expect(testReadTransaction.scanCalls).toEqual([{prefix: 'd/client1/'}]);
  });
});

test('gotCallback, query already got', () => {
  const queryHash = '12hwg3ihkijhm';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();

  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc);
  const queryManager = new QueryManager(
    mutationTracker,
    'client1',
    schema.tables,
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
    table: 'issue',
    orderBy: [['id', 'asc']],
  };

  const gotCallback1 = vi.fn<(got: boolean) => void>();
  const ttl = 200;
  queryManager.addLegacy(ast, ttl, gotCallback1);
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
          ttl,
        },
      ],
    },
  ]);

  expect(gotCallback1).nthCalledWith(1, true);

  const gotCallback2 = vi.fn<(got: boolean) => void>();
  queryManager.addLegacy(ast, ttl, gotCallback2);
  expect(send).toBeCalledTimes(1);

  expect(gotCallback2).nthCalledWith(1, true);
  expect(gotCallback1).toBeCalledTimes(1);
});

test('gotCallback, query got after add', () => {
  const queryHash = '12hwg3ihkijhm';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc);
  const queryManager = new QueryManager(
    mutationTracker,
    'client1',
    schema.tables,
    send,
    experimentalWatch,
    maxRecentQueriesSize,
  );
  expect(experimentalWatch).toBeCalledTimes(1);
  const watchCallback = experimentalWatch.mock.calls[0][0];

  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
  };

  const gotCalback1 = vi.fn<(got: boolean) => void>();
  const ttl = 'forever';
  queryManager.addLegacy(ast, ttl, gotCalback1);
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
          ttl: -1,
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
  const queryHash = '12hwg3ihkijhm';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc);
  const queryManager = new QueryManager(
    mutationTracker,
    'client1',
    schema.tables,
    send,
    experimentalWatch,
    maxRecentQueriesSize,
  );
  expect(experimentalWatch).toBeCalledTimes(1);
  const watchCallback = experimentalWatch.mock.calls[0][0];

  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
  };

  const gotCalback1 = vi.fn<(got: boolean) => void>();
  const ttl = 100;
  queryManager.addLegacy(ast, ttl, gotCalback1);
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
          ttl,
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
  const queryHash = '12hwg3ihkijhm';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(q: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc);
  const queryManager = new QueryManager(
    mutationTracker,
    'client1',
    schema.tables,
    send,
    experimentalWatch,
    maxRecentQueriesSize,
  );
  expect(experimentalWatch).toBeCalledTimes(1);
  const watchCallback = experimentalWatch.mock.calls[0][0];

  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
  };

  const gotCalback1 = vi.fn<(got: boolean) => void>();
  const ttl = 50;
  const remove = queryManager.addLegacy(ast, ttl, gotCalback1);
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
          ttl,
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
    const mutationTracker = new MutationTracker(lc);
    const queryManager = new QueryManager(
      mutationTracker,
      'client1',
      schema.tables,
      send,
      () => () => {},
      maxRecentQueriesSize,
    );

    queryManager.addLegacy(
      {
        table: 'issue',
        orderBy: [['id', 'asc']],
      },
      'forever',
    );
    const testReadTransaction = new TestTransaction();
    const patch = await queryManager.getQueriesPatch(testReadTransaction);
    expect([...patch.values()]).toEqual([
      {
        ast: {
          orderBy: [['id', 'asc']],
          table: 'issues',
          ...normalizingFields,
        },
        hash: '12hwg3ihkijhm',
        op: 'put',
        ttl: -1,
      },
    ]);
  });

  test('removes entries from the patch that are in lastPatch', async () => {
    const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => boolean>(
      () => false,
    );
    const mutationTracker = new MutationTracker(lc);
    const queryManager = new QueryManager(
      mutationTracker,
      'client1',
      schema.tables,
      send,
      () => () => {},
      0,
    );

    const clean = queryManager.addLegacy(
      {
        table: 'issue',
        orderBy: [['id', 'asc']],
      },
      'forever',
      undefined,
    );
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
              table: 'issues',
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
              table: 'issues',
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

test('gotCallback, add same got callback twice', () => {
  const queryHash = '12hwg3ihkijhm';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc);
  const queryManager = new QueryManager(
    mutationTracker,
    'client1',
    schema.tables,
    send,
    experimentalWatch,
    maxRecentQueriesSize,
  );
  expect(experimentalWatch).toBeCalledTimes(1);
  const watchCallback = experimentalWatch.mock.calls[0][0];

  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
  };

  const gotCallback = vi.fn<(got: boolean) => void>();
  const rem1 = queryManager.addLegacy(ast, -1, gotCallback);
  expect(gotCallback).toBeCalledTimes(1);
  expect(gotCallback).toBeCalledWith(false);
  gotCallback.mockClear();

  const rem2 = queryManager.addLegacy(ast, -1, gotCallback);
  expect(gotCallback).toBeCalledTimes(1);
  expect(gotCallback).toBeCalledWith(false);
  gotCallback.mockClear();

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
            orderBy: [['id', 'asc']],
            ...normalizingFields,
          } satisfies AST,
          ttl: -1,
        },
      ],
    },
  ]);

  watchCallback([
    {
      op: 'add',
      key: toGotQueriesKey(queryHash) as string & IndexKey,
      newValue: 'unused',
    },
  ]);

  expect(gotCallback).toBeCalledTimes(2);
  expect(gotCallback).nthCalledWith(1, true);
  expect(gotCallback).nthCalledWith(2, true);

  rem1();
  rem2();
});

describe('query manager & mutator interaction', () => {
  let send: (msg: ChangeDesiredQueriesMessage) => void;
  let mutationTracker: MutationTracker;
  let queryManager: QueryManager;
  const ast1: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
  };
  const ast2: AST = {
    table: 'issue',
    limit: 1,
    orderBy: [['id', 'desc']],
  };

  beforeEach(() => {
    send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
    mutationTracker = new MutationTracker(lc);
    queryManager = new QueryManager(
      mutationTracker,
      'client1',
      schema.tables,
      send,
      () => () => {},
      0,
    );
  });

  test('queries are not removed while there are pending mutations', () => {
    const remove = queryManager.addLegacy(ast1, 0);
    expect(send).toBeCalledTimes(1);

    const {ephemeralID} = mutationTracker.trackMutation();
    mutationTracker.mutationIDAssigned(ephemeralID, 1);

    // try to remove the query
    remove();

    // query was not removed, just have the `add` send
    expect(send).toBeCalledTimes(1);
  });

  test('queued queries are removed once the pending mutation count goes to 0', () => {
    const remove1 = queryManager.addLegacy(ast1, 0);
    const remove2 = queryManager.addLegacy(ast2, 0);
    // once for each add
    expect(send).toBeCalledTimes(2);

    const {ephemeralID} = mutationTracker.trackMutation();
    mutationTracker.mutationIDAssigned(ephemeralID, 1);

    remove1();
    remove2();

    // send is still stuck at 2 -- no remove calls went through
    expect(send).toBeCalledTimes(2);

    mutationTracker.onConnected(1);
    // send was called for each removed query that was queued
    expect(send).toBeCalledTimes(4);
  });

  test('queries are removed immediately if there are no pending mutations', () => {
    const remove1 = queryManager.addLegacy(ast1, 0);
    const remove2 = queryManager.addLegacy(ast2, 0);
    expect(send).toBeCalledTimes(2);
    remove1();
    expect(send).toBeCalledTimes(3);
    remove2();
    expect(send).toBeCalledTimes(4);
  });
});
