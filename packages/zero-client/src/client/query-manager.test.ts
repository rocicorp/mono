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
import {putOpSchema} from '../../../zero-protocol/src/queries-patch.ts';
import {schema} from '../../../zql/src/query/test/test-schemas.ts';
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
    schema.tables,
    send,
    () => () => {},
    maxRecentQueriesSize,
  );
  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
  };
  queryManager.add(ast, undefined);
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
          ttl: undefined,
        },
      ],
    },
  ]);

  queryManager.add(ast, undefined);
  expect(send).toBeCalledTimes(1);
});

test('add renamed fields', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const queryManager = new QueryManager(
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

  queryManager.add(ast, undefined);
  expect(send).toBeCalledTimes(1);
  expect(send.mock.calls[0][0]).toMatchInlineSnapshot(`
    [
      "changeDesiredQueries",
      {
        "desiredQueriesPatch": [
          {
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
            "op": "put",
            "ttl": undefined,
          },
        ],
      },
    ]
  `);
});

test('remove, recent queries max size 0', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const queryManager = new QueryManager(
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

  const remove1 = queryManager.add(ast, undefined);
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
          ttl: undefined,
        },
      ],
    },
  ]);

  const remove2 = queryManager.add(ast, undefined);
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
  const queryManager = new QueryManager(
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

  const remove1Ast1 = queryManager.add(ast1, undefined);
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
          ttl: undefined,
        },
      ],
    },
  ]);

  const remove2Ast1 = queryManager.add(ast1, undefined);
  expect(send).toBeCalledTimes(1);

  const removeAst2 = queryManager.add(ast2, undefined);
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
          ttl: undefined,
        },
      ],
    },
  ]);

  const removeAst3 = queryManager.add(ast3, undefined);
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
          ttl: undefined,
        },
      ],
    },
  ]);

  const removeAst4 = queryManager.add(ast4, undefined);
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
          ttl: undefined,
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
  const queryManager = new QueryManager(
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

  const remove1Ast1 = queryManager.add(ast1, undefined);
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
          ttl: undefined,
        },
      ],
    },
  ]);

  const removeAst2 = queryManager.add(ast2, undefined);
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
          ttl: undefined,
        },
      ],
    },
  ]);

  const removeAst3 = queryManager.add(ast3, undefined);
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
          ttl: undefined,
        },
      ],
    },
  ]);

  const removeAst4 = queryManager.add(ast4, undefined);
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
          ttl: undefined,
        },
      ],
    },
  ]);

  remove1Ast1();
  expect(send).toBeCalledTimes(4);

  const remove2Ast1 = queryManager.add(ast1, undefined);
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
    const queryManager = new QueryManager(
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
    queryManager.add(ast1, undefined);
    // hash 1hydj1t7t5yv4
    const ast2: AST = {
      table: 'issue',
      orderBy: [['id', 'desc']],
    };
    queryManager.add(ast2, undefined);

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
            ttl: undefined,
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
      queryManager = new QueryManager(
        'client1',
        schema.tables,
        send,
        () => () => {},
        maxRecentQueriesSize,
      );
    });

    async function add(ttl: number | undefined) {
      // hash 1hydj1t7t5yv4
      const ast: AST = {
        table: 'issue',
        orderBy: [['id', 'desc']],
      };
      queryManager.add(ast, ttl);

      const testReadTransaction = new TestTransaction();
      testReadTransaction.scanEntries = [];
      const patch = await queryManager.getQueriesPatch(testReadTransaction);
      expect(testReadTransaction.scanCalls).toEqual([{prefix: 'd/client1/'}]);
      const op = patch.get('1hydj1t7t5yv4');
      v.assert(op, putOpSchema);
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
      expect(await add(undefined)).toBe(2000);
      expect(send).toBeCalledTimes(0);
    });

    test('with first NOT having a ttl', async () => {
      expect(await add(undefined)).toBe(undefined);
      expect(send).toBeCalledTimes(1);
      expect(send.mock.calls[0]).toMatchInlineSnapshot(`
        [
          [
            "changeDesiredQueries",
            {
              "desiredQueriesPatch": [
                {
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
                  "op": "put",
                  "ttl": undefined,
                },
              ],
            },
          ],
        ]
      `);

      send.mockClear();
      expect(await add(undefined)).toBe(undefined);
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
                  "op": "put",
                  "ttl": 1000,
                },
              ],
            },
          ],
        ]
      `);

      send.mockClear();
      expect(await add(undefined)).toBe(1000);
      expect(send).toBeCalledTimes(0);
    });
  });

  test('getQueriesPatch includes recent queries in desired', async () => {
    const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
    const maxRecentQueriesSize = 2;
    const queryManager = new QueryManager(
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
    const remove1 = queryManager.add(ast1, undefined);
    const ast2: AST = {
      table: 'issue',
      orderBy: [['id', 'desc']],
    };
    const remove2 = queryManager.add(ast2, undefined);
    const ast3: AST = {
      table: 'user',
      orderBy: [['id', 'asc']],
    };
    const remove3 = queryManager.add(ast3, undefined);
    const ast4: AST = {
      table: 'user',
      orderBy: [['id', 'desc']],
    };
    const remove4 = queryManager.add(ast4, undefined);
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
        "op": "put",
        "ttl": undefined,
      },
      "2q7cds8pild5w" => {
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
        "op": "put",
        "ttl": undefined,
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
  const queryManager = new QueryManager(
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
  queryManager.add(ast, 11, gotCallback1);
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
          ttl: 11,
        },
      ],
    },
  ]);

  expect(gotCallback1).nthCalledWith(1, true);

  const gotCallback2 = vi.fn<(got: boolean) => void>();
  queryManager.add(ast, 0, gotCallback2);
  expect(send).toBeCalledTimes(1);

  expect(gotCallback2).nthCalledWith(1, true);
  expect(gotCallback1).toBeCalledTimes(1);
});

test('gotCallback, query got after add', () => {
  const queryHash = '12hwg3ihkijhm';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const queryManager = new QueryManager(
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

  const gotCallback1 = vi.fn<(got: boolean) => void>();
  queryManager.add(ast, 789, gotCallback1);
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
          ttl: 789,
        },
      ],
    },
  ]);

  expect(gotCallback1).nthCalledWith(1, false);

  watchCallback([
    {
      op: 'add',
      key: toGotQueriesKey(queryHash) as string & IndexKey,
      newValue: 'unused',
    },
  ]);

  expect(gotCallback1).nthCalledWith(2, true);
});

test('gotCallback, query got after add then removed', () => {
  const queryHash = '12hwg3ihkijhm';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const queryManager = new QueryManager(
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

  const gotCallback1 = vi.fn<(got: boolean) => void>();
  queryManager.add(ast, 456, gotCallback1);
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
          ttl: 456,
        },
      ],
    },
  ]);

  expect(gotCallback1).nthCalledWith(1, false);

  watchCallback([
    {
      op: 'add',
      key: toGotQueriesKey(queryHash) as string & IndexKey,
      newValue: 'unused',
    },
  ]);

  expect(gotCallback1).nthCalledWith(2, true);

  watchCallback([
    {
      op: 'del',
      key: toGotQueriesKey(queryHash) as string & IndexKey,
      oldValue: 'unused',
    },
  ]);

  expect(gotCallback1).nthCalledWith(3, false);
});

test('gotCallback, query got after subscription removed', () => {
  const queryHash = '12hwg3ihkijhm';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(q: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const queryManager = new QueryManager(
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

  const gotCallback1 = vi.fn<(got: boolean) => void>();
  const remove = queryManager.add(ast, 123, gotCallback1);
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
          ttl: 123,
        },
      ],
    },
  ]);

  expect(gotCallback1).nthCalledWith(1, false);

  remove();

  expect(gotCallback1).toBeCalledTimes(1);
  watchCallback([
    {
      op: 'add',
      key: toGotQueriesKey(queryHash) as string & IndexKey,
      newValue: 'unused',
    },
  ]);

  expect(gotCallback1).toBeCalledTimes(1);
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
      schema.tables,
      send,
      () => () => {},
      maxRecentQueriesSize,
    );

    queryManager.add(
      {
        table: 'issue',
        orderBy: [['id', 'asc']],
      },
      undefined,
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
        ttl: undefined,
      },
    ]);
  });

  test('removes entries from the patch that are in lastPatch', async () => {
    const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => boolean>(
      () => false,
    );
    const queryManager = new QueryManager(
      'client1',
      schema.tables,
      send,
      () => () => {},
      0,
    );

    const clean = queryManager.add(
      {
        table: 'issue',
        orderBy: [['id', 'asc']],
      },
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
            ttl: undefined,
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
            ttl: undefined,
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
