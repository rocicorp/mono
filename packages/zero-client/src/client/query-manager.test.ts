import {LogContext, type LogSink} from '@rocicorp/logger';
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  test,
  vi,
  type Mock,
} from 'vitest';
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
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import * as v from '../../../shared/src/valita.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {ChangeDesiredQueriesMessage} from '../../../zero-protocol/src/change-desired-queries.ts';
import type {ErroredQuery} from '../../../zero-protocol/src/custom-queries.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {putDesiredQueryOpSchema} from '../../../zero-protocol/src/queries-patch.ts';
import {
  hashOfAST,
  hashOfNameAndArgs,
} from '../../../zero-protocol/src/query-hash.ts';
import {schema} from '../../../zql/src/query/test/test-schemas.ts';
import {MAX_TTL_MS, type TTL} from '../../../zql/src/query/ttl.ts';
import {ClientErrorKind} from './client-error-kind.ts';
import {ClientError} from './error.ts';
import {toGotQueriesKey} from './keys.ts';
import {MutationTracker} from './mutation-tracker.ts';
import {QueryManager} from './query-manager.ts';

const slowMaterializeThreshold = Infinity; // Disable slow materialization logs for tests.

function createExperimentalWatchMock() {
  return vi.fn();
}

const ackMutations = () => {};
const onFatalError = () => {};

const queryChangeThrottleMs = 10;
const lc = createSilentLogContext();
test('add', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
  const queryManager = new QueryManager(
    lc,
    mutationTracker,
    'client1',
    schema.tables,
    send,
    () => () => {},
    maxRecentQueriesSize,
    queryChangeThrottleMs,
    slowMaterializeThreshold,
    onFatalError,
  );
  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
  };
  queryManager.addLegacy(ast, 'forever');
  queryManager.flushBatch();
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
          ttl: MAX_TTL_MS,
        },
      ],
    },
  ]);

  queryManager.addLegacy(ast, 'forever');
  queryManager.flushBatch();
  expect(send).toBeCalledTimes(1);
});

test('add and remove a custom query', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
  const queryManager = new QueryManager(
    lc,
    mutationTracker,
    'client1',
    schema.tables,
    send,
    () => () => {},
    maxRecentQueriesSize,
    queryChangeThrottleMs,
    slowMaterializeThreshold,
    onFatalError,
  );
  const ast: AST = {
    table: 'issue',
  };
  const rm1 = queryManager.addCustom(
    ast,
    {name: 'customQuery', args: [1]},
    '1m',
  );
  queryManager.flushBatch();
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

  const rm2 = queryManager.addCustom(
    ast,
    {name: 'customQuery', args: [1]},
    '1m',
  );
  queryManager.flushBatch();
  expect(send).toBeCalledTimes(1);

  rm2();
  queryManager.flushBatch();
  const rm3 = queryManager.addCustom(
    ast,
    {name: 'customQuery', args: [1]},
    '1m',
  );
  queryManager.flushBatch();
  expect(send).toBeCalledTimes(1);
  rm1();
  queryManager.flushBatch();
  rm3();
  queryManager.flushBatch();
  queryManager.addCustom(ast, {name: 'customQuery', args: [1]}, '1m');
  queryManager.flushBatch();
  // once for del, another for put
  expect(send).toBeCalledTimes(3);

  send.mockClear();

  // now update the custom query
  queryManager.updateCustom({name: 'customQuery', args: [1]}, '2m');
  queryManager.flushBatch();
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

  queryManager.updateCustom({name: 'customQuery', args: [1]}, '1m');
  queryManager.flushBatch();
  // send not called with lower ttl
  expect(send).toBeCalledTimes(1);
});

test('add renamed fields', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
  const queryManager = new QueryManager(
    lc,
    mutationTracker,
    'client1',
    schema.tables,
    send,
    () => () => {},
    maxRecentQueriesSize,
    queryChangeThrottleMs,
    slowMaterializeThreshold,
    onFatalError,
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
  queryManager.flushBatch();
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
            "ttl": 600000,
          },
        ],
      },
    ]
  `);
});

test('remove, recent queries max size 0', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
  const queryManager = new QueryManager(
    lc,
    mutationTracker,
    'client1',
    schema.tables,
    send,
    () => () => {},
    maxRecentQueriesSize,
    queryChangeThrottleMs,
    slowMaterializeThreshold,
    onFatalError,
  );
  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
  };

  const remove1 = queryManager.addLegacy(ast, 'forever');
  queryManager.flushBatch();
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
          ttl: MAX_TTL_MS,
        },
      ],
    },
  ]);

  const remove2 = queryManager.addLegacy(ast, 'forever');
  queryManager.flushBatch();
  expect(send).toBeCalledTimes(1);

  remove1();
  queryManager.flushBatch();
  expect(send).toBeCalledTimes(1);
  remove2();
  queryManager.flushBatch();
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
  queryManager.flushBatch();
  expect(send).toBeCalledTimes(2);
});

test('remove, max recent queries size 2', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 2;
  const mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
  const queryManager = new QueryManager(
    lc,
    mutationTracker,
    'client1',
    schema.tables,
    send,
    () => () => {},
    maxRecentQueriesSize,
    queryChangeThrottleMs,
    slowMaterializeThreshold,
    onFatalError,
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
  queryManager.flushBatch();
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
          ttl: MAX_TTL_MS,
        },
      ],
    },
  ]);

  const remove2Ast1 = queryManager.addLegacy(ast1, 'forever');
  queryManager.flushBatch();
  expect(send).toBeCalledTimes(1);

  const removeAst2 = queryManager.addLegacy(ast2, 'forever');
  queryManager.flushBatch();
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
          ttl: MAX_TTL_MS,
        },
      ],
    },
  ]);

  const removeAst3 = queryManager.addLegacy(ast3, 'forever');
  queryManager.flushBatch();
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
          ttl: MAX_TTL_MS,
        },
      ],
    },
  ]);

  const removeAst4 = queryManager.addLegacy(ast4, 'forever');
  queryManager.flushBatch();
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
          ttl: MAX_TTL_MS,
        },
      ],
    },
  ]);

  remove1Ast1();
  queryManager.flushBatch();
  expect(send).toBeCalledTimes(4);
  remove2Ast1();
  queryManager.flushBatch();
  expect(send).toBeCalledTimes(4);

  removeAst2();
  queryManager.flushBatch();
  expect(send).toBeCalledTimes(4);

  removeAst3();
  queryManager.flushBatch();
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
  queryManager.flushBatch();
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

test('add/remove/add/remove changes lru order max recent queries size 2', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 2;
  const mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
  const queryManager = new QueryManager(
    lc,
    mutationTracker,
    'client1',
    schema.tables,
    send,
    () => () => {},
    maxRecentQueriesSize,
    queryChangeThrottleMs,
    slowMaterializeThreshold,
    onFatalError,
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
  queryManager.flushBatch();
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
          ttl: MAX_TTL_MS,
        },
      ],
    },
  ]);

  const removeAst2 = queryManager.addLegacy(ast2, 'forever');
  queryManager.flushBatch();
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
          ttl: MAX_TTL_MS,
        },
      ],
    },
  ]);

  const removeAst3 = queryManager.addLegacy(ast3, 'forever');
  queryManager.flushBatch();
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
          ttl: MAX_TTL_MS,
        },
      ],
    },
  ]);

  const removeAst4 = queryManager.addLegacy(ast4, 'forever');
  queryManager.flushBatch();
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
          ttl: MAX_TTL_MS,
        },
      ],
    },
  ]);

  remove1Ast1();
  queryManager.flushBatch();
  expect(send).toBeCalledTimes(4);

  const remove2Ast1 = queryManager.addLegacy(ast1, 'forever');
  queryManager.flushBatch();
  expect(send).toBeCalledTimes(4);

  removeAst2();
  queryManager.flushBatch();
  expect(send).toBeCalledTimes(4);

  remove2Ast1();
  queryManager.flushBatch();
  expect(send).toBeCalledTimes(4);

  removeAst3();
  queryManager.flushBatch();

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
  queryManager.flushBatch();
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
    const mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
    const queryManager = new QueryManager(
      lc,
      mutationTracker,
      'client1',
      schema.tables,
      send,
      () => () => {},
      maxRecentQueriesSize,
      queryChangeThrottleMs,
      slowMaterializeThreshold,
      onFatalError,
    );
    // hash: 12hwg3ihkijhm
    const ast1: AST = {
      table: 'issue',
      orderBy: [['id', 'asc']],
    };
    queryManager.addLegacy(ast1, 'forever');
    queryManager.flushBatch();
    // hash 1hydj1t7t5yv4
    const ast2: AST = {
      table: 'issue',
      orderBy: [['id', 'desc']],
    };
    queryManager.addLegacy(ast2, 'forever');
    queryManager.flushBatch();

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
            ttl: MAX_TTL_MS,
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
      const mutationTracker = new MutationTracker(
        lc,
        ackMutations,
        onFatalError,
      );
      queryManager = new QueryManager(
        lc,
        mutationTracker,
        'client1',
        schema.tables,
        send,
        () => () => {},
        maxRecentQueriesSize,
        queryChangeThrottleMs,
        slowMaterializeThreshold,
        onFatalError,
      );
    });

    async function add(ttl: TTL): Promise<number | undefined> {
      // hash 1hydj1t7t5yv4
      const ast: AST = {
        table: 'issue',
        orderBy: [['id', 'desc']],
      };
      queryManager.addLegacy(ast, ttl);
      queryManager.flushBatch();

      const testReadTransaction = new TestTransaction();
      testReadTransaction.scanEntries = [];
      const patch = await queryManager.getQueriesPatch(testReadTransaction);
      expect(testReadTransaction.scanCalls).toEqual([{prefix: 'd/client1/'}]);
      const op = patch.get('1hydj1t7t5yv4');
      v.assert(op, putDesiredQueryOpSchema);
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
      expect(await add('forever')).toBe(MAX_TTL_MS);
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
      expect(await add('forever')).toBe(MAX_TTL_MS);
      expect(send).toBeCalledTimes(1);
    });
  });

  test('getQueriesPatch includes recent queries in desired', async () => {
    const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
    const maxRecentQueriesSize = 2;
    const mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
    const queryManager = new QueryManager(
      lc,
      mutationTracker,
      'client1',
      schema.tables,
      send,
      () => () => {},
      maxRecentQueriesSize,
      queryChangeThrottleMs,
      slowMaterializeThreshold,
      onFatalError,
    );
    const ast1: AST = {
      table: 'issue',
      orderBy: [['id', 'asc']],
    };
    const remove1 = queryManager.addLegacy(ast1, 'forever');
    queryManager.flushBatch();
    const ast2: AST = {
      table: 'issue',
      orderBy: [['id', 'desc']],
    };
    const remove2 = queryManager.addLegacy(ast2, 'forever');
    queryManager.flushBatch();
    const ast3: AST = {
      table: 'user',
      orderBy: [['id', 'asc']],
    };
    const remove3 = queryManager.addLegacy(ast3, 'forever');
    queryManager.flushBatch();
    const ast4: AST = {
      table: 'user',
      orderBy: [['id', 'desc']],
    };
    const remove4 = queryManager.addLegacy(ast4, 'forever');
    queryManager.flushBatch();
    remove1();
    queryManager.flushBatch();
    remove2();
    queryManager.flushBatch();
    remove3();
    queryManager.flushBatch();
    remove4();
    queryManager.flushBatch();

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
            "ttl": 600000,
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
            "ttl": 600000,
          },
        }
      `);
    expect(testReadTransaction.scanCalls).toEqual([{prefix: 'd/client1/'}]);
  });
});

test('handleClosed marks queries as errored exactly once', () => {
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
  const queryManager = new QueryManager(
    lc,
    mutationTracker,
    'client1',
    schema.tables,
    send,
    experimentalWatch,
    maxRecentQueriesSize,
    queryChangeThrottleMs,
    slowMaterializeThreshold,
    onFatalError,
  );

  const ast: AST = {table: 'issue'};
  const queryHash = hashOfAST(ast);
  const gotCallback = vi.fn<(got: boolean, error?: ErroredQuery) => void>();

  queryManager.addLegacy(ast, 0, gotCallback);
  queryManager.flushBatch();

  expect(gotCallback).toHaveBeenCalledOnce();
  expect(gotCallback).toHaveBeenCalledWith(false);

  const reason = new ClientError({
    kind: ClientErrorKind.ClientClosed,
    message: 'Zero closed by test',
  });

  queryManager.handleClosed(reason);

  expect(gotCallback).toHaveBeenCalledTimes(2);
  const [, closedError] = gotCallback.mock.calls[1];
  expect(closedError).toEqual({
    error: 'app',
    id: queryHash,
    name: 'legacy',
    message: 'Zero closed by test',
    details: {kind: ClientErrorKind.ClientClosed},
  });

  // invoking again is a no-op
  queryManager.handleClosed(reason);
  expect(gotCallback).toHaveBeenCalledTimes(2);
});

test('gotCallback, query already got', () => {
  const queryHash = '12hwg3ihkijhm';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();

  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
  const queryManager = new QueryManager(
    lc,
    mutationTracker,
    'client1',
    schema.tables,
    send,
    experimentalWatch,
    maxRecentQueriesSize,
    queryChangeThrottleMs,
    slowMaterializeThreshold,
    onFatalError,
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
  queryManager.flushBatch();
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
  queryManager.flushBatch();
  expect(send).toBeCalledTimes(1);

  expect(gotCallback2).nthCalledWith(1, true);
  expect(gotCallback1).toBeCalledTimes(1);
});

test('gotCallback, query got after add', () => {
  const queryHash = '12hwg3ihkijhm';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
  const queryManager = new QueryManager(
    lc,
    mutationTracker,
    'client1',
    schema.tables,
    send,
    experimentalWatch,
    maxRecentQueriesSize,
    queryChangeThrottleMs,
    slowMaterializeThreshold,
    onFatalError,
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
  queryManager.flushBatch();
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
          ttl: MAX_TTL_MS,
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
  const mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
  const queryManager = new QueryManager(
    lc,
    mutationTracker,
    'client1',
    schema.tables,
    send,
    experimentalWatch,
    maxRecentQueriesSize,
    queryChangeThrottleMs,
    slowMaterializeThreshold,
    onFatalError,
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
  queryManager.flushBatch();
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
  const mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
  const queryManager = new QueryManager(
    lc,
    mutationTracker,
    'client1',
    schema.tables,
    send,
    experimentalWatch,
    maxRecentQueriesSize,
    queryChangeThrottleMs,
    slowMaterializeThreshold,
    onFatalError,
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
  queryManager.flushBatch();
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
  queryManager.flushBatch();

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
    const mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
    const queryManager = new QueryManager(
      lc,
      mutationTracker,
      'client1',
      schema.tables,
      send,
      () => () => {},
      maxRecentQueriesSize,
      queryChangeThrottleMs,
      slowMaterializeThreshold,
      onFatalError,
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
        ttl: MAX_TTL_MS,
      },
    ]);
  });

  test('removes entries from the patch that are in lastPatch', async () => {
    const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => boolean>(
      () => false,
    );
    const mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
    const queryManager = new QueryManager(
      lc,
      mutationTracker,
      'client1',
      schema.tables,
      send,
      () => () => {},
      0,
      queryChangeThrottleMs,
      slowMaterializeThreshold,
      onFatalError,
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

const stubAst: AST = {
  table: 'issue',
  orderBy: [['id', 'asc']],
};

describe('query transform errors', () => {
  test('got is called with an error when an app error occurs', () => {
    const onFatalErrorMock = vi.fn();
    const nameAndArgs = {name: 'custom1', args: []};
    const nameAndArgs2 = {name: 'custom2', args: []};

    const queryHash = hashOfNameAndArgs(nameAndArgs.name, nameAndArgs.args);
    const experimentalWatch = createExperimentalWatchMock();
    const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
    const maxRecentQueriesSize = 0;
    const mutationTracker = new MutationTracker(
      lc,
      ackMutations,
      onFatalErrorMock,
    );
    const queryManager = new QueryManager(
      lc,
      mutationTracker,
      'client1',
      schema.tables,
      send,
      experimentalWatch,
      maxRecentQueriesSize,
      queryChangeThrottleMs,
      slowMaterializeThreshold,
      onFatalErrorMock,
    );

    const gotCallback1 = vi.fn<(got: boolean | Error) => void>();
    const gotCallback2 = vi.fn<(got: boolean | Error) => void>();
    const gotCallback1Dupe = vi.fn<(got: boolean | Error) => void>();

    queryManager.addCustom(stubAst, nameAndArgs, 0, gotCallback1);
    // duplicate addition of same query
    queryManager.addCustom(stubAst, nameAndArgs, 0, gotCallback1Dupe);
    queryManager.addCustom(stubAst, nameAndArgs2, 0, gotCallback2);
    queryManager.flushBatch();

    function checkInitialGots(cb: Mock<(got: boolean | Error) => void>) {
      expect(cb).toBeCalledTimes(1);
      expect(cb).toBeCalledWith(false);
    }

    checkInitialGots(gotCallback1);
    checkInitialGots(gotCallback1Dupe);
    checkInitialGots(gotCallback2);

    const err = {
      details: 'injected failure',
      error: 'app',
      id: queryHash,
      name: 'custom1',
      message: 'failed to transform query',
    } as const;

    // set an error
    queryManager.handleTransformErrors([err]);

    function checkFinalGots(cb: Mock<(got: boolean | Error) => void>) {
      expect(cb).toBeCalledTimes(2);
      expect(cb).nthCalledWith(2, false, err);
    }

    checkFinalGots(gotCallback1);
    checkFinalGots(gotCallback1Dupe);
    // error does not notify unrelated queries
    expect(gotCallback2).toHaveBeenCalledTimes(1);
    expect(onFatalErrorMock).not.toHaveBeenCalled();
  });

  test('got is called with an error when a parse error occurs', () => {
    const onFatalErrorMock = vi.fn();
    const nameAndArgs = {name: 'custom1', args: []};
    const nameAndArgs2 = {name: 'custom2', args: []};

    const queryHash = hashOfNameAndArgs(nameAndArgs.name, nameAndArgs.args);
    const experimentalWatch = createExperimentalWatchMock();
    const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
    const maxRecentQueriesSize = 0;
    const mutationTracker = new MutationTracker(
      lc,
      ackMutations,
      onFatalErrorMock,
    );
    const queryManager = new QueryManager(
      lc,
      mutationTracker,
      'client1',
      schema.tables,
      send,
      experimentalWatch,
      maxRecentQueriesSize,
      queryChangeThrottleMs,
      slowMaterializeThreshold,
      onFatalErrorMock,
    );

    const gotCallback1 = vi.fn<(got: boolean | Error) => void>();
    const gotCallback2 = vi.fn<(got: boolean | Error) => void>();

    queryManager.addCustom(stubAst, nameAndArgs, 0, gotCallback1);
    queryManager.addCustom(stubAst, nameAndArgs2, 0, gotCallback2);
    queryManager.flushBatch();

    expect(gotCallback1).toHaveBeenCalledOnce();
    expect(gotCallback1).toHaveBeenCalledWith(false);
    expect(gotCallback2).toHaveBeenCalledOnce();
    expect(gotCallback2).toHaveBeenCalledWith(false);

    const parseError: ErroredQuery = {
      error: 'parse',
      id: queryHash,
      name: 'custom1',
      message: 'invalid args',
    };

    queryManager.handleTransformErrors([parseError]);

    expect(gotCallback1).toHaveBeenCalledTimes(2);
    expect(gotCallback1).nthCalledWith(2, false, parseError);
    expect(gotCallback2).toHaveBeenCalledTimes(1);
    expect(onFatalErrorMock).not.toHaveBeenCalled();
  });

  test('calls onFatalError for non-app errors', () => {
    const onFatalErrorMock = vi.fn();
    const experimentalWatch = createExperimentalWatchMock();
    const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
    const maxRecentQueriesSize = 0;
    const mutationTracker = new MutationTracker(
      lc,
      ackMutations,
      onFatalErrorMock,
    );
    const queryManager = new QueryManager(
      lc,
      mutationTracker,
      'client1',
      schema.tables,
      send,
      experimentalWatch,
      maxRecentQueriesSize,
      queryChangeThrottleMs,
      slowMaterializeThreshold,
      onFatalErrorMock,
    );

    const ast: AST = {table: 'issue', orderBy: [['id', 'asc']]};
    const queryHash = hashOfAST(ast);
    queryManager.addLegacy(ast, 0);

    const error = {
      error: 'http' as const,
      id: queryHash,
      name: 'testQuery',
      status: 500,
      details: 'Transform failed',
    } as unknown as ErroredQuery;

    queryManager.handleTransformErrors([error]);

    expect(onFatalErrorMock).toHaveBeenCalledOnce();
    expect(onFatalErrorMock).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'ProtocolError',
        errorBody: expect.objectContaining({
          kind: ErrorKind.TransformFailed,
        }),
      }),
    );
  });

  test('calls onFatalError for each non-app error in batch', () => {
    const onFatalErrorMock = vi.fn();
    const experimentalWatch = createExperimentalWatchMock();
    const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
    const maxRecentQueriesSize = 0;
    const mutationTracker = new MutationTracker(
      lc,
      ackMutations,
      onFatalErrorMock,
    );
    const queryManager = new QueryManager(
      lc,
      mutationTracker,
      'client1',
      schema.tables,
      send,
      experimentalWatch,
      maxRecentQueriesSize,
      queryChangeThrottleMs,
      slowMaterializeThreshold,
      onFatalErrorMock,
    );

    const ast1: AST = {table: 'issue', orderBy: [['id', 'asc']]};
    const ast2: AST = {table: 'label', orderBy: [['id', 'asc']]};
    const queryHash1 = hashOfAST(ast1);
    const queryHash2 = hashOfAST(ast2);

    queryManager.addLegacy(ast1, 0);
    queryManager.addLegacy(ast2, 0);

    const errors = [
      {
        error: 'http' as const,
        id: queryHash1,
        name: 'query1',
        status: 500,
        details: 'Transform failed',
      },
      {
        error: 'app' as const,
        id: queryHash2,
        name: 'query2',
        details: 'App error',
      },
    ] as unknown as ErroredQuery[];

    queryManager.handleTransformErrors(errors);

    // Should only be called once (for the http error, not the app error)
    expect(onFatalErrorMock).toHaveBeenCalledOnce();
    expect(onFatalErrorMock).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'ProtocolError',
        errorBody: expect.objectContaining({
          kind: ErrorKind.TransformFailed,
        }),
      }),
    );
  });
});

test('gotCallback, add same got callback twice', () => {
  const queryHash = '12hwg3ihkijhm';
  const experimentalWatch = createExperimentalWatchMock();
  const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
  const queryManager = new QueryManager(
    lc,
    mutationTracker,
    'client1',
    schema.tables,
    send,
    experimentalWatch,
    maxRecentQueriesSize,
    queryChangeThrottleMs,
    slowMaterializeThreshold,
    onFatalError,
  );
  expect(experimentalWatch).toBeCalledTimes(1);
  const watchCallback = experimentalWatch.mock.calls[0][0];

  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
  };

  const gotCallback = vi.fn<(got: boolean) => void>();
  const rem1 = queryManager.addLegacy(ast, -1, gotCallback);
  queryManager.flushBatch();
  expect(gotCallback).toBeCalledTimes(1);
  expect(gotCallback).toBeCalledWith(false);
  gotCallback.mockClear();

  const rem2 = queryManager.addLegacy(ast, -1, gotCallback);
  queryManager.flushBatch();
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
          ttl: MAX_TTL_MS,
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

test('batching multiple operations in same microtask', () => {
  const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
  const queryManager = new QueryManager(
    lc,
    mutationTracker,
    'client1',
    schema.tables,
    send,
    () => () => {},
    maxRecentQueriesSize,
    queryChangeThrottleMs,
    slowMaterializeThreshold,
    onFatalError,
  );

  // Add multiple queries synchronously - should be batched
  const ast: AST = {table: 'issue', orderBy: [['id', 'desc']]};
  queryManager.addLegacy({table: 'issue', orderBy: [['id', 'asc']]}, 'forever');
  queryManager.addLegacy(ast, 'forever');

  queryManager.addCustom(ast, {name: 'customQuery1', args: [1]}, '1m');
  queryManager.addCustom(ast, {name: 'customQuery2', args: [2]}, '1m');

  expect(send).toBeCalledTimes(0); // No calls yet

  queryManager.flushBatch();

  // All 4 operations should be batched into 1 message with 4 operations
  expect(send).toBeCalledTimes(1);
  const call = send.mock.calls[0][0];
  expect(call[0]).toBe('changeDesiredQueries');
  expect(call[1].desiredQueriesPatch).toHaveLength(4);
  expect(call[1].desiredQueriesPatch.every(op => op.op === 'put')).toBe(true);
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
    mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
    mutationTracker.setClientIDAndWatch('cid', () => () => {});
    queryManager = new QueryManager(
      lc,
      mutationTracker,
      'client1',
      schema.tables,
      send,
      () => () => {},
      0,
      queryChangeThrottleMs,
      slowMaterializeThreshold,
      onFatalError,
    );
  });

  test('queries are not removed while there are pending mutations', () => {
    const remove = queryManager.addLegacy(ast1, 0);
    queryManager.flushBatch();
    expect(send).toBeCalledTimes(1);

    const {ephemeralID} = mutationTracker.trackMutation();
    mutationTracker.mutationIDAssigned(ephemeralID, 1);

    // try to remove the query
    remove();
    queryManager.flushBatch();

    // query was not removed, just have the `add` send
    expect(send).toBeCalledTimes(1);
  });

  test('queued queries are removed once the pending mutation count goes to 0', () => {
    const remove1 = queryManager.addLegacy(ast1, 0);
    queryManager.flushBatch();
    const remove2 = queryManager.addLegacy(ast2, 0);
    queryManager.flushBatch();
    // once for each add
    expect(send).toBeCalledTimes(2);

    const {ephemeralID} = mutationTracker.trackMutation();
    mutationTracker.mutationIDAssigned(ephemeralID, 1);

    remove1();
    queryManager.flushBatch();
    remove2();
    queryManager.flushBatch();

    // send is still stuck at 2 -- no remove calls went through
    expect(send).toBeCalledTimes(2);

    mutationTracker.onConnected(1);
    queryManager.flushBatch();
    // send was called once for batched removed queries that were queued
    expect(send).toBeCalledTimes(3);
  });

  test('queries are removed immediately if there are no pending mutations', () => {
    const remove1 = queryManager.addLegacy(ast1, 0);
    queryManager.flushBatch();
    const remove2 = queryManager.addLegacy(ast2, 0);
    queryManager.flushBatch();
    expect(send).toBeCalledTimes(2);
    remove1();
    queryManager.flushBatch();
    expect(send).toBeCalledTimes(3);
    remove2();
    queryManager.flushBatch();
    expect(send).toBeCalledTimes(4);
  });
});

describe('Adding a query with TTL too large should warn', () => {
  const context = {['QueryManager']: undefined} as const;
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const logSink = {
    log: vi.fn<LogSink['log']>(),
  };
  const lcTest = new LogContext('debug', {}, logSink);
  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(
    lcTest,
    ackMutations,
    onFatalError,
  );
  const queryManager = new QueryManager(
    lcTest,
    mutationTracker,
    'client1',
    schema.tables,
    send,
    () => () => {},
    maxRecentQueriesSize,
    queryChangeThrottleMs,
    slowMaterializeThreshold,
    onFatalError,
  );

  afterEach(() => {
    vi.clearAllMocks();
  });

  test('addCustom', () => {
    // Test with TTL larger than MAX_TTL_MS (600,000ms = 10 minutes)
    const largeTTL = MAX_TTL_MS + 1; // 600,001ms
    const ast: AST = {
      table: 'issue',
    };
    queryManager.addCustom(ast, {name: 'testQuery', args: ['arg1']}, largeTTL);
    queryManager.flushBatch();

    expect(logSink.log).toHaveBeenCalledExactlyOnceWith(
      'warn',
      context,
      `TTL (${largeTTL}) is too high, clamping to 10m`,
    );

    // Test with 'forever' TTL which should also warn
    logSink.log.mockClear();
    queryManager.addCustom(
      ast,
      {name: 'testQuery2', args: ['arg2']},
      'forever',
    );
    queryManager.flushBatch();

    expect(logSink.log).toHaveBeenCalledExactlyOnceWith(
      'warn',
      context,
      'TTL (forever) is too high, clamping to 10m',
    );

    // Test with valid TTL that should not warn
    logSink.log.mockClear();
    queryManager.addCustom(ast, {name: 'testQuery3', args: ['arg3']}, '5m');
    queryManager.flushBatch();

    expect(logSink.log).not.toHaveBeenCalled();
  });

  test('addLegacy', () => {
    const ast: AST = {
      table: 'issue',
      orderBy: [['id', 'asc']],
    };

    // Test with TTL larger than MAX_TTL_MS (600,000ms = 10 minutes)
    const largeTTL = MAX_TTL_MS + 1; // 600,001ms
    queryManager.addLegacy(ast, largeTTL);
    queryManager.flushBatch();

    expect(logSink.log).toHaveBeenCalledExactlyOnceWith(
      'warn',
      context,
      `TTL (${largeTTL}) is too high, clamping to 10m`,
    );

    // Test with 'forever' TTL which should also warn
    logSink.log.mockClear();
    queryManager.addLegacy(ast, 'forever');
    queryManager.flushBatch();

    expect(logSink.log).toHaveBeenCalledExactlyOnceWith(
      'warn',
      context,
      'TTL (forever) is too high, clamping to 10m',
    );
    expect(logSink.log).toHaveBeenCalledTimes(1);

    // Test with valid TTL that should not warn
    logSink.log.mockClear();
    queryManager.addLegacy(ast, '5m');
    queryManager.flushBatch();

    expect(logSink.log).not.toHaveBeenCalled();
  });
});

describe('update clamps TTL correctly', () => {
  const send = vi.fn<(arg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
  const queryManager = new QueryManager(
    lc,
    mutationTracker,
    'client1',
    schema.tables,
    send,
    () => () => {},
    maxRecentQueriesSize,
    queryChangeThrottleMs,
    slowMaterializeThreshold,
    onFatalError,
  );

  afterEach(() => {
    send.mockClear();
  });

  test('updateLegacy', () => {
    const ast: AST = {
      table: 'issue',
      orderBy: [['id', 'asc']],
    };

    // Add a query with a specific TTL
    queryManager.addLegacy(ast, '1m');
    queryManager.flushBatch();

    // Update the query with a larger TTL
    queryManager.updateLegacy(ast, '2m');
    queryManager.flushBatch();

    expect(send).toBeCalledTimes(2);
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
            ttl: 120000, // Clamped TTL value
          },
        ],
      },
    ]);
  });

  test('updateCustom', () => {
    // Add a custom query with a specific TTL
    const ast: AST = {
      table: 'issue',
    };
    queryManager.addCustom(ast, {name: 'customQuery', args: [1]}, '1m');
    queryManager.flushBatch();

    // Update the query with a larger TTL
    queryManager.updateCustom({name: 'customQuery', args: [1]}, '2m');
    queryManager.flushBatch();

    expect(send).toBeCalledTimes(2);
    expect(send).toHaveBeenLastCalledWith([
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [
          {
            op: 'put',
            hash: '2l1ig6e3tnu0a',
            name: 'customQuery',
            args: [1],
            ttl: 120000, // Clamped TTL value
          },
        ],
      },
    ]);
  });

  test('updateLegacy does not send when TTL is already at max', () => {
    const ast: AST = {
      table: 'issue',
      orderBy: [['id', 'asc']],
    };

    // Add a query with max TTL
    queryManager.addLegacy(ast, 'forever');
    queryManager.flushBatch();

    // Update the query with a larger TTL (should be no-op since already at max)
    queryManager.updateLegacy(ast, MAX_TTL_MS + 1000);
    queryManager.flushBatch();

    // Only one send should happen (the initial add)
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
            ttl: MAX_TTL_MS, // Already at max TTL
          },
        ],
      },
    ]);
  });

  test('updateCustom does not send when TTL is already at max', () => {
    // Add a custom query with max TTL
    const ast: AST = {
      table: 'issue',
    };
    queryManager.addCustom(ast, {name: 'customQuery', args: [1]}, 'forever');
    queryManager.flushBatch();

    // Update the query with a larger TTL (should be no-op since already at max)
    queryManager.updateCustom(
      {name: 'customQuery', args: [1]},
      MAX_TTL_MS + 1000,
    );
    queryManager.flushBatch();

    // Only one send should happen (the initial add)
    expect(send).toBeCalledTimes(1);
    expect(send).toHaveBeenLastCalledWith([
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [
          {
            op: 'put',
            hash: '2l1ig6e3tnu0a',
            name: 'customQuery',
            args: [1],
            ttl: MAX_TTL_MS, // Already at max TTL
          },
        ],
      },
    ]);
  });
});

test('Getting the AST of custom query', () => {
  const send = vi.fn<(msg: ChangeDesiredQueriesMessage) => void>();
  const maxRecentQueriesSize = 0;
  const mutationTracker = new MutationTracker(lc, ackMutations, onFatalError);
  const queryManager = new QueryManager(
    lc,
    mutationTracker,
    'client1',
    schema.tables,
    send,
    () => () => {},
    maxRecentQueriesSize,
    queryChangeThrottleMs,
    slowMaterializeThreshold,
    onFatalError,
  );

  const ast: AST = {
    table: 'issue',
  };
  queryManager.addCustom(ast, {name: 'customQuery', args: [1]}, '1m');

  const queryID = hashOfNameAndArgs('customQuery', [1]);
  expect(queryManager.getAST(queryID)).toEqual({
    table: 'issue',
  });
});
