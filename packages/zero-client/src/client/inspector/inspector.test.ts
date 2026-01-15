import {beforeEach, describe, expect, test, vi} from 'vitest';
import {assert} from '../../../../shared/src/asserts.ts';
import {TDigest, type ReadonlyTDigest} from '../../../../shared/src/tdigest.ts';
import type {AnalyzeQueryResult} from '../../../../zero-protocol/src/analyze-query-result.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {
  type InspectDownMessage,
  type InspectMetricsDown,
  type InspectQueriesDown,
} from '../../../../zero-protocol/src/inspect-down.ts';
import type {AnalyzeQueryOptions} from '../../../../zero-protocol/src/inspect-up.ts';
import {relationships} from '../../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import type {Schema} from '../../../../zero-types/src/schema.ts';
import {createBuilder} from '../../../../zql/src/query/create-builder.ts';
import {asQueryInternals} from '../../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../../zql/src/query/query.ts';
import {schema} from '../../../../zql/src/query/test/test-schemas.ts';
import {nanoid} from '../../util/nanoid.ts';
import type {CustomMutatorDefs} from '../custom.ts';
import type {TestZero} from '../test-utils.ts';
import {
  asCustomQuery,
  MockSocket,
  queryID,
  zeroForTest,
} from '../test-utils.ts';
import type {Inspector} from './inspector.ts';
import type {Metrics} from './lazy-inspector.ts';
import type {Query} from './query.ts';

const emptyMetrics = {
  'query-materialization-client': new TDigest(),
  'query-materialization-end-to-end': new TDigest(),
  'query-materialization-server': new TDigest(),
  'query-update-client': new TDigest(),
  'query-update-server': new TDigest(),
};

async function waitForID(socketP: Promise<MockSocket>, op: string) {
  const socket = await socketP;
  return new Promise<string>(resolve => {
    const cleanup = socket.onUpstream(message => {
      const data = JSON.parse(message);
      if (data[0] === 'inspect' && data[1].op === op) {
        cleanup();
        resolve(data[1].id);
      }
    });
  });
}

async function getMetrics<
  S extends Schema,
  MD extends CustomMutatorDefs | undefined,
  C,
>(
  inspector: Inspector,
  z: TestZero<S, MD, C>,

  metricsResponseValue?: InspectMetricsDown['value'],
): Promise<Metrics> {
  const socket = await z.socket;
  const idPromise = new Promise<string>(resolve => {
    const cleanup = socket.onUpstream(message => {
      const data = JSON.parse(message);
      if (data[0] === 'inspect' && data[1].op === 'metrics') {
        cleanup();
        resolve(data[1].id);
      }
    });
  });
  const p = inspector.metrics();
  const id = await idPromise;

  await z.triggerMessage([
    'inspect',
    {
      op: 'metrics',
      id,
      value: metricsResponseValue ?? {
        'query-materialization-server': [1000],
        'query-update-server': [1000],
      },
    },
  ]);

  return p;
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.stubGlobal('WebSocket', MockSocket);
  return () => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  };
});

test('basics', async () => {
  const z = zeroForTest();

  const {client, clientGroup} = z.inspector;
  expect(client.id).toBe(z.clientID);
  expect(client.clientGroup).toBe(clientGroup);
  expect(await clientGroup.id).toBe(await z.clientGroupID);

  await z.close();
});

test('basics 2 clients', async () => {
  const userID = nanoid();
  const z1 = zeroForTest({userID, kvStore: 'idb'});
  const z2 = zeroForTest({userID, kvStore: 'idb'});

  expect(await z1.inspector.clients()).toEqual([
    {
      clientGroup: {
        id: await z1.clientGroupID,
      },
      id: z1.clientID,
    },
    {
      clientGroup: {
        id: await z2.clientGroupID,
      },
      id: z2.clientID,
    },
  ]);

  await z1.close();
  await z2.close();
});

test('client queries', async () => {
  const userID = nanoid();
  const z = zeroForTest({userID, schema, kvStore: 'idb'});
  await z.triggerConnected();

  expect(await z.inspector.clients()).toEqual([
    {
      clientGroup: {
        id: await z.clientGroupID,
      },
      id: z.clientID,
    },
  ]);

  await z.socket;

  const t = async (
    response: InspectQueriesDown['value'],
    expected: Query[],
  ) => {
    (await z.socket).messages.length = 0;
    const idPromise = waitForID(z.socket, 'queries');
    const p = z.inspector.client.queries();
    const id = await idPromise;
    await z.triggerMessage([
      'inspect',
      {
        op: 'queries',
        id,
        value: response,
      },
    ] satisfies InspectDownMessage);
    expect(await p).toEqual(expected);
  };

  await t([], []);
  await t(
    [
      {
        clientID: z.clientID,
        queryID: '1',
        ast: {table: 'issues'},
        name: null,
        args: null,
        deleted: false,
        got: true,
        inactivatedAt: null,
        rowCount: 10,
        ttl: 60_000,
        metrics: null,
      },
    ],
    [
      expect.objectContaining({
        clientID: z.clientID,
        clientZQL: null,
        serverZQL: 'issues',
        name: null,
        args: null,
        deleted: false,
        got: true,
        id: '1',
        inactivatedAt: null,
        rowCount: 10,
        ttl: '1m',
        metrics: emptyMetrics,
        analyze: expect.any(Function),
      }),
    ],
  );
  const d = Date.UTC(2025, 2, 25, 14, 52, 10);
  await t(
    [
      {
        clientID: z.clientID,
        queryID: '1',
        ast: {
          table: 'issues',
          where: {
            type: 'simple',
            left: {type: 'column', name: 'owner_id'},
            op: '=',
            right: {type: 'literal', value: 'arv'},
          },
        },
        name: null,
        args: null,
        deleted: false,
        got: true,
        inactivatedAt: d,
        rowCount: 10,
        ttl: 60_000,
        metrics: null,
      },
    ],
    [
      expect.objectContaining({
        clientID: z.clientID,
        clientZQL: null,
        serverZQL: "issues.where('owner_id', 'arv')",
        name: null,
        args: null,
        deleted: false,
        got: true,
        id: '1',
        inactivatedAt: new Date(d),
        rowCount: 10,
        ttl: '1m',
        metrics: emptyMetrics,
        analyze: expect.any(Function),
      }),
    ],
  );

  await z.close();
});

test('clientGroup queries', async () => {
  const ast: AST = {
    table: 'issues',
    where: {
      type: 'or',
      conditions: [
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: '1'},
        },
        {
          type: 'simple',
          op: '!=',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: '2'},
        },
      ],
    },
  };
  const z = zeroForTest({schema});
  await z.triggerConnected();

  const idPromise = waitForID(z.socket, 'queries');
  const p = z.inspector.clientGroup.queries();
  const id = await idPromise;
  await z.triggerMessage([
    'inspect',
    {
      op: 'queries',
      id,
      value: [
        {
          clientID: z.clientID,
          queryID: '1',
          ast,
          name: null,
          args: null,
          deleted: false,
          got: true,
          inactivatedAt: null,
          rowCount: 10,
          ttl: 60_000,
          metrics: null,
        },
      ],
    },
  ] satisfies InspectDownMessage);
  expect(await p).toEqual([
    expect.objectContaining({
      name: null,
      args: null,
      clientID: z.clientID,
      clientZQL: null,
      deleted: false,
      got: true,
      id: '1',
      inactivatedAt: null,
      rowCount: 10,
      ttl: '1m',
      serverZQL:
        "issues.where(({cmp, or}) => or(cmp('id', '1'), cmp('id', '!=', '2')))",
      metrics: emptyMetrics,
      hydrateClient: null,
      hydrateServer: null,
      hydrateTotal: null,
      updateClientP50: null,
      updateClientP95: null,
      updateServerP50: null,
      updateServerP95: null,
      analyze: expect.any(Function),
    }),
  ]);
});

describe('query metrics', () => {
  test('real query metrics integration', async () => {
    const z = zeroForTest({schema});
    await z.triggerConnected();

    const zql = createBuilder(schema);

    const issueQuery = zql.issue;
    await z.run(issueQuery);

    const metrics = await getMetrics(z.inspector, z);
    expect(metrics['query-materialization-client'].count()).toBe(1);
    expect(
      metrics['query-materialization-client'].quantile(0.5),
    ).toBeGreaterThanOrEqual(0);
    await z.close();
  });

  test('Attaching the metrics to the query', async () => {
    const z = zeroForTest({schema});
    await z.triggerConnected();

    const zql = createBuilder(schema);

    const issueQuery = zql.issue.orderBy('id', 'desc');
    const view = z.materialize(issueQuery);

    await z.triggerGotQueriesPatch(issueQuery);

    const idPromise = waitForID(z.socket, 'queries');
    const queriesP = z.inspector.client.queries();
    const id = await idPromise;

    // Simulate the server response with query data
    await z.triggerMessage([
      'inspect',
      {
        op: 'queries',
        id,
        value: [
          {
            clientID: z.clientID,
            queryID: asQueryInternals(issueQuery).hash(),
            ast: {
              table: 'issue',
              orderBy: [['id', 'desc']],
            },
            name: null,
            args: null,
            deleted: false,
            got: true,
            inactivatedAt: null,
            rowCount: 1,
            ttl: 60_000,
            metrics: null,
          },
        ],
      },
    ] satisfies InspectDownMessage);

    const queries = await queriesP;
    expect(queries).toHaveLength(1);
    expect(asQueryInternals(issueQuery).hash()).toBe(queries[0].id);

    // We should have metrics for all.. even if empty
    expect(queries[0].metrics).toMatchInlineSnapshot(`
      {
        "query-materialization-client": [
          1000,
          0,
          1,
        ],
        "query-materialization-end-to-end": [
          1000,
        ],
        "query-materialization-server": [
          1000,
        ],
        "query-update-client": [
          1000,
        ],
        "query-update-server": [
          1000,
        ],
      }
    `);

    view.destroy();
    await z.close();
  });

  test('metrics collection during query materialization', async () => {
    const z = zeroForTest({schema});
    await z.triggerConnected();

    const zql = createBuilder(schema);

    // Execute multiple queries to generate real metrics
    const query1 = zql.issue;
    const query2 = zql.issue.where('id', '1');

    await z.run(query1);
    await z.run(query2);

    // Check that metrics were actually collected

    const metrics = await getMetrics(z.inspector, z);
    expect(metrics['query-materialization-client'].count()).toBe(2);

    const digest = metrics['query-materialization-client'];
    expect(digest.count()).toBe(2);

    expect(digest.quantile(0.5)).toBeGreaterThanOrEqual(0);

    await z.close();
  });

  test('query-specific metrics integration test', async () => {
    const z = zeroForTest({schema});
    await z.triggerConnected();

    const zql = createBuilder(schema);

    // Execute queries with different characteristics to test metrics collection
    await z.run(asCustomQuery(zql.issue, 'a', undefined)); // Simple table query
    await z.run(asCustomQuery(zql.issue.where('id', '1'), 'b', undefined)); // Filtered query
    await z.run(asCustomQuery(zql.issue.where('id', '2'), 'c', undefined)); // Another filtered query

    // Test that the inspector can access the real metrics

    // Verify global metrics were collected
    const metrics = await getMetrics(z.inspector, z);
    const globalMetricsQueryMaterializationClient =
      metrics['query-materialization-client'];
    expect(globalMetricsQueryMaterializationClient.count()).toBe(3);

    const ensureRealData = (digest: ReadonlyTDigest) => {
      // Test that percentiles work with real data
      const p50 = digest.quantile(0.5);
      const p90 = digest.quantile(0.9);

      expect(Number.isFinite(p50)).toBe(true);
      expect(Number.isFinite(p90)).toBe(true);
      expect(p50).toBeGreaterThanOrEqual(0);
      expect(p90).toBeGreaterThanOrEqual(p50);

      // Test CDF functionality
      const cdf0 = digest.cdf(0);
      const cdfMax = digest.cdf(Number.MAX_VALUE);
      expect(cdf0).toBeGreaterThanOrEqual(0);
      expect(cdfMax).toBe(1);
    };

    ensureRealData(globalMetricsQueryMaterializationClient);

    const q = asCustomQuery(zql.issue, 'a', undefined);
    const view = z.materialize(q);
    await z.triggerGotQueriesPatch(q);

    {
      const metrics = await getMetrics(z.inspector, z);
      const globalMetricsQueryMaterializationEndToEnd =
        metrics['query-materialization-end-to-end'];

      await vi.waitFor(() => {
        expect(globalMetricsQueryMaterializationEndToEnd.count()).toBe(1);
      });

      ensureRealData(globalMetricsQueryMaterializationEndToEnd);
    }
    view.destroy();

    await z.close();
  });

  test('query-update metrics collection', async () => {
    const z = zeroForTest({schema});
    await z.triggerConnected();

    const zql = createBuilder(schema);

    // Create a query and materialize a view to set up the query pipeline
    const issueQuery = zql.issue;
    const view = z.materialize(issueQuery);
    await z.triggerGotQueriesPatch(issueQuery);

    // Get initial inspector to verify no query-update metrics initially
    const initialInspector = z.inspector;
    const initialMetrics = await getMetrics(initialInspector, z);
    expect(initialMetrics['query-update-client'].count()).toBe(0);

    // Trigger row updates to generate query-update metrics
    await z.triggerPoke(null, '2', {
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {
            id: 'issue1',
            title: 'Test Issue 1',
            description: 'Test description 1',
            closed: false,
            createdAt: Date.now(),
          },
        },
        {
          op: 'put',
          tableName: 'issues',
          value: {
            id: 'issue2',
            title: 'Test Issue 2',
            description: 'Test description 2',
            closed: false,
            createdAt: Date.now(),
          },
        },
      ],
    });

    const metrics = await getMetrics(z.inspector, z);

    // Wait for the updates to process and check metrics
    await vi.waitFor(() => {
      const updateMetrics = metrics['query-update-client'];
      expect(updateMetrics.count()).toBeGreaterThan(0);
    });

    // Final verification of the query-update-client metrics
    const queryUpdateMetrics = metrics['query-update-client'];

    expect(queryUpdateMetrics.count()).toBeGreaterThan(0);
    expect(queryUpdateMetrics.quantile(0.5)).toBeGreaterThanOrEqual(0);

    view.destroy();
    await z.close();
  });

  test('query-update metrics in query-specific metrics', async () => {
    const z = zeroForTest({schema});
    await z.triggerConnected();

    const zql = createBuilder(schema);

    const issueQuery = zql.issue.orderBy('id', 'desc');
    const customQuery = asCustomQuery(issueQuery, 'myCustomQuery', []);
    const view = z.materialize(customQuery);
    await z.triggerGotQueriesPatch(customQuery);

    // Trigger row updates to generate query-update metrics for this specific query
    await z.triggerPoke(null, '2', {
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {
            id: 'issue1',
            title: 'Updated Issue 1',
            description: 'Updated description',
            closed: false,
            createdAt: Date.now(),
          },
        },
      ],
    });

    const metrics1 = await getMetrics(z.inspector, z);

    // Wait for the update to be processed
    await vi.waitFor(() => {
      const updateMetrics = metrics1['query-update-client'];
      expect(updateMetrics.count()).toBeGreaterThan(0);
    });

    // Get query-specific metrics through the inspector
    const idPromise = waitForID(z.socket, 'queries');
    const queriesP = z.inspector.client.queries();
    const id = await idPromise;

    // Simulate the server response with query data
    await z.triggerMessage([
      'inspect',
      {
        op: 'queries',
        id,
        value: [
          {
            clientID: z.clientID,
            queryID: queryID(customQuery),
            ast: {
              table: 'issue',
              orderBy: [['id', 'desc']],
            },
            name: 'customQuery',
            args: [],
            deleted: false,
            got: true,
            inactivatedAt: null,
            rowCount: 1,
            ttl: 60_000,
            metrics: {
              'query-materialization-server': [1000, 1, 2],
              'query-update-server': [100, 3, 4],
            },
          },
        ],
      },
    ] satisfies InspectDownMessage);

    const queries = await queriesP;
    expect(queries).toHaveLength(1);
    expect(queryID(customQuery)).toBe(queries[0].id);

    const {metrics} = queries[0];
    expect(metrics).toMatchInlineSnapshot(`
      {
        "query-materialization-client": [
          1000,
          0,
          1,
        ],
        "query-materialization-end-to-end": [
          1000,
          50,
          1,
        ],
        "query-materialization-server": [
          1000,
          1,
          2,
        ],
        "query-update-client": [
          1000,
          0,
          1,
        ],
        "query-update-server": [
          100,
          3,
          4,
        ],
      }
    `);

    view.destroy();
    await z.close();
  });
});

test('server version', async () => {
  const z = zeroForTest({schema});
  await z.triggerConnected();

  const idPromise = waitForID(z.socket, 'version');
  const p = z.inspector.serverVersion();
  const id = await idPromise;

  await z.triggerMessage([
    'inspect',
    {
      op: 'version',
      id,
      value: '1.2.34',
    },
  ] satisfies InspectDownMessage);

  expect(await p).toBe('1.2.34');

  await z.close();
});

// clientZQL is only populated for legacy queries since the QueryManager only
// tracks the client-side AST when legacy queries are enabled.
test('clientZQL with legacy queries', async () => {
  const legacySchema = {...schema, enableLegacyQueries: true} as const;
  const z = zeroForTest({schema: legacySchema});
  await z.triggerConnected();

  const zql = createBuilder(legacySchema);

  // Trigger QueryManager.#add by materializing a query and marking it as got
  const issueQuery = zql.issue.where('ownerId', 'arv');
  const view = z.materialize(issueQuery);
  await z.triggerGotQueriesPatch(issueQuery);

  const idPromise = waitForID(z.socket, 'queries');
  const queriesP = z.inspector.client.queries();
  const id = await idPromise;

  // Send fake inspect/queries response for this query
  await z.triggerMessage([
    'inspect',
    {
      op: 'queries',
      id,
      value: [
        {
          clientID: z.clientID,
          queryID: asQueryInternals(issueQuery).hash(),
          ast: {
            table: 'issues',
            where: {
              type: 'simple',
              left: {type: 'column', name: 'owner_id'},
              op: '=',
              right: {type: 'literal', value: 'arv'},
            },
          },
          name: null,
          args: null,
          deleted: false,
          got: true,
          inactivatedAt: null,
          rowCount: 0,
          ttl: 60_000,
          metrics: null,
        },
      ],
    },
  ] satisfies InspectDownMessage);

  const queries = await queriesP;
  expect(queries).toHaveLength(1);
  expect(queries[0].id).toBe(asQueryInternals(issueQuery).hash());
  expect(queries[0].clientZQL).toBe("issue.where('ownerId', 'arv')");
  expect(queries[0].serverZQL).toBe("issues.where('owner_id', 'arv')");

  view.destroy();
  await z.close();
});

// Custom queries (named queries) always track the client-side AST in the
// QueryManager, so clientZQL is always populated for them.
test('clientZQL with custom queries', async () => {
  const z = zeroForTest({schema});
  await z.triggerConnected();

  const zql = createBuilder(schema);

  // Create a custom query by adding name and args to a regular query
  const baseQuery = zql.issue.where('ownerId', 'arv');
  const customQuery = asCustomQuery(baseQuery, 'myCustomQuery', 'arv');
  const customQueryID = queryID(customQuery);

  // Trigger QueryManager.addCustom by materializing a custom query and marking it as got
  const view = z.materialize(customQuery);
  await z.triggerGotQueriesPatch(customQuery);

  const idPromise = waitForID(z.socket, 'queries');
  const queriesP = z.inspector.client.queries();
  const id = await idPromise;

  // Send fake inspect/queries response for this custom query
  await z.triggerMessage([
    'inspect',
    {
      op: 'queries',
      id,
      value: [
        {
          clientID: z.clientID,
          queryID: customQueryID,
          ast: {
            table: 'issues',
            where: {
              type: 'simple',
              left: {type: 'column', name: 'owner_id'},
              op: '=',
              right: {type: 'literal', value: 'arv'},
            },
          },
          name: 'myCustomQuery',
          args: ['arv'],
          deleted: false,
          got: true,
          inactivatedAt: null,
          rowCount: 0,
          ttl: 60_000,
          metrics: null,
        },
      ],
    },
  ] satisfies InspectDownMessage);

  const queries = await queriesP;
  expect(queries).toHaveLength(1);
  expect(queries[0].id).toBe(customQueryID);
  expect(queries[0].name).toBe('myCustomQuery');
  expect(queries[0].args).toEqual(['arv']);
  expect(queries[0].clientZQL).toBe("issue.where('ownerId', 'arv')");
  expect(queries[0].serverZQL).toBe("issues.where('owner_id', 'arv')");

  view.destroy();
  await z.close();
});

describe('query analyze', () => {
  test('analyze method sends AST for unnamed queries', async () => {
    const z = zeroForTest({schema});
    await z.triggerConnected();

    // Create a query to analyze
    const ast: AST = {table: 'issues'};
    const analyzeOptions = {
      syncedRows: true,
      vendedRows: true,
    };

    // Setup queries first

    const idP = waitForID(z.socket, 'queries');

    const queriesP = z.inspector.client.queries();
    const id = await idP;

    await z.triggerMessage([
      'inspect',
      {
        op: 'queries',
        id,
        value: [
          {
            clientID: z.clientID,
            queryID: '1',
            ast,
            name: null,
            args: null,
            deleted: false,
            got: true,
            inactivatedAt: null,
            rowCount: 5,
            ttl: 60_000,
            metrics: null,
          },
        ],
      },
    ]);

    const queries = await queriesP;
    const query = queries[0];

    (await z.socket).messages.length = 0; // Clear previous messages

    const analyzeIdPromise = waitForID(z.socket, 'analyze-query');
    const analyzePromise = query.analyze(analyzeOptions);
    const analyzeId = await analyzeIdPromise;

    // Verify the message sent to server
    expect((await z.socket).jsonMessages).toEqual([
      [
        'inspect',
        {
          op: 'analyze-query',
          id: analyzeId,
          value: ast,
          options: analyzeOptions,
        },
      ],
    ]);

    // Mock the server response
    const mockAnalyzeResult = {
      warnings: [],
      syncedRowCount: 5,
      start: 1000,
      end: 1050,
      syncedRows: {
        issues: [
          {id: '1', title: 'Test Issue'},
          {id: '2', title: 'Another Issue'},
        ],
      },
      readRows: {
        issues: {
          'SELECT * FROM issues': [
            {id: '1', title: 'Test Issue'},
            {id: '2', title: 'Another Issue'},
            {id: '3', title: 'Third Issue'},
            {id: '4', title: 'Fourth Issue'},
            {id: '5', title: 'Fifth Issue'},
          ],
        },
      },
      readRowCount: 5,
      readRowCountsByQuery: {
        issues: {
          'SELECT * FROM issues': 5,
        },
      },
      sqlitePlans: {
        'SELECT * FROM issues': ['SCAN issues'],
      },
    };

    await z.triggerMessage([
      'inspect',
      {
        op: 'analyze-query',
        id: analyzeId,
        value: mockAnalyzeResult,
      },
    ]);

    const result = await analyzePromise;
    expect(result).toEqual(mockAnalyzeResult);

    await z.close();
  });

  test('analyze method sends name and args for named queries', async () => {
    const z = zeroForTest({schema});
    await z.triggerConnected();

    const ast: AST = {table: 'issues'};
    const analyzeOptions = {
      syncedRows: true,
      vendedRows: false,
    };

    // Setup a named query
    const idP = waitForID(z.socket, 'queries');
    const queriesP = z.inspector.client.queries();
    const id = await idP;

    await z.triggerMessage([
      'inspect',
      {
        op: 'queries',
        id,
        value: [
          {
            clientID: z.clientID,
            queryID: '1',
            ast,
            name: 'myCustomQuery',
            args: ['arg1', 42, {key: 'value'}],
            deleted: false,
            got: true,
            inactivatedAt: null,
            rowCount: 10,
            ttl: 60_000,
            metrics: null,
          },
        ],
      },
    ]);

    const queries = await queriesP;
    const query = queries[0];

    (await z.socket).messages.length = 0; // Clear previous messages

    // Wait for the analyze-query ID to be generated
    const analyzeIdP = waitForID(z.socket, 'analyze-query');
    const analyzePromise = query.analyze(analyzeOptions);
    const analyzeId = await analyzeIdP;

    // For named queries, should send name and args instead of AST
    const messages = (await z.socket).jsonMessages;

    expect(messages).toEqual([
      [
        'inspect',
        {
          args: [
            'arg1',
            42,
            {
              key: 'value',
            },
          ],
          id: analyzeId,
          name: 'myCustomQuery',
          op: 'analyze-query',
          options: {
            syncedRows: true,
            vendedRows: false,
          },
        },
      ],
    ]);

    // Mock the server response
    const mockAnalyzeResult = {
      warnings: [],
      syncedRowCount: 10,
      start: 1000,
      end: 1100,
      syncedRows: {
        issues: [{id: '1', title: 'Custom Query Result'}],
      },
      readRowCount: 10,
      readRowCountsByQuery: {},
      sqlitePlans: {},
    };

    await z.triggerMessage([
      'inspect',
      {
        op: 'analyze-query',
        id: analyzeId,
        value: mockAnalyzeResult,
      },
    ]);

    const result = await analyzePromise;
    expect(result).toEqual(mockAnalyzeResult);

    await z.close();
  });

  test('analyze method works with queries that have server AST', async () => {
    const userID = nanoid();
    const z = zeroForTest({userID, schema, kvStore: 'idb'});
    await z.triggerConnected();

    // Setup a query with server AST

    const idP = waitForID(z.socket, 'queries');
    const queriesP = z.inspector.client.queries();
    const id = await idP;

    await z.triggerMessage([
      'inspect',
      {
        op: 'queries',
        id,
        value: [
          {
            clientID: z.clientID,
            queryID: '1',
            ast: {
              table: 'issues',
              where: {
                type: 'simple',
                left: {type: 'column', name: 'id'},
                op: '=',
                right: {type: 'literal', value: '1'},
              },
            },
            name: null,
            args: null,
            deleted: false,
            got: true,
            inactivatedAt: null,
            rowCount: 1,
            ttl: 60_000,
            metrics: null,
          },
        ],
      },
    ]);

    const queries = await queriesP;

    const query = queries[0];

    (await z.socket).messages.length = 0; // Clear previous messages

    const analyzeIdPromise = waitForID(z.socket, 'analyze-query');
    const analyzePromise = query.analyze({syncedRows: false});
    const analyzeId = await analyzeIdPromise;

    // Verify the message sent to server
    expect((await z.socket).jsonMessages).toEqual([
      [
        'inspect',
        {
          op: 'analyze-query',
          id: analyzeId,
          value: {
            table: 'issues',
            where: {
              type: 'simple',
              left: {type: 'column', name: 'id'},
              op: '=',
              right: {type: 'literal', value: '1'},
            },
          },
          options: {syncedRows: false},
        },
      ],
    ]);

    const mockResult = {
      warnings: [],
      syncedRowCount: 1,
      start: 2000,
      end: 2050,
    };

    await z.triggerMessage([
      'inspect',
      {
        op: 'analyze-query',
        id: analyzeId,
        value: mockResult,
      },
    ]);

    const result = await analyzePromise;
    expect(result).toEqual(mockResult);

    await z.close();
  });

  test('analyze method throws when no server AST available', async () => {
    const z = zeroForTest({schema});
    await z.triggerConnected();

    // Setup a query without server AST

    const idP = waitForID(z.socket, 'queries');

    const queriesP = z.inspector.client.queries();
    const id = await idP;

    await z.triggerMessage([
      'inspect',
      {
        op: 'queries',
        id,
        value: [
          {
            clientID: z.clientID,
            queryID: '1',
            ast: null, // No server AST
            name: null,
            args: null,
            deleted: false,
            got: true,
            inactivatedAt: null,
            rowCount: 0,
            ttl: 60_000,
            metrics: null,
          },
        ],
      },
    ]);

    const queries = await queriesP;

    const query = queries[0];

    await expect(query.analyze()).rejects.toThrow(
      'AST is required for unnamed queries',
    );

    await z.close();
  });

  test('analyze method handles default options', async () => {
    const z = zeroForTest({schema});
    await z.triggerConnected();
    const idP = waitForID(z.socket, 'queries');

    const queriesP = z.inspector.client.queries();
    const id = await idP;

    await z.triggerMessage([
      'inspect',
      {
        op: 'queries',
        id,
        value: [
          {
            clientID: z.clientID,
            queryID: '1',
            ast: {table: 'users'},
            name: null,
            args: null,
            deleted: false,
            got: true,
            inactivatedAt: null,
            rowCount: 5,
            ttl: 60_000,
            metrics: null,
          },
        ],
      },
    ]);

    const queries = await queriesP;
    const query = queries[0];
    (await z.socket).messages.length = 0;

    const analyzeIdPromise = waitForID(z.socket, 'analyze-query');
    const analyzePromise = query.analyze(); // No options provided
    const analyzeId = await analyzeIdPromise;

    // Verify the message sent to server (default options should be undefined)
    expect((await z.socket).jsonMessages).toEqual([
      [
        'inspect',
        {
          op: 'analyze-query',
          id: analyzeId,
          value: {table: 'users'},
          options: undefined,
        },
      ],
    ]);

    await z.triggerMessage([
      'inspect',
      {
        op: 'analyze-query',
        id: analyzeId,
        value: {
          warnings: [],
          syncedRowCount: 5,
          start: 3000,
          end: 3100,
        },
      },
    ]);

    const result = await analyzePromise;
    expect(result.syncedRowCount).toBe(5);

    await z.close();
  });

  test('analyze result includes sqlitePlans when readRowCountsByQuery is populated (regression)', async () => {
    // This test verifies the fix for the bug where result.sqlitePlans was not being populated
    // because the server code was using deprecated vendedRowCounts instead of readRowCountsByQuery.
    // The server should populate sqlitePlans based on readRowCountsByQuery, and the client should receive it.
    const z = zeroForTest({schema});
    await z.triggerConnected();

    const idP = waitForID(z.socket, 'queries');
    const queriesP = z.inspector.client.queries();
    const id = await idP;

    await z.triggerMessage([
      'inspect',
      {
        op: 'queries',
        id,
        value: [
          {
            clientID: z.clientID,
            queryID: '1',
            ast: {table: 'issues'},
            name: null,
            args: null,
            deleted: false,
            got: true,
            inactivatedAt: null,
            rowCount: 5,
            ttl: 60_000,
            metrics: null,
          },
        ],
      },
    ]);

    const queries = await queriesP;

    const query = queries[0];
    (await z.socket).messages.length = 0;

    const analyzeIdPromise = waitForID(z.socket, 'analyze-query');
    const analyzePromise = query.analyze();
    const analyzeId = await analyzeIdPromise;

    // Verify the message sent to server
    expect((await z.socket).jsonMessages).toEqual([
      [
        'inspect',
        {
          op: 'analyze-query',
          id: analyzeId,
          value: {table: 'issues'},
          options: undefined,
        },
      ],
    ]);

    // Simulate what the server should return after the fix:
    // - readRowCountsByQuery is populated (new property)
    // - plans is populated based on readRowCountsByQuery
    // - vendedRowCounts is not present (deprecated)
    const mockAnalyzeResult = {
      warnings: [],
      syncedRowCount: 5,
      start: 1000,
      end: 1050,
      readRowCount: 5,
      readRowCountsByQuery: {
        issues: {
          'SELECT * FROM issues': 5,
        },
      },
      // The bug was that sqlitePlans would be empty because the server was using
      // vendedRowCounts (undefined) instead of readRowCountsByQuery
      sqlitePlans: {
        'SELECT * FROM issues': ['SCAN issues'],
      },
    };

    await z.triggerMessage([
      'inspect',
      {
        op: 'analyze-query',
        id: analyzeId,
        value: mockAnalyzeResult,
      },
    ]);

    const result = await analyzePromise;

    // Critical assertion: sqlitePlans should be populated
    expect(result.sqlitePlans).toEqual({
      'SELECT * FROM issues': ['SCAN issues'],
    });

    // Verify readRowCountsByQuery is present (new property)
    expect(result.readRowCountsByQuery).toEqual({
      issues: {
        'SELECT * FROM issues': 5,
      },
    });

    // Ensure deprecated vendedRowCounts is not in the result
    expect(result.vendedRowCounts).toBeUndefined();

    await z.close();
  });

  test('analyze result includes elapsed time', async () => {
    const z = zeroForTest({schema});
    await z.triggerConnected();

    const idP = waitForID(z.socket, 'queries');
    const queriesP = z.inspector.client.queries();
    const id = await idP;

    await z.triggerMessage([
      'inspect',
      {
        op: 'queries',
        id,
        value: [
          {
            clientID: z.clientID,
            queryID: '1',
            ast: {table: 'issues'},
            name: null,
            args: null,
            deleted: false,
            got: true,
            inactivatedAt: null,
            rowCount: 5,
            ttl: 60_000,
            metrics: null,
          },
        ],
      },
    ]);

    const queries = await queriesP;

    const query = queries[0];
    (await z.socket).messages.length = 0;

    const analyzeIdPromise = waitForID(z.socket, 'analyze-query');
    const analyzePromise = query.analyze();
    const analyzeId = await analyzeIdPromise;

    // Verify the message sent to server
    expect((await z.socket).jsonMessages).toEqual([
      [
        'inspect',
        {
          op: 'analyze-query',
          id: analyzeId,
          value: {table: 'issues'},
          options: undefined,
        },
      ],
    ]);

    // Mock analyze result with elapsed (new property) and end (deprecated)
    const mockAnalyzeResult = {
      warnings: [],
      syncedRowCount: 5,
      start: 1000,
      end: 1075, // Deprecated but still present
      elapsed: 75, // New property
      readRowCount: 5,
      readRowCountsByQuery: {
        issues: {
          'SELECT * FROM issues': 5,
        },
      },
      sqlitePlans: {
        'SELECT * FROM issues': ['SCAN issues'],
      },
    };

    await z.triggerMessage([
      'inspect',
      {
        op: 'analyze-query',
        id: analyzeId,
        value: mockAnalyzeResult,
      },
    ]);

    const result = await analyzePromise;

    // Verify elapsed is present (new property)
    expect(result.elapsed).toBe(75);

    // Verify elapsed matches end - start
    expect(result.elapsed).toBe(result.end - result.start);

    // Verify deprecated 'end' is still present for backward compatibility
    expect(result.end).toBe(1075);
    expect(result.start).toBe(1000);

    await z.close();
  });

  test('analyze result handles missing elapsed', async () => {
    const z = zeroForTest({schema});
    await z.triggerConnected();

    const idP = waitForID(z.socket, 'queries');
    const queriesP = z.inspector.client.queries();
    const id = await idP;

    await z.triggerMessage([
      'inspect',
      {
        op: 'queries',
        id,
        value: [
          {
            clientID: z.clientID,
            queryID: '1',
            ast: {table: 'issues'},
            name: null,
            args: null,
            deleted: false,
            got: true,
            inactivatedAt: null,
            rowCount: 5,
            ttl: 60_000,
            metrics: null,
          },
        ],
      },
    ]);

    const queries = await queriesP;

    const query = queries[0];
    (await z.socket).messages.length = 0;

    const analyzeIdPromise = waitForID(z.socket, 'analyze-query');
    const analyzePromise = query.analyze();
    const analyzeId = await analyzeIdPromise;

    // Verify the message sent to server
    expect((await z.socket).jsonMessages).toEqual([
      [
        'inspect',
        {
          op: 'analyze-query',
          id: analyzeId,
          value: {table: 'issues'},
          options: undefined,
        },
      ],
    ]);

    // Mock analyze result WITHOUT elapsed (old server response)
    const mockAnalyzeResult = {
      warnings: [],
      syncedRowCount: 5,
      start: 2000,
      end: 2100,
      // No elapsed property - testing backward compatibility
      readRowCountsByQuery: {
        issues: {
          'SELECT * FROM issues': 5,
        },
      },
    };

    await z.triggerMessage([
      'inspect',
      {
        op: 'analyze-query',
        id: analyzeId,
        value: mockAnalyzeResult,
      },
    ]);

    const result = await analyzePromise;

    // Verify elapsed is undefined (not provided by old server)
    expect(result.elapsed).toBeUndefined();

    // Verify we can still calculate it from end - start if needed
    expect(result.end - result.start).toBe(100);

    await z.close();
  });
});

describe('authenticate', () => {
  test('authenticate rpc dance', async () => {
    const z = zeroForTest({schema});
    await z.triggerConnected();
    await Promise.resolve();

    const idPromise = waitForID(z.socket, 'version');
    const p = z.inspector.serverVersion();
    const id = await idPromise;
    (await z.socket).messages.length = 0;

    vi.stubGlobal('prompt', () => 'test-user');

    // Set up listener for authenticate request before triggering
    const authIdPromise = waitForID(z.socket, 'authenticate');

    await z.triggerMessage([
      'inspect',
      {
        op: 'authenticated',
        id,
        value: false,
      },
    ] satisfies InspectDownMessage);

    const authId = await authIdPromise;
    (await z.socket).messages.length = 0;

    // Set up listener for the retried version request
    const retryVersionIdPromise = waitForID(z.socket, 'version');

    await z.triggerMessage([
      'inspect',
      {
        op: 'authenticated',
        id: authId,
        value: true,
      },
    ] satisfies InspectDownMessage);

    const retryVersionId = await retryVersionIdPromise;
    (await z.socket).messages.length = 0;

    await z.triggerMessage([
      'inspect',
      {
        op: 'version',
        id: retryVersionId,
        value: '1.2.34',
      },
    ] satisfies InspectDownMessage);

    expect(await p).toBe('1.2.34');

    await z.close();
  });

  test('cancel prompt', async () => {
    const z = zeroForTest({schema});
    await z.triggerConnected();
    await Promise.resolve();

    const idPromise = waitForID(z.socket, 'version');
    const p = z.inspector.serverVersion();
    const id = await idPromise;
    (await z.socket).messages.length = 0;

    vi.stubGlobal('prompt', () => null);

    await z.triggerMessage([
      'inspect',
      {
        op: 'authenticated',
        id,
        value: false,
      },
    ] satisfies InspectDownMessage);

    await expect(p).rejects.toThrowError(`Authentication failed`);
    await z.close();
  });

  test('wrong password rejects', async () => {
    const z = zeroForTest({schema});
    await z.triggerConnected();
    await Promise.resolve();

    const idPromise = waitForID(z.socket, 'version');
    const p = z.inspector.serverVersion();
    const id = await idPromise;
    (await z.socket).messages.length = 0;

    vi.stubGlobal('prompt', () => 'test-user');

    // Set up listener for authenticate request before triggering
    const authIdPromise = waitForID(z.socket, 'authenticate');

    await z.triggerMessage([
      'inspect',
      {
        op: 'authenticated',
        id,
        value: false,
      },
    ] satisfies InspectDownMessage);

    const authId = await authIdPromise;
    (await z.socket).messages.length = 0;

    await z.triggerMessage([
      'inspect',
      {
        op: 'authenticated',
        id: authId,
        value: false,
      },
    ] satisfies InspectDownMessage);

    await expect(p).rejects.toThrowError(`Authentication failed`);

    await z.close();
  });

  describe('Query metrics properties', () => {
    test('hydration and update metrics are extracted from server metrics', async () => {
      const z = zeroForTest({schema});
      await z.triggerConnected();

      // Create TDigest instances with test data for server metrics
      const serverHydrationDigest = new TDigest();
      serverHydrationDigest.add(50, 1); // 50ms
      serverHydrationDigest.add(75, 1); // 75ms
      serverHydrationDigest.add(100, 1); // 100ms

      const serverUpdateDigest = new TDigest();
      serverUpdateDigest.add(5, 1); // 5ms
      serverUpdateDigest.add(10, 1); // 10ms
      serverUpdateDigest.add(15, 1); // 15ms
      serverUpdateDigest.add(20, 1); // 20ms
      serverUpdateDigest.add(25, 1); // 25ms
      serverUpdateDigest.add(30, 1); // 30ms
      serverUpdateDigest.add(35, 1); // 35ms
      serverUpdateDigest.add(40, 1); // 40ms
      serverUpdateDigest.add(45, 1); // 45ms
      serverUpdateDigest.add(50, 1); // 50ms

      // Use waitForID pattern which awaits socket before calling inspector
      const idPromise = waitForID(z.socket, 'queries');
      const p = z.inspector.client.queries();
      const id = await idPromise;

      // Mock server response with metrics
      await z.triggerMessage([
        'inspect',
        {
          op: 'queries',
          id,
          value: [
            {
              clientID: z.clientID,
              queryID: 'test-query-1',
              ast: {table: 'issues'},
              name: null,
              args: null,
              deleted: false,
              got: true,
              inactivatedAt: null,
              rowCount: 10,
              ttl: 60_000,
              metrics: {
                'query-materialization-server': serverHydrationDigest.toJSON(),
                'query-update-server': serverUpdateDigest.toJSON(),
              },
            },
          ],
        },
      ] satisfies InspectDownMessage);

      const queries = await p;
      expect(queries).toHaveLength(1);

      const query = queries[0];

      // Test hydration metrics (P50/median) - client metrics will default to null since no client metrics are provided
      expect(query.hydrateClient).toBeNull(); // null since no client metrics
      expect(query.hydrateServer).toBe(75); // P50 of [50, 75, 100]
      expect(query.hydrateTotal).toBeNull(); // null since no client metrics

      // Test update metrics
      expect(query.updateClientP50).toBeNull(); // null since no client metrics
      expect(query.updateClientP95).toBeNull(); // null since no client metrics
      expect(query.updateServerP50).toBe(27.5); // P50 of [5,10,15,20,25,30,35,40,45,50]
      expect(query.updateServerP95).toBe(50); // P95 of [5,10,15,20,25,30,35,40,45,50] - TDigest returns 50

      await z.close();
    });

    test('metrics properties default to null when no metrics available', async () => {
      const z = zeroForTest({schema});
      await z.triggerConnected();

      // Use waitForID pattern which awaits socket before calling inspector
      const idPromise = waitForID(z.socket, 'queries');
      const queriesP = z.inspector.client.queries();
      const id = await idPromise;

      // Mock server response with null metrics
      await z.triggerMessage([
        'inspect',
        {
          op: 'queries',
          id,
          value: [
            {
              clientID: z.clientID,
              queryID: 'test-query-no-metrics',
              ast: {table: 'issues'},
              name: null,
              args: null,
              deleted: false,
              got: true,
              inactivatedAt: null,
              rowCount: 0,
              ttl: 60_000,
              metrics: null,
            },
          ],
        },
      ] satisfies InspectDownMessage);

      const queries = await queriesP;
      expect(queries).toHaveLength(1);

      const query = queries[0];

      // All properties should default to null when no metrics available
      expect(query.hydrateClient).toBeNull();
      expect(query.hydrateServer).toBeNull();
      expect(query.hydrateTotal).toBeNull();
      expect(query.updateClientP50).toBeNull();
      expect(query.updateClientP95).toBeNull();
      expect(query.updateServerP50).toBeNull();
      expect(query.updateServerP95).toBeNull();

      await z.close();
    });

    test('metrics properties handle empty TDigest correctly', async () => {
      const z = zeroForTest({schema});
      await z.triggerConnected();

      // Mock server response with empty metrics (empty TDigest)
      const emptyDigest = new TDigest();

      // Use waitForID pattern which awaits socket before calling inspector
      const idPromise = waitForID(z.socket, 'queries');
      const p = z.inspector.client.queries();
      const id = await idPromise;

      await z.triggerMessage([
        'inspect',
        {
          op: 'queries',
          id,
          value: [
            {
              clientID: z.clientID,
              queryID: 'test-query-empty-metrics',
              ast: {table: 'issues'},
              name: null,
              args: null,
              deleted: false,
              got: true,
              inactivatedAt: null,
              rowCount: 0,
              ttl: 60_000,
              metrics: {
                'query-materialization-server': emptyDigest.toJSON(),
                'query-update-server': emptyDigest.toJSON(),
              },
            },
          ],
        },
      ] satisfies InspectDownMessage);

      const queries = await p;
      expect(queries).toHaveLength(1);

      const query = queries[0];

      // Empty TDigest quantile returns NaN, but our implementation should default to null
      expect(query.hydrateClient).toBeNull();
      expect(query.hydrateServer).toBeNull();
      expect(query.hydrateTotal).toBeNull();
      expect(query.updateClientP50).toBeNull();
      expect(query.updateClientP95).toBeNull();
      expect(query.updateServerP50).toBeNull();
      expect(query.updateServerP95).toBeNull();

      await z.close();
    });

    test('metrics properties handle single data point correctly', async () => {
      const z = zeroForTest({schema});
      await z.triggerConnected();

      // Create TDigest instances with single data points
      const singlePointDigest = new TDigest();
      singlePointDigest.add(42, 1); // Single value: 42ms

      // Use waitForID pattern which awaits socket before calling inspector
      const idPromise = waitForID(z.socket, 'queries');
      const queriesP = z.inspector.client.queries();
      const id = await idPromise;

      // Mock server response with single-point metrics
      await z.triggerMessage([
        'inspect',
        {
          op: 'queries',
          id,
          value: [
            {
              clientID: z.clientID,
              queryID: 'test-query-single-point',
              ast: {table: 'issues'},
              name: null,
              args: null,
              deleted: false,
              got: true,
              inactivatedAt: null,
              rowCount: 1,
              ttl: 60_000,
              metrics: {
                'query-materialization-server': singlePointDigest.toJSON(),
                'query-update-server': singlePointDigest.toJSON(),
              },
            },
          ],
        },
      ] satisfies InspectDownMessage);

      const queries = await queriesP;
      expect(queries).toHaveLength(1);

      const query = queries[0];

      // Single data point should return that value for all percentiles (server-side)
      expect(query.hydrateClient).toBeNull(); // null since no client metrics
      expect(query.hydrateServer).toBe(42);
      expect(query.hydrateTotal).toBeNull(); // null since no client metrics
      expect(query.updateClientP50).toBeNull(); // null since no client metrics
      expect(query.updateClientP95).toBeNull(); // null since no client metrics
      expect(query.updateServerP50).toBe(42);
      expect(query.updateServerP95).toBe(42);

      await z.close();
    });

    test('queries are sorted descending by server hydrate time', async () => {
      const z = zeroForTest({schema});
      await z.triggerConnected();

      // Create TDigest instances with different hydration times
      const slowDigest = new TDigest();
      slowDigest.add(100, 1); // 100ms

      const mediumDigest = new TDigest();
      mediumDigest.add(50, 1); // 50ms

      const fastDigest = new TDigest();
      fastDigest.add(10, 1); // 10ms

      const emptyDigest = new TDigest(); // Empty digest: quantile() returns NaN, converted to null hydrateServer
      const emptyUpdateDigest = new TDigest();

      // Use waitForID pattern which awaits socket before calling inspector
      const idPromise = waitForID(z.socket, 'queries');
      const p = z.inspector.client.queries();
      const id = await idPromise;

      // Send queries in non-sorted order: slow, fast, no-metrics, medium
      await z.triggerMessage([
        'inspect',
        {
          op: 'queries',
          id,
          value: [
            {
              clientID: z.clientID,
              queryID: 'slow-query',
              ast: {table: 'issues'},
              name: null,
              args: null,
              deleted: false,
              got: true,
              inactivatedAt: null,
              rowCount: 10,
              ttl: 60_000,
              metrics: {
                'query-materialization-server': slowDigest.toJSON(),
                'query-update-server': emptyUpdateDigest.toJSON(),
              },
            },
            {
              clientID: z.clientID,
              queryID: 'fast-query',
              ast: {table: 'issues'},
              name: null,
              args: null,
              deleted: false,
              got: true,
              inactivatedAt: null,
              rowCount: 5,
              ttl: 60_000,
              metrics: {
                'query-materialization-server': fastDigest.toJSON(),
                'query-update-server': emptyUpdateDigest.toJSON(),
              },
            },
            {
              clientID: z.clientID,
              queryID: 'no-metrics-query',
              ast: {table: 'issues'},
              name: null,
              args: null,
              deleted: false,
              got: true,
              inactivatedAt: null,
              rowCount: 3,
              ttl: 60_000,
              metrics: {
                'query-materialization-server': emptyDigest.toJSON(),
                'query-update-server': emptyUpdateDigest.toJSON(),
              },
            },
            {
              clientID: z.clientID,
              queryID: 'medium-query',
              ast: {table: 'issues'},
              name: null,
              args: null,
              deleted: false,
              got: true,
              inactivatedAt: null,
              rowCount: 7,
              ttl: 60_000,
              metrics: {
                'query-materialization-server': mediumDigest.toJSON(),
                'query-update-server': emptyUpdateDigest.toJSON(),
              },
            },
          ],
        },
      ] satisfies InspectDownMessage);

      const queries = await p;
      expect(queries).toHaveLength(4);

      // Queries should be sorted descending by hydrateServer (slowest first)
      expect(queries[0].id).toBe('slow-query');
      expect(queries[0].hydrateServer).toBe(100);

      expect(queries[1].id).toBe('medium-query');
      expect(queries[1].hydrateServer).toBe(50);

      expect(queries[2].id).toBe('fast-query');
      expect(queries[2].hydrateServer).toBe(10);

      // Query with null hydrateServer (treated as 0) should be sorted to the end
      expect(queries[3].id).toBe('no-metrics-query');
      expect(queries[3].hydrateServer).toBeNull();

      await z.close();
    });
  });

  describe('RPC error handling', () => {
    test('handles validation error for wrong op in response', async () => {
      const z = zeroForTest({schema});
      await z.triggerConnected();
      await Promise.resolve();

      const idPromise = waitForID(z.socket, 'version');
      const p = z.inspector.serverVersion();
      const id = await idPromise;

      // Simulate error response - this will fail schema validation
      // because inspectVersionDownSchema expects op: 'version', not op: 'error'
      await z.triggerMessage([
        'inspect',
        {
          op: 'error',
          id,
          value: 'Server encountered an internal error',
        },
      ] satisfies InspectDownMessage);

      // The RPC will reject with a validation error since the response
      // doesn't match the expected schema
      await expect(p).rejects.toThrow('Expected literal value "version"');

      await z.close();
    });

    test('handles validation error for malformed response', async () => {
      const z = zeroForTest({schema});
      await z.triggerConnected();
      await Promise.resolve();

      const idPromise = waitForID(z.socket, 'version');
      const p = z.inspector.serverVersion();
      const id = await idPromise;

      // Simulate malformed response that doesn't match any schema
      // Use type assertion to bypass compile-time check
      await z.triggerMessage([
        'inspect',
        {
          op: 'invalid-operation',
          id,
          value: 'something',
        } as unknown as InspectDownMessage[1],
      ]);

      await expect(p).rejects.toThrow('Expected literal value "version"');

      await z.close();
    });

    test('handles missing value in response', async () => {
      const z = zeroForTest({schema});
      await z.triggerConnected();
      await Promise.resolve();

      const idPromise = waitForID(z.socket, 'version');
      const p = z.inspector.serverVersion();
      const id = await idPromise;

      // Simulate response with missing value field
      // Use type assertion to bypass compile-time check
      await z.triggerMessage([
        'inspect',
        {
          op: 'version',
          id,
          // missing 'value' field
        } as InspectDownMessage[1],
      ]);

      await expect(p).rejects.toThrow('Missing property value');

      await z.close();
    });
  });
});

describe('inspector.analyzeQuery name mapping', () => {
  async function testAnalyzeQuery<S extends Schema>(
    z: TestZero<S>,
    query: AnyQuery,
    options: AnalyzeQueryOptions | undefined,
    mockResult: AnalyzeQueryResult,
  ): Promise<{result: AnalyzeQueryResult; sentAst: AST | undefined}> {
    const idPromise = waitForID(z.socket, 'analyze-query');
    const analyzePromise = z.inspector.analyzeQuery(query, options);
    const id = await idPromise;

    // Capture the AST sent to server
    const socket = await z.socket;
    const sentMessages = socket.jsonMessages.filter(msg => {
      if (!Array.isArray(msg)) return false;
      const [type, data] = msg;
      return (
        type === 'inspect' &&
        typeof data === 'object' &&
        data !== null &&
        'op' in data &&
        data.op === 'analyze-query'
      );
    });

    expect(sentMessages).toHaveLength(1);
    let sentAst: AST | undefined;
    const firstMessage = sentMessages[0];
    if (Array.isArray(firstMessage) && firstMessage.length > 1) {
      const data = firstMessage[1];
      if (typeof data === 'object' && data !== null && 'ast' in data) {
        sentAst = data.ast as AST;
      }
    }

    // Send mock response
    await z.triggerMessage([
      'inspect',
      {
        op: 'analyze-query',
        id,
        value: mockResult,
      },
    ]);

    const result = await analyzePromise;
    return {result, sentAst};
  }

  test('maps client table names to server names', async () => {
    // Define a schema with mapped table names
    const issueTable = table('issue')
      .from('_issues') // Map client name 'issue' to server name '_issues'
      .columns({
        id: string(),
        title: string(),
      })
      .primaryKey('id');

    const mappedSchema = createSchema({
      tables: [issueTable],
    });

    const z = zeroForTest({schema: mappedSchema});
    await z.triggerConnected();

    const zql = createBuilder(schema);

    const {result, sentAst} = await testAnalyzeQuery(
      z,
      zql.issue,
      {syncedRows: true},
      {
        syncedRowCount: 5,
        readRowCountsByQuery: {_issues: {'SELECT * FROM _issues': 5}},
        warnings: [],
        start: 0,
        end: 100,
        elapsed: 100,
      },
    );

    // The AST should have the SERVER table name (_issues), not client name (issue)
    expect(sentAst?.table).toBe('_issues');
    expect(result.syncedRowCount).toBe(5);
    await z.close();
  });

  test('maps client column names to server names in where clause', async () => {
    const issueTable = table('issue')
      .columns({
        id: string(),
        creatorID: string()
          .from('creator_id') // Map client name 'creatorID' to server name 'creator_id'
          .optional(),
      })
      .primaryKey('id');

    const mappedSchema = createSchema({
      tables: [issueTable],
    });

    const z = zeroForTest({schema: mappedSchema});
    await z.triggerConnected();

    const zql = createBuilder(mappedSchema);

    const {result, sentAst} = await testAnalyzeQuery(
      z,
      zql.issue.where('creatorID', '=', 'user123'),
      {syncedRows: true},
      {
        syncedRowCount: 1,
        readRowCountsByQuery: {},
        warnings: [],
        start: 1000,
        end: 1100,
      },
    );

    // The AST where clause should reference the server column name
    expect(sentAst?.where).toBeDefined();
    expect(sentAst?.where?.type).toBe('simple');
    // Type narrowing: we know where is defined and is simple from above assertions
    assert(sentAst?.where?.type === 'simple');
    expect(sentAst.where.left.type).toBe('column');
    assert(sentAst.where.left.type === 'column');
    expect(sentAst.where.left.name).toBe('creator_id');
    expect(result.syncedRowCount).toBe(1);
    await z.close();
  });

  test('handles queries without name mapping (schema has no mapped names)', async () => {
    const issueTable = table('issue')
      .columns({
        id: string(),
        title: string(),
      })
      .primaryKey('id');

    const simpleSchema = createSchema({
      tables: [issueTable],
    });

    const z = zeroForTest({schema: simpleSchema});
    await z.triggerConnected();

    const zql = createBuilder(simpleSchema);

    const {result, sentAst} = await testAnalyzeQuery(
      z,
      zql.issue,
      {syncedRows: true},
      {
        syncedRowCount: 3,
        readRowCountsByQuery: {},
        warnings: [],
        start: 2000,
        end: 2100,
      },
    );

    // Table name should remain 'issue' since there's no mapping
    expect(sentAst?.table).toBe('issue');
    expect(result.syncedRowCount).toBe(3);
    await z.close();
  });

  test('maps names in related queries', async () => {
    const issueTable = table('issue')
      .from('_issues')
      .columns({
        id: string(),
        projectID: string().from('project_id').optional(),
      })
      .primaryKey('id');

    const projectTable = table('project')
      .from('_projects')
      .columns({
        id: string(),
        name: string(),
      })
      .primaryKey('id');

    const issueRelationships = relationships(issueTable, connect => ({
      project: connect.one({
        sourceField: ['projectID'],
        destField: ['id'],
        destSchema: projectTable,
      }),
    }));

    const mappedSchema = createSchema({
      tables: [issueTable, projectTable],
      relationships: [issueRelationships],
    });

    const z = zeroForTest({schema: mappedSchema});
    await z.triggerConnected();

    const zql = createBuilder(mappedSchema);

    const {result, sentAst} = await testAnalyzeQuery(
      z,
      zql.issue.related('project', q => q.one()),
      {syncedRows: true},
      {
        syncedRowCount: 2,
        readRowCountsByQuery: {},
        warnings: [],
        start: 3000,
        end: 3100,
      },
    );

    // Main table should be mapped
    expect(sentAst?.table).toBe('_issues');

    // Related query should also be mapped
    const relatedQuery = sentAst?.related?.[0];
    expect(relatedQuery?.subquery.table).toBe('_projects');
    expect(relatedQuery?.correlation.parentField[0]).toBe('project_id');
    expect(result.syncedRowCount).toBe(2);
    await z.close();
  });
});
