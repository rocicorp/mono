import {beforeEach, describe, expect, test, vi} from 'vitest';
import {TDigest, type ReadonlyTDigest} from '../../../../shared/src/tdigest.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {
  type InspectDownMessage,
  type InspectMetricsDown,
  type InspectQueriesDown,
} from '../../../../zero-protocol/src/inspect-down.ts';
import type {Schema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {schema} from '../../../../zql/src/query/test/test-schemas.ts';
import {nanoid} from '../../util/nanoid.ts';
import type {CustomMutatorDefs} from '../custom.ts';
import {MockSocket, TestZero, zeroForTest} from '../test-utils.ts';
import type {Inspector, Metrics, Query} from './types.ts';

const emptyMetrics = {
  'query-materialization-client': new TDigest(),
  'query-materialization-end-to-end': new TDigest(),
  'query-materialization-server': new TDigest(),
  'query-update-client': new TDigest(),
  'query-update-server': new TDigest(),
};

async function getMetrics<
  S extends Schema,
  MD extends CustomMutatorDefs | undefined,
>(
  inspector: Inspector,
  z: TestZero<S, MD>,
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
  vi.spyOn(globalThis, 'WebSocket').mockImplementation(
    () => new MockSocket('ws://localhost:1234') as unknown as WebSocket,
  );
  return () => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  };
});

test('basics', async () => {
  const z = zeroForTest();
  const inspector = await z.inspect();

  expect(inspector.client).toEqual({
    clientGroup: {
      id: await z.clientGroupID,
    },
    id: z.clientID,
  });
  expect(inspector.clientGroup).toEqual({
    id: await z.clientGroupID,
  });

  await z.close();
});

test('basics 2 clients', async () => {
  const userID = nanoid();
  const z1 = zeroForTest({userID, kvStore: 'idb'});
  const z2 = zeroForTest({userID, kvStore: 'idb'});

  const inspector = await z1.inspect();

  expect(await inspector.clients()).toEqual([
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

  const inspector = await z.inspect();
  expect(await inspector.clients()).toEqual([
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
    // The RPC uses our nanoid which uses Math.random
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
    (await z.socket).messages.length = 0;
    const p = inspector.client.queries();
    await Promise.resolve();
    expect((await z.socket).messages.map(s => JSON.parse(s))).toEqual([
      [
        'inspect',
        {
          op: 'queries',
          clientID: z.clientID,
          id: '000000000000000000000',
        },
      ],
    ]);
    await z.triggerMessage([
      'inspect',
      {
        op: 'queries',
        id: '000000000000000000000',
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
        ast: {table: 'issue'},
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
      {
        clientID: z.clientID,
        clientZQL: null,
        serverZQL: 'issue',
        name: null,
        args: null,
        deleted: false,
        got: true,
        id: '1',
        inactivatedAt: null,
        rowCount: 10,
        ttl: '1m',
        metrics: emptyMetrics,
      },
    ],
  );
  const d = Date.UTC(2025, 2, 25, 14, 52, 10);
  await t(
    [
      {
        clientID: z.clientID,
        queryID: '1',
        ast: {
          table: 'issue',
          where: {
            type: 'simple',
            left: {type: 'column', name: 'id'},
            op: '=',
            right: {type: 'literal', value: 123},
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
      {
        clientID: z.clientID,
        clientZQL: null,
        serverZQL: "issue.where('id', 123)",
        name: null,
        args: null,
        deleted: false,
        got: true,
        id: '1',
        inactivatedAt: new Date(d),
        rowCount: 10,
        ttl: '1m',
        metrics: emptyMetrics,
      },
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

  vi.spyOn(Math, 'random').mockImplementation(() => 0.5);
  const inspector = await z.inspect();
  const p = inspector.clientGroup.queries();
  await Promise.resolve();
  expect((await z.socket).messages).toMatchInlineSnapshot(`
    [
      "["inspect",{"op":"queries","id":"000000000000000000000"}]",
    ]
  `);
  await z.triggerMessage([
    'inspect',
    {
      op: 'queries',
      id: '000000000000000000000',
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
    {
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
    },
  ]);
});

describe('query metrics', () => {
  test('real query metrics integration', async () => {
    const z = zeroForTest({schema});
    await z.triggerConnected();

    const issueQuery = z.query.issue;
    await issueQuery.run();

    const inspector = await z.inspect();
    const metrics = await getMetrics(inspector, z);
    expect(metrics['query-materialization-client'].count()).toBe(1);
    expect(
      metrics['query-materialization-client'].quantile(0.5),
    ).toBeGreaterThanOrEqual(0);
    await z.close();
  });

  test('Attaching the metrics to the query', async () => {
    const z = zeroForTest({schema});
    await z.triggerConnected();

    const issueQuery = z.query.issue.orderBy('id', 'desc');
    const view = issueQuery.materialize();

    await z.triggerGotQueriesPatch(issueQuery);

    vi.spyOn(Math, 'random').mockImplementation(() => 0.5);
    const inspector = await z.inspect();
    const p = inspector.client.queries();
    await Promise.resolve();

    // Simulate the server response with query data
    await z.triggerMessage([
      'inspect',
      {
        op: 'queries',
        id: '000000000000000000000',
        value: [
          {
            clientID: z.clientID,
            queryID: issueQuery.hash(),
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

    const queries = await p;
    expect(queries).toHaveLength(1);
    expect(issueQuery.hash()).toBe(queries[0].id);

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

    // Execute multiple queries to generate real metrics
    const query1 = z.query.issue;
    const query2 = z.query.issue.where('id', '1');

    await query1.run();
    await query2.run();

    // Check that metrics were actually collected
    const inspector = await z.inspect();

    const metrics = await getMetrics(inspector, z);
    expect(metrics['query-materialization-client'].count()).toBe(2);

    const digest = metrics['query-materialization-client'];
    expect(digest.count()).toBe(2);

    expect(digest.quantile(0.5)).toBeGreaterThanOrEqual(0);

    await z.close();
  });

  test('query-specific metrics integration test', async () => {
    const z = zeroForTest({schema});
    await z.triggerConnected();

    // Execute queries with different characteristics to test metrics collection
    await z.query.issue.run(); // Simple table query
    await z.query.issue.where('id', '1').run(); // Filtered query
    await z.query.issue.where('id', '2').run(); // Another filtered query

    // Test that the inspector can access the real metrics
    const inspector = await z.inspect();

    // Verify global metrics were collected
    const metrics = await getMetrics(inspector, z);
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

    const q = z.query.issue;
    const view = q.materialize();
    await z.triggerGotQueriesPatch(q);

    {
      const metrics = await getMetrics(inspector, z);
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

    // Create a query and materialize a view to set up the query pipeline
    const issueQuery = z.query.issue;
    const view = issueQuery.materialize();
    await z.triggerGotQueriesPatch(issueQuery);

    // Get initial inspector to verify no query-update metrics initially
    const initialInspector = await z.inspect();
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

    const inspector = await z.inspect();
    const metrics = await getMetrics(inspector, z);

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

    const issueQuery = z.query.issue.orderBy('id', 'desc');
    const view = issueQuery.materialize();
    await z.triggerGotQueriesPatch(issueQuery);

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

    const inspector = await z.inspect();
    const metrics1 = await getMetrics(inspector, z);

    // Wait for the update to be processed
    await vi.waitFor(() => {
      const updateMetrics = metrics1['query-update-client'];
      expect(updateMetrics.count()).toBeGreaterThan(0);
    });

    // Get query-specific metrics through the inspector
    vi.spyOn(Math, 'random').mockImplementation(() => 0.5);
    const p = inspector.client.queries();
    await Promise.resolve();

    // Simulate the server response with query data
    await z.triggerMessage([
      'inspect',
      {
        op: 'queries',
        id: '000000000000000000000',
        value: [
          {
            clientID: z.clientID,
            queryID: issueQuery.hash(),
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
            metrics: {
              'query-materialization-server': [1000, 1, 2],
              'query-update-server': [100, 3, 4],
            },
          },
        ],
      },
    ] satisfies InspectDownMessage);

    const queries = await p;
    expect(queries).toHaveLength(1);
    expect(issueQuery.hash()).toBe(queries[0].id);

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
  await Promise.resolve();
  const inspector = await z.inspect();
  vi.spyOn(Math, 'random').mockImplementation(() => 0.5);
  await z.socket;
  const p = inspector.serverVersion();
  await Promise.resolve();
  expect((await z.socket).messages).toEqual([
    JSON.stringify(['inspect', {op: 'version', id: '000000000000000000000'}]),
  ]);

  await z.triggerMessage([
    'inspect',
    {
      op: 'version',
      id: '000000000000000000000',
      value: '1.2.34',
    },
  ] satisfies InspectDownMessage);

  expect(await p).toBe('1.2.34');

  await z.close();
});

test('clientZQL', async () => {
  const z = zeroForTest({schema});
  await z.triggerConnected();
  await Promise.resolve();
  const inspector = await z.inspect();
  vi.spyOn(Math, 'random').mockImplementation(() => 0.5);
  await z.socket;
  const p = inspector.client.queries();

  // Trigger QueryManager.#add by materializing a query and marking it as got
  const issueQuery = z.query.issue.where('ownerId', 'arv');
  const view = issueQuery.materialize();
  await z.triggerGotQueriesPatch(issueQuery);

  // Send fake inspect/queries response for this query
  await z.triggerMessage([
    'inspect',
    {
      op: 'queries',
      id: '000000000000000000000',
      value: [
        {
          clientID: z.clientID,
          queryID: issueQuery.hash(),
          ast: {
            table: 'issues',
            where: {
              type: 'simple',
              left: {type: 'column', name: 'owner_id'},
              op: '=',
              right: {type: 'literal', value: 'arv'},
            },
            orderBy: [['id', 'asc']],
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

  const queries = await p;
  expect(queries).toHaveLength(1);
  expect(queries[0].id).toBe(issueQuery.hash());
  expect(queries[0].clientZQL).toBe(
    "issue.where('ownerId', 'arv').orderBy('id', 'asc')",
  );
  expect(queries[0].serverZQL).toBe(
    "issues.where('owner_id', 'arv').orderBy('id', 'asc')",
  );

  view.destroy();
  await z.close();
});
