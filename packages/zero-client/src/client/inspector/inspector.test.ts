import {beforeEach, describe, expect, test, vi} from 'vitest';
import type {ReadonlyTDigest} from '../../../../shared/src/tdigest.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {
  InspectDownMessage,
  InspectQueriesDown,
} from '../../../../zero-protocol/src/inspect-down.ts';
import {schema} from '../../../../zql/src/query/test/test-schemas.ts';
import {nanoid} from '../../util/nanoid.ts';
import {MockSocket, zeroForTest} from '../test-utils.ts';
import type {Query} from './types.ts';

beforeEach(() => {
  vi.spyOn(globalThis, 'WebSocket').mockImplementation(
    () => new MockSocket('ws://localhost:1234') as unknown as WebSocket,
  );
  return () => {
    vi.restoreAllMocks();
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
      },
    ],
    [
      {
        clientID: z.clientID,
        ast: {table: 'issue'},
        name: null,
        args: null,
        deleted: false,
        got: true,
        id: '1',
        inactivatedAt: null,
        rowCount: 10,
        ttl: '1m',
        zql: 'issue',
        metrics: null,
      },
    ],
  );
  const d = Date.UTC(2025, 2, 25, 14, 52, 10);
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
        inactivatedAt: d,
        rowCount: 10,
        ttl: 60_000,
      },
    ],
    [
      {
        clientID: z.clientID,
        ast: {table: 'issue'},
        name: null,
        args: null,
        deleted: false,
        got: true,
        id: '1',
        inactivatedAt: new Date(d),
        rowCount: 10,
        ttl: '1m',
        zql: 'issue',
        metrics: null,
      },
    ],
  );

  await z.close();
});

test('clientGroup queries', async () => {
  const ast: AST = {
    table: 'issue',
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
    alias: undefined,
    limit: undefined,
    orderBy: undefined,
    related: undefined,
    schema: undefined,
    start: undefined,
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
        },
      ],
    },
  ] satisfies InspectDownMessage);
  expect(await p).toEqual([
    {
      ast,
      name: null,
      args: null,
      clientID: z.clientID,
      deleted: false,
      got: true,
      id: '1',
      inactivatedAt: null,
      rowCount: 10,
      ttl: '1m',
      zql: "issue.where(({cmp, or}) => or(cmp('id', '1'), cmp('id', '!=', '2')))",
      metrics: null,
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
    expect(inspector.metrics['query-materialization-client'].count()).toBe(1);
    expect(
      inspector.metrics['query-materialization-client'].quantile(0.5),
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
          },
        ],
      },
    ] satisfies InspectDownMessage);

    const queries = await p;
    expect(queries).toHaveLength(1);
    expect(issueQuery.hash()).toBe(queries[0].id);
    const {metrics} = queries[0];

    expect(metrics?.['query-materialization-client'].count()).toBe(1);
    expect(
      metrics?.['query-materialization-client'].quantile(0.5),
    ).toBeGreaterThanOrEqual(0);
    await vi.waitFor(() => {
      expect(metrics?.['query-materialization-end-to-end'].count()).toBe(1);
    });
    expect(
      metrics?.['query-materialization-end-to-end'].quantile(0.5),
    ).toBeGreaterThanOrEqual(0);

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

    expect(inspector.metrics['query-materialization-client'].count()).toBe(2);

    const digest = inspector.metrics['query-materialization-client'];
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
    const globalMetricsQueryMaterializationClient =
      inspector.metrics['query-materialization-client'];
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

    const globalMetricsQueryMaterializationEndToEnd =
      inspector.metrics['query-materialization-end-to-end'];
    await vi.waitFor(() => {
      expect(globalMetricsQueryMaterializationEndToEnd.count()).toBe(1);
    });

    ensureRealData(globalMetricsQueryMaterializationEndToEnd);

    view.destroy();

    await z.close();
  });
});
