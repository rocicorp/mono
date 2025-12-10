import {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import type {MemorySource} from '../../../zql/src/ivm/memory-source.ts';
import {consume} from '../../../zql/src/ivm/stream.ts';
import type {MetricMap} from '../../../zql/src/query/metrics-delegate.ts';
import type {CustomQueryID} from '../../../zql/src/query/named.ts';
import {newQuery} from '../../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';
import {
  ZeroContext,
  type AddCustomQuery,
  type AddQuery,
  type FlushQueryChanges,
  type UpdateCustomQuery,
  type UpdateQuery,
} from './context.ts';
import {IVMSourceBranch} from './ivm-branch.ts';

const testBatchViewUpdates = (applyViewUpdates: () => void) =>
  applyViewUpdates();

function assertValidRunOptions(): void {}

// Helper function to create sequential performance.now() mocks
function mockPerformanceNow(times: number[]): void {
  let callCount = 0;
  vi.spyOn(performance, 'now').mockImplementation(() => {
    const time = times[callCount] ?? 0;
    callCount++;
    return time;
  });
}

// Helper function for simple timing scenarios (t0, client metric timing)
function mockClientTiming(t0: number, clientTime: number): void {
  mockPerformanceNow([t0, clientTime]);
}

// Helper function for end-to-end timing scenarios (t0, client metric, end-to-end timing)
function mockEndToEndTiming(
  t0: number,
  clientTime: number,
  endToEndTime: number,
): void {
  mockPerformanceNow([t0, clientTime, endToEndTime]);
}

// Helper function to create a standard query
function createTestQuery(): AnyQuery {
  return newQuery(schema, 'users');
}

const schema = createSchema({
  tables: [
    table('users')
      .columns({
        id: string(),
        name: string(),
      })
      .primaryKey('id'),
  ],
});

// Type for views returned by materialize
interface MockView {
  data?: unknown[];
  destroy: () => void;
}

describe('query materialization metrics', () => {
  let addMetricSpy: ReturnType<typeof vi.fn>;
  let queryDelegate: ZeroContext;
  let gotCallback: ((got: boolean) => void) | undefined;
  let addServerQuerySpy: ReturnType<typeof vi.fn>;
  let addCustomQuerySpy: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.useFakeTimers();

    addMetricSpy =
      vi.fn<
        <K extends keyof MetricMap>(
          metric: K,
          value: number,
          ...args: MetricMap[K]
        ) => void
      >();

    addServerQuerySpy = vi
      .fn<
        (ast: AST, ttl: number, callback?: (got: boolean) => void) => () => void
      >()
      .mockImplementation((_ast, _ttl, callback) => {
        gotCallback = callback;
        return () => {};
      });

    addCustomQuerySpy = vi
      .fn<
        (
          ast: AST,
          customQueryID: CustomQueryID,
          ttl: number,
          callback?: (got: boolean) => void,
        ) => () => void
      >()
      .mockImplementation((_ast, _customQueryID, _ttl, callback) => {
        gotCallback = callback;
        return () => {};
      });

    queryDelegate = new ZeroContext(
      new LogContext('info'),
      new IVMSourceBranch(schema.tables),
      addServerQuerySpy as unknown as AddQuery,
      addCustomQuerySpy as unknown as AddCustomQuery,
      (() => {}) as unknown as UpdateQuery,
      (() => {}) as unknown as UpdateCustomQuery,
      (() => {}) as unknown as FlushQueryChanges,
      testBatchViewUpdates,
      addMetricSpy,
      assertValidRunOptions,
    );
  });

  describe('query-materialization-client metric', () => {
    test.each([
      {
        name: 'records client metric immediately when query is materialized',
        t0: 100,
        clientTime: 125,
        expectedDelta: 25,
        setup: () => createTestQuery(),
      },
      {
        name: 'records client metric for custom queries',
        t0: 200,
        clientTime: 235,
        expectedDelta: 35,
        setup: () => {
          const query = createTestQuery();
          return asQueryInternals(query).nameAndArgs('getUsers', []);
        },
        additionalChecks: () => {
          expect(addCustomQuerySpy).toHaveBeenCalledOnce();
        },
      },
    ])('$name', ({t0, clientTime, expectedDelta, setup, additionalChecks}) => {
      mockClientTiming(t0, clientTime);

      const query = setup();
      const view = queryDelegate.materialize(query) as unknown as MockView;

      additionalChecks?.();

      expect(addMetricSpy).toHaveBeenCalledWith(
        'query-materialization-client',
        expectedDelta,
        expect.any(String),
      );

      view.destroy();
    });

    test('records client metric with different timing for multiple queries', () => {
      // Mock performance.now() for multiple client queries
      mockPerformanceNow([
        100,
        120, // First query: t0=100, client=120 (20ms)
        200,
        230, // Second query: t0=200, client=230 (30ms)
        300,
        340, // Third query: t0=300, client=340 (40ms)
      ]);

      const queries = [];
      const views = [];

      // Create multiple queries
      for (let i = 0; i < 3; i++) {
        const query = createTestQuery();
        queries.push(query);
        views.push(queryDelegate.materialize(query) as unknown as MockView);
      }

      // Verify all client metrics were recorded
      const clientCalls = addMetricSpy.mock.calls.filter(
        call => call[0] === 'query-materialization-client',
      );
      expect(clientCalls).toHaveLength(3);

      // Verify the timing for each call
      expect(clientCalls[0][1]).toBe(20); // 120-100=20ms
      expect(clientCalls[1][1]).toBe(30); // 230-200=30ms
      expect(clientCalls[2][1]).toBe(40); // 340-300=40ms

      // Cleanup
      views.forEach(view => view.destroy());
    });

    test('records client metric with custom view factory', () => {
      // Mock performance.now() for view factory
      mockClientTiming(250, 275);

      const query = createTestQuery();

      // Use custom view factory
      const customViewFactory = () => ({
        destroy() {},
      });

      const view = queryDelegate.materialize(
        query,
        customViewFactory,
      ) as MockView;

      // Should record client metric even with custom view factory
      expect(addMetricSpy).toHaveBeenCalledWith(
        'query-materialization-client',
        25, // 275ms - 250ms = 25ms elapsed
        expect.any(String),
      );

      view.destroy();
    });

    test('records precise timing with fractional milliseconds', () => {
      // Test precision with fractional milliseconds
      mockClientTiming(1000.123, 1025.789);

      const query = createTestQuery();

      const view = queryDelegate.materialize(query) as unknown as MockView;

      // Should capture precise timing (accounting for floating-point precision)
      expect(addMetricSpy).toHaveBeenCalledWith(
        'query-materialization-client',
        expect.closeTo(25.666, 0.001), // 1025.789 - 1000.123 = 25.666ms
        expect.any(String),
      );

      view.destroy();
    });

    test('records client metric', () => {
      // Mock performance.now() for query
      mockClientTiming(500, 545);

      const query = createTestQuery();

      const view = queryDelegate.materialize(query) as unknown as MockView;

      // Should record client metric
      expect(addMetricSpy).toHaveBeenCalledWith(
        'query-materialization-client',
        45, // 545ms - 500ms = 45ms elapsed
        expect.any(String),
      );

      view.destroy();
    });
  });

  describe('query-materialization-end-to-end metric', () => {
    test.each([
      {
        name: 'records end-to-end metric when server query completes',
        t0: 150,
        clientTime: 200,
        serverTime: 300,
        expectedDelta: 150,
        setup: () => createTestQuery(),
        additionalChecks: () => {
          expect(addServerQuerySpy).toHaveBeenCalledOnce();
        },
      },
      {
        name: 'records end-to-end metric when custom query completes',
        t0: 250,
        clientTime: 300,
        serverTime: 400,
        expectedDelta: 150,
        setup: () => {
          const query = createTestQuery();
          return asQueryInternals(query).nameAndArgs('getUsers', []);
        },
        additionalChecks: () => {
          expect(addCustomQuerySpy).toHaveBeenCalledOnce();
        },
      },
    ])(
      '$name',
      ({
        t0,
        clientTime,
        serverTime,
        expectedDelta,
        setup,
        additionalChecks,
      }) => {
        mockEndToEndTiming(t0, clientTime, serverTime);

        const query = setup();
        const view = queryDelegate.materialize(query) as unknown as MockView;

        additionalChecks?.();
        expect(gotCallback).toBeDefined();

        // oxlint-disable-next-line no-non-null-assertion
        gotCallback!(true);

        expect(addMetricSpy).toHaveBeenCalledWith(
          'query-materialization-end-to-end',
          expectedDelta,
          expect.any(String),
          expect.objectContaining({
            table: 'users',
          }),
        );

        view.destroy();
      },
    );

    test('does not record end-to-end metric when server never responds', () => {
      // Mock performance.now() for basic timing - only 2 calls expected (t0 and client metric)
      mockClientTiming(150, 200);

      const query = createTestQuery();

      const view = queryDelegate.materialize(query) as unknown as MockView;

      // Never call gotCallback

      // Verify that end-to-end metric was NOT recorded
      const endToEndCalls = addMetricSpy.mock.calls.filter(
        call => call[0] === 'query-materialization-end-to-end',
      );
      expect(endToEndCalls).toHaveLength(0);

      view.destroy();
    });

    test('always records client-side metric regardless of server response', () => {
      // Mock performance.now() for basic timing
      mockClientTiming(100, 150);

      const query = createTestQuery();

      const view = queryDelegate.materialize(query) as unknown as MockView;

      // Client-side metric should be recorded immediately
      expect(addMetricSpy).toHaveBeenCalledWith(
        'query-materialization-client',
        50, // 150ms - 100ms = 50ms
        expect.any(String), // queryID
      );

      view.destroy();
    });

    test('records different timing for multiple queries', () => {
      // Mock performance.now() calls:
      // Query1: t0=200, client=250, end-to-end=400
      // Query2: t0=300, client=350, end-to-end=500
      mockPerformanceNow([
        200,
        250, // First query: t0=200, client=250
        300,
        350, // Second query: t0=300, client=350
        400,
        500, // End-to-end callbacks: first=400, second=500
      ]);

      // First query
      const query1 = createTestQuery();

      const view1 = queryDelegate.materialize(query1) as unknown as MockView;
      const firstGotCallback = gotCallback!;

      // Second query
      const query2 = createTestQuery();

      const view2 = queryDelegate.materialize(query2) as unknown as MockView;
      const secondGotCallback = gotCallback!;

      // First query completes
      firstGotCallback(true);

      // Second query completes later
      secondGotCallback(true);

      // Verify both metrics were recorded with correct timing
      // Check that the metrics were called in the right order and with right parameters
      const endToEndCalls = addMetricSpy.mock.calls.filter(
        call => call[0] === 'query-materialization-end-to-end',
      );

      expect(endToEndCalls).toHaveLength(2);

      expect(endToEndCalls[0]).toEqual([
        'query-materialization-end-to-end',
        200, // 400ms - 200ms = 200ms elapsed for first query
        expect.any(String),
        expect.objectContaining({
          table: 'users',
        }),
      ]);

      expect(endToEndCalls[1]).toEqual([
        'query-materialization-end-to-end',
        200, // 500ms - 300ms = 200ms elapsed for second query
        expect.any(String),
        expect.objectContaining({
          table: 'users',
        }),
      ]);

      view1.destroy();
      view2.destroy();
    });

    test('handles server responding with false (query not available)', () => {
      // Mock performance.now() for server failure scenario
      mockClientTiming(150, 200);

      const query = createTestQuery();

      const view = queryDelegate.materialize(query) as unknown as MockView;

      // Server responds with false (query not available)
      gotCallback!(false);

      // Should not record end-to-end metric when server responds with false
      const endToEndCalls = addMetricSpy.mock.calls.filter(
        call => call[0] === 'query-materialization-end-to-end',
      );
      expect(endToEndCalls).toHaveLength(0);

      view.destroy();
    });

    test('handles very fast server responses', () => {
      // Mock performance.now() for very fast response (same time for t0 and callback)
      mockEndToEndTiming(101, 102, 101);

      const query = createTestQuery();

      const view = queryDelegate.materialize(query) as unknown as MockView;

      // Server responds immediately
      gotCallback!(true);

      // Should record a very small time
      expect(addMetricSpy).toHaveBeenCalledWith(
        'query-materialization-end-to-end',
        0, // 101 - 101 = 0ms
        expect.any(String),
        expect.objectContaining({
          table: 'users',
        }),
      );

      view.destroy();
    });

    test('measures time correctly with view factory', () => {
      // Mock performance.now() for view factory timing
      mockEndToEndTiming(150, 200, 300);

      const query = createTestQuery();

      // Use custom view factory
      const customViewFactory = vi.fn().mockReturnValue({
        destroy: vi.fn(),
      });

      const view = queryDelegate.materialize(
        query,
        customViewFactory,
      ) as MockView;

      gotCallback!(true);

      // Should still record the metric correctly with custom factory
      expect(addMetricSpy).toHaveBeenCalledWith(
        'query-materialization-end-to-end',
        150, // 300ms - 150ms = 150ms elapsed
        expect.any(String),
        expect.objectContaining({
          table: 'users',
        }),
      );

      view.destroy();
    });

    test('integration with real data flow', () => {
      // Mock performance.now() to simulate proper timing
      mockEndToEndTiming(150, 200, 300);

      // Add some test data to the source
      const source = queryDelegate.getSource('users') as MemorySource;
      consume(source.push({type: 'add', row: {id: 'user1', name: 'John'}}));
      consume(source.push({type: 'add', row: {id: 'user2', name: 'Jane'}}));

      const query = createTestQuery();

      const view = queryDelegate.materialize(query) as unknown as MockView & {
        data: unknown[];
      };

      // Verify view has data - should be ordered by id ascending
      expect(view.data).toHaveLength(2);
      expect(view.data[0]).toEqual(
        expect.objectContaining({id: 'user1', name: 'John'}),
      );
      expect(view.data[1]).toEqual(
        expect.objectContaining({id: 'user2', name: 'Jane'}),
      );

      gotCallback!(true);

      // Verify metrics were recorded
      expect(addMetricSpy).toHaveBeenCalledWith(
        'query-materialization-client',
        50, // 200ms - 150ms = 50ms
        expect.any(String),
      );

      expect(addMetricSpy).toHaveBeenCalledWith(
        'query-materialization-end-to-end',
        150, // 300ms - 150ms = 150ms elapsed
        expect.any(String),
        expect.objectContaining({
          table: 'users',
        }),
      );

      view.destroy();
    });
  });

  describe('edge cases and error scenarios', () => {
    let addMetricSpy: ReturnType<typeof vi.fn>;
    let context: ZeroContext;

    beforeEach(() => {
      vi.useFakeTimers();

      addMetricSpy = vi.fn();

      context = new ZeroContext(
        new LogContext('info'),
        new IVMSourceBranch(schema.tables),
        (() => () => {}) as unknown as AddQuery,
        (() => () => {}) as unknown as AddCustomQuery,
        (() => {}) as unknown as UpdateQuery,
        (() => {}) as unknown as UpdateCustomQuery,
        (() => {}) as unknown as FlushQueryChanges,
        testBatchViewUpdates,
        addMetricSpy,
        assertValidRunOptions,
      );
    });

    test('handles rapid materialization and destruction', () => {
      // Mock performance.now() for multiple rapid queries
      // Each query has 2 calls: t0 and client metric
      // Return sequential times for each query
      mockPerformanceNow([
        100,
        101, // Query 1: t0=100, client=101 (1ms)
        101,
        102, // Query 2: t0=101, client=102 (1ms)
        102,
        103, // Query 3: t0=102, client=103 (1ms)
        103,
        104, // Query 4: t0=103, client=104 (1ms)
        104,
        105, // Query 5: t0=104, client=105 (1ms)
      ]);

      for (let i = 0; i < 5; i++) {
        const query = createTestQuery();

        const view = context.materialize(query) as unknown as MockView;
        view.destroy();
      }

      // Should record client metrics for all materializations
      const clientCalls = addMetricSpy.mock.calls.filter(
        call => call[0] === 'query-materialization-client',
      );
      expect(clientCalls).toHaveLength(5);
    });

    test('performance.now() timing precision', () => {
      // Test that we're using performance.now() correctly
      let mockTime = 1000.123; // fractional milliseconds

      const performanceNowSpy = vi
        .spyOn(performance, 'now')
        .mockImplementation(() => mockTime);

      const query = createTestQuery();

      mockTime = 1050.456;
      const view = context.materialize(query) as unknown as MockView;

      // Should capture the precise timing
      expect(performanceNowSpy).toHaveBeenCalled();

      view.destroy();
      performanceNowSpy.mockRestore();
    });
  });
});
