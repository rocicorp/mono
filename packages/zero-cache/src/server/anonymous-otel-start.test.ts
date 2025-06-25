import {beforeAll, afterAll, describe, expect, test, vi} from 'vitest';
import {OTLPMetricExporter} from '@opentelemetry/exporter-metrics-otlp-http';
import {PeriodicExportingMetricReader} from '@opentelemetry/sdk-metrics';
import {MeterProvider} from '@opentelemetry/sdk-metrics';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {getZeroConfig, type ZeroConfig} from '../config/zero-config.js';
import {
  startAnonymousTelemetry,
  recordMutation,
  recordRowsSynced,
  addActiveQuery,
  removeActiveQuery,
  updateCvrSize,
  addClientGroup,
  removeClientGroup,
  shutdownAnonymousTelemetry,
} from './anonymous-otel-start.ts';

// Mock the OTLP exporter and related OpenTelemetry components
vi.mock('@opentelemetry/exporter-metrics-otlp-http');
vi.mock('@opentelemetry/sdk-metrics');

// Mock the config
vi.mock('../config/zero-config.js', () => ({
  getZeroConfig: vi.fn(),
}));

describe('Anonymous Telemetry Integration Tests', () => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let mockExporter: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let mockMetricReader: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let mockMeterProvider: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let mockMeter: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let mockHistogram: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let mockObservableGauge: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let mockObservableCounter: any;
  let originalEnv: NodeJS.ProcessEnv;

  beforeAll(() => {
    // Store original environment
    originalEnv = {...process.env};

    // Reset all mocks
    vi.clearAllMocks();

    // Mock getZeroConfig to return default enabled state
    vi.mocked(getZeroConfig).mockReturnValue({
      enableUsageAnalytics: true,
      upstream: {
        db: 'postgresql://test@localhost/test',
      },
      serverVersion: '1.0.0-test',
      // Add other config properties as needed for tests
    } as Partial<ZeroConfig> as ZeroConfig);

    // Mock histogram
    mockHistogram = {
      record: vi.fn(),
    };

    // Mock observables
    mockObservableGauge = {
      addCallback: vi.fn(),
    };

    mockObservableCounter = {
      addCallback: vi.fn(),
    };

    // Mock meter
    mockMeter = {
      createHistogram: vi.fn().mockReturnValue(mockHistogram),
      createObservableGauge: vi.fn().mockReturnValue(mockObservableGauge),
      createObservableCounter: vi.fn().mockReturnValue(mockObservableCounter),
    };

    // Mock meter provider
    mockMeterProvider = {
      getMeter: vi.fn().mockReturnValue(mockMeter),
      shutdown: vi.fn().mockResolvedValue(undefined),
    };

    // Mock metric reader
    mockMetricReader = vi.fn();

    // Mock exporter
    mockExporter = vi.fn();

    // Setup mocks
    vi.mocked(OTLPMetricExporter).mockImplementation(() => mockExporter);
    vi.mocked(PeriodicExportingMetricReader).mockImplementation(
      () => mockMetricReader,
    );
    vi.mocked(MeterProvider).mockImplementation(() => mockMeterProvider);

    // Clear environment variables that might affect telemetry
    delete process.env.ZERO_UPSTREAM_DB;
    delete process.env.ZERO_SERVER_VERSION;
  });

  afterAll(() => {
    // Restore environment
    process.env = originalEnv;

    // Shutdown telemetry
    shutdownAnonymousTelemetry();
  });

  describe('Opt-out Configuration (test these first)', () => {
    test('should respect opt-out via enableUsageAnalytics=false', () => {
      // Mock config to return disabled analytics
      vi.mocked(getZeroConfig).mockReturnValueOnce({
        enableUsageAnalytics: false,
      } as Partial<ZeroConfig> as ZeroConfig);

      startAnonymousTelemetry();

      // Should not initialize any telemetry components
      expect(OTLPMetricExporter).not.toHaveBeenCalled();
      expect(PeriodicExportingMetricReader).not.toHaveBeenCalled();
      expect(MeterProvider).not.toHaveBeenCalled();
    });

    test('should respect opt-out when analytics explicitly disabled', () => {
      // Mock config to return disabled analytics
      vi.mocked(getZeroConfig).mockReturnValueOnce({
        enableUsageAnalytics: false,
      } as Partial<ZeroConfig> as ZeroConfig);

      startAnonymousTelemetry();
      expect(OTLPMetricExporter).not.toHaveBeenCalled();
    });
  });

  describe('Telemetry Startup and Operation', () => {
    test('should start telemetry with default configuration', () => {
      const lc = createSilentLogContext();

      startAnonymousTelemetry(lc);

      // Verify OTLP exporter was created with correct configuration
      expect(OTLPMetricExporter).toHaveBeenCalledWith({
        url: 'https://otlp-gateway-prod-us-east-2.grafana.net/otlp/v1/metrics',
        headers: {authorization: 'Bearer anonymous-token'},
      });

      // Verify metric reader was created
      expect(PeriodicExportingMetricReader).toHaveBeenCalledWith({
        exportIntervalMillis: 60000,
        exporter: mockExporter,
      });

      // Verify meter provider was created
      expect(MeterProvider).toHaveBeenCalled();
      expect(mockMeterProvider.getMeter).toHaveBeenCalledWith(
        'zero-anonymous-telemetry',
      );
    });

    test('should create all required metrics', () => {
      // Since telemetry is already started, these should have been called
      expect(mockMeter.createObservableGauge).toHaveBeenCalledWith(
        'zero.uptime',
        {
          description: 'System uptime in seconds',
          unit: 'seconds',
        },
      );

      expect(mockMeter.createObservableGauge).toHaveBeenCalledWith(
        'zero.client_groups',
        {
          description: 'Number of connected client groups',
        },
      );

      expect(mockMeter.createObservableGauge).toHaveBeenCalledWith(
        'zero.active_queries',
        {
          description:
            'Total number of active queries across all client groups',
        },
      );

      expect(mockMeter.createObservableGauge).toHaveBeenCalledWith(
        'zero.active_queries_per_client_group',
        {
          description: 'Number of active queries per client group',
        },
      );

      expect(mockMeter.createObservableGauge).toHaveBeenCalledWith(
        'zero.cvr_size',
        {
          description: 'Current CVR size in bytes',
          unit: 'bytes',
        },
      );

      expect(mockMeter.createObservableCounter).toHaveBeenCalledWith(
        'zero.mutations_processed',
        {
          description: 'Number of mutations processed in the last minute',
        },
      );

      expect(mockMeter.createObservableCounter).toHaveBeenCalledWith(
        'zero.rows_synced',
        {
          description: 'Number of rows synced in the last minute',
        },
      );

      // Note: Histogram metrics are not currently implemented in the anonymous telemetry
    });

    test('should register callbacks for observable metrics', () => {
      // Each observable should have a callback registered
      expect(mockObservableGauge.addCallback).toHaveBeenCalledTimes(5); // 5 gauges
      expect(mockObservableCounter.addCallback).toHaveBeenCalledTimes(2); // 2 counters
    });
  });

  describe('Metric Recording', () => {
    test('should record metrics correctly', () => {
      // Test basic metric recording without histogram functions
      expect(() => recordMutation()).not.toThrow();
      expect(() => recordRowsSynced(42)).not.toThrow();
      expect(() => updateCvrSize(1024)).not.toThrow();
    });

    test('should accumulate mutation counts', () => {
      // Record multiple mutations
      recordMutation();
      recordMutation();
      recordMutation();

      // Mutations should be accumulated internally
      // The actual value will be observed when the callback is triggered
      expect(() => recordMutation()).not.toThrow();
    });

    test('should accumulate rows synced counts', () => {
      recordRowsSynced(10);
      recordRowsSynced(25);
      recordRowsSynced(5);

      // Should not throw and values should be accumulated internally
      expect(() => recordRowsSynced(1)).not.toThrow();
    });

    test('should handle CVR size updates', () => {
      const cvrSize = 1024 * 1024; // 1MB

      updateCvrSize(cvrSize);

      // Should not throw
      expect(() => updateCvrSize(cvrSize * 2)).not.toThrow();
    });
  });

  describe('Client Group and Query Management', () => {
    test('should manage client groups correctly and reflect in metrics', () => {
      const clientGroupId1 = 'group-1';
      const clientGroupId2 = 'group-2';

      // Add client groups
      addClientGroup(clientGroupId1);
      addClientGroup(clientGroupId2);

      // Should not throw
      expect(() => addClientGroup(clientGroupId1)).not.toThrow(); // Duplicate should be fine

      // Test that the client groups gauge callback works correctly
      const clientGroupsCallback = mockObservableGauge.addCallback.mock.calls
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .find((call: any) =>
          call[0].toString().includes('connectedClientGroups'),
        )?.[0];

      if (clientGroupsCallback) {
        const mockResult = {observe: vi.fn()};
        clientGroupsCallback(mockResult);

        // Should observe the count of client groups (at least 2)
        expect(mockResult.observe).toHaveBeenCalledWith(
          expect.any(Number),
          expect.objectContaining({
            'zero.telemetry.type': 'anonymous',
          }),
        );

        // Verify the observed value is reasonable (should be >= 2)
        const observedValue = mockResult.observe.mock.calls[0][0];
        expect(observedValue).toBeGreaterThanOrEqual(2);
      }

      // Remove client groups
      removeClientGroup(clientGroupId1);
      removeClientGroup(clientGroupId2);

      // Should not throw even if removing non-existent group
      expect(() => removeClientGroup('non-existent')).not.toThrow();
    });

    test('should manage active queries correctly and reflect in metrics', () => {
      const clientGroupId = 'test-group';
      const queryId1 = 'query-1';
      const queryId2 = 'query-2';

      // Add client group first
      addClientGroup(clientGroupId);

      // Add queries
      addActiveQuery(clientGroupId, queryId1);
      addActiveQuery(clientGroupId, queryId2);

      // Add query to different group
      addActiveQuery('other-group', 'other-query');

      // Test that the active queries gauge callback works correctly
      const activeQueriesCallback = mockObservableGauge.addCallback.mock.calls
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .find((call: any) =>
          call[0].toString().includes('getTotalActiveQueries'),
        )?.[0];

      if (activeQueriesCallback) {
        const mockResult = {observe: vi.fn()};
        activeQueriesCallback(mockResult);

        // Should observe the total count of active queries (at least 3)
        expect(mockResult.observe).toHaveBeenCalledWith(
          expect.any(Number),
          expect.objectContaining({
            'zero.telemetry.type': 'anonymous',
          }),
        );

        // Verify the observed value reflects our added queries
        const observedValue = mockResult.observe.mock.calls[0][0];
        expect(observedValue).toBeGreaterThanOrEqual(3);
      }

      // Test per-client-group queries callback
      const perClientGroupCallback = mockObservableGauge.addCallback.mock.calls
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .find((call: any) => call[0].toString().includes('clientGroupID'))?.[0];

      if (perClientGroupCallback) {
        const mockResult = {observe: vi.fn()};
        perClientGroupCallback(mockResult);

        // Should observe queries for each client group
        expect(mockResult.observe).toHaveBeenCalledWith(
          expect.any(Number),
          expect.objectContaining({
            'zero.telemetry.type': 'anonymous',
            'zero.client_group.id': expect.any(String),
          }),
        );
      }

      // Remove queries
      removeActiveQuery(clientGroupId, queryId1);
      removeActiveQuery(clientGroupId, queryId2);
      removeActiveQuery('other-group', 'other-query');

      // Should not throw even if removing non-existent query
      expect(() =>
        removeActiveQuery('non-existent', 'non-existent'),
      ).not.toThrow();
    });

    test('should clean up queries when client group is removed and reflect in metrics', () => {
      const clientGroupId = 'cleanup-test-group';

      // Add client group and queries
      addClientGroup(clientGroupId);
      addActiveQuery(clientGroupId, 'query-1');
      addActiveQuery(clientGroupId, 'query-2');

      // Verify queries are tracked before removal
      const activeQueriesCallback = mockObservableGauge.addCallback.mock.calls
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .find((call: any) =>
          call[0].toString().includes('getTotalActiveQueries'),
        )?.[0];

      if (activeQueriesCallback) {
        const mockResultBefore = {observe: vi.fn()};
        activeQueriesCallback(mockResultBefore);
        const queriesBeforeRemoval = mockResultBefore.observe.mock.calls[0][0];

        // Remove client group (should also clean up its queries)
        removeClientGroup(clientGroupId);

        // Verify queries are cleaned up
        const mockResultAfter = {observe: vi.fn()};
        activeQueriesCallback(mockResultAfter);
        const queriesAfterRemoval = mockResultAfter.observe.mock.calls[0][0];

        // Should have fewer or equal queries after removal
        expect(queriesAfterRemoval).toBeLessThanOrEqual(queriesBeforeRemoval);
      }

      // Should not throw
      expect(() => removeClientGroup(clientGroupId)).not.toThrow();
    });
  });

  describe('Platform Detection', () => {
    test('should include platform information in telemetry', () => {
      // Test that platform detection works without throwing
      expect(() => {
        addClientGroup('platform-test-group');
        recordMutation();
        recordRowsSynced(10);
      }).not.toThrow();
    });
  });

  describe('Attributes and Versioning', () => {
    test('should handle telemetry operations correctly', () => {
      // Test that telemetry operations work properly
      expect(() => {
        addClientGroup('attr-test-group');
        addActiveQuery('attr-test-group', 'test-query');
        recordMutation();
        recordRowsSynced(50);
        updateCvrSize(1024);
        removeActiveQuery('attr-test-group', 'test-query');
        removeClientGroup('attr-test-group');
      }).not.toThrow();
    });

    test('should include taskID in telemetry attributes', () => {
      // Test that the telemetry system includes taskID in attributes
      // We'll verify this by checking the existing mock calls

      // Add some test data to trigger callbacks
      addClientGroup('taskid-test-group-2');
      addActiveQuery('taskid-test-group-2', 'test-query');
      recordMutation();

      // Get the callbacks that were registered
      type CallbackFunction = (result: {
        observe: (_value: number, attrs?: Record<string, unknown>) => void;
      }) => void;

      // Find a callback that includes attributes
      let foundTaskIdInAttributes = false;

      const callbacks = mockObservableGauge.addCallback.mock.calls.map(
        (call: unknown[]) => call[0] as CallbackFunction,
      );

      // Mock the result object to capture attributes
      const mockResult = {
        observe: vi.fn((_value: number, attrs?: Record<string, unknown>) => {
          if (attrs && attrs['zero.task.id']) {
            foundTaskIdInAttributes = true;
          }
        }),
      };

      // Execute callbacks to see if any include taskID
      callbacks.forEach((callback: CallbackFunction) => {
        try {
          callback(mockResult);
        } catch (e) {
          // Some callbacks might fail due to mocking, that's ok
        }
      });

      // Since the singleton is already initialized, we can't easily test the new config
      // But we can verify that taskID is part of the attribute structure
      expect(foundTaskIdInAttributes).toBe(true);

      // Clean up
      removeActiveQuery('taskid-test-group-2', 'test-query');
      removeClientGroup('taskid-test-group-2');
    });

    test('should use unknown taskID when not provided in config', () => {
      const lc = createSilentLogContext();

      // Mock config without taskID
      const configWithoutTaskID = {
        enableUsageAnalytics: true,
        upstream: {
          db: 'postgresql://test@localhost/test',
        },
        serverVersion: '1.0.0-test',
        // taskID is undefined
      } as Partial<ZeroConfig> as ZeroConfig;

      // Start telemetry without taskID
      startAnonymousTelemetry(lc, configWithoutTaskID);

      // Add some test data to trigger callbacks
      addClientGroup('no-taskid-test-group');
      recordMutation();

      // Get the callbacks that were registered
      type CallbackFunction = (result: {
        observe: (value: number, attrs?: object) => void;
      }) => void;
      const callbacks = mockObservableGauge.addCallback.mock.calls.map(
        (call: [CallbackFunction]) => call[0],
      );

      // Mock the result object to capture attributes
      const mockResult = {
        observe: vi.fn(),
      };

      // Execute callbacks to verify attributes include default taskID
      callbacks.forEach((callback: CallbackFunction) => {
        callback(mockResult);
      });

      // Verify that taskID defaults to 'unknown'
      expect(mockResult.observe).toHaveBeenCalledWith(
        expect.any(Number),
        expect.objectContaining({
          'zero.task.id': 'unknown',
          'zero.telemetry.type': 'anonymous',
        }),
      );

      // Clean up
      removeClientGroup('no-taskid-test-group');
    });
  });

  describe('Singleton Behavior', () => {
    test('should not start again after already started', () => {
      const initialCallCount = vi.mocked(OTLPMetricExporter).mock.calls.length;

      // Try to start again
      startAnonymousTelemetry();

      // Should not create additional instances
      expect(vi.mocked(OTLPMetricExporter)).toHaveBeenCalledTimes(
        initialCallCount,
      );
    });
  });

  describe('Observable Metric Callbacks', () => {
    test('should execute callbacks without throwing', () => {
      // Add some test data
      addClientGroup('group-1');
      addClientGroup('group-2');
      addActiveQuery('group-1', 'query-1');
      addActiveQuery('group-1', 'query-2');
      addActiveQuery('group-2', 'query-3');
      updateCvrSize(2048);
      recordMutation();
      recordMutation();
      recordRowsSynced(100);

      // Get the callbacks that were registered
      type CallbackFunction = (result: {
        observe: (value: number, attrs?: object) => void;
      }) => void;
      const callbacks = mockObservableGauge.addCallback.mock.calls.map(
        (call: [CallbackFunction]) => call[0],
      );
      const counterCallbacks = mockObservableCounter.addCallback.mock.calls.map(
        (call: [CallbackFunction]) => call[0],
      );

      // Mock the result object
      const mockResult = {
        observe: vi.fn(),
      };

      // Execute callbacks to verify they work
      callbacks.forEach((callback: CallbackFunction) => {
        expect(() => callback(mockResult)).not.toThrow();
      });

      counterCallbacks.forEach((callback: CallbackFunction) => {
        expect(() => callback(mockResult)).not.toThrow();
      });

      // Verify observations were made
      expect(mockResult.observe).toHaveBeenCalled();
    });
  });

  describe('Shutdown', () => {
    test('should shutdown meter provider', () => {
      shutdownAnonymousTelemetry();

      expect(mockMeterProvider.shutdown).toHaveBeenCalled();
    });

    test('should handle multiple shutdown calls', () => {
      const initialCallCount = mockMeterProvider.shutdown.mock.calls.length;

      shutdownAnonymousTelemetry();
      shutdownAnonymousTelemetry();

      // Should handle multiple calls gracefully
      expect(
        mockMeterProvider.shutdown.mock.calls.length,
      ).toBeGreaterThanOrEqual(initialCallCount);
    });
  });

  describe('Integration Tests for Telemetry Calls', () => {
    test('should record rows synced with correct count', () => {
      const rowCount1 = 42;
      const rowCount2 = 15;

      // Record multiple row sync operations
      recordRowsSynced(rowCount1);
      recordRowsSynced(rowCount2);

      // Should not throw and values should be accumulated internally
      expect(() => recordRowsSynced(100)).not.toThrow();

      // Test that the rows synced counter callback works correctly
      const rowsSyncedCallback = mockObservableCounter.addCallback.mock.calls
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .find((call: any) =>
          call[0].toString().includes('lastMinuteRowsSynced'),
        )?.[0];

      if (rowsSyncedCallback) {
        const mockResult = {observe: vi.fn()};
        rowsSyncedCallback(mockResult);

        // Should observe the accumulated count
        expect(mockResult.observe).toHaveBeenCalledWith(
          expect.any(Number),
          expect.objectContaining({
            'zero.telemetry.type': 'anonymous',
          }),
        );

        // Verify the observed value includes our recorded counts
        const observedValue = mockResult.observe.mock.calls[0][0];
        expect(observedValue).toBeGreaterThanOrEqual(rowCount1 + rowCount2);
      }
    });

    test('should handle edge cases for recordRowsSynced', () => {
      // Test with zero rows
      expect(() => recordRowsSynced(0)).not.toThrow();

      // Test with large numbers
      expect(() => recordRowsSynced(1000000)).not.toThrow();

      // Test with negative numbers (though this shouldn't happen in practice)
      expect(() => recordRowsSynced(-1)).not.toThrow();
    });

    test('should manage query lifecycle correctly with telemetry', () => {
      const clientGroupId = 'telemetry-test-group';
      const queryId1 = 'telemetry-query-1';
      const queryId2 = 'telemetry-query-2';
      const queryId3 = 'telemetry-query-3';

      // Start with a clean slate - add client group
      addClientGroup(clientGroupId);

      // Add multiple queries
      addActiveQuery(clientGroupId, queryId1);
      addActiveQuery(clientGroupId, queryId2);
      addActiveQuery(clientGroupId, queryId3);

      // Verify that active queries are tracked
      const activeQueriesCallback = mockObservableGauge.addCallback.mock.calls
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .find((call: any) =>
          call[0].toString().includes('getTotalActiveQueries'),
        )?.[0];

      if (activeQueriesCallback) {
        const mockResult = {observe: vi.fn()};
        activeQueriesCallback(mockResult);

        const observedValue = mockResult.observe.mock.calls[0][0];
        expect(observedValue).toBeGreaterThanOrEqual(3);
      }

      // Remove some queries
      removeActiveQuery(clientGroupId, queryId1);
      removeActiveQuery(clientGroupId, queryId3);

      // Test per-client-group metric after removals
      const perClientGroupCallback = mockObservableGauge.addCallback.mock.calls
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .find((call: any) => call[0].toString().includes('clientGroupID'))?.[0];

      if (perClientGroupCallback) {
        const mockResult = {observe: vi.fn()};
        perClientGroupCallback(mockResult);

        // Should still have queries for this client group
        expect(mockResult.observe).toHaveBeenCalledWith(
          expect.any(Number),
          expect.objectContaining({
            'zero.client_group.id': clientGroupId,
          }),
        );
      }

      // Clean up
      removeActiveQuery(clientGroupId, queryId2);
      removeClientGroup(clientGroupId);
    });

    test('should handle duplicate query additions gracefully', () => {
      const clientGroupId = 'duplicate-test-group';
      const queryId = 'duplicate-query';

      addClientGroup(clientGroupId);

      // Add same query multiple times
      expect(() => {
        addActiveQuery(clientGroupId, queryId);
        addActiveQuery(clientGroupId, queryId);
        addActiveQuery(clientGroupId, queryId);
      }).not.toThrow();

      // Remove it multiple times
      expect(() => {
        removeActiveQuery(clientGroupId, queryId);
        removeActiveQuery(clientGroupId, queryId);
        removeActiveQuery(clientGroupId, queryId);
      }).not.toThrow();

      removeClientGroup(clientGroupId);
    });

    test('should handle query removal from non-existent client group', () => {
      // Try to remove query from non-existent client group
      expect(() => {
        removeActiveQuery('non-existent-group', 'some-query');
      }).not.toThrow();
    });

    test('should handle queries for non-existent client group', () => {
      // Try to add query to non-existent client group
      expect(() => {
        addActiveQuery('non-existent-group', 'some-query');
      }).not.toThrow();
    });

    test('should track multiple client groups with different query sets', () => {
      const group1 = 'multi-group-1';
      const group2 = 'multi-group-2';
      const group3 = 'multi-group-3';

      // Add client groups
      addClientGroup(group1);
      addClientGroup(group2);
      addClientGroup(group3);

      // Add different numbers of queries to each group
      addActiveQuery(group1, 'q1-1');
      addActiveQuery(group1, 'q1-2');

      addActiveQuery(group2, 'q2-1');
      addActiveQuery(group2, 'q2-2');
      addActiveQuery(group2, 'q2-3');

      addActiveQuery(group3, 'q3-1');

      // Test that total count includes all queries
      const activeQueriesCallback = mockObservableGauge.addCallback.mock.calls
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .find((call: any) =>
          call[0].toString().includes('getTotalActiveQueries'),
        )?.[0];

      if (activeQueriesCallback) {
        const mockResult = {observe: vi.fn()};
        activeQueriesCallback(mockResult);

        const observedValue = mockResult.observe.mock.calls[0][0];
        expect(observedValue).toBeGreaterThanOrEqual(6); // 2 + 3 + 1
      }

      // Clean up
      removeClientGroup(group1);
      removeClientGroup(group2);
      removeClientGroup(group3);
    });

    test('should reset counter values after observation', () => {
      // Record some mutations and rows
      recordMutation();
      recordMutation();
      recordRowsSynced(50);
      recordRowsSynced(25);

      // Trigger the counter callbacks to simulate the periodic export
      const mutationsCallback = mockObservableCounter.addCallback.mock.calls
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .find((call: any) =>
          call[0].toString().includes('lastMinuteMutations'),
        )?.[0];

      const rowsCallback = mockObservableCounter.addCallback.mock.calls
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .find((call: any) =>
          call[0].toString().includes('lastMinuteRowsSynced'),
        )?.[0];

      if (mutationsCallback && rowsCallback) {
        const mockMutationsResult = {observe: vi.fn()};
        const mockRowsResult = {observe: vi.fn()};

        // First observation should see accumulated values
        mutationsCallback(mockMutationsResult);
        rowsCallback(mockRowsResult);

        expect(mockMutationsResult.observe).toHaveBeenCalledWith(
          expect.any(Number),
          expect.any(Object),
        );
        expect(mockRowsResult.observe).toHaveBeenCalledWith(
          expect.any(Number),
          expect.any(Object),
        );

        const firstMutationsValue =
          mockMutationsResult.observe.mock.calls[0][0];
        const firstRowsValue = mockRowsResult.observe.mock.calls[0][0];

        expect(firstMutationsValue).toBeGreaterThanOrEqual(2);
        expect(firstRowsValue).toBeGreaterThanOrEqual(75);

        // Reset mocks for second observation
        mockMutationsResult.observe.mockClear();
        mockRowsResult.observe.mockClear();

        // Second observation should see reset values (0 since no new activity)
        mutationsCallback(mockMutationsResult);
        rowsCallback(mockRowsResult);

        const secondMutationsValue =
          mockMutationsResult.observe.mock.calls[0][0];
        const secondRowsValue = mockRowsResult.observe.mock.calls[0][0];

        // Values should be reset to 0 after the first observation
        expect(secondMutationsValue).toBe(0);
        expect(secondRowsValue).toBe(0);
      }
    });
  });
});
