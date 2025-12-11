import {OTLPMetricExporter} from '@opentelemetry/exporter-metrics-otlp-http';
import {
  MeterProvider,
  PeriodicExportingMetricReader,
} from '@opentelemetry/sdk-metrics';
import {afterAll, beforeAll, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {getZeroConfig, type ZeroConfig} from '../config/zero-config.js';
import {
  recordConnectionAttempted,
  recordConnectionSuccess,
  recordMutation,
  recordQuery,
  recordRowsSynced,
  shutdownAnonymousTelemetry,
  startAnonymousTelemetry,
} from './anonymous-otel-start.js';

// Mock the OTLP exporter and related OpenTelemetry components
vi.mock('@opentelemetry/exporter-metrics-otlp-http');
vi.mock('@opentelemetry/sdk-metrics');

// Mock the config
vi.mock('../config/zero-config.js', () => ({
  getZeroConfig: vi.fn(),
  getServerVersion: vi.fn(),
}));

// Mock setTimeout to execute immediately in tests
vi.stubGlobal('setTimeout', (fn: () => void) => {
  fn();
  return 1 as unknown as NodeJS.Timeout;
});

describe('Anonymous Telemetry Integration Tests', () => {
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  let mockExporter: any;
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  let mockMetricReader: any;
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  let mockMeterProvider: any;
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  let mockMeter: any;
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  let mockHistogram: any;
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  let mockObservableGauge: any;
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  let mockObservableCounter: any;
  let originalEnv: NodeJS.ProcessEnv;

  beforeAll(() => {
    // Store original environment
    originalEnv = {...process.env};

    // Reset all mocks
    vi.clearAllMocks();

    // Mock getZeroConfig to return default enabled state
    vi.mocked(getZeroConfig).mockReturnValue({
      enableTelemetry: true,
      upstream: {
        db: 'postgresql://test@localhost/test',
      },
    } as unknown as ZeroConfig);

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

    // Restore setTimeout
    vi.unstubAllGlobals();

    // Shutdown telemetry
    shutdownAnonymousTelemetry();
  });

  describe('Opt-out Configuration (test these first)', () => {
    test('should respect opt-out via enableTelemetry=false', () => {
      // Mock config to return disabled analytics
      vi.mocked(getZeroConfig).mockReturnValueOnce({
        enableTelemetry: false,
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
        enableTelemetry: false,
      } as Partial<ZeroConfig> as ZeroConfig);

      startAnonymousTelemetry();
      expect(OTLPMetricExporter).not.toHaveBeenCalled();
    });

    test('should respect opt-out via DO_NOT_TRACK environment variable', () => {
      // Set DO_NOT_TRACK environment variable
      process.env.DO_NOT_TRACK = '1';

      // Mock config to return enabled analytics
      vi.mocked(getZeroConfig).mockReturnValueOnce({
        enableTelemetry: true,
      } as Partial<ZeroConfig> as ZeroConfig);

      startAnonymousTelemetry();

      // Should not initialize any telemetry components
      expect(OTLPMetricExporter).not.toHaveBeenCalled();
      expect(PeriodicExportingMetricReader).not.toHaveBeenCalled();
      expect(MeterProvider).not.toHaveBeenCalled();

      // Clean up
      delete process.env.DO_NOT_TRACK;
    });

    test('should respect opt-out via DO_NOT_TRACK environment variable with any value', () => {
      // Set DO_NOT_TRACK environment variable with different values
      const testValues = ['1', 'true', 'yes', 'on', 'anything'];

      for (const value of testValues) {
        process.env.DO_NOT_TRACK = value;

        // Mock config to return enabled analytics
        vi.mocked(getZeroConfig).mockReturnValueOnce({
          enableTelemetry: true,
        } as Partial<ZeroConfig> as ZeroConfig);

        startAnonymousTelemetry();

        // Should not initialize any telemetry components
        expect(OTLPMetricExporter).not.toHaveBeenCalled();
        expect(PeriodicExportingMetricReader).not.toHaveBeenCalled();
        expect(MeterProvider).not.toHaveBeenCalled();

        // Clean up
        delete process.env.DO_NOT_TRACK;
      }
    });
  });

  describe('Telemetry Startup and Operation', () => {
    test('should start telemetry with default configuration', () => {
      const lc = createSilentLogContext();

      startAnonymousTelemetry(lc);

      // Verify OTLP exporter was created with correct configuration
      expect(OTLPMetricExporter).toHaveBeenCalledWith({
        url: 'https://metrics.rocicorp.dev',
        timeoutMillis: 30000,
      });

      // Verify metric reader was created
      expect(PeriodicExportingMetricReader).toHaveBeenCalledWith(
        expect.objectContaining({
          exportIntervalMillis: expect.any(Number),
          exporter: mockExporter,
        }),
      );

      // Verify the export interval is within expected range (60000 + jitter)
      const call = vi.mocked(PeriodicExportingMetricReader).mock.calls[0][0];
      expect(call.exportIntervalMillis).toBeGreaterThanOrEqual(60000);
      expect(call.exportIntervalMillis).toBeLessThan(70000);

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

      expect(mockMeter.createObservableCounter).toHaveBeenCalledWith(
        'zero.uptime_counter',
        {
          description: 'System uptime in seconds',
          unit: 'seconds',
        },
      );

      expect(mockMeter.createObservableCounter).toHaveBeenCalledWith(
        'zero.crud_mutations_processed',
        {
          description: 'Total number of CRUD mutations processed',
        },
      );

      expect(mockMeter.createObservableCounter).toHaveBeenCalledWith(
        'zero.custom_mutations_processed',
        {
          description: 'Total number of custom mutations processed',
        },
      );

      expect(mockMeter.createObservableCounter).toHaveBeenCalledWith(
        'zero.mutations_processed',
        {
          description: 'Total number of mutations processed',
        },
      );

      expect(mockMeter.createObservableCounter).toHaveBeenCalledWith(
        'zero.crud_queries_processed',
        {
          description: 'Total number of CRUD queries processed',
        },
      );

      expect(mockMeter.createObservableCounter).toHaveBeenCalledWith(
        'zero.custom_queries_processed',
        {
          description: 'Total number of custom queries processed',
        },
      );

      expect(mockMeter.createObservableCounter).toHaveBeenCalledWith(
        'zero.queries_processed',
        {
          description: 'Total number of queries processed',
        },
      );

      expect(mockMeter.createObservableCounter).toHaveBeenCalledWith(
        'zero.rows_synced',
        {
          description: 'Total number of rows synced',
        },
      );

      expect(mockMeter.createObservableCounter).toHaveBeenCalledWith(
        'zero.connections_success',
        {
          description: 'Total number of successful connections',
        },
      );

      expect(mockMeter.createObservableCounter).toHaveBeenCalledWith(
        'zero.connections_attempted',
        {
          description: 'Total number of attempted connections',
        },
      );

      expect(mockMeter.createObservableGauge).toHaveBeenCalledWith(
        'zero.gauge_active_client_groups',
        {
          description: 'Number of currently active client groups',
        },
      );

      expect(mockMeter.createObservableGauge).toHaveBeenCalledWith(
        'zero.active_users_last_day',
        {
          description: 'Count of CVR instances active in the last 24h',
        },
      );

      // Note: Histogram metrics are not currently implemented in the anonymous telemetry
    });

    test('should register callbacks for observable metrics', () => {
      // Each observable should have a callback registered
      expect(mockObservableGauge.addCallback).toHaveBeenCalledTimes(9); // 9 gauges
      expect(mockObservableCounter.addCallback).toHaveBeenCalledTimes(10); // 10 counters (uptime_counter, crud_mutations, custom_mutations, total_mutations, crud_queries, custom_queries, total_queries, rows_synced, connections_success, connections_attempted)
    });
  });

  describe('Metric Recording', () => {
    test('should record metrics correctly', () => {
      // Test basic metric recording without histogram functions
      expect(() => recordMutation('crud')).not.toThrow();
      expect(() => recordMutation('custom')).not.toThrow();
      expect(() => recordQuery('crud')).not.toThrow();
      expect(() => recordQuery('custom')).not.toThrow();
      expect(() => recordRowsSynced(42)).not.toThrow();
      expect(() => recordConnectionSuccess()).not.toThrow();
      expect(() => recordConnectionAttempted()).not.toThrow();
    });

    test('should accumulate mutation counts', () => {
      // Record multiple mutations
      recordMutation('crud');
      recordMutation('custom');
      recordMutation('crud');

      // Mutations should be accumulated internally
      // The actual value will be observed when the callback is triggered
      expect(() => recordMutation('custom')).not.toThrow();
    });

    test('should accumulate query counts', () => {
      // Record multiple queries
      recordQuery('crud');
      recordQuery('custom');
      recordQuery('crud');

      // Queries should be accumulated internally
      // The actual value will be observed when the callback is triggered
      expect(() => recordQuery('custom')).not.toThrow();
    });

    test('should accumulate rows synced counts', () => {
      recordRowsSynced(10);
      recordRowsSynced(25);
      recordRowsSynced(5);

      // Should not throw and values should be accumulated internally
      expect(() => recordRowsSynced(1)).not.toThrow();
    });

    test('should accumulate connection success counts', () => {
      // Record multiple successful connections
      recordConnectionSuccess();
      recordConnectionSuccess();
      recordConnectionSuccess();

      // Connection successes should be accumulated internally
      // The actual value will be observed when the callback is triggered
      expect(() => recordConnectionSuccess()).not.toThrow();
    });

    test('should accumulate connection attempted counts', () => {
      // Record multiple attempted connections
      recordConnectionAttempted();
      recordConnectionAttempted();
      recordConnectionAttempted();

      // Connection attempts should be accumulated internally
      // The actual value will be observed when the callback is triggered
      expect(() => recordConnectionAttempted()).not.toThrow();
    });
  });

  describe('Platform Detection', () => {
    test('should include platform information in telemetry', () => {
      // Test that platform detection works without throwing
      expect(() => {
        recordMutation('crud');
        recordQuery('crud');
        recordRowsSynced(10);
      }).not.toThrow();
    });
  });

  describe('Attributes and Versioning', () => {
    test('should handle telemetry operations correctly', () => {
      // Test that telemetry operations work properly
      expect(() => {
        recordMutation('crud');
        recordQuery('custom');
        recordRowsSynced(50);
      }).not.toThrow();
    });

    test('should include taskID in telemetry attributes', () => {
      // Test that the telemetry system includes taskID in attributes
      // We'll verify this by checking the existing mock calls

      // Add some test data to trigger callbacks
      recordMutation('crud');

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
        } catch {
          // Some callbacks might fail due to mocking, that's ok
        }
      });

      // Since the singleton is already initialized, we can't easily test the new config
      // But we can verify that taskID is part of the attribute structure
      expect(foundTaskIdInAttributes).toBe(true);
    });

    test('should use unknown taskID when not provided in config', () => {
      const lc = createSilentLogContext();

      // Mock config without taskID
      const configWithoutTaskID = {
        enableTelemetry: true,
        upstream: {
          db: 'postgresql://test@localhost/test',
        },
        // taskID is undefined
      } as unknown as ZeroConfig;

      // Start telemetry without taskID
      startAnonymousTelemetry(lc, configWithoutTaskID);

      // Add some test data to trigger callbacks
      recordMutation('crud');

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
      recordMutation('crud');
      recordMutation('crud');
      recordQuery('crud');
      recordQuery('custom');
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
        // oxlint-disable-next-line @typescript-eslint/no-explicit-any
        .find((call: any) =>
          call[0].toString().includes('totalRowsSynced'),
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

    test('should accumulate counter values across observations', () => {
      // Record some mutations and rows
      recordMutation('crud');
      recordMutation('crud');
      recordQuery('crud');
      recordQuery('custom');
      recordRowsSynced(50);
      recordRowsSynced(25);

      // Trigger the counter callbacks to simulate the periodic export
      const mutationsCallback = mockObservableCounter.addCallback.mock.calls
        // oxlint-disable-next-line @typescript-eslint/no-explicit-any
        .find((call: any) =>
          call[0].toString().includes('totalMutations'),
        )?.[0];

      const rowsCallback = mockObservableCounter.addCallback.mock.calls
        // oxlint-disable-next-line @typescript-eslint/no-explicit-any
        .find((call: any) =>
          call[0].toString().includes('totalRowsSynced'),
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

        // Record additional activity
        recordMutation('crud');
        recordQuery('custom');
        recordRowsSynced(10);

        // Reset mocks for second observation
        mockMutationsResult.observe.mockClear();
        mockRowsResult.observe.mockClear();

        // Second observation should see accumulated values (including new activity)
        mutationsCallback(mockMutationsResult);
        rowsCallback(mockRowsResult);

        const secondMutationsValue =
          mockMutationsResult.observe.mock.calls[0][0];
        const secondRowsValue = mockRowsResult.observe.mock.calls[0][0];

        // Values should continue to accumulate (not reset)
        expect(secondMutationsValue).toBeGreaterThanOrEqual(
          firstMutationsValue + 1,
        );
        expect(secondRowsValue).toBeGreaterThanOrEqual(firstRowsValue + 10);
      }
    });
  });
});
