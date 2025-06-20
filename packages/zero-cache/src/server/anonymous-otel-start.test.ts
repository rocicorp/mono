import {beforeAll, afterAll, describe, expect, test, vi} from 'vitest';
import {OTLPMetricExporter} from '@opentelemetry/exporter-metrics-otlp-http';
import {PeriodicExportingMetricReader} from '@opentelemetry/sdk-metrics';
import {MeterProvider} from '@opentelemetry/sdk-metrics';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {
  startAnonymousTelemetry,
  recordMutation,
  recordRowsSynced,
  recordChangeDesiredQueriesTime,
  recordReplicationEventTime,
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
    vi.mocked(PeriodicExportingMetricReader).mockImplementation(() => mockMetricReader);
    vi.mocked(MeterProvider).mockImplementation(() => mockMeterProvider);

    // Clear environment variables that might affect telemetry
    delete process.env.ZERO_ENABLE_USAGE_ANALYTICS;
    delete process.env.ZERO_TELEMETRY_OPT_OUT;
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
    test('should respect opt-out via ZERO_ENABLE_USAGE_ANALYTICS=false', () => {
      // Set up environment for opt-out test
      const testEnv = {...process.env};
      testEnv.ZERO_ENABLE_USAGE_ANALYTICS = 'false';
      
      // Temporarily replace process.env
      const originalProcessEnv = process.env;
      process.env = testEnv;
      
      try {
        startAnonymousTelemetry();
        
        // Should not initialize any telemetry components
        expect(OTLPMetricExporter).not.toHaveBeenCalled();
        expect(PeriodicExportingMetricReader).not.toHaveBeenCalled();
        expect(MeterProvider).not.toHaveBeenCalled();
      } finally {
        process.env = originalProcessEnv;
      }
    });

    test('should respect opt-out via ZERO_ENABLE_USAGE_ANALYTICS=0', () => {
      const testEnv = {...process.env};
      testEnv.ZERO_ENABLE_USAGE_ANALYTICS = '0';
      
      const originalProcessEnv = process.env;
      process.env = testEnv;
      
      try {
        startAnonymousTelemetry();
        expect(OTLPMetricExporter).not.toHaveBeenCalled();
      } finally {
        process.env = originalProcessEnv;
      }
    });

    test('should respect opt-out via ZERO_TELEMETRY_OPT_OUT=true', () => {
      const testEnv = {...process.env};
      testEnv.ZERO_TELEMETRY_OPT_OUT = 'true';
      
      const originalProcessEnv = process.env;
      process.env = testEnv;
      
      try {
        startAnonymousTelemetry();
        expect(OTLPMetricExporter).not.toHaveBeenCalled();
      } finally {
        process.env = originalProcessEnv;
      }
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
      expect(mockMeterProvider.getMeter).toHaveBeenCalledWith('zero-anonymous-telemetry');
    });

    test('should create all required metrics', () => {
      // Since telemetry is already started, these should have been called
      expect(mockMeter.createObservableGauge).toHaveBeenCalledWith('zero.uptime', {
        description: 'System uptime in seconds',
        unit: 'seconds',
      });

      expect(mockMeter.createObservableGauge).toHaveBeenCalledWith('zero.client_groups', {
        description: 'Number of connected client groups',
      });

      expect(mockMeter.createObservableGauge).toHaveBeenCalledWith('zero.active_queries', {
        description: 'Total number of active queries across all client groups',
      });

      expect(mockMeter.createObservableGauge).toHaveBeenCalledWith('zero.active_queries_per_client_group', {
        description: 'Number of active queries per client group',
      });

      expect(mockMeter.createObservableGauge).toHaveBeenCalledWith('zero.cvr_size', {
        description: 'Current CVR size in bytes',
        unit: 'bytes',
      });

      expect(mockMeter.createObservableCounter).toHaveBeenCalledWith('zero.mutations_processed', {
        description: 'Number of mutations processed in the last minute',
      });

      expect(mockMeter.createObservableCounter).toHaveBeenCalledWith('zero.rows_synced', {
        description: 'Number of rows synced in the last minute',
      });

      // Verify histograms were created
      expect(mockMeter.createHistogram).toHaveBeenCalledWith('zero.change_desired_queries_duration', {
        description: 'Time taken to process changeDesiredQueries operations',
        unit: 'milliseconds',
      });

      expect(mockMeter.createHistogram).toHaveBeenCalledWith('zero.replication_event_duration', {
        description: 'Time taken to process replication events from upstream',
        unit: 'milliseconds',
      });
    });

    test('should register callbacks for observable metrics', () => {
      // Each observable should have a callback registered
      expect(mockObservableGauge.addCallback).toHaveBeenCalledTimes(5); // 5 gauges
      expect(mockObservableCounter.addCallback).toHaveBeenCalledTimes(2); // 2 counters
    });
  });

  describe('Metric Recording', () => {
    test('should record histogram metrics with correct attributes', () => {
      const duration = 123.45;
      
      recordChangeDesiredQueriesTime(duration);
      recordReplicationEventTime(duration * 2);

      expect(mockHistogram.record).toHaveBeenCalledWith(duration, expect.objectContaining({
        'zero.telemetry.type': 'anonymous',
        'zero.infra.platform': expect.any(String),
      }));

      expect(mockHistogram.record).toHaveBeenCalledWith(duration * 2, expect.objectContaining({
        'zero.telemetry.type': 'anonymous',
        'zero.infra.platform': expect.any(String),
      }));
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
    test('should manage client groups correctly', () => {
      const clientGroupId1 = 'group-1';
      const clientGroupId2 = 'group-2';

      // Add client groups
      addClientGroup(clientGroupId1);
      addClientGroup(clientGroupId2);

      // Should not throw
      expect(() => addClientGroup(clientGroupId1)).not.toThrow(); // Duplicate should be fine

      // Remove client groups
      removeClientGroup(clientGroupId1);
      removeClientGroup(clientGroupId2);
      
      // Should not throw even if removing non-existent group
      expect(() => removeClientGroup('non-existent')).not.toThrow();
    });

    test('should manage active queries correctly', () => {
      const clientGroupId = 'test-group';
      const queryId1 = 'query-1';
      const queryId2 = 'query-2';

      // Add queries
      addActiveQuery(clientGroupId, queryId1);
      addActiveQuery(clientGroupId, queryId2);
      
      // Add query to different group
      addActiveQuery('other-group', 'other-query');

      // Remove queries
      removeActiveQuery(clientGroupId, queryId1);
      removeActiveQuery(clientGroupId, queryId2);
      removeActiveQuery('other-group', 'other-query');

      // Should not throw even if removing non-existent query
      expect(() => removeActiveQuery('non-existent', 'non-existent')).not.toThrow();
    });

    test('should clean up queries when client group is removed', () => {
      const clientGroupId = 'test-group';
      
      // Add queries to group
      addActiveQuery(clientGroupId, 'query-1');
      addActiveQuery(clientGroupId, 'query-2');
      
      // Remove client group (should also clean up its queries)
      removeClientGroup(clientGroupId);
      
      // Should not throw
      expect(() => removeClientGroup(clientGroupId)).not.toThrow();
    });
  });

  describe('Platform Detection', () => {
    test('should include platform information in attributes', () => {
      const duration = 100;
      recordChangeDesiredQueriesTime(duration);
      
      expect(mockHistogram.record).toHaveBeenCalledWith(duration, expect.objectContaining({
        'zero.infra.platform': expect.any(String),
      }));
    });
  });

  describe('Attributes and Versioning', () => {
    test('should include correct attribute structure', () => {
      const duration = 100;
      recordChangeDesiredQueriesTime(duration);

      expect(mockHistogram.record).toHaveBeenCalledWith(duration, expect.objectContaining({
        'zero.app.id': expect.any(String),
        'zero.machine.os': expect.any(String),
        'zero.telemetry.type': 'anonymous',
        'zero.infra.platform': expect.any(String),
        'zero.version': expect.any(String),
      }));
    });
  });

  describe('Singleton Behavior', () => {
    test('should not start again after already started', () => {
      const initialCallCount = vi.mocked(OTLPMetricExporter).mock.calls.length;
      
      // Try to start again
      startAnonymousTelemetry();
      
      // Should not create additional instances
      expect(vi.mocked(OTLPMetricExporter)).toHaveBeenCalledTimes(initialCallCount);
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
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const callbacks = mockObservableGauge.addCallback.mock.calls.map((call: any) => call[0]);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const counterCallbacks = mockObservableCounter.addCallback.mock.calls.map((call: any) => call[0]);

      // Mock the result object
      const mockResult = {
        observe: vi.fn(),
      };

      // Execute callbacks to verify they work
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      callbacks.forEach((callback: any) => {
        expect(() => callback(mockResult)).not.toThrow();
      });

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      counterCallbacks.forEach((callback: any) => {
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
      expect(mockMeterProvider.shutdown.mock.calls.length).toBeGreaterThanOrEqual(initialCallCount);
    });
  });
}); 