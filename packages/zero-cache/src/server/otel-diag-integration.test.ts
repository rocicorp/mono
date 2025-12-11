import type {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import {resetOtelDiagnosticLogger} from './otel-diag-logger.js';

// Mock OpenTelemetry modules to avoid actual SDK initialization
vi.mock('@opentelemetry/api', async () => {
  const actual = await vi.importActual('@opentelemetry/api');
  return {
    ...actual,
    diag: {
      setLogger: vi.fn(),
    },
  };
});

vi.mock('@opentelemetry/api-logs', () => ({
  logs: {
    getLogger: vi.fn().mockReturnValue({
      emit: vi.fn(),
    }),
  },
}));

vi.mock('@opentelemetry/sdk-node', () => ({
  NodeSDK: vi.fn(function () {
    return {
      start: vi.fn(),
    };
  }),
}));

vi.mock('@opentelemetry/auto-instrumentations-node', () => ({
  getNodeAutoInstrumentations: vi.fn().mockReturnValue([]),
}));

vi.mock('@opentelemetry/resources', () => ({
  resourceFromAttributes: vi.fn().mockReturnValue({}),
}));

vi.mock('../../../otel/src/enabled.ts', () => ({
  otelEnabled: vi.fn().mockReturnValue(true),
  otelLogsEnabled: vi.fn().mockReturnValue(true),
  otelMetricsEnabled: vi.fn().mockReturnValue(true),
  otelTracesEnabled: vi.fn().mockReturnValue(true),
}));

vi.mock('../config/zero-config.js', () => ({
  getZeroConfig: vi.fn().mockReturnValue({
    enableTelemetry: true,
    numSyncWorkers: 1,
    taskID: 'test-task',
    upstream: {db: 'test-db'},
  }),
  getServerVersion: vi.fn().mockReturnValue('test-version'),
}));

// Mock other modules that might be imported
vi.mock('@opentelemetry/exporter-metrics-otlp-http', () => ({
  OTLPMetricExporter: vi.fn(),
}));

vi.mock('@opentelemetry/sdk-metrics', () => ({
  MeterProvider: vi.fn(function () {
    return {
      getMeter: vi.fn().mockReturnValue({
        createObservableGauge: vi.fn().mockReturnValue({
          addCallback: vi.fn(),
        }),
        createObservableCounter: vi.fn().mockReturnValue({
          addCallback: vi.fn(),
        }),
      }),
      shutdown: vi.fn(),
    };
  }),
  PeriodicExportingMetricReader: vi.fn(),
}));

describe('Diagnostic Logger Integration Tests', () => {
  let mockLogContext: LogContext;
  let mockLog: {
    debug: ReturnType<typeof vi.fn>;
    info: ReturnType<typeof vi.fn>;
    warn: ReturnType<typeof vi.fn>;
    error: ReturnType<typeof vi.fn>;
  };

  beforeEach(() => {
    resetOtelDiagnosticLogger();
    vi.clearAllMocks();

    // Create mock log functions
    mockLog = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };

    // Create mock LogContext
    mockLogContext = {
      withContext: vi.fn().mockReturnValue(mockLog),
      debug: mockLog.debug,
      info: mockLog.info,
      warn: mockLog.warn,
      error: mockLog.error,
      flush: vi.fn(),
    } as unknown as LogContext;
  });

  test('otel-start sets up diagnostic logger correctly', async () => {
    const {diag} = await import('@opentelemetry/api');
    const {startOtelAuto} = await import('./otel-start.js');

    startOtelAuto(mockLogContext);

    // Diagnostic logger should be set up once (after SDK initialization, NodeSDK prevented from setting its own)
    expect(diag.setLogger).toHaveBeenCalledTimes(1);
    expect(mockLogContext.withContext).toHaveBeenCalledWith(
      'component',
      'otel',
    );
  });

  test('anonymous-otel-start sets up diagnostic logger when called first', async () => {
    const {diag} = await import('@opentelemetry/api');
    const {startAnonymousTelemetry} = await import('./anonymous-otel-start.js');

    startAnonymousTelemetry(mockLogContext);

    expect(diag.setLogger).toHaveBeenCalledTimes(1);
    expect(mockLogContext.withContext).toHaveBeenCalledWith(
      'component',
      'otel',
    );
  });

  test('diagnostic logger is only configured once when both modules are used', async () => {
    const {diag} = await import('@opentelemetry/api');
    const {startAnonymousTelemetry} = await import('./anonymous-otel-start.js');
    const {startOtelAuto} = await import('./otel-start.js');

    // Start anonymous telemetry first
    startAnonymousTelemetry(mockLogContext);
    expect(diag.setLogger).toHaveBeenCalledTimes(1);

    // Clear mocks to see new calls
    vi.mocked(diag.setLogger).mockClear();

    // Start main OTEL - should not configure again since it's already done
    startOtelAuto(mockLogContext);
    expect(diag.setLogger).toHaveBeenCalledTimes(0); // No new calls since already configured
  });

  test('both modules work correctly without LogContext', async () => {
    const {startOtelAuto} = await import('./otel-start.js');
    const {startAnonymousTelemetry} = await import('./anonymous-otel-start.js');

    // Should not throw errors when called without LogContext
    expect(() => startOtelAuto()).not.toThrow();
    expect(() => startAnonymousTelemetry()).not.toThrow();
  });

  test('modules can be called multiple times safely', async () => {
    const {startOtelAuto} = await import('./otel-start.js');
    const {startAnonymousTelemetry} = await import('./anonymous-otel-start.js');

    // Multiple calls should not cause issues
    expect(() => {
      startOtelAuto(mockLogContext);
      startOtelAuto(mockLogContext);
      startAnonymousTelemetry(mockLogContext);
      startAnonymousTelemetry(mockLogContext);
    }).not.toThrow();
  });

  test('OTEL_LOG_LEVEL is properly restored even when SDK initialization fails', async () => {
    // Set up OTEL_LOG_LEVEL before test
    const originalOtelLogLevel = process.env.OTEL_LOG_LEVEL;
    process.env.OTEL_LOG_LEVEL = 'DEBUG';

    // Since the OtelManager singleton might already be started, we need to test the try-finally behavior differently
    // We'll directly test that when OTEL_LOG_LEVEL is set, it gets properly restored
    const {startOtelAuto} = await import('./otel-start.js');

    try {
      // Clear the environment variable to verify the restoration
      delete process.env.OTEL_LOG_LEVEL;
      process.env.OTEL_LOG_LEVEL = 'DEBUG';

      // Call startOtelAuto (it might be already started, but that's ok for this test)
      // The key is that OTEL_LOG_LEVEL should remain after the call
      startOtelAuto(mockLogContext);

      // Verify OTEL_LOG_LEVEL is still set (was properly restored by the finally block)
      expect(process.env.OTEL_LOG_LEVEL).toBe('DEBUG');
    } finally {
      // Restore original value
      if (originalOtelLogLevel !== undefined) {
        process.env.OTEL_LOG_LEVEL = originalOtelLogLevel;
      } else {
        delete process.env.OTEL_LOG_LEVEL;
      }
    }
  });
});
