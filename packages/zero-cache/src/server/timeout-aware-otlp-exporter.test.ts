/* eslint-disable @typescript-eslint/no-explicit-any */
import {OTLPMetricExporter} from '@opentelemetry/exporter-metrics-otlp-http';
import {ExportResultCode} from '@opentelemetry/core';
import {afterAll, beforeAll, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {TimeoutAwareOTLPExporter} from './timeout-aware-otlp-exporter.ts';

// Mock the OTLP exporter
vi.mock('@opentelemetry/exporter-metrics-otlp-http');

describe('TimeoutAwareOTLPExporter', () => {
  let mockExporter: any;

  beforeAll(() => {
    // Mock exporter
    mockExporter = {
      export: vi.fn(),
      forceFlush: vi.fn().mockResolvedValue(undefined),
      shutdown: vi.fn().mockResolvedValue(undefined),
      selectAggregationTemporality: vi.fn(),
    };

    // Setup mock
    vi.mocked(OTLPMetricExporter).mockImplementation(() => mockExporter);
  });

  afterAll(() => {
    vi.clearAllMocks();
  });

  test('should handle request timeout and convert to success', () => {
    const lc = createSilentLogContext();
    const timeoutAwareExporter = new TimeoutAwareOTLPExporter(
      {url: 'https://test.example.com'},
      lc,
    );
    const mockCallback = vi.fn();

    const mockTimeoutResult = {
      code: ExportResultCode.FAILED,
      error: new Error('Request Timeout occurred'),
    };

    vi.mocked(mockExporter.export).mockImplementation(
      (_metrics: any, callback: any) => {
        callback(mockTimeoutResult);
      },
    );

    timeoutAwareExporter.export({} as any, mockCallback);

    expect(mockCallback).toHaveBeenCalledWith({code: ExportResultCode.SUCCESS});
  });

  test('should handle 502 server response and convert to success', () => {
    const lc = createSilentLogContext();
    const timeoutAwareExporter = new TimeoutAwareOTLPExporter(
      {url: 'https://test.example.com'},
      lc,
    );
    const mockCallback = vi.fn();

    const mock502Result = {
      code: ExportResultCode.FAILED,
      error: new Error('Unexpected server response: 502'),
    };

    vi.mocked(mockExporter.export).mockImplementation(
      (_metrics: any, callback: any) => {
        callback(mock502Result);
      },
    );

    timeoutAwareExporter.export({} as any, mockCallback);

    expect(mockCallback).toHaveBeenCalledWith({code: ExportResultCode.SUCCESS});
  });

  test('should handle retryable export errors and convert to success', () => {
    const lc = createSilentLogContext();
    const timeoutAwareExporter = new TimeoutAwareOTLPExporter(
      {url: 'https://test.example.com'},
      lc,
    );
    const mockCallback = vi.fn();

    const mockRetryableResult = {
      code: ExportResultCode.FAILED,
      error: new Error(
        'PeriodicExportingMetricReader: metrics export failed (error OTLPExporterError: Export failed with retryable status)',
      ),
    };

    vi.mocked(mockExporter.export).mockImplementation(
      (_metrics: any, callback: any) => {
        callback(mockRetryableResult);
      },
    );

    timeoutAwareExporter.export({} as any, mockCallback);

    expect(mockCallback).toHaveBeenCalledWith({code: ExportResultCode.SUCCESS});
  });

  test('should pass through non-timeout errors unchanged', () => {
    const lc = createSilentLogContext();
    const timeoutAwareExporter = new TimeoutAwareOTLPExporter(
      {url: 'https://test.example.com'},
      lc,
    );
    const mockCallback = vi.fn();

    // Mock a different type of error
    const mockErrorResult = {
      code: ExportResultCode.FAILED,
      error: new Error('Some other error'),
    };

    vi.mocked(mockExporter.export).mockImplementation(
      (_metrics: any, callback: any) => {
        callback(mockErrorResult);
      },
    );

    timeoutAwareExporter.export({} as any, mockCallback);

    expect(mockCallback).toHaveBeenCalledWith(mockErrorResult);
  });

  test('should pass through successful results unchanged', () => {
    const lc = createSilentLogContext();
    const timeoutAwareExporter = new TimeoutAwareOTLPExporter(
      {url: 'https://test.example.com'},
      lc,
    );
    const mockCallback = vi.fn();

    // Mock a successful result
    const mockSuccessResult = {
      code: ExportResultCode.SUCCESS,
    };

    vi.mocked(mockExporter.export).mockImplementation(
      (_metrics: any, callback: any) => {
        callback(mockSuccessResult);
      },
    );

    // Test that success results pass through
    timeoutAwareExporter.export({} as any, mockCallback);

    // Should pass through the original success result
    expect(mockCallback).toHaveBeenCalledWith(mockSuccessResult);
  });

  test('should delegate forceFlush to underlying exporter', async () => {
    const lc = createSilentLogContext();
    const timeoutAwareExporter = new TimeoutAwareOTLPExporter(
      {url: 'https://test.example.com'},
      lc,
    );

    await timeoutAwareExporter.forceFlush();

    expect(mockExporter.forceFlush).toHaveBeenCalled();
  });

  test('should delegate shutdown to underlying exporter', async () => {
    const lc = createSilentLogContext();
    const timeoutAwareExporter = new TimeoutAwareOTLPExporter(
      {url: 'https://test.example.com'},
      lc,
    );

    await timeoutAwareExporter.shutdown();

    expect(mockExporter.shutdown).toHaveBeenCalled();
  });

  test('should delegate selectAggregationTemporality to underlying exporter', () => {
    const lc = createSilentLogContext();
    const timeoutAwareExporter = new TimeoutAwareOTLPExporter(
      {url: 'https://test.example.com'},
      lc,
    );
    const mockInstrumentType = 'counter' as any;

    timeoutAwareExporter.selectAggregationTemporality(mockInstrumentType);

    expect(mockExporter.selectAggregationTemporality).toHaveBeenCalledWith(
      mockInstrumentType,
    );
  });
});
