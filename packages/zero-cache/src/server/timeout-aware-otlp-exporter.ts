import {OTLPMetricExporter} from '@opentelemetry/exporter-metrics-otlp-http';
import type {
  PushMetricExporter,
  ResourceMetrics,
  AggregationTemporality,
  InstrumentType,
} from '@opentelemetry/sdk-metrics';
import {ExportResultCode, type ExportResult} from '@opentelemetry/core';
import type {LogContext} from '@rocicorp/logger';

/**
 * A wrapper around OTLPMetricExporter that handles recoverable errors gracefully.
 * Instead of failing the export on timeout, 502, or retryable errors, it logs a warning
 * and treats them as success to avoid SDK error logging.
 */
export class TimeoutAwareOTLPExporter implements PushMetricExporter {
  readonly #exporter: OTLPMetricExporter;
  readonly #lc: LogContext | undefined;

  constructor(config: {url?: string}, lc?: LogContext) {
    this.#exporter = new OTLPMetricExporter(config);
    this.#lc = lc;
  }

  export(
    metrics: ResourceMetrics,
    resultCallback: (result: ExportResult) => void,
  ): void {
    this.#exporter.export(metrics, result => {
      if (result.code === ExportResultCode.FAILED && result.error) {
        const errorMessage = result.error.message || '';

        // Check for recoverable errors that should be treated as warnings
        const recoverableError = this.#getRecoverableErrorType(errorMessage);
        if (recoverableError) {
          this.#lc?.warn?.(
            `telemetry: metrics export ${recoverableError} error, will retry on next interval`,
          );
          resultCallback({code: ExportResultCode.SUCCESS}); // Treat as success to avoid SDK error logging
          return;
        }
      }

      // Pass through all other results (success or non-recoverable failures)
      resultCallback(result);
    });
  }

  #getRecoverableErrorType(errorMessage: string): string | null {
    if (errorMessage.includes('Request Timeout')) {
      return 'timeout';
    }
    if (errorMessage.includes('Unexpected server response: 502')) {
      return '502';
    }
    if (errorMessage.includes('Export failed with retryable status')) {
      return 'retryable';
    }
    return null;
  }

  forceFlush(): Promise<void> {
    return this.#exporter.forceFlush();
  }

  shutdown(): Promise<void> {
    return this.#exporter.shutdown();
  }

  selectAggregationTemporality(
    instrumentType: InstrumentType,
  ): AggregationTemporality {
    return this.#exporter.selectAggregationTemporality(instrumentType);
  }
}
