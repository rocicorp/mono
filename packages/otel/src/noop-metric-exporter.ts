import {
  type PushMetricExporter,
  type ResourceMetrics,
} from '@opentelemetry/sdk-metrics';
import type {ExportResult} from '@opentelemetry/core';

export class NoopMetricExporter implements PushMetricExporter {
  export(
    _metrics: ResourceMetrics,
    _resultCallback: (result: ExportResult) => void,
  ): void {}

  forceFlush(): Promise<void> {
    return Promise.resolve();
  }

  shutdown(): Promise<void> {
    return Promise.resolve();
  }
}
