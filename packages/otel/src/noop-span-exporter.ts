import {ExportResultCode, type ExportResult} from '@opentelemetry/core';
import {
  type SpanExporter,
  type ReadableSpan,
} from '@opentelemetry/sdk-trace-node';

export class NoopSpanExporter implements SpanExporter {
  export(
    _spans: ReadableSpan[],
    resultCallback: (result: ExportResult) => void,
  ): void {
    resultCallback({code: ExportResultCode.SUCCESS});
  }

  forceFlush(): Promise<void> {
    return Promise.resolve();
  }

  shutdown(): Promise<void> {
    return Promise.resolve();
  }
}
