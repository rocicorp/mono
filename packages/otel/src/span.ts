import {
  type Attributes,
  type Span,
  type Tracer,
  context,
} from '@opentelemetry/api';

export function startSpan<T>(
  tracer: Tracer,
  name: string,
  cb: (span: Omit<Span, 'end'>) => T,
): T {
  return tracer.startActiveSpan(name, span => {
    try {
      return cb(span);
    } finally {
      span.end();
    }
  });
}

export function startAsyncSpan<T>(
  tracer: Tracer,
  name: string,
  cb: (span: Omit<Span, 'end'>) => Promise<T>,
): Promise<T> {
  return tracer.startActiveSpan(name, async span => {
    try {
      return await cb(span);
    } finally {
      span.end();
    }
  });
}

export function manualSpan(
  tracer: Tracer,
  name: string,
  durationMs: number,
  attributes?: Attributes,
): void {
  const startTime = Date.now() - durationMs;
  const span = tracer.startSpan(name, {startTime}, context.active());
  if (attributes) {
    span.setAttributes(attributes);
  }
  span.end(startTime + durationMs);
}
