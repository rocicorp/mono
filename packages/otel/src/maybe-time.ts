import type {LogContext} from '@rocicorp/logger';

export function timeSampled<T>(
  lc: LogContext,
  numerator: number,
  denominator: number,
  cb: () => T,
  threshold: number = 0,
  prefix: () => string = () => '',
) {
  if (denominator > 0 && numerator % denominator === 0) {
    const start = performance.now();
    const result = cb();
    const duration = performance.now() - start;
    if (duration > threshold) {
      lc.warn?.(`${prefix()} duration: ${duration}ms`);
    }

    return result;
  }
  return cb();
}
