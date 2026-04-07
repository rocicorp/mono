// TODO(arv): Remove workaround once docs/ builds cleanly without this.
declare function requestIdleCallback(
  callback: () => void,
  options?: {timeout?: number},
): number;

/**
 * A Promise wrapper for requestIdleCallback with fallback to setTimeout for
 * browsers without support (aka Safari)
 */
export function requestIdle(timeout: number): Promise<void> {
  return new Promise(resolve => {
    if (typeof requestIdleCallback === 'function') {
      requestIdleCallback(() => resolve(), {timeout});
    } else {
      setTimeout(() => resolve(), timeout);
    }
  });
}
