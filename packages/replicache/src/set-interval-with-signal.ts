export function setIntervalWithSignal(
  fn: () => void,
  ms: number,
  signal: AbortSignal,
): void {
  if (!signal.aborted) {
    const interval = setInterval(fn, ms);
    signal.addEventListener('abort', () => {
      clearInterval(interval);
    });
  }
}
