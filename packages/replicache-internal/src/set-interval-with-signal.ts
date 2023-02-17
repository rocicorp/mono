export function setIntervalWithSignal(
  fn: () => void,
  ms: number,
  signal: AbortSignal,
): void {
  const interval = setInterval(fn, ms);
  signal.addEventListener('abort', () => {
    clearInterval(interval);
  });
}
