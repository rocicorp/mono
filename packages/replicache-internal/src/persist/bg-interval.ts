import type {LogContext} from '@rocicorp/logger';

export function initBgIntervalProcess(
  processName: string,
  process: () => Promise<unknown>,
  intervalMs: number,
  lc: LogContext,
  signal: AbortSignal,
): void {
  if (signal.aborted) {
    return;
  }

  lc = lc.addContext('bgIntervalProcess', processName);
  const intervalID = setInterval(async () => {
    lc.debug?.('Running');
    try {
      await process();
    } catch (e) {
      if (signal.aborted) {
        lc.debug?.('Error running most likely due to close.', e);
      } else {
        lc.error?.('Error running.', e);
      }
    }
  }, intervalMs);
  lc = lc.addContext('intervalID', intervalID);
  lc.debug?.('Starting');

  signal.addEventListener('abort', () => {
    lc.debug?.('Stopping');
    clearInterval(intervalID);
  });
}
