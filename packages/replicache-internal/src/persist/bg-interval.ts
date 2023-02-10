import type {LogContext} from '@rocicorp/logger';

export function initBgIntervalProcess(
  processName: string,
  process: () => Promise<unknown>,
  intervalMs: number,
  lc: LogContext,
): () => void {
  lc = lc.addContext('bgIntervalProcess', processName);
  let closed = false;
  const intervalID = setInterval(async () => {
    lc.debug?.('Running');
    try {
      await process();
    } catch (e) {
      if (closed) {
        lc.debug?.('Error running most likely due to close.', e);
      } else {
        lc.error?.('Error running.', e);
      }
    }
  }, intervalMs);
  lc = lc.addContext('intervalID', intervalID);
  lc.debug?.('Starting');

  return () => {
    lc.debug?.('Stopping');
    closed = true;
    clearInterval(intervalID);
  };
}
