import type {LogContext} from '@rocicorp/logger';
import {AbortError} from 'shared/src/abort-error.js';
import {sleep} from 'shared/src/sleep.js';

export function initBgIntervalProcess(
  processName: string,
  process: () => Promise<unknown>,
  delayMs: () => number,
  lc: LogContext,
  signal: AbortSignal,
): void {
  void runBgIntervalProcess(processName, process, delayMs, lc, signal);
}

async function runBgIntervalProcess(
  processName: string,
  process: () => Promise<unknown>,
  delayMs: () => number,
  lc: LogContext,
  signal: AbortSignal,
): Promise<void> {
  if (signal.aborted) {
    return;
  }
  lc = lc.withContext('bgIntervalProcess', processName);
  lc.debug?.('Starting');
  while (!signal.aborted) {
    try {
      await sleep(delayMs(), signal);
    } catch (e) {
      if (!(e instanceof AbortError)) {
        throw e;
      }
    }
    if (!signal.aborted) {
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
    }
  }
  lc.debug?.('Stopping');
}
