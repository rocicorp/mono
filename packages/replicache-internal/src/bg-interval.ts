import type {LogContext} from '@rocicorp/logger';
import {AbortError} from './abort-error';
import {sleep} from './sleep';

export function initBgIntervalProcess(
  processName: string,
  process: () => Promise<unknown>,
  intervalMs: number,
  lc: LogContext,
  signal: AbortSignal,
): void {
  void runBgIntervalProcess(processName, process, intervalMs, lc, signal);
}

async function runBgIntervalProcess(
  processName: string,
  process: () => Promise<unknown>,
  intervalMs: number,
  lc: LogContext,
  signal: AbortSignal,
): Promise<void> {
  if (signal.aborted) {
    return;
  }
  lc = lc.addContext('bgIntervalProcess', processName);
  lc.debug?.('Starting');
  while (!signal.aborted) {
    try {
      await sleep(intervalMs, signal);
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
