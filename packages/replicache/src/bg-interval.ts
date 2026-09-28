import type {LogContext} from '@rocicorp/logger';
import {AbortError} from '../../shared/src/abort-error.ts';
import {sleep} from '../../shared/src/sleep.ts';
import {IDBNotFoundError} from './kv/idb-store.ts';
import {
  getStorageFailure,
  type StorageFailureError,
} from './storage-failure.ts';

export function initBgIntervalProcess(
  processName: string,
  process: () => Promise<unknown>,
  delayMs: () => number,
  lc: LogContext,
  signal: AbortSignal,
  onStorageFailure?: ((failure: StorageFailureError) => void) | undefined,
): void {
  void runBgIntervalProcess(
    processName,
    process,
    delayMs,
    lc,
    signal,
    onStorageFailure,
  );
}

async function runBgIntervalProcess(
  processName: string,
  process: () => Promise<unknown>,
  delayMs: () => number,
  lc: LogContext,
  signal: AbortSignal,
  onStorageFailure?: ((failure: StorageFailureError) => void) | undefined,
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
        } else if (e instanceof IDBNotFoundError) {
          lc.info?.('IndexedDB was deleted externally.', e);
        } else {
          const failure = getStorageFailure(e);
          if (failure === undefined) {
            lc.error?.('Error running.', e);
          } else {
            // The store's storage failed, not this process: running it again
            // at the interval fails the same way (see `onStorageFailure`).
            // warn, not error: the instance reports it once through the
            // callback, and there is nothing for a developer to fix.
            lc.warn?.('Storage failed; stopping.', e);
            onStorageFailure?.(failure);
            break;
          }
        }
      }
    }
  }
  lc.debug?.('Stopping');
}
