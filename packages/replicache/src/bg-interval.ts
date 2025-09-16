/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import type {LogContext} from '@rocicorp/logger';
import {AbortError} from '../../shared/src/abort-error.ts';
import {sleep} from '../../shared/src/sleep.ts';

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
