import {LogContext} from '@rocicorp/logger';
import {expect, test, vi} from 'vitest';
import {TestLogSink} from '../../../shared/src/logging-test-utils.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ConnectionStatus} from './connection-status.ts';
import {
  RELOAD_BACKOFF_STATE_KEY,
  reportReloadReason,
} from './reload-error-handler.ts';
import {storageMock, zeroForTest} from './test-utils.ts';

const schemaError = {
  kind: ErrorKind.SchemaVersionNotSupported,
  message:
    'The "execution_results" table is missing a primary key or non-null unique index and thus cannot be synced to the client',
  origin: ErrorOrigin.ZeroCache,
} as const;

async function waitForReload(
  reloadTimes: number[],
  previousReloadCount: number,
) {
  while (reloadTimes.length === previousReloadCount) {
    await vi.advanceTimersToNextTimerAsync();
  }
}

test('schema version error reload loop timing', async () => {
  const storage: Record<string, string> = {};
  vi.spyOn(globalThis, 'sessionStorage', 'get').mockImplementation(() =>
    storageMock(storage),
  );
  vi.useFakeTimers({now: 0});

  const reloadTimes: number[] = [];
  const errorTimes: number[] = [];
  const backoffStates: unknown[] = [];
  const pageLoadToSchemaErrorMs = 1_500;

  for (let pageLoad = 0; pageLoad < 7; pageLoad++) {
    const z = zeroForTest(undefined, false);
    z.reload = () => reloadTimes.push(Date.now());

    reportReloadReason(new LogContext('debug', {}, new TestLogSink()));
    await z.triggerConnected();
    await vi.advanceTimersByTimeAsync(pageLoadToSchemaErrorMs);
    errorTimes.push(Date.now());

    await z.triggerError(schemaError);
    await z.waitForConnectionStatus(ConnectionStatus.Error);

    const previousReloadCount = reloadTimes.length;
    if (storage[RELOAD_BACKOFF_STATE_KEY]) {
      backoffStates.push(JSON.parse(storage[RELOAD_BACKOFF_STATE_KEY]));
    }
    await waitForReload(reloadTimes, previousReloadCount);
    await z.close();
  }

  const intervalsBetweenReloads = reloadTimes
    .slice(1)
    .map((reloadTime, i) => reloadTime - reloadTimes[i]);

  // Keep this visible when running the repro directly.
  // eslint-disable-next-line no-console
  console.table({
    errorTimes,
    reloadTimes,
    intervalsBetweenReloads,
    backoffStates,
  });

  expect(intervalsBetweenReloads).toEqual([
    1_500, 1_500, 2_000, 4_000, 8_000, 16_000,
  ]);
});
