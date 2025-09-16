/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import {expect, test, vi} from 'vitest';
import {sleep} from '../../shared/src/sleep.ts';
import {initReplicacheTesting, replicacheForTesting} from './test-util.ts';

initReplicacheTesting();

test('collect IDB databases', async () => {
  if (!indexedDB.databases) {
    // Firefox does not support indexedDB.databases
    return;
  }

  const MINUTES = 1000 * 60 * 1;
  const HOURS = 1000 * 60 * 60;
  const ONE_MONTH = 1000 * 60 * 60 * 24 * 30;

  const rep = await replicacheForTesting('collect-idb-databases-1');
  await rep.close();

  expect(await getDatabases()).to.deep.equal(['collect-idb-databases-1']);

  await vi.advanceTimersByTimeAsync(ONE_MONTH);

  const rep2 = await replicacheForTesting('collect-idb-databases-2');
  await rep2.close();

  expect(await getDatabases()).to.deep.equal([
    'collect-idb-databases-1',
    'collect-idb-databases-2',
  ]);

  await vi.advanceTimersByTimeAsync(12 * HOURS);

  // Open one more database and keep it open long enough to trigger the collection.
  const rep3 = await replicacheForTesting('collect-idb-databases-3');
  await vi.advanceTimersByTimeAsync(5 * MINUTES);
  await rep3.close();

  // Restore real timers and wait a few ms to let the idb state "flush"
  vi.useRealTimers();
  await sleep(500);

  expect(await getDatabases()).to.deep.equal([
    'collect-idb-databases-2',
    'collect-idb-databases-3',
  ]);

  async function getDatabases() {
    function parseName(idbName: string | undefined): string | undefined {
      return idbName && /^rep:[^:]+:([^:]+):\d+$/.exec(idbName)?.[1];
    }

    return (await indexedDB.databases())
      .map(({name}) => parseName(name))
      .filter(name => name && name.startsWith('collect-idb-databases'))
      .sort();
  }
});
