import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, expect, test, vi} from 'vitest';
import {assertNotUndefined} from '../../../shared/src/asserts.ts';
import type {Read} from '../dag/store.ts';
import {TestStore} from '../dag/test-store.ts';
import {getDeletedClients, removeDeletedClients} from '../deleted-clients.ts';
import {newRandomHash} from '../hash.ts';
import {withRead, withWrite} from '../with-transactions.ts';
import {
  CLIENT_MAX_INACTIVE_TIME,
  GC_INTERVAL,
  getLatestGCUpdate,
  initClientGC,
} from './client-gc.ts';
import {makeClientV5, setClientsForTesting} from './clients-test-helpers.ts';
import {
  type ClientMap,
  getClients,
  type OnClientsDeleted,
  setClient,
} from './clients.ts';

const START_TIME = 0;
const MINUTES = 60 * 1000;
const HOURS = 60 * 60 * 1000;

beforeEach(() => {
  vi.useFakeTimers({now: START_TIME});
});

afterEach(() => {
  vi.useRealTimers();
});

function awaitLatestGCUpdate(): Promise<ClientMap> {
  const latest = getLatestGCUpdate();
  assertNotUndefined(latest);
  return latest;
}

test('initClientGC starts 5 min interval that collects clients that have been inactive for > 24 hours', async () => {
  const dagStore = new TestStore();
  const client1 = makeClientV5({
    heartbeatTimestampMs: START_TIME,
    headHash: newRandomHash(),

    // mutationID: 100,
    // lastServerAckdMutationID: 90,
  });
  const client2 = makeClientV5({
    heartbeatTimestampMs: START_TIME,
    headHash: newRandomHash(),
  });
  const client3 = makeClientV5({
    heartbeatTimestampMs: START_TIME + 6 * 60 * 1000,
    headHash: newRandomHash(),
  });
  const client4 = makeClientV5({
    heartbeatTimestampMs: START_TIME + 6 * 60 * 1000,
    headHash: newRandomHash(),
  });
  const clientMap = new Map(
    Object.entries({
      client1,
      client2,
      client3,
      client4,
    }),
  );

  await setClientsForTesting(clientMap, dagStore);

  const controller = new AbortController();
  const onClientsDeleted = vi.fn<OnClientsDeleted>();
  initClientGC(
    'client1',
    dagStore,
    CLIENT_MAX_INACTIVE_TIME,
    GC_INTERVAL,
    onClientsDeleted,
    new LogContext(),
    controller.signal,
  );

  await withRead(dagStore, async (read: Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(clientMap);
  });

  vi.setSystemTime(Date.now() + 24 * HOURS);
  await vi.advanceTimersByTimeAsync(5 * MINUTES);
  await awaitLatestGCUpdate();

  // client1 is not collected because it is the current client (despite being old enough to collect)
  // client2 is collected because it is > 24 hours inactive
  // client3 is not collected because it is < 24 hours inactive (by 1 minute)
  // client4 is not collected because it is < 24 hours inactive (by 1 minute)
  await withRead(dagStore, async (read: Read) => {
    const readClientMap = await getClients(read);
    expect(Object.fromEntries(readClientMap)).to.deep.equal({
      client1,
      client3,
      client4,
    });
  });
  expect(onClientsDeleted).toHaveBeenCalledTimes(1);
  expect(onClientsDeleted).toHaveBeenCalledWith([
    {clientGroupID: 'make-client-group-id', clientID: 'client2'},
  ]);
  onClientsDeleted.mockClear();

  expect(await withRead(dagStore, getDeletedClients)).toEqual([
    {clientGroupID: 'make-client-group-id', clientID: 'client2'},
  ]);

  // Update client4's heartbeat to now
  const client4UpdatedHeartbeat = {
    ...client4,
    heartbeatTimestampMs: Date.now(),
  };

  await withWrite(dagStore, async dagWrite => {
    await setClient('client4', client4UpdatedHeartbeat, dagWrite);
  });

  await vi.advanceTimersByTimeAsync(5 * MINUTES);
  await awaitLatestGCUpdate();

  // client1 is not collected because it is the current client (despite being old enough to collect)
  // client3 is collected because it is > 24 hours inactive (by 4 mins)
  // client4 is not collected because its update heartbeat is < 24 hours inactive (24 hours - 5 mins)
  await withRead(dagStore, async (read: Read) => {
    const readClientMap = await getClients(read);
    expect(Object.fromEntries(readClientMap)).to.deep.equal({
      client1,
      client4: client4UpdatedHeartbeat,
    });
  });
  expect(onClientsDeleted).toHaveBeenCalledTimes(1);
  // 'client2' is still in the deleted clients list
  expect(onClientsDeleted).toHaveBeenCalledWith([
    {clientGroupID: 'make-client-group-id', clientID: 'client2'},
    {clientGroupID: 'make-client-group-id', clientID: 'client3'},
  ]);
  onClientsDeleted.mockClear();

  // Clear client2 from deleted clients list
  await withWrite(dagStore, dagWrite =>
    removeDeletedClients(dagWrite, [
      {clientGroupID: 'make-client-group-id', clientID: 'client2'},
    ]),
  );

  vi.setSystemTime(Date.now() + 24 * HOURS - 5 * MINUTES * 2 + 1);
  await vi.advanceTimersByTimeAsync(5 * MINUTES);
  await awaitLatestGCUpdate();

  // client1 is not collected because it is the current client (despite being old enough to collect)
  // client4 is collected because it is > 24 hours inactive
  await withRead(dagStore, async (read: Read) => {
    const readClientMap = await getClients(read);
    expect(Object.fromEntries(readClientMap)).to.deep.equal({
      client1,
    });
  });

  expect(onClientsDeleted).toHaveBeenCalledTimes(1);
  expect(onClientsDeleted).toHaveBeenCalledWith([
    {clientGroupID: 'make-client-group-id', clientID: 'client3'},
    {clientGroupID: 'make-client-group-id', clientID: 'client4'},
  ]);
});
