import {LogContext} from '@rocicorp/logger';
import {expect} from 'chai';
import {assertNotUndefined} from '../../../shared/src/asserts.js';
import {type SinonFakeTimers, useFakeTimers} from 'sinon';
import type {Read} from '../dag/store.js';
import {TestStore} from '../dag/test-store.js';
import {newRandomHash} from '../hash.js';
import {withRead, withWrite} from '../with-transactions.js';
import {
  CLIENT_MAX_INACTIVE_TIME,
  GC_INTERVAL,
  getLatestGCUpdate,
  initClientGC,
} from './client-gc.js';
import {makeClientV4, setClientsForTesting} from './clients-test-helpers.js';
import {type ClientMap, getClients, setClient} from './clients.js';

let clock: SinonFakeTimers;
const START_TIME = 0;
const MINUTES = 60 * 1000;
const HOURS = 60 * 60 * 1000;

setup(() => {
  clock = useFakeTimers(START_TIME);
});

teardown(() => {
  clock.restore();
});

function awaitLatestGCUpdate(): Promise<ClientMap> {
  const latest = getLatestGCUpdate();
  assertNotUndefined(latest);
  return latest;
}

test('initClientGC starts 5 min interval that collects clients that have been inactive for > 24 hours', async () => {
  const dagStore = new TestStore();
  const client1 = makeClientV4({
    heartbeatTimestampMs: START_TIME,
    headHash: newRandomHash(),
    mutationID: 100,
    lastServerAckdMutationID: 90,
  });
  const client2 = makeClientV4({
    heartbeatTimestampMs: START_TIME,
    headHash: newRandomHash(),
  });
  const client3 = makeClientV4({
    heartbeatTimestampMs: START_TIME + 6 * 60 * 1000,
    headHash: newRandomHash(),
  });
  const client4 = makeClientV4({
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
  initClientGC(
    'client1',
    dagStore,
    CLIENT_MAX_INACTIVE_TIME,
    GC_INTERVAL,
    new LogContext(),
    controller.signal,
  );

  await withRead(dagStore, async (read: Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(clientMap);
  });

  clock.tick(24 * HOURS);
  await clock.tickAsync(5 * MINUTES);
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

  // Update client4's heartbeat to now
  const client4WUpdatedHeartbeat = {
    ...client4,
    heartbeatTimestampMs: clock.now,
  };

  await withWrite(dagStore, async dagWrite => {
    await setClient('client4', client4WUpdatedHeartbeat, dagWrite);
  });

  await clock.tickAsync(5 * MINUTES);
  await awaitLatestGCUpdate();

  // client1 is not collected because it is the current client (despite being old enough to collect)
  // client3 is collected because it is > 24 hours inactive (by 4 mins)
  // client4 is not collected because its update heartbeat is < 24 hours inactive (24 hours - 5 mins)
  await withRead(dagStore, async (read: Read) => {
    const readClientMap = await getClients(read);
    expect(Object.fromEntries(readClientMap)).to.deep.equal({
      client1,
      client4: client4WUpdatedHeartbeat,
    });
  });

  clock.tick(24 * HOURS - 5 * MINUTES * 2 + 1);
  await clock.tickAsync(5 * MINUTES);
  await awaitLatestGCUpdate();

  // client1 is not collected because it is the current client (despite being old enough to collect)
  // client4 is collected because it is > 24 hours inactive
  await withRead(dagStore, async (read: Read) => {
    const readClientMap = await getClients(read);
    expect(Object.fromEntries(readClientMap)).to.deep.equal({
      client1,
    });
  });
});
