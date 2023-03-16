import {LogContext} from '@rocicorp/logger';
import {expect} from '@esm-bundle/chai';
import {SinonFakeTimers, useFakeTimers} from 'sinon';
import * as dag from '../dag/mod.js';
import {ClientMap, getClients, setClient} from './clients.js';
import {newUUIDHash} from '../hash.js';
import {initClientGC, getLatestGCUpdate} from './client-gc.js';
import {makeClientV4, setClientsForTesting} from './clients-test-helpers.js';
import {assertNotUndefined} from 'shared/asserts.js';
import {withRead, withWrite} from '../with-transactions.js';

let clock: SinonFakeTimers;
const START_TIME = 0;
const SEVEN_DAYS_IN_MS = 7 * 24 * 60 * 60 * 1000;
const FIVE_MINS_IN_MS = 5 * 60 * 1000;
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

test('initClientGC starts 5 min interval that collects clients that have been inactive for > 7 days', async () => {
  const dagStore = new dag.TestStore();
  const client1 = makeClientV4({
    heartbeatTimestampMs: START_TIME,
    headHash: newUUIDHash(),
    mutationID: 100,
    lastServerAckdMutationID: 90,
  });
  const client2 = makeClientV4({
    heartbeatTimestampMs: START_TIME,
    headHash: newUUIDHash(),
  });
  const client3 = makeClientV4({
    heartbeatTimestampMs: START_TIME + 6 * 60 * 1000,
    headHash: newUUIDHash(),
  });
  const client4 = makeClientV4({
    heartbeatTimestampMs: START_TIME + 6 * 60 * 1000,
    headHash: newUUIDHash(),
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
  initClientGC('client1', dagStore, new LogContext(), controller.signal);

  await withRead(dagStore, async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(clientMap);
  });

  clock.tick(SEVEN_DAYS_IN_MS);
  await clock.tickAsync(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // client1 is not collected because it is the current client (despite being old enough to collect)
  // client2 is collected because it is > 7 days inactive
  // client3 is not collected because it is < 7 days inactive (by 1 minute)
  // client4 is not collected because it is < 7 days inactive (by 1 minute)
  await withRead(dagStore, async (read: dag.Read) => {
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
    await dagWrite.commit();
  });

  await clock.tickAsync(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // client1 is not collected because it is the current client (despite being old enough to collect)
  // client3 is collected because it is > 7 days inactive (by 4 mins)
  // client4 is not collected because its update heartbeat is < 7 days inactive (7 days - 5 mins)
  await withRead(dagStore, async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(Object.fromEntries(readClientMap)).to.deep.equal({
      client1,
      client4: client4WUpdatedHeartbeat,
    });
  });

  clock.tick(SEVEN_DAYS_IN_MS - FIVE_MINS_IN_MS * 2 + 1);
  await clock.tickAsync(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // client1 is not collected because it is the current client (despite being old enough to collect)
  // client4 is collected because it is > 7 days inactive
  await withRead(dagStore, async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(Object.fromEntries(readClientMap)).to.deep.equal({
      client1,
    });
  });
});
