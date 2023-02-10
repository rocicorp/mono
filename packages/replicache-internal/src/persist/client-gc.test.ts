import {LogContext} from '@rocicorp/logger';
import {expect} from '@esm-bundle/chai';
import {SinonFakeTimers, useFakeTimers} from 'sinon';
import * as dag from '../dag/mod';
import {ClientMap, getClients, updateClients} from './clients';
import {fakeHash} from '../hash';
import {initClientGC, getLatestGCUpdate} from './client-gc';
import {makeClient, setClients} from './clients-test-helpers';
import {assertNotUndefined} from '../asserts';

let clock: SinonFakeTimers;
const START_TIME = 0;
const SEVEN_DAYS_IN_MS = 7 * 24 * 60 * 60 * 1000;
const FIVE_MINS_IN_MS = 5 * 60 * 1000;
setup(() => {
  clock = useFakeTimers(0);
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
  const client1 = makeClient({
    heartbeatTimestampMs: START_TIME,
    headHash: fakeHash('headclient1'),
    mutationID: 100,
    lastServerAckdMutationID: 90,
  });
  const client2 = makeClient({
    heartbeatTimestampMs: START_TIME,
    headHash: fakeHash('headclient2'),
  });
  const client3 = makeClient({
    heartbeatTimestampMs: START_TIME + 60 * 1000,
    headHash: fakeHash('headclient3'),
  });
  const client4 = makeClient({
    heartbeatTimestampMs: START_TIME + 60 * 1000,
    headHash: fakeHash('headclient4'),
  });
  const clientMap = new Map(
    Object.entries({
      client1,
      client2,
      client3,
      client4,
    }),
  );

  await setClients(clientMap, dagStore);

  initClientGC('client1', dagStore, new LogContext());

  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(clientMap);
  });

  clock.tick(SEVEN_DAYS_IN_MS + 1);
  await awaitLatestGCUpdate();

  // client1 is not collected because it is the current client (despite being old enough to collect)
  // client2 is collected because it is > 7 days inactive
  // client3 is not collected because it is < 7 days inactive (by 1 minute)
  // client4 is not collected because it is < 7 days inactive (by 1 minute)
  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(
      new Map(
        Object.entries({
          client1,
          client3,
          client4,
        }),
      ),
    );
  });

  // Update client4's heartbeat to now
  const client4WUpdatedHeartbeat = {
    ...client4,
    heartbeatTimestampMs: clock.now,
  };

  await updateClients(clients => {
    return Promise.resolve({
      clients: new Map(clients).set('client4', client4WUpdatedHeartbeat),
    });
  }, dagStore);

  clock.tick(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // client1 is not collected because it is the current client (despite being old enough to collect)
  // client3 is collected because it is > 7 days inactive (by 4 mins)
  // client4 is not collected because its update heartbeat is < 7 days inactive (7 days - 5 mins)
  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(
      new Map(
        Object.entries({
          client1,
          client4: client4WUpdatedHeartbeat,
        }),
      ),
    );
  });

  clock.tick(SEVEN_DAYS_IN_MS);
  await awaitLatestGCUpdate();

  // client1 is not collected because it is the current client (despite being old enough to collect)
  // client4 is collected because it is > 7 days inactive
  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(
      new Map(
        Object.entries({
          client1,
        }),
      ),
    );
  });
});

test('calling function returned by initClientGC, stops Client GCs', async () => {
  const dagStore = new dag.TestStore();
  const client1 = makeClient({
    heartbeatTimestampMs: START_TIME,
    headHash: fakeHash('headclient1'),
  });
  const client2 = makeClient({
    heartbeatTimestampMs: START_TIME,
    headHash: fakeHash('headclient2'),
  });
  const client3 = makeClient({
    heartbeatTimestampMs: START_TIME + 60 * 1000,
    headHash: fakeHash('headclient3'),
  });
  const clientMap = new Map(
    Object.entries({
      client1,
      client2,
      client3,
    }),
  );

  await setClients(clientMap, dagStore);

  const stopClientGC = initClientGC('client1', dagStore, new LogContext());

  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(clientMap);
  });

  clock.tick(SEVEN_DAYS_IN_MS + 1);
  await awaitLatestGCUpdate();

  // client1 is not collected because it is the current client (despite being old enough to collect)
  // client2 is collected because it is > 7 days inactive
  // client3 is not collected because it is < 7 days inactive (by 1 minute)
  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(
      new Map(
        Object.entries({
          client1,
          client3,
        }),
      ),
    );
  });

  stopClientGC();
  clock.tick(FIVE_MINS_IN_MS);
  await awaitLatestGCUpdate();

  // client3 is not collected because GC has been stopped
  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(
      new Map(
        Object.entries({
          client1,
          client3,
        }),
      ),
    );
  });
});
