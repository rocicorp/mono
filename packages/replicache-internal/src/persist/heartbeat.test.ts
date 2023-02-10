import {LogContext} from '@rocicorp/logger';
import {expect} from '@esm-bundle/chai';
import * as sinon from 'sinon';
import {SinonFakeTimers, useFakeTimers} from 'sinon';
import * as dag from '../dag/mod';
import {
  latestHeartbeatUpdate,
  startHeartbeats,
  writeHeartbeat,
} from './heartbeat';
import {ClientMap, ClientStateNotFoundError, getClients} from './clients';
import {fakeHash} from '../hash';
import {makeClient, setClients} from './clients-test-helpers';
import {assertNotUndefined} from '../asserts';

let clock: SinonFakeTimers;
const START_TIME = 100000;
const ONE_MIN_IN_MS = 60 * 1000;
setup(() => {
  clock = useFakeTimers(START_TIME);
});

teardown(() => {
  clock.restore();
});

function awaitLatestHeartbeatUpdate(): Promise<ClientMap> {
  const latest = latestHeartbeatUpdate;
  assertNotUndefined(latest);
  return latest;
}

test('startHeartbeats starts interval that writes heartbeat each minute', async () => {
  const dagStore = new dag.TestStore();
  const client1 = {
    heartbeatTimestampMs: 1000,
    headHash: fakeHash('headclient1'),
    mutationID: 10,
    lastServerAckdMutationID: 10,
  };
  const client2 = {
    heartbeatTimestampMs: 3000,
    headHash: fakeHash('headclient2'),
    mutationID: 100,
    lastServerAckdMutationID: 90,
  };
  const clientMap = new Map(
    Object.entries({
      client1,
      client2,
    }),
  );
  await setClients(clientMap, dagStore);

  startHeartbeats('client1', dagStore, () => undefined, new LogContext());

  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(clientMap);
  });

  await clock.tickAsync(ONE_MIN_IN_MS);
  await awaitLatestHeartbeatUpdate();

  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(
      new Map(
        Object.entries({
          client1: {
            ...client1,
            heartbeatTimestampMs: START_TIME + ONE_MIN_IN_MS,
          },
          client2,
        }),
      ),
    );
  });

  await clock.tickAsync(ONE_MIN_IN_MS);
  await awaitLatestHeartbeatUpdate();

  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(
      new Map(
        Object.entries({
          client1: {
            ...client1,
            heartbeatTimestampMs: START_TIME + ONE_MIN_IN_MS + ONE_MIN_IN_MS,
          },
          client2,
        }),
      ),
    );
  });
});

test('calling function returned by startHeartbeats, stops heartbeats', async () => {
  const dagStore = new dag.TestStore();
  const client1 = makeClient({
    heartbeatTimestampMs: 1000,
    headHash: fakeHash('headclient1'),
  });
  const client2 = makeClient({
    heartbeatTimestampMs: 3000,
    headHash: fakeHash('headclient2'),
  });
  const clientMap = new Map(
    Object.entries({
      client1,
      client2,
    }),
  );
  await setClients(clientMap, dagStore);

  const stopHeartbeats = startHeartbeats(
    'client1',
    dagStore,
    () => undefined,
    new LogContext(),
  );

  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(clientMap);
  });

  await clock.tickAsync(ONE_MIN_IN_MS);
  await awaitLatestHeartbeatUpdate();

  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(Object.fromEntries(readClientMap)).to.deep.equal({
      client1: {
        ...client1,
        heartbeatTimestampMs: START_TIME + ONE_MIN_IN_MS,
      },
      client2,
    });
  });

  stopHeartbeats();
  clock.tick(ONE_MIN_IN_MS);
  await awaitLatestHeartbeatUpdate();

  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(
      new Map(
        Object.entries({
          client1: {
            ...client1,
            // Heartbeat *NOT* updated to START_TIME + ONE_MIN_IN_MS + ONE_MIN_IN_MS
            heartbeatTimestampMs: START_TIME + ONE_MIN_IN_MS,
          },
          client2,
        }),
      ),
    );
  });
});

test('writeHeartbeat writes heartbeat', async () => {
  const dagStore = new dag.TestStore();
  const client1 = {
    heartbeatTimestampMs: 1000,
    headHash: fakeHash('headclient1'),
    mutationID: 10,
    lastServerAckdMutationID: 10,
  };
  const client2 = {
    heartbeatTimestampMs: 3000,
    headHash: fakeHash('headclient2'),
    mutationID: 100,
    lastServerAckdMutationID: 90,
  };
  const clientMap = new Map(
    Object.entries({
      client1,
      client2,
    }),
  );

  await setClients(clientMap, dagStore);

  const TICK_IN_MS = 20000;
  clock.tick(TICK_IN_MS);

  await writeHeartbeat('client1', dagStore);
  await dagStore.withRead(async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(
      new Map(
        Object.entries({
          client1: {
            ...client1,
            heartbeatTimestampMs: START_TIME + TICK_IN_MS,
          },
          client2,
        }),
      ),
    );
  });
});

test('writeHeartbeat throws Error if no Client is found for clientID', async () => {
  const dagStore = new dag.TestStore();
  let e;
  try {
    await writeHeartbeat('client1', dagStore);
  } catch (ex) {
    e = ex;
  }
  expect(e)
    .to.be.instanceOf(ClientStateNotFoundError)
    .property('id', 'client1');
});

test('heartbeat with missing client calls callback', async () => {
  const dagStore = new dag.TestStore();
  const onClientStateNotFound = sinon.fake();
  const stop = startHeartbeats(
    'client1',
    dagStore,
    onClientStateNotFound,
    new LogContext(),
  );
  await clock.tickAsync(ONE_MIN_IN_MS);
  expect(onClientStateNotFound.callCount).to.equal(1);
  stop();
});
