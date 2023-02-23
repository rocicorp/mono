import {LogContext} from '@rocicorp/logger';
import {expect} from '@esm-bundle/chai';
import * as sinon from 'sinon';
import {SinonFakeTimers, useFakeTimers} from 'sinon';
import * as dag from '../dag/mod.js';
import {
  latestHeartbeatUpdate,
  startHeartbeats,
  writeHeartbeat,
} from './heartbeat.js';
import {ClientMap, ClientStateNotFoundError, getClients} from './clients.js';
import {assertHash, fakeHash} from '../hash.js';
import {makeClientDD31, setClientsForTesting} from './clients-test-helpers.js';
import {assertNotUndefined} from '../asserts.js';
import {IDBNotFoundError, IDBStore} from '../kv/idb-store.js';
import {dropIDBStore} from '../kv/mod.js';
import {resolver} from '@rocicorp/resolver';
import {withRead} from '../with-transactions.js';

let clock: SinonFakeTimers;
const START_TIME = 100000;
const ONE_MIN_IN_MS = 60 * 1000;
setup(() => {
  clock = useFakeTimers(START_TIME);
});

teardown(() => {
  sinon.restore();
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
    headHash: fakeHash('eadc1e1'),
    mutationID: 10,
    lastServerAckdMutationID: 10,
  };
  const client2 = {
    heartbeatTimestampMs: 3000,
    headHash: fakeHash('eadc1e2'),
    mutationID: 100,
    lastServerAckdMutationID: 90,
  };
  const clientMap = new Map(
    Object.entries({
      client1,
      client2,
    }),
  );
  await setClientsForTesting(clientMap, dagStore);

  const controller = new AbortController();
  startHeartbeats(
    'client1',
    dagStore,
    () => undefined,
    new LogContext(),
    controller.signal,
  );

  await withRead(dagStore, async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(clientMap);
  });

  await clock.tickAsync(ONE_MIN_IN_MS);
  await awaitLatestHeartbeatUpdate();

  await withRead(dagStore, async (read: dag.Read) => {
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

  await withRead(dagStore, async (read: dag.Read) => {
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
  const client1 = makeClientDD31({
    heartbeatTimestampMs: 1000,
    headHash: fakeHash('eadc1e1'),
  });
  const client2 = makeClientDD31({
    heartbeatTimestampMs: 3000,
    headHash: fakeHash('eadc1e2'),
  });
  const clientMap = new Map(
    Object.entries({
      client1,
      client2,
    }),
  );
  await setClientsForTesting(clientMap, dagStore);

  const controller = new AbortController();
  startHeartbeats(
    'client1',
    dagStore,
    () => undefined,
    new LogContext(),
    controller.signal,
  );

  await withRead(dagStore, async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).to.deep.equal(clientMap);
  });

  await clock.tickAsync(ONE_MIN_IN_MS);
  await awaitLatestHeartbeatUpdate();

  await withRead(dagStore, async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(Object.fromEntries(readClientMap)).to.deep.equal({
      client1: {
        ...client1,
        heartbeatTimestampMs: START_TIME + ONE_MIN_IN_MS,
        tempRefreshHash: null,
      },
      client2,
    });
  });

  controller.abort();
  clock.tick(ONE_MIN_IN_MS);
  await awaitLatestHeartbeatUpdate();

  await withRead(dagStore, async (read: dag.Read) => {
    const readClientMap = await getClients(read);
    expect(Object.fromEntries(readClientMap)).to.deep.equal({
      client1: {
        ...client1,
        // Heartbeat *NOT* updated to START_TIME + ONE_MIN_IN_MS + ONE_MIN_IN_MS
        heartbeatTimestampMs: START_TIME + ONE_MIN_IN_MS,
        tempRefreshHash: null,
      },
      client2,
    });
  });
});

test('writeHeartbeat writes heartbeat', async () => {
  const dagStore = new dag.TestStore();
  const client1 = {
    heartbeatTimestampMs: 1000,
    headHash: fakeHash('eadc1e1'),
    mutationID: 10,
    lastServerAckdMutationID: 10,
  };
  const client2 = {
    heartbeatTimestampMs: 3000,
    headHash: fakeHash('eadc1e2'),
    mutationID: 100,
    lastServerAckdMutationID: 90,
  };
  const clientMap = new Map(
    Object.entries({
      client1,
      client2,
    }),
  );

  await setClientsForTesting(clientMap, dagStore);

  const TICK_IN_MS = 20000;
  clock.tick(TICK_IN_MS);

  await writeHeartbeat('client1', dagStore);
  await withRead(dagStore, async (read: dag.Read) => {
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
  const controller = new AbortController();
  startHeartbeats(
    'client1',
    dagStore,
    onClientStateNotFound,
    new LogContext(),
    controller.signal,
  );
  await clock.tickAsync(ONE_MIN_IN_MS);
  expect(onClientStateNotFound.callCount).to.equal(1);
  controller.abort();
});

test('heartbeat with dropped idb throws', async () => {
  const {resolve, promise} = resolver();
  const consoleErrorStub = sinon.stub(console, 'error').callsFake(() => {
    resolve();
  });
  const name = `heartbeat-test-dropped-idb-${Math.random()}`;
  const ibdStore = new IDBStore(name);
  const dagStore = new dag.StoreImpl(ibdStore, dag.uuidChunkHasher, assertHash);
  const onClientStateNotFound = sinon.fake();
  const controller = new AbortController();

  startHeartbeats(
    'client1',
    dagStore,
    onClientStateNotFound,
    new LogContext(),
    controller.signal,
  );

  await clock.tickAsync(ONE_MIN_IN_MS / 2);

  await dropIDBStore(name);

  await clock.tickAsync(ONE_MIN_IN_MS / 2);

  expect(onClientStateNotFound.callCount).to.equal(0);

  await promise;

  expect(consoleErrorStub.callCount).to.equal(1);
  expect(consoleErrorStub.args[0][2]).to.be.instanceOf(IDBNotFoundError);
  expect(consoleErrorStub.args[0][2].message).equal(
    `Replicache IndexedDB not found: ${name}`,
  );

  controller.abort();
});
