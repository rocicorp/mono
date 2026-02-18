import {LogContext, type LogSink} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {afterEach, beforeEach, expect, test, vi} from 'vitest';
import {assert, assertNotUndefined} from '../../../shared/src/asserts.ts';
import {StoreImpl} from '../dag/store-impl.ts';
import type {Read} from '../dag/store.ts';
import {TestStore} from '../dag/test-store.ts';
import {assertHash, fakeHash, newRandomHash} from '../hash.ts';
import {dropIDBStoreWithMemFallback} from '../kv/idb-store-with-mem-fallback.ts';
import {IDBNotFoundError, IDBStore} from '../kv/idb-store.ts';
import {withRead} from '../with-transactions.ts';
import {
  makeClientV5,
  makeClientV6,
  setClientsForTesting,
} from './clients-test-helpers.ts';
import {
  type ClientMap,
  ClientStateNotFoundError,
  getClients,
} from './clients.ts';
import {
  HEARTBEAT_INTERVAL,
  latestHeartbeatUpdate,
  startHeartbeats,
  writeHeartbeat,
} from './heartbeat.ts';

const START_TIME = 100000;
const ONE_MIN_IN_MS = 60 * 1000;

beforeEach(() => {
  vi.useFakeTimers({now: START_TIME});
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

function awaitLatestHeartbeatUpdate(): Promise<ClientMap> {
  const latest = latestHeartbeatUpdate;
  assertNotUndefined(latest);
  return latest;
}

test('startHeartbeats starts interval that writes heartbeat each minute', async () => {
  const dagStore = new TestStore();
  const client1 = makeClientV6({
    heartbeatTimestampMs: 1000,
    refreshHashes: [fakeHash('eadc1e1')],
  });
  const client2 = makeClientV6({
    heartbeatTimestampMs: 3000,
    refreshHashes: [fakeHash('eadc1e2')],
  });
  const clientMap: ClientMap = new Map(
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
    HEARTBEAT_INTERVAL,
    new LogContext(),
    controller.signal,
  );

  await withRead(dagStore, async (read: Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).toEqual(clientMap);
  });

  await vi.advanceTimersByTimeAsync(ONE_MIN_IN_MS);
  await awaitLatestHeartbeatUpdate();

  await withRead(dagStore, async (read: Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).toEqual(
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

  await vi.advanceTimersByTimeAsync(ONE_MIN_IN_MS);
  await awaitLatestHeartbeatUpdate();

  await withRead(dagStore, async (read: Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).toEqual(
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
  const dagStore = new TestStore();
  const client1 = makeClientV5({
    heartbeatTimestampMs: 1000,
    headHash: fakeHash('eadc1e1'),
  });
  const client2 = makeClientV5({
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
    HEARTBEAT_INTERVAL,
    new LogContext(),
    controller.signal,
  );

  await withRead(dagStore, async (read: Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).toEqual(clientMap);
  });

  await vi.advanceTimersByTimeAsync(ONE_MIN_IN_MS);
  await awaitLatestHeartbeatUpdate();

  await withRead(dagStore, async (read: Read) => {
    const readClientMap = await getClients(read);
    expect(Object.fromEntries(readClientMap)).toEqual({
      client1: {
        ...client1,
        heartbeatTimestampMs: START_TIME + ONE_MIN_IN_MS,
        tempRefreshHash: null,
      },
      client2,
    });
  });

  controller.abort();
  vi.setSystemTime(Date.now() + ONE_MIN_IN_MS);
  await awaitLatestHeartbeatUpdate();

  await withRead(dagStore, async (read: Read) => {
    const readClientMap = await getClients(read);
    expect(Object.fromEntries(readClientMap)).toEqual({
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
  const dagStore = new TestStore();
  const client1 = makeClientV6({
    heartbeatTimestampMs: 1000,
    refreshHashes: [fakeHash('eadc1e1')],
  });
  const client2 = makeClientV6({
    heartbeatTimestampMs: 3000,
    refreshHashes: [fakeHash('eadc1e2')],
  });
  const clientMap: ClientMap = new Map(
    Object.entries({
      client1,
      client2,
    }),
  );

  await setClientsForTesting(clientMap, dagStore);

  const TICK_IN_MS = 20000;
  vi.setSystemTime(Date.now() + TICK_IN_MS);

  await writeHeartbeat('client1', dagStore);
  await withRead(dagStore, async (read: Read) => {
    const readClientMap = await getClients(read);
    expect(readClientMap).toEqual(
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
  const dagStore = new TestStore();
  let e;
  try {
    await writeHeartbeat('client1', dagStore);
  } catch (ex) {
    e = ex;
  }
  expect(e).toBeInstanceOf(ClientStateNotFoundError);
  expect(e).toHaveProperty('id', 'client1');
});

test('heartbeat with missing client calls callback', async () => {
  const dagStore = new TestStore();
  const onClientStateNotFound = vi.fn();
  const controller = new AbortController();
  startHeartbeats(
    'client1',
    dagStore,
    onClientStateNotFound,
    HEARTBEAT_INTERVAL,
    new LogContext(),
    controller.signal,
  );
  await vi.advanceTimersByTimeAsync(ONE_MIN_IN_MS);
  expect(onClientStateNotFound).toBeCalledTimes(1);
  controller.abort();
});

test('heartbeat with dropped idb throws', async () => {
  const {resolve, promise} = resolver();
  const name = `heartbeat-test-dropped-idb-${Math.random()}`;
  const ibdStore = new IDBStore(name);
  const dagStore = new StoreImpl(ibdStore, newRandomHash, assertHash);
  const onClientStateNotFound = vi.fn();
  const controller = new AbortController();

  let message: unknown[] = [];
  const testLogSink: LogSink = {
    log(_level, ...args) {
      message = args;
      resolve();
    },
  };

  startHeartbeats(
    'client1',
    dagStore,
    onClientStateNotFound,
    HEARTBEAT_INTERVAL,
    new LogContext('info', undefined, testLogSink),
    controller.signal,
  );

  await vi.advanceTimersByTimeAsync(ONE_MIN_IN_MS / 2);

  await dropIDBStoreWithMemFallback(name);

  await vi.advanceTimersByTimeAsync(ONE_MIN_IN_MS / 2);

  expect(onClientStateNotFound).not.toBeCalled();

  await promise;

  expect(message).lengthOf(3);
  assert(message[2] instanceof IDBNotFoundError);
  expect(message[2].message).toMatch(/^Expected IndexedDB not found: /);

  controller.abort();
});
