import {afterEach, beforeEach, expect, test, vi} from 'vitest';
import {MockSocket, zeroForTest} from './test-utils.ts';
import {ConnectionStatus} from './connection-status.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {decodeSecProtocols} from '../../../zero-protocol/src/connect.ts';
import {sleep} from '../../../shared/src/sleep.ts';

beforeEach(() => {
  vi.stubGlobal('WebSocket', MockSocket as unknown as typeof WebSocket);
});

afterEach(() => {
  vi.restoreAllMocks();
});

test('repro run-loop error->connect race passes with sleep', async () => {
  const z = zeroForTest({auth: 'initial-token'});

  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);

  // Trigger a non-auth error
  await z.triggerError({
    kind: ErrorKind.Internal,
    message: 'internal error',
    origin: ErrorOrigin.ZeroCache,
  });
  await z.waitForConnectionStatus(ConnectionStatus.Error);

  await sleep(1);

  // Reconnect without providing auth opts - should keep existing auth
  // never resolves
  await z.connection.connect();
  const currentSocket = await z.socket;
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(
    'initial-token',
  );
  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);
});

test('repro run-loop error->connect race using state.subscribe passes with sleep', async () => {
  const z = zeroForTest({auth: 'initial-token'});

  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);

  let connectPromise: Promise<void> | undefined = undefined;
  z.connection.state.subscribe(async state => {
    if (state.name === ConnectionStatus.Error) {
      await sleep(1);
      connectPromise = z.connection.connect();
    }
  });

  // Trigger a non-auth error
  await z.triggerError({
    kind: ErrorKind.Internal,
    message: 'internal error',
    origin: ErrorOrigin.ZeroCache,
  });
  await z.waitForConnectionStatus(ConnectionStatus.Error);

  // Reconnect without providing auth opts - should keep existing auth
  await vi.waitFor(() => expect(connectPromise).toBeDefined());
  await connectPromise;
  const currentSocket = await z.socket;
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(
    'initial-token',
  );
  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);
});

test('repro run-loop error->connect race timesout without sleep', async () => {
  const z = zeroForTest({auth: 'initial-token'});

  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);

  // Trigger a non-auth error
  await z.triggerError({
    kind: ErrorKind.Internal,
    message: 'internal error',
    origin: ErrorOrigin.ZeroCache,
  });
  await z.waitForConnectionStatus(ConnectionStatus.Error);

  // Reconnect without providing auth opts - should keep existing auth
  await z.connection.connect();
  await sleep(1000);
  await Promise.resolve();
  const currentSocket = await z.socket;
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(
    'initial-token',
  );
  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);
});

test('repro run-loop error->connect race using state.subscribe timesout without sleep', async () => {
  const z = zeroForTest({auth: 'initial-token'});

  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);

  let connectPromise: Promise<void> | undefined = undefined;
  z.connection.state.subscribe(state => {
    if (state.name === ConnectionStatus.Error) {
      connectPromise = z.connection.connect();
    }
  });

  // Trigger a non-auth error
  await z.triggerError({
    kind: ErrorKind.Internal,
    message: 'internal error',
    origin: ErrorOrigin.ZeroCache,
  });
  await z.waitForConnectionStatus(ConnectionStatus.Error);

  // Reconnect without providing auth opts - should keep existing auth
  await vi.waitFor(() => expect(connectPromise).toBeDefined());
  await connectPromise;
  const currentSocket = await z.socket;
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(
    'initial-token',
  );
  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);
});
