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

test('run-loop error->connect race', async () => {
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

test('run-loop error->connect race using state.subscribe', async () => {
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

test('run-loop needs-auth->connect race', async () => {
  const z = zeroForTest({auth: 'initial-token'});

  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);

  // Trigger auth error
  await z.triggerError({
    kind: ErrorKind.Unauthorized,
    message: 'auth error',
    origin: ErrorOrigin.ZeroCache,
  });
  await z.waitForConnectionStatus(ConnectionStatus.NeedsAuth);

  await z.connection.connect({auth: 'next-token'});
  const currentSocket = await z.socket;
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(
    'next-token',
  );
  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);
});

test('run-loop needs-auth->connect race using state.subscribe', async () => {
  const z = zeroForTest({auth: 'initial-token'});

  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);

  let connectPromise: Promise<void> | undefined = undefined;
  z.connection.state.subscribe(state => {
    if (state.name === ConnectionStatus.NeedsAuth) {
      connectPromise = z.connection.connect({auth: 'next-token'});
    }
  });

  // Trigger auth error
  await z.triggerError({
    kind: ErrorKind.Unauthorized,
    message: 'auth error',
    origin: ErrorOrigin.ZeroCache,
  });
  await z.waitForConnectionStatus(ConnectionStatus.NeedsAuth);

  await vi.waitFor(() => expect(connectPromise).toBeDefined());
  await connectPromise;
  const currentSocket = await z.socket;
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(
    'next-token',
  );
  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);
});

test('connect without auth then auth before needs-auth', async () => {
  const z = zeroForTest({auth: undefined});

  await z.connection.connect();
  const initialSocket = await z.socket;
  expect(decodeSecProtocols(initialSocket.protocol).authToken).toBe(undefined);

  await z.connection.connect({auth: 'next-token'});

  // Trigger auth error
  await z.triggerError({
    kind: ErrorKind.Unauthorized,
    message: 'auth error',
    origin: ErrorOrigin.ZeroCache,
  });
  await z.waitForConnectionStatus(ConnectionStatus.NeedsAuth);

  await z.connection.connect({auth: 'next-token'});
  const currentSocket = await z.socket;
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(
    'next-token',
  );
  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);
});

test('connect({auth}) while connecting keeps current socket', async () => {
  const z = zeroForTest({auth: undefined});

  const initialSocket = await z.socket;
  expect(z.connectionStatus).toBe(ConnectionStatus.Connecting);
  expect(decodeSecProtocols(initialSocket.protocol).authToken).toBe(undefined);

  await z.connection.connect({auth: 'next-token'});

  const currentSocket = await z.socket;
  expect(currentSocket).toBe(initialSocket);
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(undefined);
});
