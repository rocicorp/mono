import {afterEach, beforeEach, expect, test, vi} from 'vitest';
import * as valita from '../../../shared/src/valita.ts';
import {decodeSecProtocols} from '../../../zero-protocol/src/connect.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {updateAuthMessageSchema} from '../../../zero-protocol/src/update-auth.ts';
import {ConnectionStatus} from './connection-status.ts';
import {MockSocket, zeroForTest} from './test-utils.ts';

beforeEach(() => {
  vi.stubGlobal('WebSocket', MockSocket as unknown as typeof WebSocket);
});

afterEach(() => {
  vi.restoreAllMocks();
});

test('connect({auth: null}) does not send auth', async () => {
  const z = zeroForTest();
  await z.triggerConnected();

  const socket = await z.socket;
  socket.messages.length = 0;

  await z.connection.connect({auth: null});

  expect(socket.messages).toHaveLength(0);
});

test('connect({auth}) sends updateAuth set', async () => {
  const z = zeroForTest();
  await z.triggerConnected();

  const socket = await z.socket;
  socket.messages.length = 0;

  await z.connection.connect({auth: 'next-token'});

  expect(socket.messages).toHaveLength(1);
  const msg = valita.parse(
    JSON.parse(socket.messages[0]),
    updateAuthMessageSchema,
  );
  expect(msg).toEqual(['updateAuth', {auth: 'next-token'}]);
});

test('sends updateAuth even when auth changes', async () => {
  const z = zeroForTest({auth: 'test-auth'});
  await z.triggerConnected();

  const socket = await z.socket;
  socket.messages.length = 0;

  await z.connection.connect({auth: 'test-auth'});

  expect(socket.messages).toHaveLength(1);
  const msg = valita.parse(
    JSON.parse(socket.messages[0]),
    updateAuthMessageSchema,
  );
  expect(msg).toEqual(['updateAuth', {auth: 'test-auth'}]);
});

test('auth set while needs-auth is carried in next handshake', async () => {
  const z = zeroForTest({auth: 'initial-token'});
  await z.triggerConnected();

  await z.triggerError({
    kind: ErrorKind.Unauthorized,
    message: 'auth error',
    origin: ErrorOrigin.ZeroCache,
  });
  await z.waitForConnectionStatus(ConnectionStatus.NeedsAuth);

  await z.connection.connect({auth: 'next-token'});
  const socket = await z.socket;
  expect(decodeSecProtocols(socket.protocol).authToken).toBe('next-token');
});

test('updated auth is sent once and reused for later reconnect handshakes', async () => {
  const z = zeroForTest({auth: 'initial-token'});
  await z.triggerConnected();

  await z.triggerError({
    kind: ErrorKind.Unauthorized,
    message: 'auth error',
    origin: ErrorOrigin.ZeroCache,
  });
  await z.waitForConnectionStatus(ConnectionStatus.NeedsAuth);

  await z.connection.connect({auth: 'token-1'});
  let socket = await z.socket;
  expect(decodeSecProtocols(socket.protocol).authToken).toBe('token-1');

  await z.triggerConnected();
  socket.messages.length = 0;

  await z.connection.connect({auth: 'token-2'});

  expect(socket.messages).toHaveLength(1);
  const msg = valita.parse(
    JSON.parse(socket.messages[0]),
    updateAuthMessageSchema,
  );
  expect(msg).toEqual(['updateAuth', {auth: 'token-2'}]);

  await z.triggerError({
    kind: ErrorKind.Unauthorized,
    message: 'auth error again',
    origin: ErrorOrigin.ZeroCache,
  });
  await z.waitForConnectionStatus(ConnectionStatus.NeedsAuth);

  await z.connection.connect();
  socket = await z.socket;
  expect(decodeSecProtocols(socket.protocol).authToken).toBe('token-2');
});
