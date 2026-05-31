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

test('sends updateAuth even when auth is unchanged', async () => {
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

test('allows logged-out construction without a userID', async () => {
  const z = zeroForTest({
    auth: undefined,
    userID: undefined,
  });

  expect(z.userID).toBeUndefined();

  await z.close();
});

test('rejects constructing a logged-out client with an empty userID', () => {
  expect(() =>
    zeroForTest({
      auth: undefined,
      userID: '',
    }),
  ).toThrow(
    'ZeroOptions.userID should not be empty. Omit it entirely for logged-out clients.',
  );
});

test('warns when constructing a logged-out client with legacy anon userID', async () => {
  const z = zeroForTest({
    auth: undefined,
    userID: 'anon',
  });

  expect(z.userID).toBe('anon');
  expect(
    z.testLogSink.messages.some(
      ([level, _context, args]) =>
        level === 'warn' &&
        args[0] ===
          'ZeroOptions.userID "anon" is deprecated for logged-out clients. Omit it entirely for logged-out clients.',
    ),
  ).toBe(true);

  await z.close();
});

test('does not warn for authenticated users whose userID is anon', async () => {
  const z = zeroForTest({
    auth: 'auth-token',
    userID: 'anon',
  });

  expect(
    z.testLogSink.messages.some(
      ([level, _context, args]) =>
        level === 'warn' &&
        args[0] ===
          'ZeroOptions.userID "anon" is deprecated for logged-out clients. Omit it entirely for logged-out clients.',
    ),
  ).toBe(false);

  await z.close();
});

test('rejects constructing an authenticated client without a userID', () => {
  expect(() =>
    zeroForTest({
      auth: 'auth-token',
      userID: undefined,
    }),
  ).toThrow('ZeroOptions.userID is required when auth is set.');
});

test('connect({auth}) rejects on a logged-out client without a userID', async () => {
  const z = zeroForTest({
    auth: undefined,
    userID: undefined,
  });
  await z.triggerConnected();

  const socket = await z.socket;
  socket.messages.length = 0;

  await expect(z.connection.connect({auth: 'next-token'})).rejects.toThrow(
    'ZeroOptions.userID is required when auth is set.',
  );

  await z.close();
});
