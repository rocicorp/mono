import {expect, test} from 'vitest';
import {getServer} from './server-option.js';

test('getServer', () => {
  expect(getServer(null, null)).equal(null);

  expect(getServer('https://myapp-myteam.reflect.net/', null)).equal(
    'https://myapp-myteam.reflect.net/',
  );
  expect(getServer('http://myapp-myteam.reflect.net/', null)).equal(
    'http://myapp-myteam.reflect.net/',
  );
  expect(getServer('https://myapp-myteam.reflect.net', null)).equal(
    'https://myapp-myteam.reflect.net/',
  );
  expect(getServer('http://myapp-myteam.reflect.net', null)).equal(
    'http://myapp-myteam.reflect.net/',
  );

  expect(getServer(null, 'wss://myapp-myteam.reflect.net/')).equal(
    'https://myapp-myteam.reflect.net/',
  );
  expect(getServer(null, 'ws://myapp-myteam.reflect.net/')).equal(
    'http://myapp-myteam.reflect.net/',
  );
  expect(getServer(null, 'wss://myapp-myteam.reflect.net')).equal(
    'https://myapp-myteam.reflect.net/',
  );
  expect(getServer(null, 'ws://myapp-myteam.reflect.net')).equal(
    'http://myapp-myteam.reflect.net/',
  );

  expect(
    getServer('https://myapp-myteam.reflect.net/', 'ws://ignore.net'),
  ).equal('https://myapp-myteam.reflect.net/');
  expect(
    getServer('http://myapp-myteam.reflect.net/', 'ws://ignore.net'),
  ).equal('http://myapp-myteam.reflect.net/');
  expect(
    getServer('https://myapp-myteam.reflect.net', 'ws://ignore.net'),
  ).equal('https://myapp-myteam.reflect.net/');
  expect(getServer('http://myapp-myteam.reflect.net', 'ws://ignore.net')).equal(
    'http://myapp-myteam.reflect.net/',
  );

  const expectError = (
    server: string | null,
    socketOrigin: string | null,
    expectedError: string,
  ) => {
    expect(() => getServer(server, socketOrigin)).to.throw(expectedError);
  };

  expectError(
    'myapp-myteam.reflect.net',
    null,
    `ReflectOptions.server must use the "http" or "https" scheme.`,
  );
  expectError(
    null,
    'myapp-myteam.reflect.net',
    `ReflectOptions.socketOrigin must use the "ws" or "wss" scheme.`,
  );

  expectError(
    'https://myapp-myteam.reflect.net/x',
    null,
    'ReflectOptions.server must not contain a path component (other than "/"). For example: "https://myapp-myteam.reflect.net/".',
  );
  expectError(
    null,
    'wss://myapp-myteam.reflect.net/x',
    'ReflectOptions.socketOrigin must not contain a path component (other than "/"). For example: "wss://myapp-myteam.reflect.net/".',
  );

  expectError(
    'https://myapp-myteam.reflect.net/x/',
    null,
    'ReflectOptions.server must not contain a path component (other than "/"). For example: "https://myapp-myteam.reflect.net/".',
  );
  expectError(
    null,
    'wss://myapp-myteam.reflect.net/x/',

    'ReflectOptions.socketOrigin must not contain a path component (other than "/"). For example: "wss://myapp-myteam.reflect.net/".',
  );

  expectError(
    'https://myapp-myteam.reflect.net/?',
    null,
    'ReflectOptions.server must not contain a search component. For example: "https://myapp-myteam.reflect.net/".',
  );

  expectError(
    'https://myapp-myteam.reflect.net/?a',
    null,
    'ReflectOptions.server must not contain a search component. For example: "https://myapp-myteam.reflect.net/".',
  );

  expectError(
    'https://myapp-myteam.reflect.net/#a',
    null,
    'ReflectOptions.server must not contain a hash component. For example: "https://myapp-myteam.reflect.net/".',
  );

  expectError(
    'https://myapp-myteam.reflect.net/#',
    null,
    'ReflectOptions.server must not contain a hash component. For example: "https://myapp-myteam.reflect.net/".',
  );
});
