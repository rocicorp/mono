import {expect, test} from 'vitest';
import {getServer} from './server-option.ts';

test('getServer', () => {
  expect(getServer(null)).toBe(null);
  expect(getServer(undefined)).toBe(null);

  expect(getServer('http://myapp-myteam.zero.ms/')).toBe(
    'http://myapp-myteam.zero.ms/',
  );
  expect(getServer('https://myapp-myteam.zero.ms')).toBe(
    'https://myapp-myteam.zero.ms/',
  );
  expect(getServer('http://myapp-myteam.zero.ms')).toBe(
    'http://myapp-myteam.zero.ms/',
  );
  expect(getServer('https://myapp-myteam.zero.ms/foo')).toBe(
    'https://myapp-myteam.zero.ms/foo',
  );
  expect(getServer('https://myapp-myteam.zero.ms/foo/')).toBe(
    'https://myapp-myteam.zero.ms/foo/',
  );
  expect(getServer('https://myapp-myteam.zero.ms//')).toBe(
    'https://myapp-myteam.zero.ms//',
  );

  const expectError = (server: string, expectedError: string) => {
    expect(() => getServer(server)).toThrow(expectedError);
  };

  expectError(
    'myapp-myteam.zero.ms',
    `ZeroOptions.server must use the "http" or "https" scheme.`,
  );

  expectError(
    'https://myapp-myteam.zero.ms/foo/bar',
    `ZeroOptions.server may have at most one path component. For example: "https://myapp-myteam.zero.ms/zero".`,
  );
  expectError(
    'https://myapp-myteam.zero.ms/foo//',
    `ZeroOptions.server may have at most one path component. For example: "https://myapp-myteam.zero.ms/zero".`,
  );
  expectError(
    'https://myapp-myteam.zero.ms///',
    `ZeroOptions.server may have at most one path component. For example: "https://myapp-myteam.zero.ms/zero".`,
  );

  expectError(
    'https://myapp-myteam.zero.ms/?',
    'ZeroOptions.server must not contain a search component. For example: "https://myapp-myteam.zero.ms/".',
  );

  expectError(
    'https://myapp-myteam.zero.ms/?a',
    'ZeroOptions.server must not contain a search component. For example: "https://myapp-myteam.zero.ms/".',
  );

  expectError(
    'https://myapp-myteam.zero.ms/#a',
    'ZeroOptions.server must not contain a hash component. For example: "https://myapp-myteam.zero.ms/".',
  );

  expectError(
    'https://myapp-myteam.zero.ms/#',
    'ZeroOptions.server must not contain a hash component. For example: "https://myapp-myteam.zero.ms/".',
  );
});
