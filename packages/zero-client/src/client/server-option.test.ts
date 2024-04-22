import {expect, test} from 'vitest';
import {getServer} from './server-option.js';

test('getServer', () => {
  expect(getServer(null)).equal(null);
  expect(getServer(undefined)).equal(null);

  expect(getServer('http://myapp-myteam.zero.ms/')).equal(
    'http://myapp-myteam.zero.ms/',
  );
  expect(getServer('https://myapp-myteam.zero.ms')).equal(
    'https://myapp-myteam.zero.ms/',
  );
  expect(getServer('http://myapp-myteam.zero.ms')).equal(
    'http://myapp-myteam.zero.ms/',
  );

  const expectError = (server: string, expectedError: string) => {
    expect(() => getServer(server)).to.throw(expectedError);
  };

  expectError(
    'myapp-myteam.zero.ms',
    `ZeroOptions.server must use the "http" or "https" scheme.`,
  );
  expectError(
    'https://myapp-myteam.zero.ms/x',
    'ZeroOptions.server must not contain a path component (other than "/"). For example: "https://myapp-myteam.zero.ms/".',
  );
  expectError(
    'https://myapp-myteam.zero.ms/x/',
    'ZeroOptions.server must not contain a path component (other than "/"). For example: "https://myapp-myteam.zero.ms/".',
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
