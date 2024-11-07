import {expect, test} from 'vitest';
import {
  appendPath,
  assertHTTPString,
  assertWSString,
  toHTTPString,
  toWSString,
} from './http-string.js';

test('toWSString', () => {
  expect(toWSString('http://example.com')).equal('ws://example.com');
  expect(toWSString('https://example.com')).equal('wss://example.com');
});

test('toHTTPString', () => {
  expect(toHTTPString('ws://example.com')).equal('http://example.com');
  expect(toHTTPString('wss://example.com')).equal('https://example.com');
});

test('assertHTTPString', () => {
  expect(() => assertHTTPString('http://example.com')).not.throw();
  expect(() => assertHTTPString('https://example.com')).not.throw();
  expect(() => assertHTTPString('ws://example.com')).throw();
  expect(() => assertHTTPString('wss://example.com')).throw();
});

test('assertWSString', () => {
  expect(() => assertWSString('ws://example.com')).not.throw();
  expect(() => assertWSString('wss://example.com')).not.throw();
  expect(() => assertWSString('http://example.com')).throw();
  expect(() => assertWSString('https://example.com')).throw();
});

test('appendPath', () => {
  expect(appendPath('http://example.com', '/foo/bar')).toEqual(
    'http://example.com/foo/bar',
  );
  expect(appendPath('wss://example.com', '/foo/bar')).toEqual(
    'wss://example.com/foo/bar',
  );
  expect(appendPath('http://example.com/', '/foo/bar')).toEqual(
    'http://example.com/foo/bar',
  );
  expect(appendPath('http://example.com', '/foo/bar/')).toEqual(
    'http://example.com/foo/bar/',
  );
});
