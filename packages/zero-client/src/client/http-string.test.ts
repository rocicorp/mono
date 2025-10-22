import {expect, test} from 'vitest';
import {
  appendPath,
  assertHTTPString,
  assertWSString,
  toHTTPString,
  toWSString,
} from './http-string.ts';

test('toWSString', () => {
  expect(toWSString('http://example.com')).toBe('ws://example.com');
  expect(toWSString('https://example.com')).toBe('wss://example.com');
});

test('toHTTPString', () => {
  expect(toHTTPString('ws://example.com')).toBe('http://example.com');
  expect(toHTTPString('wss://example.com')).toBe('https://example.com');
});

test('assertHTTPString', () => {
  expect(() => assertHTTPString('http://example.com')).not.toThrow();
  expect(() => assertHTTPString('https://example.com')).not.toThrow();
  expect(() => assertHTTPString('ws://example.com')).toThrow();
  expect(() => assertHTTPString('wss://example.com')).toThrow();
});

test('assertWSString', () => {
  expect(() => assertWSString('ws://example.com')).not.toThrow();
  expect(() => assertWSString('wss://example.com')).not.toThrow();
  expect(() => assertWSString('http://example.com')).toThrow();
  expect(() => assertWSString('https://example.com')).toThrow();
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
