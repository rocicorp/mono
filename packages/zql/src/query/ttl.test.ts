import {expect, test} from 'vitest';
import {parseTTL} from './ttl.ts';

test.each([
  ['none', 0],
  ['forever', -1],
  [0, 0],
  [-0, -0],
  [Infinity, -1],
  [-Infinity, -1],
  [NaN, 0],
  [-0.5, -1],
  [1, 1],
  ['1s', 1000],
  ['1m', 60 * 1000],
  ['1h', 60 * 60 * 1000],
  ['1d', 24 * 60 * 60 * 1000],
  ['1y', 365 * 24 * 60 * 60 * 1000],
  ['1.5s', 1500],
  ['1.5m', 1.5 * 60 * 1000],
  ['1.5h', 1.5 * 60 * 60 * 1000],
  ['1.5d', 1.5 * 24 * 60 * 60 * 1000],
  ['1.5y', 1.5 * 365 * 24 * 60 * 60 * 1000],
] as const)('parseTTL(%o) === %i', (ttl, expected) => {
  expect(parseTTL(ttl)).toBe(expected);
});
