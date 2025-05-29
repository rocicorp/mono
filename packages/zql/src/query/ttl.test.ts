import {expect, test} from 'vitest';
import {compareTTL, normalizeTTL, parseTTL} from './ttl.ts';

test.each([
  [0, 0],
  [-0, -0],
  [Infinity, -1],
  [-Infinity, -1],
  [NaN, 0],
  [-0.5, -1],
  [1, 1],
  ['1s', 1000],
  ['1m', 60 * 1000],
  ['1.5s', 1500],
  ['1.5m', 1.5 * 60 * 1000],
] as const)('parseTTL(%o) === %i', (ttl, expected) => {
  expect(parseTTL(ttl)).toBe(expected);
});

test.each([
  ['none', 'none', 0],
  ['none', 0, 0],
  [1, 2, -1],
  [1000, '1s', 0],
  ['1s', '1m', -59 * 1000],
] as const)('compareTTL(%o, %o) === %i', (a, b, expected) => {
  expect(compareTTL(a, a)).toBe(0);
  expect(compareTTL(b, b)).toBe(0);
  expect(compareTTL(a, b)).toBe(expected);
  const neg = expected === 0 ? 0 : -expected;
  expect(compareTTL(b, a)).toBe(neg);
});

test.each([
  ['none', 'none'],
  [0, 'none'],
  [-1, 'forever'],
  [1, 1],
  [1000, '1s'],
  [60 * 1000, '1m'],
  [60 * 60 * 1000, '1h'],
  [24 * 60 * 60 * 1000, '1d'],
  [365 * 24 * 60 * 60 * 1000, '1y'],
  [1500, 1500],
  [1.5 * 60 * 1000, '90s'],
  [1.5 * 60 * 60 * 1000, '90m'],
  [1.5 * 24 * 60 * 60 * 1000, '36h'],
  [1.5 * 365 * 24 * 60 * 60 * 1000, '1.5y'],
  [1.25 * 365 * 24 * 60 * 60 * 1000, '1.25y'],
] as const)('normalizeTTL(%o) === %o', (ttl, expected) => {
  expect(normalizeTTL(ttl)).toBe(expected);
});
