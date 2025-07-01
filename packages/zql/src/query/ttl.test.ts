import type {LogContext} from '@rocicorp/logger';
import {expect, test, vi} from 'vitest';
import {clampTTL, compareTTL, MAX_TTL, normalizeTTL, parseTTL} from './ttl.ts';

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

test.each([
  ['none', 'none', 0],
  ['none', 'forever', -1],
  ['none', 0, 0],
  ['forever', 'forever', 0],
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
  ['forever', 'forever'],
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

test.each([
  ['none', 0],
  ['forever', 10 * 60 * 1000, true],
  [0, 0],
  [-1, 10 * 60 * 1000, true],
  [1, 1],
  [1000, 1000],
  [10 * 60 * 1000, 10 * 60 * 1000], // Exactly at the max TTL
  [10 * 60 * 1000 + 1, 10 * 60 * 1000, true], // Just above the max TTL
  ['1h', 10 * 60 * 1000, true], // Above max TTL in string format
  ['1m', 60 * 1000], // Below max TTL in string format
] as const)('clampTTL(%o) === %o', (ttl, expected, expectError?) => {
  const mockLogContext = {
    warn: vi.fn(),
  } as unknown as LogContext;
  expect(clampTTL(ttl, mockLogContext)).toBe(expected);
  if (expectError) {
    expect(mockLogContext.warn).toHaveBeenCalledWith(
      `TTL (${ttl}) is too high, clamping to ${MAX_TTL}`,
    );
  } else {
    expect(mockLogContext.warn).not.toHaveBeenCalled();
  }
});
