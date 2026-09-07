import {expect, test} from 'vitest';
import {
  DEFAULT_CIRCUIT_BREAKER_OPEN_MS,
  HydrationCircuitBreaker,
} from './hydration-circuit-breaker.ts';

test('disabled breaker never exceeds and never opens', () => {
  let now = 0;
  const breaker = new HydrationCircuitBreaker(0, 1000, () => now);

  expect(breaker.enabled).toBe(false);
  expect(breaker.exceeded(Number.MAX_SAFE_INTEGER)).toBe(false);

  breaker.trip('h1');
  now = 10;
  expect(breaker.isOpen('h1')).toBe(true);
});

test('exceeded is inclusive of the timeout', () => {
  const breaker = new HydrationCircuitBreaker(100, 1000, () => 0);

  expect(breaker.enabled).toBe(true);
  expect(breaker.exceeded(99.9)).toBe(false);
  expect(breaker.exceeded(100)).toBe(true);
  expect(breaker.exceeded(1000)).toBe(true);
});

test('a tripped hash is open until the cooldown elapses', () => {
  let now = 50;
  const breaker = new HydrationCircuitBreaker(100, 1000, () => now);

  expect(breaker.isOpen('h1')).toBe(false);
  breaker.trip('h1');
  expect(breaker.isOpen('h1')).toBe(true);
  expect(breaker.isOpen('h2')).toBe(false);

  now = 1049;
  expect(breaker.isOpen('h1')).toBe(true);
  now = 1050;
  expect(breaker.isOpen('h1')).toBe(false);
  // The expired entry was dropped, so it does not come back.
  now = 0;
  expect(breaker.isOpen('h1')).toBe(false);
});

test('tripping again restarts the cooldown', () => {
  let now = 0;
  const breaker = new HydrationCircuitBreaker(100, 1000, () => now);

  breaker.trip('h1');
  now = 900;
  breaker.trip('h1');
  now = 1500;
  expect(breaker.isOpen('h1')).toBe(true);
  now = 1900;
  expect(breaker.isOpen('h1')).toBe(false);
});

test('defaults to a five minute cooldown', () => {
  expect(DEFAULT_CIRCUIT_BREAKER_OPEN_MS).toBe(300_000);
  expect(new HydrationCircuitBreaker(100).openMs).toBe(300_000);
});

test('rejects invalid timeouts', () => {
  expect(() => new HydrationCircuitBreaker(-1)).toThrow();
  expect(() => new HydrationCircuitBreaker(1.5)).toThrow();
  expect(() => new HydrationCircuitBreaker(100, -1)).toThrow();
});
