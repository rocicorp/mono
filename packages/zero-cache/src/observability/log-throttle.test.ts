import {expect, test} from 'vitest';
import {LogThrottle} from './log-throttle.ts';

function clock() {
  let now = 0;
  return {
    now: () => now,
    advance: (ms: number) => {
      now += ms;
    },
  };
}

test('admits a key once per window and reports suppressed occurrences', () => {
  const c = clock();
  const throttle = new LogThrottle({windowMs: 1000, now: c.now});

  expect(throttle.admit('a')).toBe(0);
  expect(throttle.admit('a')).toBeUndefined();
  c.advance(999);
  expect(throttle.admit('a')).toBeUndefined();

  // Other keys are independent.
  expect(throttle.admit('b')).toBe(0);

  c.advance(1);
  expect(throttle.admit('a')).toBe(2);
  expect(throttle.admit('a')).toBeUndefined();
  c.advance(1000);
  expect(throttle.admit('a')).toBe(1);
});

test('windowMs of 0 admits every occurrence', () => {
  const c = clock();
  const throttle = new LogThrottle({windowMs: 0, now: c.now});
  expect(throttle.admit('a')).toBe(0);
  expect(throttle.admit('a')).toBe(0);
  expect(throttle.admit('a')).toBe(0);
});

test('caps admissions per window across keys', () => {
  const c = clock();
  const throttle = new LogThrottle({
    windowMs: 1000,
    maxPerWindow: 2,
    now: c.now,
  });

  expect(throttle.admit('a')).toBe(0);
  expect(throttle.admit('b')).toBe(0);
  // Over budget: new and known keys alike are suppressed.
  expect(throttle.admit('c')).toBeUndefined();
  expect(throttle.admit('c')).toBeUndefined();
  expect(throttle.admit('a')).toBeUndefined();

  c.advance(1000);
  // The occurrences suppressed by the budget are reported with the next
  // admission of the key.
  expect(throttle.admit('c')).toBe(2);
  expect(throttle.admit('a')).toBe(1);
  expect(throttle.admit('b')).toBeUndefined();
});

test('forgets the least recently admitted key beyond maxKeys', () => {
  const c = clock();
  const throttle = new LogThrottle({windowMs: 1000, maxKeys: 2, now: c.now});

  expect(throttle.admit('a')).toBe(0);
  expect(throttle.admit('b')).toBe(0);
  expect(throttle.admit('a')).toBeUndefined();
  expect(throttle.admit('c')).toBe(0);

  // 'a' was evicted to make room for 'c', so it is admitted again (and its
  // suppressed count is lost).
  expect(throttle.admit('a')).toBe(0);
  // Which in turn evicted 'b'.
  expect(throttle.admit('b')).toBe(0);
  expect(throttle.admit('a')).toBeUndefined();
});
