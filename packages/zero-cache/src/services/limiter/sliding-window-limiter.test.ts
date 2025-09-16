/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {afterEach, beforeEach, expect, test, vi} from 'vitest';
import {SlidingWindowLimiter} from './sliding-window-limiter.ts';

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

test('all mutations occur at prior window start', () => {
  vi.setSystemTime(10);

  const limiter = new SlidingWindowLimiter(10, 10);
  for (let i = 0; i < 10; i++) {
    expect(limiter.canDo()).toBe(true);
  }

  // 11th call fails
  expect(limiter.canDo()).toBe(false);
  // and 12th of course
  expect(limiter.canDo()).toBe(false);

  // failed limiter calls do not bump the count
  expect(limiter.totalCallsForTime(10)).toBe(10);
});

// Sliding window setup should look like:
// |----|----|
//|----|
test('all mutations occur at prior window end', () => {
  // prior window end is start + windowSizeMs - 1
  vi.setSystemTime(9);

  const limiter = new SlidingWindowLimiter(10, 10);
  for (let i = 0; i < 10; i++) {
    expect(limiter.canDo()).toBe(true);
  }

  // 11th call fails
  expect(limiter.canDo()).toBe(false);
});

test('fill the window then slide the window', () => {
  vi.setSystemTime(9);

  const limiter = new SlidingWindowLimiter(10, 10);
  for (let i = 0; i < 10; i++) {
    expect(limiter.canDo()).toBe(true);
  }

  expect(limiter.totalCallsForTime(9)).toBe(10);

  // sliding out of the past with no new writes should decimate the count
  for (let i = 0; i < 10; i++) {
    expect(limiter.totalCallsForTime(10 + i)).toBe(10 - i - 1);
  }

  // sliding into the future while writing should keep the count constant
  for (let i = 0; i < 10; i++) {
    vi.setSystemTime(10 + i);
    limiter.canDo();
    expect(limiter.totalCallsForTime(10 + i)).toBe(10);
  }
});

test('all mutations occur at next window start', () => {
  vi.setSystemTime(0);

  const limiter = new SlidingWindowLimiter(10, 10);
  vi.setSystemTime(10);

  for (let i = 0; i < 10; i++) {
    expect(limiter.canDo()).toBe(true);
  }

  // 11th call fails
  expect(limiter.canDo()).toBe(false);
});
test('all mutations occur at next window end', () => {
  vi.setSystemTime(0);

  const limiter = new SlidingWindowLimiter(10, 10);
  vi.setSystemTime(19);

  for (let i = 0; i < 10; i++) {
    expect(limiter.canDo()).toBe(true);
  }

  // 11th call fails
  expect(limiter.canDo()).toBe(false);
});
