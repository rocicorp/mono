import {expect, test} from 'vitest';
import {RefCount} from './ref-count.ts';

test('inc increases reference count and returns true on first add', () => {
  const rc = new RefCount();
  const obj1 = {};
  const obj2 = {};

  // First addition should return true
  expect(rc.inc(obj1)).toBe(true);

  // Second addition should return false
  expect(rc.inc(obj1)).toBe(false);

  // Different object should return true on first add
  expect(rc.inc(obj2)).toBe(true);
});

test('dec decreases reference count', () => {
  const rc = new RefCount();
  const obj = {};

  // Add twice
  rc.inc(obj);
  rc.inc(obj);

  // First decrease should return false (not removed)
  expect(rc.dec(obj)).toBe(false);

  // Second decrease should return true (removed)
  expect(rc.dec(obj)).toBe(true);
});

test('dec throws when trying to decrease non-existent object', () => {
  const rc = new RefCount();
  const obj = {};

  // Trying to decrease a non-existent object should throw
  expect(() => rc.dec(obj)).toThrow();
});
