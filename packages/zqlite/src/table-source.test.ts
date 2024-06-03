import {describe, test} from 'vitest';

test('add', () => {});
test('delete', () => {});
describe('message upstream', () => {
  test('bare select', () => {});
  test('not consuming the entire iterator', () => {
    // 1. huge table. Get `1` perf about same as `limit 1`
    // 2. re-query the same source after early bail on `iterate`
  });
  test('hoisted conditions', () => {});
  test('different ordering', () => {});
});
