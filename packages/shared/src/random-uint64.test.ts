import {afterEach, expect, test, vi} from 'vitest';
import {randomUint64} from './random-uint64.ts';

afterEach(() => {
  vi.unstubAllGlobals();
});

test('combines two 32-bit words from crypto.getRandomValues', () => {
  vi.stubGlobal('crypto', {
    getRandomValues: (array: Uint32Array) => {
      array[0] = 0x12345678;
      array[1] = 0x9abcdef0;
      return array;
    },
  });
  expect(randomUint64()).toBe(0x123456789abcdef0n);
});

test('produces 64-bit values', () => {
  const seen = new Set<bigint>();
  for (let i = 0; i < 100; i++) {
    const n = randomUint64();
    expect(n).toBeGreaterThanOrEqual(0n);
    expect(n).toBeLessThan(1n << 64n);
    seen.add(n);
  }
  expect(seen.size).toBe(100);
});
