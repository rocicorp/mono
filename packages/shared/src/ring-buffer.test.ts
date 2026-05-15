import {describe, expect, test} from 'vitest';
import {RingBuffer} from './ring-buffer.ts';

describe('RingBuffer', () => {
  test('push and shift in FIFO order', () => {
    const buf = new RingBuffer<string>();
    buf.push('a');
    buf.push('b');
    buf.push('c');
    expect(buf.size).toBe(3);
    expect(buf.shift()).toBe('a');
    expect(buf.shift()).toBe('b');
    expect(buf.shift()).toBe('c');
    expect(buf.size).toBe(0);
    expect(buf.shift()).toBeUndefined();
  });

  test('grows when capacity is exceeded', () => {
    const buf = new RingBuffer<number>(16);
    for (let i = 0; i < 100; i++) {
      buf.push(i);
    }
    expect(buf.size).toBe(100);
    for (let i = 0; i < 100; i++) {
      expect(buf.shift()).toBe(i);
    }
    expect(buf.size).toBe(0);
  });

  test('wraps around correctly', () => {
    // Use minimum capacity (16) and cycle through to force wrapping
    const buf = new RingBuffer<number>(16);
    // Fill half
    for (let i = 0; i < 10; i++) buf.push(i);
    // Remove some to advance head
    for (let i = 0; i < 8; i++) expect(buf.shift()).toBe(i);
    // Now head is at index 8, push more to wrap around
    for (let i = 10; i < 25; i++) buf.push(i);
    // Verify FIFO order
    for (let i = 8; i < 25; i++) {
      expect(buf.shift()).toBe(i);
    }
    expect(buf.size).toBe(0);
  });

  test('drain returns all elements in order', () => {
    const buf = new RingBuffer<number>(16);
    // Advance head to force wrap-around
    for (let i = 0; i < 10; i++) buf.push(i);
    for (let i = 0; i < 8; i++) buf.shift();
    for (let i = 10; i < 18; i++) buf.push(i);

    expect(buf.size).toBe(10);
    const drained = buf.drain();
    expect(drained).toEqual([8, 9, 10, 11, 12, 13, 14, 15, 16, 17]);
    expect(buf.size).toBe(0);
  });

  test('delete removes matching elements', () => {
    const buf = new RingBuffer<string>();
    buf.push('a');
    buf.push('b');
    buf.push('a');
    buf.push('c');
    buf.push('a');
    expect(buf.delete('a')).toBe(3);
    expect(buf.size).toBe(2);
    expect(buf.shift()).toBe('b');
    expect(buf.shift()).toBe('c');
  });

  test('delete with identity equality', () => {
    const obj1 = {id: 1};
    const obj2 = {id: 1}; // same shape, different identity
    const buf = new RingBuffer<{id: number}>();
    buf.push(obj1);
    buf.push(obj2);
    expect(buf.delete(obj1)).toBe(1);
    expect(buf.size).toBe(1);
    expect(buf.shift()).toBe(obj2);
  });

  test('delete returns 0 when nothing matches', () => {
    const buf = new RingBuffer<string>();
    buf.push('a');
    buf.push('b');
    expect(buf.delete('c')).toBe(0);
    expect(buf.size).toBe(2);
  });

  test('empty buffer operations', () => {
    const buf = new RingBuffer<number>();
    expect(buf.size).toBe(0);
    expect(buf.shift()).toBeUndefined();
    expect(buf.drain()).toEqual([]);
    expect(buf.delete(42)).toBe(0);
  });

  test('interleaved push/shift maintains order', () => {
    const buf = new RingBuffer<number>();
    const values: number[] = [];
    for (let i = 0; i < 1000; i++) {
      buf.push(i);
      if (i % 3 === 0) {
        values.push(buf.shift()!);
      }
    }
    // Drain remaining
    while (buf.size > 0) {
      values.push(buf.shift()!);
    }
    // All values should be 0..999 in order
    expect(values).toEqual(Array.from({length: 1000}, (_, i) => i));
  });

  test('large buffer drains efficiently (O(n) not O(n^2))', () => {
    const buf = new RingBuffer<number>();
    const n = 100_000;
    for (let i = 0; i < n; i++) {
      buf.push(i);
    }
    expect(buf.size).toBe(n);

    const start = performance.now();
    let sum = 0;
    for (let i = 0; i < n; i++) {
      sum += buf.shift()!;
    }
    const elapsed = performance.now() - start;

    expect(sum).toBe((n * (n - 1)) / 2);
    expect(buf.size).toBe(0);

    // With O(1) ring buffer shift, 100k items takes ~2ms.
    expect(elapsed).toBeLessThan(200);
  });
});
