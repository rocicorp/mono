import {describe, expect, test} from 'vitest';
import {HeadIndexedQueue} from './head-indexed-queue.ts';

describe('HeadIndexedQueue', () => {
  test('keeps FIFO order across many shifts without exposing compacted slots', () => {
    const queue = new HeadIndexedQueue<number>();
    for (let i = 0; i < 2050; i++) {
      queue.push(i);
    }

    for (let i = 0; i < 1500; i++) {
      expect(queue.shift()).toBe(i);
    }
    queue.push(2050);

    expect(queue.size).toBe(551);
    expect(queue.peek()).toBe(1500);
    expect(queue.last()).toBe(2050);
    expect(queue.toArray()).toEqual(
      Array.from({length: 551}, (_, i) => i + 1500),
    );
  });

  test('delete and replace operations skip holes left in the active window', () => {
    const queue = new HeadIndexedQueue<string>();
    queue.push('a');
    queue.push('b');
    queue.push('c');
    queue.push('d');

    expect(queue.deleteFirst('a')).toBe(true);
    expect(queue.deleteMatching(value => value === 'c')).toBe(1);
    queue.replaceLast('D');

    expect(queue.size).toBe(2);
    expect(queue.peek()).toBe('b');
    expect(queue.last()).toBe('D');
    expect(queue.toArray()).toEqual(['b', 'D']);
    expect(queue.shift()).toBe('b');
    expect(queue.shift()).toBe('D');
    expect(queue.shift()).toBeUndefined();
  });

  test('stores undefined values without confusing them with deleted slots', () => {
    const queue = new HeadIndexedQueue<string | undefined>();
    queue.push(undefined);
    queue.push('next');

    expect(queue.size).toBe(2);
    expect(queue.shift()).toBeUndefined();
    expect(queue.size).toBe(1);
    expect(queue.shift()).toBe('next');
    expect(queue.size).toBe(0);
  });
});
