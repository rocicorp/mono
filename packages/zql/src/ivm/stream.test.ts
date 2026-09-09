import {describe, expect, test} from 'vitest';
import {firstPull, type PullStream, type Stream, take} from './stream.ts';

describe('take', () => {
  test('take the first n elements from the stream', () => {
    const stream: Stream<number> = [1, 2, 3, 4, 5];
    const result = [...take(stream, 3)];
    expect(result).toEqual([1, 2, 3]);
  });

  test('return an empty array if limit is less than 1', () => {
    const stream: Stream<number> = [1, 2, 3, 4, 5];
    const result = [...take(stream, 0)];
    expect(result).toEqual([]);
  });

  test('return the entire stream if limit is greater than stream length', () => {
    const stream: Stream<number> = [1, 2, 3];
    const result = [...take(stream, 5)];
    expect(result).toEqual([1, 2, 3]);
  });
});

describe('firstPull', () => {
  const tracked = (values: number[]) => {
    let i = 0;
    let closed = false;
    const stream: PullStream<number> = {
      next: () => (i < values.length ? values[i++] : undefined),
      close: () => {
        closed = true;
      },
    };
    return {
      stream,
      pulls: () => i,
      closed: () => closed,
    };
  };

  test('yields one value and closes the source', () => {
    const src = tracked([1, 2, 3]);
    const first = firstPull(src.stream);
    expect(first.next()).toBe(1);
    expect(src.pulls()).toBe(1);
    expect(src.closed()).toBe(true);
    expect(first.next()).toBe(undefined);
    expect(src.pulls()).toBe(1);
  });

  test('closes the source when the consumer never pulls', () => {
    const src = tracked([1, 2, 3]);
    firstPull(src.stream).close();
    expect(src.pulls()).toBe(0);
    expect(src.closed()).toBe(true);
  });

  test('handles an empty source', () => {
    const src = tracked([]);
    expect(firstPull(src.stream).next()).toBe(undefined);
    expect(src.closed()).toBe(true);
  });
});
