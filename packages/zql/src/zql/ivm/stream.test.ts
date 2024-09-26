import {describe, expect, test} from 'vitest';
import {first, type Stream, take} from './stream.js';

describe('take', () => {
  test('take the first n elements from the stream', () => {
    const stream: Stream<number> = [1, 2, 3, 4, 5];
    const result = Array.from(take(stream, 3));
    expect(result).toEqual([1, 2, 3]);
  });

  test('return an empty array if limit is less than 1', () => {
    const stream: Stream<number> = [1, 2, 3, 4, 5];
    const result = Array.from(take(stream, 0));
    expect(result).toEqual([]);
  });

  test('return the entire stream if limit is greater than stream length', () => {
    const stream: Stream<number> = [1, 2, 3];
    const result = Array.from(take(stream, 5));
    expect(result).toEqual([1, 2, 3]);
  });
});

describe('first', () => {
  test('return the first element of the stream', () => {
    const stream: Stream<number> = [1, 2, 3, 4, 5];
    const result = first(stream);
    expect(result).toBe(1);
  });

  test('return undefined if the stream is empty', () => {
    const stream: Stream<number> = [];
    const result = first(stream);
    expect(result).toBeUndefined();
  });
});
