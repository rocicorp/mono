import {describe, expect, test} from 'vitest';
import {average, max, percentile} from './util.ts';

describe('util statistics', () => {
  describe('percentile (nearest rank method)', () => {
    test('returns 0 for empty array', () => {
      expect(percentile([], 50)).toBe(0);
      expect(percentile([], 99)).toBe(0);
    });

    test('returns single element for any percentile', () => {
      expect(percentile([42], 0)).toBe(42);
      expect(percentile([42], 50)).toBe(42);
      expect(percentile([42], 99)).toBe(42);
      expect(percentile([42], 100)).toBe(42);
    });

    test('computes exact nearest-rank percentiles on uniform 1..100 array', () => {
      const values = Array.from({length: 100}, (_, i) => i + 1);
      // For N=100, rank = ceil(p/100 * 100) = p, index = p - 1 -> value = p
      expect(percentile(values, 1)).toBe(1);
      expect(percentile(values, 50)).toBe(50);
      expect(percentile(values, 75)).toBe(75);
      expect(percentile(values, 90)).toBe(90);
      expect(percentile(values, 95)).toBe(95);
      expect(percentile(values, 99)).toBe(99);
      expect(percentile(values, 100)).toBe(100);
    });

    test('computes exact percentiles on small 4-element array', () => {
      const values = [10, 20, 30, 40];
      // p=50: rank = ceil(0.50 * 4) = 2 -> index 1 -> 20
      expect(percentile(values, 50)).toBe(20);
      // p=75: rank = ceil(0.75 * 4) = 3 -> index 2 -> 30
      expect(percentile(values, 75)).toBe(30);
      // p=90: rank = ceil(0.90 * 4) = 4 -> index 3 -> 40
      expect(percentile(values, 90)).toBe(40);
      // p=99: rank = ceil(0.99 * 4) = 4 -> index 3 -> 40
      expect(percentile(values, 99)).toBe(40);
    });

    test('handles unsorted input arrays correctly without mutating original', () => {
      const original = [40, 10, 30, 20];
      const copy = [...original];
      expect(percentile(original, 50)).toBe(20);
      expect(original).toEqual(copy);
    });
  });

  describe('average', () => {
    test('returns 0 for empty array', () => {
      expect(average([])).toBe(0);
    });

    test('computes exact arithmetic mean', () => {
      expect(average([10])).toBe(10);
      expect(average([10, 20, 30])).toBe(20);
      expect(average([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])).toBe(5.5);
    });
  });

  describe('max', () => {
    test('returns 0 for empty array', () => {
      expect(max([])).toBe(0);
    });

    test('finds maximum among positive numbers', () => {
      expect(max([10, 50, 30])).toBe(50);
    });

    test('finds maximum among negative numbers without returning 0 false positive', () => {
      expect(max([-50, -10, -30])).toBe(-10);
    });
  });
});
