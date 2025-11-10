import {describe, expect, test} from 'vitest';
import {HyperLogLog} from './hyperloglog.ts';

describe('HyperLogLog', () => {
  describe('basic functionality', () => {
    test('empty sketch has zero cardinality', () => {
      const hll = new HyperLogLog();
      expect(hll.count()).toBe(0);
      expect(hll.isEmpty()).toBe(true);
    });

    test('single value has cardinality ~1', () => {
      const hll = new HyperLogLog();
      hll.add('value');
      expect(hll.count()).toBeGreaterThan(0.5);
      expect(hll.count()).toBeLessThan(2);
      expect(hll.isEmpty()).toBe(false);
    });

    test('handles duplicates correctly', () => {
      const hll = new HyperLogLog();
      hll.add('value');
      hll.add('value');
      hll.add('value');
      // Should still estimate ~1
      expect(hll.count()).toBeGreaterThan(0.5);
      expect(hll.count()).toBeLessThan(2);
    });

    test('clear resets sketch to empty', () => {
      const hll = new HyperLogLog();
      hll.add('a');
      hll.add('b');
      hll.add('c');
      expect(hll.isEmpty()).toBe(false);

      hll.clear();
      expect(hll.isEmpty()).toBe(true);
      expect(hll.count()).toBe(0);
    });
  });

  describe('type handling', () => {
    test('handles string values', () => {
      const hll = new HyperLogLog();
      hll.add('alice');
      hll.add('bob');
      hll.add('charlie');
      expectWithinError(hll.count(), 3, 0.2);
    });

    test('handles number values', () => {
      const hll = new HyperLogLog();
      hll.add(1);
      hll.add(2);
      hll.add(3);
      expectWithinError(hll.count(), 3, 0.2);
    });

    test('handles boolean values', () => {
      const hll = new HyperLogLog();
      hll.add(true);
      hll.add(false);
      expectWithinError(hll.count(), 2, 0.2);
    });

    test('handles null and undefined', () => {
      const hll = new HyperLogLog();
      hll.add(null);
      hll.add(undefined);
      // null and undefined stringify differently
      expectWithinError(hll.count(), 2, 0.2);
    });

    test('distinguishes between types', () => {
      const hll = new HyperLogLog();
      hll.add(1);
      hll.add('1'); // Note: String(1) === '1', so this is a duplicate
      hll.add('a');
      hll.add(true);
      // 1 and '1' collide after string conversion, so expect 3 distinct
      expectWithinError(hll.count(), 3, 0.2);
    });
  });

  describe('accuracy tests', () => {
    test('small cardinality (100 distinct values)', () => {
      const hll = new HyperLogLog();
      const actualCardinality = 100;

      for (let i = 0; i < actualCardinality; i++) {
        hll.add(`value-${i}`);
      }

      expectWithinError(hll.count(), actualCardinality, 0.05);
    });

    test('medium cardinality (1000 distinct values)', () => {
      const hll = new HyperLogLog();
      const actualCardinality = 1000;

      for (let i = 0; i < actualCardinality; i++) {
        hll.add(`value-${i}`);
      }

      expectWithinError(hll.count(), actualCardinality, 0.05);
    });

    test('large cardinality (10000 distinct values)', () => {
      const hll = new HyperLogLog();
      const actualCardinality = 10000;

      for (let i = 0; i < actualCardinality; i++) {
        hll.add(`value-${i}`);
      }

      expectWithinError(hll.count(), actualCardinality, 0.05);
    });

    test('very large cardinality (100000 distinct values)', () => {
      const hll = new HyperLogLog();
      const actualCardinality = 100000;

      for (let i = 0; i < actualCardinality; i++) {
        hll.add(`value-${i}`);
      }

      expectWithinError(hll.count(), actualCardinality, 0.05);
    });

    test('with duplicates (1000 distinct, 10000 total)', () => {
      const hll = new HyperLogLog();
      const distinctValues = 1000;
      const totalValues = 10000;

      for (let i = 0; i < totalValues; i++) {
        // Each value appears ~10 times
        hll.add(`value-${i % distinctValues}`);
      }

      expectWithinError(hll.count(), distinctValues, 0.05);
    });
  });

  describe('merge operation', () => {
    test('merges two non-overlapping sketches', () => {
      const hll1 = new HyperLogLog();
      const hll2 = new HyperLogLog();

      // Add disjoint sets
      for (let i = 0; i < 100; i++) {
        hll1.add(`set1-${i}`);
        hll2.add(`set2-${i}`);
      }

      hll1.merge(hll2);
      expectWithinError(hll1.count(), 200, 0.1);
    });

    test('merges two overlapping sketches', () => {
      const hll1 = new HyperLogLog();
      const hll2 = new HyperLogLog();

      // Add overlapping sets
      for (let i = 0; i < 100; i++) {
        hll1.add(`value-${i}`);
      }
      for (let i = 50; i < 150; i++) {
        hll2.add(`value-${i}`);
      }

      hll1.merge(hll2);
      // Union should be 150 distinct values (0-149)
      expectWithinError(hll1.count(), 150, 0.1);
    });

    test('merges empty sketch has no effect', () => {
      const hll1 = new HyperLogLog();
      const hll2 = new HyperLogLog();

      for (let i = 0; i < 100; i++) {
        hll1.add(`value-${i}`);
      }

      const countBefore = hll1.count();
      hll1.merge(hll2);
      expect(hll1.count()).toBe(countBefore);
    });

    test('merge is commutative', () => {
      const hll1a = new HyperLogLog();
      const hll1b = new HyperLogLog();
      const hll2a = new HyperLogLog();
      const hll2b = new HyperLogLog();

      for (let i = 0; i < 50; i++) {
        hll1a.add(`set1-${i}`);
        hll1b.add(`set1-${i}`);
        hll2a.add(`set2-${i}`);
        hll2b.add(`set2-${i}`);
      }

      // Merge in different orders
      hll1a.merge(hll2a);
      hll2b.merge(hll1b);

      // Results should be the same (within floating point error)
      expect(Math.abs(hll1a.count() - hll2b.count())).toBeLessThan(0.1);
    });

    test('throws on precision mismatch', () => {
      const hll1 = new HyperLogLog(14);
      const hll2 = new HyperLogLog(12);

      expect(() => hll1.merge(hll2)).toThrow(/precision/i);
    });
  });

  describe('serialization', () => {
    test('serializes and deserializes empty sketch', () => {
      const hll = new HyperLogLog();
      const json = hll.toJSON();

      expect(json.precision).toBe(14);
      expect(json.registers).toHaveLength(16384);
      expect(json.registers.every(r => r === 0)).toBe(true);

      const restored = HyperLogLog.fromJSON(json);
      expect(restored.count()).toBe(0);
      expect(restored.isEmpty()).toBe(true);
    });

    test('serializes and deserializes populated sketch', () => {
      const hll = new HyperLogLog();

      for (let i = 0; i < 1000; i++) {
        hll.add(`value-${i}`);
      }

      const countBefore = hll.count();
      const json = hll.toJSON();
      const restored = HyperLogLog.fromJSON(json);
      const countAfter = restored.count();

      expect(countAfter).toBe(countBefore);
      expectWithinError(countAfter, 1000, 0.05);
    });

    test('clone creates independent copy', () => {
      const hll1 = new HyperLogLog();
      for (let i = 0; i < 100; i++) {
        hll1.add(`value-${i}`);
      }

      const hll2 = hll1.clone();

      // Verify counts match
      expect(hll2.count()).toBe(hll1.count());

      // Modify hll2, ensure hll1 unchanged
      for (let i = 100; i < 200; i++) {
        hll2.add(`value-${i}`);
      }

      expect(hll2.count()).toBeGreaterThan(hll1.count());
      expectWithinError(hll1.count(), 100, 0.05);
      expectWithinError(hll2.count(), 200, 0.05);
    });

    test('throws on invalid precision in JSON', () => {
      const json = {
        precision: 12, // Wrong precision
        registers: new Array(4096).fill(0),
      };

      expect(() => HyperLogLog.fromJSON(json)).toThrow(/precision/i);
    });
  });

  describe('edge cases', () => {
    test('handles empty strings', () => {
      const hll = new HyperLogLog();
      hll.add('');
      hll.add('');
      expectWithinError(hll.count(), 1, 0.2);
    });

    test('handles very long strings', () => {
      const hll = new HyperLogLog();
      const longString = 'x'.repeat(10000);
      hll.add(longString);
      expectWithinError(hll.count(), 1, 0.2);
    });

    test('handles special characters', () => {
      const hll = new HyperLogLog();
      const specialChars = ['🚀', '中文', '🎉', 'café', '\n', '\t', '\0'];

      for (const char of specialChars) {
        hll.add(char);
      }

      expectWithinError(hll.count(), specialChars.length, 0.2);
    });

    test('handles numeric edge cases', () => {
      const hll = new HyperLogLog();
      hll.add(0);
      hll.add(-1);
      hll.add(Number.MAX_SAFE_INTEGER);
      hll.add(Number.MIN_SAFE_INTEGER);
      hll.add(Infinity);
      hll.add(-Infinity);
      hll.add(NaN);

      expectWithinError(hll.count(), 7, 0.5);
    });
  });

  describe('statistical properties', () => {
    test('standard error is approximately 1.6%', () => {
      const trials = 10;
      const actualCardinality = 10000;
      const errors: number[] = [];

      for (let trial = 0; trial < trials; trial++) {
        const hll = new HyperLogLog();

        // Use different seeds for each trial
        for (let i = 0; i < actualCardinality; i++) {
          hll.add(`trial-${trial}-value-${i}`);
        }

        const estimate = hll.count();
        const relativeError =
          Math.abs(estimate - actualCardinality) / actualCardinality;
        errors.push(relativeError);
      }

      // Average error should be within expected range
      const avgError = errors.reduce((a, b) => a + b, 0) / errors.length;
      expect(avgError).toBeLessThan(0.03); // 3% average error
    });
  });
});

/**
 * Helper to assert that a value is within a relative error of the expected value.
 */
function expectWithinError(
  actual: number,
  expected: number,
  relativeError: number,
): void {
  const diff = Math.abs(actual - expected);
  const maxDiff = expected * relativeError;
  expect(diff).toBeLessThan(maxDiff);
}
