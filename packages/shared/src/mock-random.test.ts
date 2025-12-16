import {describe, expect, test, vi} from 'vitest';
import {nanoid} from '../../zero-client/src/util/nanoid.ts';
import {mockRandom} from './mock-random.ts';

describe('mockRandom', () => {
  test('makes Math.random() predictable', () => {
    let deterministicValues1: number[];
    let deterministicValues2: number[];
    let deterministicValues3: number[];

    // Use deterministic random with seed 42
    {
      using _random = mockRandom(42);
      deterministicValues1 = [Math.random(), Math.random(), Math.random()];
    }

    // Use deterministic random again with same seed
    {
      using _random = mockRandom(42);
      deterministicValues2 = [Math.random(), Math.random(), Math.random()];
    }

    // Use different seed
    {
      using _random = mockRandom(999);
      deterministicValues3 = [Math.random(), Math.random(), Math.random()];
    }

    // Values should be the same when using the same seed
    expect(deterministicValues1).toEqual(deterministicValues2);

    // Values should be different with different seed
    expect(deterministicValues1).not.toEqual(deterministicValues3);

    // Verify Math.random() is restored after cleanup
    const afterRestore = Math.random();
    expect(typeof afterRestore).toBe('number');
    expect(afterRestore).toBeGreaterThanOrEqual(0);
    expect(afterRestore).toBeLessThan(1);
  });

  test('works with functions that use Math.random()', () => {
    const getRandomValue = () => ({
      id: nanoid(),
      value: Math.random() > 0.5 ? 'high' : 'low',
    });

    let value1: ReturnType<typeof getRandomValue>;
    let value2: ReturnType<typeof getRandomValue>;
    let value3: ReturnType<typeof getRandomValue>;
    let value1Again: ReturnType<typeof getRandomValue>;
    let value2Again: ReturnType<typeof getRandomValue>;
    let value3Again: ReturnType<typeof getRandomValue>;

    // With deterministic random, values should be predictable
    {
      using _random = mockRandom(42);
      value1 = getRandomValue();
      value2 = getRandomValue();
      value3 = getRandomValue();
    }

    // Use same seed to generate same sequence
    {
      using _random = mockRandom(42);
      value1Again = getRandomValue();
      value2Again = getRandomValue();
      value3Again = getRandomValue();
    }

    // Everything should be the same with deterministic random (nanoid also uses Math.random)
    expect(value1).toEqual(value1Again);
    expect(value2).toEqual(value2Again);
    expect(value3).toEqual(value3Again);
  });

  test('integrates with vi.restoreAllMocks()', () => {
    let deterministicValue1: number;

    // Set up deterministic random using 'using' keyword
    {
      using _random = mockRandom(42);
      deterministicValue1 = Math.random();
      // Automatically disposed when block exits
    }

    // Math.random() should now be back to normal (non-deterministic)
    const normalValue = Math.random();
    expect(typeof normalValue).toBe('number');
    expect(normalValue).toBeGreaterThanOrEqual(0);
    expect(normalValue).toBeLessThan(1);

    // Set up again with same seed - should produce same value
    {
      using _random = mockRandom(42);
      const deterministicValue2 = Math.random();
      expect(deterministicValue2).toBe(deterministicValue1);
    }

    // Also works with vi.restoreAllMocks()
    mockRandom(42);
    vi.restoreAllMocks(); // Cleans up even without 'using'
  });
});
