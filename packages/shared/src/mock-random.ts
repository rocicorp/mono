import {vi, type Mock} from 'vitest';

/**
 * Mocks Math.random() to return deterministic values using a seeded pseudo-random number generator.
 * Uses Vitest's mocking system so it integrates properly with vi.restoreAllMocks().
 * 
 * Uses a Linear Congruential Generator (LCG) algorithm with parameters from Numerical Recipes
 * (a = 1664525, c = 1013904223, m = 2^32) which provides a good balance of speed and randomness
 * for testing purposes.
 *
 * @see https://en.wikipedia.org/wiki/Linear_congruential_generator
 *
 * @param seed - The seed value for the random number generator (default: 12345)
 * @returns A vitest Mock that can be used to control and inspect Math.random() calls
 *
 * @example
 * ```typescript
 * test('generates predictable random values', () => {
 *   const mock = mockRandom();
 *   expect(Math.random()).toBeCloseTo(0.00028, 5);
 *   expect(Math.random()).toBeCloseTo(0.75595, 5);
 *   mock.mockRestore();
 * });
 * ```
 *
 * @example
 * ```typescript
 * test('uses custom seed', () => {
 *   const mock = mockRandom(42);
 *   const value1 = Math.random();
 *   const value2 = Math.random();
 *   expect(value1).not.toBe(value2);
 *   mock.mockRestore();
 * });
 * ```
 */
export function mockRandom(seed = 12345): Mock<typeof Math.random> {
  let currentSeed = seed;

  // Linear Congruential Generator (LCG)
  // Using parameters from Numerical Recipes (a = 1664525, c = 1013904223, m = 2^32)
  return vi.spyOn(Math, 'random').mockImplementation(() => {
    currentSeed = (1664525 * currentSeed + 1013904223) % 0x100000000;
    return currentSeed / 0x100000000;
  });
}
