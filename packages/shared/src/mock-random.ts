import {vi} from 'vitest';

/**
 * Mocks Math.random() to return deterministic values using a seeded pseudo-random number generator.
 * Uses Vitest's mocking system so it integrates properly with vi.restoreAllMocks().
 * Returns a Disposable for use with the `using` keyword for automatic cleanup.
 *
 * Uses a simple Linear Congruential Generator (LCG) algorithm.
 *
 * https://en.wikipedia.org/wiki/Linear_congruential_generator
 *
 * @param seed - The seed value for the random number generator (default: 12345)
 * @returns A Disposable that automatically restores Math.random() when disposed
 *
 * @example
 * ```typescript
 * test('my test', () => {
 *   using _random = mockRandom();
 *   // Math.random() now returns predictable values
 *   const value = Math.random();
 *   // Automatically restored when _random goes out of scope
 * });
 * ```
 *
 * @example
 * ```typescript
 * test('my test', () => {
 *   using _random = mockRandom(42);
 *   // Use custom seed for different sequence
 *   const value = Math.random();
 * });
 * ```
 */
export function mockRandom(seed = 12345): Disposable {
  let currentSeed = seed;

  // Linear Congruential Generator (LCG)
  // Using parameters from Numerical Recipes (a = 1664525, c = 1013904223, m = 2^32)
  return vi.spyOn(Math, 'random').mockImplementation(() => {
    currentSeed = (1664525 * currentSeed + 1013904223) % 0x100000000;
    return currentSeed / 0x100000000;
  });
}
