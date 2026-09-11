import {Rng} from '../../../../shared/src/rng.ts';

/**
 * The seed of one step's own draws: SimPG's shuffles and run IDs, and whatever
 * the code under test draws from `Math.random`. It depends only on the run's
 * seed and the step's ID. The ID is generated with the step, so removing a
 * step while shrinking leaves the draws of every other step unchanged.
 */
export function stepSeed(runSeed: number, stepID: number): number {
  const rng = new Rng((runSeed ^ Math.imul(stepID + 1, 0x9e3779b1)) >>> 0);
  // Nearby seeds start out correlated in mulberry32; one draw separates them.
  rng.float();
  return rng.int(2 ** 32);
}

/**
 * Replaces `Math.random` with a seeded generator until disposed. The
 * simulator reseeds it at the start of every step, with {@link stepSeed}.
 */
export class SeededMathRandom implements Disposable {
  readonly #original = Math.random;
  #rng: Rng;

  constructor(seed: number) {
    this.#rng = new Rng(seed);
    Math.random = () => this.#rng.float();
  }

  reseed(seed: number): void {
    this.#rng = new Rng(seed);
  }

  [Symbol.dispose](): void {
    Math.random = this.#original;
  }
}
