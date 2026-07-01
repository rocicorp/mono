/**
 * A small **deterministic, seeded PRNG** (the mono analog of rusty-ivm's
 * `SmallRng::seed_from_u64`, used by the swarm / mutation / random-tail layers). The
 * generated query stream is a pure function of the seed, so a divergence prints its seed
 * (the repro key — design §9) and replays bit-for-bit. We do **not** match Rust's exact
 * sequence — only mono-internal determinism matters.
 *
 * The core is `mulberry32` (a 32-bit-state generator): fast, allocation-free, and stable
 * across runs. Seeds are taken mod 2^32, which comfortably fits the small literal seeds
 * the layers use (`0x00c0ffee`, `0xf00d`, `seed ^ 0x5eed`, …).
 */

/** A seeded pseudo-random generator. Deterministic for a given seed. */
export class Rng {
  #state: number;

  constructor(seed: number) {
    // Mix the seed so a small / zero seed still yields a well-distributed stream.
    this.#state = (Math.trunc(seed) ^ 0x9e3779b9) >>> 0;
  }

  /** The next float in `[0, 1)` (mulberry32). */
  float(): number {
    this.#state = (this.#state + 0x6d2b79f5) | 0;
    let t = Math.imul(this.#state ^ (this.#state >>> 15), 1 | this.#state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  /** An integer in `[0, n)` (0 if `n <= 0`). */
  int(n: number): number {
    return n <= 0 ? 0 : Math.floor(this.float() * n);
  }

  /** `true` with probability `p` (default 0.5). */
  bool(p = 0.5): boolean {
    return this.float() < p;
  }

  /** A uniformly random element of `arr`, or `undefined` if it is empty. */
  choose<T>(arr: readonly T[]): T | undefined {
    return arr.length === 0 ? undefined : arr[this.int(arr.length)];
  }

  /** A shuffled **copy** of `arr` (Fisher–Yates), leaving the input untouched. */
  shuffle<T>(arr: readonly T[]): T[] {
    const out = [...arr];
    for (let i = out.length - 1; i > 0; i--) {
      const j = this.int(i + 1);
      [out[i], out[j]] = [out[j], out[i]];
    }
    return out;
  }
}

/** A `Rng` seeded by `seed` (the repro key — design §9). */
export function rng(seed: number): Rng {
  return new Rng(seed);
}
