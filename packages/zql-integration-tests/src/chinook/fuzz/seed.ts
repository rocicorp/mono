/**
 * The fuzzer's **repro key** and **budget**, read from the environment so a scheduled run
 * explores fresh cases while a pull request replays a fixed set (design §9).
 *
 * - `ZERO_FUZZ_SEED` — the seed, decimal or `0x` hex, below 2^32. Unset: {@link
 *   DEFAULT_SEED}, so per-PR runs stay deterministic. The nightly passes its run number.
 * - `ZERO_FUZZ_BUDGET` — a positive integer multiplying the case counts of the randomized
 *   lanes. Unset: 1. Above 1 it also turns on the lanes too heavy for pull requests
 *   (`chinook-fuzz-extended-push.pg.test.ts`).
 *
 * A value that does not parse fails loudly, and so does any other `ZERO_FUZZ_*` variable:
 * rindle's nightly once misspelled its seed variable and silently re-ran its fixed seed
 * every night.
 */

/** The seed every lane used before it was configurable, and still the per-PR default. */
export const DEFAULT_SEED = 0x00c0ffee;

const SEED_VAR = 'ZERO_FUZZ_SEED';
const BUDGET_VAR = 'ZERO_FUZZ_BUDGET';
const KNOWN_VARS: ReadonlySet<string> = new Set([SEED_VAR, BUDGET_VAR]);

type Env = Readonly<Record<string, string | undefined>>;

/** The seed a lane in this module instance actually used, for {@link reproHint}. */
let seedUsed: number | undefined;

function checkNames(env: Env): void {
  for (const name of Object.keys(env)) {
    if (name.startsWith('ZERO_FUZZ_') && !KNOWN_VARS.has(name)) {
      throw new Error(
        `Unknown fuzzer variable ${name}; expected one of ${[...KNOWN_VARS].join(', ')}`,
      );
    }
  }
}

function read(env: Env, name: string): string | undefined {
  checkNames(env);
  const raw = env[name]?.trim();
  return raw === '' ? undefined : raw;
}

/** `0x`-prefixed, zero-padded to 8 hex digits: the form a failure prints. */
export function formatSeed(seed: number): string {
  return `0x${seed.toString(16).padStart(8, '0')}`;
}

/** The seed from `ZERO_FUZZ_SEED`, or {@link DEFAULT_SEED}. */
export function fuzzSeed(env: Env = process.env): number {
  const raw = read(env, SEED_VAR);
  const seed = raw === undefined ? DEFAULT_SEED : Number(raw);
  if (!Number.isInteger(seed) || seed < 0 || seed > 0xffffffff) {
    throw new Error(
      `${SEED_VAR}=${raw} is not an integer in [0, 2^32) (decimal or 0x hex)`,
    );
  }
  seedUsed = seed;
  return seed;
}

/** The case-count multiplier from `ZERO_FUZZ_BUDGET`, or 1. */
export function fuzzBudget(env: Env = process.env): number {
  const raw = read(env, BUDGET_VAR);
  const budget = raw === undefined ? 1 : Number(raw);
  if (!Number.isInteger(budget) || budget < 1) {
    throw new Error(`${BUDGET_VAR}=${raw} is not a positive integer`);
  }
  return budget;
}

/** How to replay a failing run, or `''` if no lane in this file read a seed. */
export function reproHint(): string {
  return seedUsed === undefined
    ? ''
    : `\n\nReplay with ${SEED_VAR}=${formatSeed(seedUsed)}`;
}
