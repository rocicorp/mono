import fc from 'fast-check';
import {describe, expect, test, vi} from 'vitest';
import {runSimulation} from './simulation.ts';
import {runConfigArb, stepsArb, type RunConfig, type Step} from './steps.ts';

vi.mock(import('../../../../zqlite/src/db.ts'), async importOriginal => {
  const {trackDatabases} = await import('./sqlite.ts');
  return trackDatabases(await importOriginal());
});
vi.mock(import('../../observability/metrics.ts'), async importOriginal => {
  const {observeMetrics} = await import('./census.ts');
  return observeMetrics(await importOriginal());
});

/** 50 is the D1 gate; PR runs sample fewer. */
const REPLAY_SEEDS = Number(process.env['ZERO_SIM_REPLAY_SEEDS'] ?? 5);
const MAX_STEPS = 30;
const TIMEOUT_MS = 30 * 60_000;

/**
 * The hash of a run's trace, or of how it failed: a failing run must fail the
 * same way when replayed, too.
 */
async function hashOf(config: RunConfig, steps: readonly Step[]) {
  try {
    return (await runSimulation(config, steps, {guard: true})).hash;
  } catch (e) {
    return `failed: ${e instanceof Error ? e.message : String(e)}`;
  }
}

describe('sim/determinism', () => {
  test(
    'a seed and a step list replay to the same trace hash',
    async () => {
      const runs = fc.sample(fc.tuple(runConfigArb, stepsArb(MAX_STEPS)), {
        numRuns: REPLAY_SEEDS,
        seed: 0x5eed,
      });
      for (const [config, steps] of runs) {
        const first = await hashOf(config, steps);
        expect(first).not.toMatch(/^failed: /);
        expect(await hashOf(config, steps)).toBe(first);
      }
    },
    TIMEOUT_MS,
  );

  // State that outlives a run in one process -- a module-level counter, a
  // metric instrument, a process listener -- shows up as A hashing differently
  // after B.
  test(
    'a run hashes the same after another run in the same process',
    async () => {
      const [[configA, stepsA], [configB, stepsB]] = fc.sample(
        fc.tuple(runConfigArb, stepsArb(MAX_STEPS)),
        {numRuns: 2, seed: 0xaba},
      );
      const first = await hashOf(configA, stepsA);
      expect(first).not.toMatch(/^failed: /);
      await hashOf(configB, stepsB);
      expect(await hashOf(configA, stepsA)).toBe(first);
    },
    TIMEOUT_MS,
  );
});
