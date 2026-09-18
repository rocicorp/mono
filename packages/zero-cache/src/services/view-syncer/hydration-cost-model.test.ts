import {beforeEach, describe, expect, test} from 'vitest';
import {HydrationCostModel} from './hydration-cost-model.ts';

describe('HydrationCostModel', () => {
  let now: number;
  let model: HydrationCostModel;

  beforeEach(() => {
    now = 0;
    model = new HydrationCostModel(() => now);
  });

  function hydrate(wallTimeMs: number, processTimeMs: number) {
    const pass = model.beginHydration();
    now += wallTimeMs;
    pass.observe(processTimeMs);
    pass.end();
  }

  test('estimates process time until a hydration is observed', () => {
    expect(model.estimate(100)).toBe(100);
  });

  test('shares the observed wall-time multiplier', () => {
    hydrate(1400, 100);

    expect(model.estimate(100)).toBe(1400);
    expect(model.estimate(50)).toBe(700);
  });

  test('smooths the multiplier across passes', () => {
    hydrate(1400, 100);
    hydrate(200, 100);

    // (1400 * 0.75 + 200) / (100 * 0.75 + 100)
    expect(model.estimate(175)).toBeCloseTo(1250, 6);
  });

  test('weights passes by their process time', () => {
    hydrate(20_000, 10_000);
    // Dominated by fixed overhead, e.g. the query transform round trip.
    hydrate(100, 5);

    expect(model.estimate(100)).toBeCloseTo(201.2, 1);
  });

  test('clamps the observed multiplier', () => {
    hydrate(100_000, 100);
    expect(model.estimate(100)).toBe(2000);

    model = new HydrationCostModel(() => now);
    hydrate(50, 100);
    expect(model.estimate(100)).toBe(100);
  });

  test('ignores passes that are not observed', () => {
    const pass = model.beginHydration();
    now += 1400;
    pass.end();

    expect(model.estimate(100)).toBe(100);
  });

  test('prices in active hydrations immediately', () => {
    const pass1 = model.beginHydration();
    const pass2 = model.beginHydration();

    expect(model.estimate(100)).toBe(300);

    pass1.end();
    expect(model.estimate(100)).toBe(200);
    pass2.end();
    expect(model.estimate(100)).toBe(100);

    expect(() => pass2.end()).toThrow('Hydration pass already ended');
  });

  test('caps the price of active hydrations', () => {
    const passes = Array.from({length: 200}, () => model.beginHydration());
    expect(model.estimate(100)).toBe(500);

    // The cap applies to the estimate, not to the count.
    for (const pass of passes.splice(0, 197)) {
      pass.end();
    }
    expect(model.estimate(100)).toBe(400);
    for (const pass of passes) {
      pass.end();
    }
    expect(model.estimate(100)).toBe(100);
  });

  test('does not learn contention from concurrent passes', () => {
    // Two passes share the thread for their entire duration, so each takes
    // twice as long as it would have alone.
    const pass1 = model.beginHydration();
    const pass2 = model.beginHydration();
    now += 1000;
    pass1.observe(250);
    pass1.end();
    pass2.observe(250);
    pass2.end();

    // 1000 ms wall / 2 concurrent passes / 250 ms process = 2x
    expect(model.estimate(100)).toBe(200);

    // The contention is priced in by the active count instead.
    const pass3 = model.beginHydration();
    expect(model.estimate(100)).toBe(400);
    pass3.end();
  });

  test('averages concurrency over the pass', () => {
    const pass1 = model.beginHydration();
    now += 500;
    // Overlaps with the second half of pass1.
    const pass2 = model.beginHydration();
    now += 500;
    // 1000 ms wall at an average concurrency of 1.5
    pass1.observe(1000 / 1.5);
    pass1.end();
    pass2.end();

    expect(model.estimate(100)).toBeCloseTo(100, 10);
  });
});
