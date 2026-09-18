import {describe, expect, test} from 'vitest';
import {HydrationCostModel} from './hydration-cost-model.ts';

describe('HydrationCostModel', () => {
  test('shares a smoothed wall-time multiplier', () => {
    const model = new HydrationCostModel();
    model.observe(1400, 100);

    expect(model.estimate(100)).toBe(425);
  });

  test('prices in active hydrations immediately', () => {
    const model = new HydrationCostModel();
    model.beginHydration();
    model.beginHydration();

    expect(model.estimate(100)).toBe(300);

    model.endHydration();
    expect(model.estimate(100)).toBe(200);
    model.endHydration();
    expect(model.estimate(100)).toBe(100);
  });
});
