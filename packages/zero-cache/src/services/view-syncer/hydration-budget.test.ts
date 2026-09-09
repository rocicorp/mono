import {expect, test} from 'vitest';
import {HydrationBudget} from './hydration-budget.ts';

test('disabled budget never exhausts', () => {
  let now = 0;
  const budget = new HydrationBudget(0, () => now);

  now = Number.MAX_SAFE_INTEGER;
  expect(budget.exhausted()).toBe(false);
});

test('budget does not exhaust before its limit', () => {
  let now = 10;
  const budget = new HydrationBudget(5, () => now);

  now = 14;
  expect(budget.exhausted()).toBe(false);
});

test('budget exhausts at its limit', () => {
  let now = 10;
  const budget = new HydrationBudget(5, () => now);

  now = 15;
  expect(budget.exhausted()).toBe(true);
});

test('elapsed time uses the injected monotonic clock', () => {
  let now = 101.25;
  const budget = new HydrationBudget(100, () => now);

  now = 124.75;
  expect(budget.elapsedMs()).toBe(23.5);
});
