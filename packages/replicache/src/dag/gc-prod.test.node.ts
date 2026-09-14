import {afterEach, beforeEach, expect, test, vi} from 'vitest';
import {fakeHash} from '../hash.ts';
import type {RefCountUpdatesDelegate} from './gc.ts';

// The normal test suite runs with NODE_ENV=test, where skipGCAsserts is false,
// so a test there cannot tell whether the negative ref count check is still
// gated on it. This file re-evaluates the modules with NODE_ENV=production.
// That needs process.env to be read at runtime, which is only true in the
// node project; the browser project bakes NODE_ENV in with a define.
//
// Dynamic imports are required here: the modules under test must be loaded
// after the env var is stubbed and the module cache reset.

beforeEach(() => {
  vi.stubEnv('NODE_ENV', 'production');
  vi.resetModules();
});

afterEach(() => {
  vi.unstubAllEnvs();
  vi.resetModules();
});

function delegateWithRefCount(refCount: number): RefCountUpdatesDelegate {
  return {
    getRefCount: () => refCount,
    getRefs: () => [],
  };
}

test('the modules see production mode', async () => {
  const {isProd} = await import('../../../shared/src/config.ts');
  expect(isProd).toBe(true);
  const {skipGCAsserts} = await import('../config.ts');
  expect(skipGCAsserts).toBe(true);
});

test('computeRefCountUpdates rejects a negative ref count update in production', async () => {
  const {computeRefCountUpdates} = await import('./gc.ts');
  // The store says the chunk is a head but reports its ref count as 0, so
  // moving the head away would take the count to -1.
  const h = fakeHash('bad');
  await expect(
    computeRefCountUpdates(
      [{old: h, new: undefined}],
      new Set(),
      delegateWithRefCount(0),
    ),
  ).rejects.toThrow(/^Invalid ref count -1 for /);
});

test('computeRefCountUpdates treats a NaN ref count as 0 and rejects it in production', async () => {
  // `ensureRefCountLoaded` coerces a falsy count (including NaN) to 0, so a
  // NaN from the store is rejected as -1 like a missing count. The check
  // itself is written as `!(update >= 0)` so that a NaN update, should one
  // ever reach it, is rejected too.
  const {computeRefCountUpdates} = await import('./gc.ts');
  const h = fakeHash('bad');
  await expect(
    computeRefCountUpdates(
      [{old: h, new: undefined}],
      new Set(),
      delegateWithRefCount(NaN),
    ),
  ).rejects.toThrow(/^Invalid ref count -1 for /);
});
