import {afterEach, describe, expect, test, vi} from 'vitest';
import type {resolver as resolverFn} from './resolver.ts';

const {withResolvers} = Promise;
let loads = 0;

afterEach(() => {
  Promise.withResolvers = withResolvers;
});

describe.each(['native', 'fallback'])('%s', mode => {
  // The implementation is chosen when the module loads, so load a fresh copy
  // for each test. The query string does that in both node and the browser,
  // where vi.resetModules has no effect.
  async function load() {
    const spy = vi.fn(withResolvers);
    // @ts-expect-error undefined simulates a runtime without withResolvers.
    Promise.withResolvers = mode === 'native' ? spy : undefined;
    const {resolver} = (await import(
      /* @vite-ignore */ `./resolver.ts?load=${loads++}`
    )) as {resolver: typeof resolverFn};
    return {resolver, spy};
  }

  test('resolve', async () => {
    const {resolver, spy} = await load();
    const {promise, resolve} = resolver<number>();
    resolve(42);
    expect(await promise).toBe(42);
    expect(spy).toHaveBeenCalledTimes(mode === 'native' ? 1 : 0);
  });

  test('reject', async () => {
    const {resolver} = await load();
    const {promise, reject} = resolver<number, Error>();
    reject(new Error('boom'));
    await expect(promise).rejects.toThrow('boom');
  });
});
