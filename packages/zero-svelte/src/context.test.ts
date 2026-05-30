import {describe, expect, test, vi} from 'vitest';

const contextStore = new Map<unknown, unknown>();

vi.mock('svelte', () => ({
  getContext: (key: unknown) => contextStore.get(key),
  setContext: (key: unknown, value: unknown) => {
    contextStore.set(key, value);
  },
}));

import {useZero, setZero, createUseZero} from './context.ts';

describe('context', () => {
  test('useZero throws when no context set', () => {
    contextStore.clear();
    expect(() => useZero()).toThrow('useZero must be used within a ZeroProvider');
  });

  test('setZero + useZero round-trips', () => {
    contextStore.clear();
    const fakeZ = {clientID: 'ctx-test'} as Parameters<typeof setZero>[0];
    setZero(fakeZ);
    expect(useZero()).toBe(fakeZ);
  });

  test('createUseZero returns typed accessor', () => {
    contextStore.clear();
    const fakeZ = {clientID: 'typed-test'} as Parameters<typeof setZero>[0];
    setZero(fakeZ);
    const useTypedZero = createUseZero();
    expect(useTypedZero()).toBe(fakeZ);
  });
});
