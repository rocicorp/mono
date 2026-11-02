import {afterEach, describe, expect, test, vi} from 'vitest';
import {getRandomValues} from './random-values.ts';

describe('getRandomValues', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  test('uses crypto.getRandomValues when available', () => {
    const fill = vi.fn((array: Uint8Array) => array.fill(7));
    vi.stubGlobal('crypto', {getRandomValues: fill});
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});

    const array = new Uint8Array(4);
    expect(getRandomValues(array)).toBe(array);
    expect(fill).toHaveBeenCalledWith(array);
    expect([...array]).toEqual([7, 7, 7, 7]);
    expect(warn).not.toHaveBeenCalled();
  });

  test('falls back to Math.random with a single warning', () => {
    vi.stubGlobal('crypto', undefined);
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});

    const bytes = getRandomValues(new Uint8Array(64));
    expect(bytes.some(b => b !== 0)).toBe(true);
    const words = getRandomValues(new Uint32Array(8));
    expect(words.some(w => w > 0xff)).toBe(true);

    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0][0]).toMatch(/Math\.random/);
  });
});
