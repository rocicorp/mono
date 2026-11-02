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

  test('prefers crypto.getRandomValues over ExpoCrypto', () => {
    const fill = vi.fn((array: Uint8Array) => array.fill(7));
    const expoFill = vi.fn((array: Uint8Array) => array.fill(9));
    vi.stubGlobal('crypto', {getRandomValues: fill});
    vi.stubGlobal('expo', {modules: {ExpoCrypto: {getRandomValues: expoFill}}});

    expect([...getRandomValues(new Uint8Array(2))]).toEqual([7, 7]);
    expect(expoFill).not.toHaveBeenCalled();
  });

  test('uses ExpoCrypto when crypto is not available', () => {
    const expoFill = vi.fn((array: Uint8Array | Uint32Array) => {
      array.fill(9);
    });
    vi.stubGlobal('crypto', undefined);
    vi.stubGlobal('expo', {modules: {ExpoCrypto: {getRandomValues: expoFill}}});
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});

    const bytes = new Uint8Array(4);
    expect(getRandomValues(bytes)).toBe(bytes);
    expect(expoFill).toHaveBeenCalledWith(bytes);
    expect([...bytes]).toEqual([9, 9, 9, 9]);

    const words = new Uint32Array(2);
    expect(getRandomValues(words)).toBe(words);
    expect([...words]).toEqual([9, 9]);

    expect(warn).not.toHaveBeenCalled();
  });

  test('falls back to Math.random with a single warning', () => {
    vi.stubGlobal('crypto', undefined);
    vi.stubGlobal('expo', {modules: {}});
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});

    const bytes = getRandomValues(new Uint8Array(64));
    expect(bytes.some(b => b !== 0)).toBe(true);
    const words = getRandomValues(new Uint32Array(8));
    expect(words.some(w => w > 0xff)).toBe(true);

    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0][0]).toMatch(/Math\.random/);
  });

  test('falls back to Math.random when loading ExpoCrypto throws', () => {
    vi.stubGlobal('crypto', undefined);
    vi.stubGlobal('expo', {
      modules: {
        get ExpoCrypto() {
          throw new Error('native module failed to load');
        },
      },
    });
    vi.spyOn(console, 'warn').mockImplementation(() => {});

    const bytes = getRandomValues(new Uint8Array(64));
    expect(bytes.some(b => b !== 0)).toBe(true);
  });
});
