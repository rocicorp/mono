import {expect, test} from 'vitest';
import {compareCookies, type Cookie} from './cookies.ts';

test('compareCookies', () => {
  const t = (a: Cookie, b: Cookie, expected: number) => {
    expect(compareCookies(a, b)).toBe(expected);
    // -expected || 0 converts -0 to 0 to avoid Object.is(-0, 0) === false
    expect(compareCookies(b, a)).toBe(-expected || 0);
  };

  t(null, null, 0);
  t(null, 'a', -1);
  t('a', 'b', -1);
  t('a', 'a', 0);
  t('a', 1, 1);
  t(2, 1, 1);
  t(3, 0, 3);
  t(1, 1, 0);
  t(1, 'a', -1);
  t('a', {order: 'a'}, 0);
  t({order: 'a'}, {order: 'b'}, -1);
  t({order: 'a'}, {order: 'a'}, 0);
  t({order: 'a'}, 1, 1);
});
