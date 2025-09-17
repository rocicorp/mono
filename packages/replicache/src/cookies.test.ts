/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members, @typescript-eslint/prefer-promise-reject-errors */
import {expect, test} from 'vitest';
import {compareCookies, type Cookie} from './cookies.ts';

test('compareCookies', () => {
  const t = (a: Cookie, b: Cookie, expected: number) => {
    expect(compareCookies(a, b)).to.equal(expected, `${a} < ${b}`);
    expect(compareCookies(b, a)).to.equal(-expected);
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
