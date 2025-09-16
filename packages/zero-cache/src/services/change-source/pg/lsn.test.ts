/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {describe, expect, test} from 'vitest';
import {
  versionFromLexi,
  type LexiVersion,
} from '../../../types/lexi-version.ts';
import {
  fromBigInt,
  fromLexiVersion,
  toBigInt,
  toLexiVersion,
  type LSN,
} from './lsn.ts';

describe('lsn to/from LexiVersion', () => {
  type Case = [LSN, LexiVersion, bigint];
  const cases: Case[] = [
    ['0/0', '00', 0n],
    ['0/A', '0a', 10n],
    ['16/B374D848', '718sh0nk8', 97500059720n],
    ['FFFFFFFF/FFFFFFFF', 'c3w5e11264sgsf', 2n ** 64n - 1n],
  ];
  test.each(cases)('convert(%s <=> %s)', (lsn, lexi, ver) => {
    expect(toLexiVersion(lsn)).toBe(lexi);
    expect(toBigInt(lsn)).toBe(ver);
    expect(fromBigInt(ver)).toBe(lsn);
    expect(versionFromLexi(lexi).toString()).toBe(ver.toString());
    expect(fromLexiVersion(lexi)).toBe(lsn);
  });
});
