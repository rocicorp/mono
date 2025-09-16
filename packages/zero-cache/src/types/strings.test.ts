/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {describe, expect, test} from 'vitest';
import {elide} from './strings.ts';

describe('types/strings', () => {
  test('elide byte count', () => {
    const elidedASCII = elide('fo' + 'o'.repeat(150), 123);
    expect(elidedASCII).toMatchInlineSnapshot(
      `"fooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo..."`,
    );
    expect(elidedASCII).toHaveLength(123);

    const elidedFullWidth = elide('こんにちは' + 'あ'.repeat(150), 123);
    expect(elidedFullWidth).toMatchInlineSnapshot(
      `"こんにちはあああああああああああああああああああああああああああああああああああ..."`,
    );
    expect(
      new TextEncoder().encode(elidedFullWidth).length,
    ).toBeLessThanOrEqual(123);
  });
});
