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
