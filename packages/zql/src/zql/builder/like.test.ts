import {expect, test} from 'vitest';
import {getLikePredicate} from './like.js';

export const cases: {
  pattern: string;
  flags: 'i' | '';
  inputs: [string, boolean][];
}[] = [
  {
    pattern: 'foo',
    flags: '',
    inputs: [
      ['foo', true],
      ['bar', false],
      ['Foo', false],
      ['FOO', false],
      ['fo', false],
      ['fooa', false],
      ['afoo', false],
      ['afoob', false],
    ],
  },
  {
    pattern: 'foo',
    flags: 'i',
    inputs: [
      ['foo', true],
      ['bar', false],
      ['Foo', true],
      ['FOO', true],
      ['fo', false],
      ['fooa', false],
      ['afoo', false],
      ['afoob', false],
    ],
  },
  {
    pattern: 'foo%',
    flags: '',
    inputs: [
      ['foo', true],
      ['foobar', true],
      ['bar', false],
      ['Foo', false],
      ['FOO', false],
      ['fo', false],
      ['fooa', true],
      ['afoo', false],
      ['afoob', false],
    ],
  },
  {
    pattern: 'foo%',
    flags: 'i',
    inputs: [
      ['foo', true],
      ['foobar', true],
      ['bar', false],
      ['Foo', true],
      ['FOO', true],
      ['fo', false],
      ['fooa', true],
      ['afoo', false],
      ['afoob', false],
    ],
  },
  {
    pattern: 'foo_',
    flags: '',
    inputs: [
      ['foo', false],
      ['foobar', false],
      ['foob', true],
      ['bar', false],
      ['Foo', false],
      ['FOO', false],
      ['fo', false],
      ['afoo', false],
      ['afoob', false],
    ],
  },
  {
    pattern: 'foo\\%',
    flags: '',
    inputs: [
      ['foo%', true],
      ['foobar', false],
      ['bar', false],
      ['Foo', false],
      ['FOO', false],
      ['fo', false],
      ['fooa', false],
      ['afoo', false],
      ['afoob', false],
    ],
  },
  {
    pattern: 'foo\\%',
    flags: 'i',
    inputs: [
      ['foo%', true],
      ['FOO%', true],
      ['foobar', false],
      ['bar', false],
      ['Foo', false],
      ['FOO', false],
      ['fo', false],
      ['fooa', false],
      ['afoo', false],
      ['afoob', false],
    ],
  },
  {
    pattern: 'foo\\_',
    flags: '',
    inputs: [
      ['foo_', true],
      ['FOO_', false],
      ['foobar', false],
      ['bar', false],
      ['Foo', false],
      ['FOO', false],
      ['fo', false],
      ['fooa', false],
      ['afoo', false],
      ['afoob', false],
    ],
  },
  {
    pattern: '%foo',
    flags: '',
    inputs: [
      ['foo', true],
      ['foobar', false],
      ['bar', false],
      ['Foo', false],
      ['FOO', false],
      ['fo', false],
      ['fooa', false],
      ['afoo', true],
      ['afoob', false],
    ],
  },
  {
    pattern: '%foo%',
    flags: '',
    inputs: [
      ['foo', true],
      ['foobar', true],
      ['bar', false],
      ['Foo', false],
      ['FOO', false],
      ['fo', false],
      ['fooa', true],
      ['afoo', true],
      ['afoob', true],
    ],
  },
];

test('basics', () => {
  for (const {pattern, flags, inputs} of cases) {
    const op = getLikePredicate(pattern, flags);
    for (const [input, expected] of inputs) {
      expect(op(input), JSON.stringify({pattern, flags, input})).equal(
        expected,
      );
    }
  }
});
