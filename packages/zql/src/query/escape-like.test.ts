import {expect, test} from 'vitest';
import {escapeLike} from './escape-like.js';

test('basics', () => {
  const cases: {
    input: string;
    expected: string;
  }[] = [
    {
      input: '',
      expected: '',
    },
    {
      input: 'foo',
      expected: 'foo',
    },
    {
      input: '%',
      expected: '\\%',
    },
    {
      input: '%_',
      expected: '\\%\\_',
    },
    {
      input: '%_foo_%',
      expected: '\\%\\_foo\\_\\%',
    },
  ];

  for (const c of cases) {
    expect(escapeLike(c.input), JSON.stringify(c)).toEqual(c.expected);
  }
});
