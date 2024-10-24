import {expect, test} from 'vitest';
import {like} from './like.js';

test('basics', () => {
  const cases: {
    input: () => string;
    expected: string;
    expectedError: string;
  }[] = [
    {
      input: () => like``,
      expected: '',
      expectedError: '',
    },
    {
      input: () => like`${'foo'}`,
      expected: 'foo',
      expectedError: '',
    },
    {
      input: () => like`disallowed`,
      expected: '',
      expectedError: `Invalid character 'd' in LIKE pattern`,
    },
    {
      input: () => like`%`,
      expected: '%',
      expectedError: '',
    },
    {
      input: () => like`%%`,
      expected: '%%',
      expectedError: '',
    },
    {
      input: () => like`_`,
      expected: '_',
      expectedError: '',
    },
    {
      input: () => like`__`,
      expected: '__',
      expectedError: '',
    },
    {
      input: () => like`%_`,
      expected: '%_',
      expectedError: '',
    },
    {
      input: () => like`%foo`,
      expected: '',
      expectedError: `Invalid character 'f' in LIKE pattern`,
    },
    {
      input: () => like`%${'foo'}%`,
      expected: '%foo%',
      expectedError: '',
    },
    {
      input: () => like`%_${'foo'}_%`,
      expected: '%_foo_%',
      expectedError: '',
    },
    {
      input: () => like`%${'foo'}%${'bar'}%`,
      expected: '%foo%bar%',
      expectedError: '',
    },
    {
      input: () => like`${'foo'}${'bar'}`,
      expected: 'foobar',
      expectedError: '',
    },
  ];

  for (const c of cases) {
    if (c.expectedError) {
      expect(c.input).toThrow(c.expectedError);
    } else {
      expect(c.input(), JSON.stringify(c)).toEqual(c.expected);
    }
  }
});
