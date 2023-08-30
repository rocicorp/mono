import {describe, test, expect} from '@jest/globals';
import {isTrueEnvValue} from './env.js';

describe('isTrueEnvValue', () => {
  function t(input: string | undefined, expected: boolean) {
    test(input ?? 'undefined', () => {
      expect(isTrueEnvValue(input)).toEqual(expected);
    });
  }

  t('true', true);
  t('TruE', true);
  t('TRUE', true);
  t('1', true);
  t(' true', false);
  t('true ', false);
  t(undefined, false);
  t('0', false);
  t('false', false);
  t('FALSE', false);
  t('FOO', false);
});
