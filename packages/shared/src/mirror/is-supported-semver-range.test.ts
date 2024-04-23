import {Range} from 'semver';
import {expect, test} from 'vitest';
import {isSupportedSemverRange} from './is-supported-semver-range.js';

test('isSupportedSemverRange', () => {
  const t = (range: string, expected: boolean) =>
    expect(isSupportedSemverRange(new Range(range))).toBe(expected);

  t('^0.1.0', true);
  t('~0.1.0', true);
  t('0.1.0', false);

  t('1.0.0', false);
  t('^1.0.0', true);
  t('~1.0.0', false);

  t('2.0.0', false);
  t('^2.0.0', true);
  t('~2.0.0', false);

  t('1.2.0', false);
  t('^1.2.0', true);
  t('~1.2.0', false);

  t('1.2.3', false);
  t('^1.2.3', true);
  t('~1.2.3', false);

  t('>0', true);
  t('>0.0', true);
  t('>0.0.0', true);

  t('>=0.0.1', true);
  t('>=0.1.0', true);
  t('>=1.0.0', true);

  t('>0.0.1', true);
  t('>0.1.0', true);
  t('>1.0.0', true);

  t('>=0.0.1 <0.1.0', true);

  // Multiple ranges no good
  t('>=0.0.1 <0.1.0 || >=1.0.0 <2.0.0', false);

  // `<` for above 1.0 not allowed
  t('>=1.0.0 <1.1.0', false);
  t('>=1.0.0 <=1.1.0', false);

  t('>=1.0.0 <2.0.0', true);
  t('>=1.0.0 <3.0.0', true);
  t('>=1.2.0 <2.0.0', true);
  t('>=1.0.2 <2.0.0', true);
  t('>=1.0.0 <2.0.0', true);

  t('>=1.0.0 <=2.0.0', true);
  t('>=1.0.0 <=3.0.0', true);
  t('>=1.2.0 <=2.0.0', true);
  t('>=1.0.2 <=2.0.0', true);
  t('>=1.0.0 <=2.0.0', true);

  t('>1.0.0 <=2.0.0', true);
  t('>1.0.0 <=3.0.0', true);
  t('>1.2.0 <=2.0.0', true);
  t('>1.0.2 <=2.0.0', true);
  t('>1.0.0 <=2.0.0', true);

  t('>1.0.0 <2.0.0', true);
  t('>1.0.0 <3.0.0', true);
  t('>1.2.0 <2.0.0', true);
  t('>1.0.2 <2.0.0', true);
  t('>1.0.0 <2.0.0', true);

  t('1.x', true);
  t('1.2.x', false);

  t('0.x', true);
  t('0.1.x', true);

  t('<0.2', true);
  t('<1.0', true);
  t('<2.0', true);
  t('<1.2', false);

  t('<=0.2', true);
  t('<=1.0.0', true);
  t('<=2.0.0', true);
  t('<=1.2.0', false);
});
