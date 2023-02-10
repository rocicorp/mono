import {test, expect} from '@jest/globals';
import {compareVersions} from './version.js';

test('compareVersion', () => {
  expect(compareVersions(null, null)).toBe(0);
  expect(compareVersions(1, null)).toBe(1);
  expect(compareVersions(null, 1)).toBe(-1);
  expect(compareVersions(1, 1)).toBe(0);
  expect(compareVersions(1, 2)).toBe(-1);
  expect(compareVersions(2, 1)).toBe(1);
});
