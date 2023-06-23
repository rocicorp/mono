import {expect, test} from '@jest/globals';
import {version} from './version.js';

test('version', () => {
  expect(typeof version).toBe('string');
  expect(version).not.toBe('0.0.0');
});
