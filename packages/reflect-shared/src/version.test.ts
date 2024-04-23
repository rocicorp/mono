import {expect, test} from 'vitest';
import {version} from './version.js';

test('version', () => {
  expect(typeof version).toBe('string');
  expect(version).not.toBe('0.0.0');
});
