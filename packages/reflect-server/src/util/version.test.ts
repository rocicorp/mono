import {expect, test} from '@jest/globals';
import {version as reflectVersion} from '@rocicorp/reflect';
import {version as modVersion} from './../mod.js';
import {version} from './version.js';

test('version', () => {
  expect(typeof version).toBe('string');
  expect(version).toBe(reflectVersion);
  expect(version).toBe(modVersion);
});
