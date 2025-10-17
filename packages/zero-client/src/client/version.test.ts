import {expect, test} from 'vitest';
import {version} from './version.ts';

test('version basics', () => {
  expect(version).toMatch(/^\d+\.\d+\.\d+(\+[a-f0-9]+)?$/);
});
