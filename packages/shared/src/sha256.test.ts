import {expect, test} from 'vitest';
import {sha256OfString} from './sha256.js';

test('basic', async () => {
  expect(await sha256OfString('foo')).toBe(
    '2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae',
  );
});
