import {expect, test} from 'vitest';
import {getESLibVersion} from './get-es-lib-version.ts';

test('lib >= ES2021', () => {
  // sanity check that we are using es2021. If this starts failing then we need
  // to add the polyfill back
  expect(getESLibVersion()).toBeGreaterThanOrEqual(2021);
});
