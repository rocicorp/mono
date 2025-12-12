import {expect, test} from 'vitest';
import data from '../../../tsconfig.json' with {type: 'json'};

function getESLibVersion(libs: string[]): number {
  const esVersion = libs.find(lib => lib.toLowerCase().startsWith('es'));
  if (!esVersion) {
    throw new Error('Could not find ES lib version');
  }
  return parseInt(esVersion.slice(2), 10);
}

test('lib >= ES2021', () => {
  // sanity check that we are using es2021. If this starts failing then we need
  // to add the polyfill back
  expect(getESLibVersion(data.compilerOptions.lib)).toBeGreaterThanOrEqual(
    2021,
  );
});
