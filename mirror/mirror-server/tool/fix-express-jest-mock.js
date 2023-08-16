// @ts-check

// This script fixes the firestore-jest-mock package to use the correct
// Mock type from jest-mock instead of the one from @types/jest.
// This is needed because the @types/jest Mock type is not compatible with
// the jest-mock Mock type.

import {readFileSync, writeFileSync} from 'node:fs';
import {createRequire} from 'node:module';

const require = createRequire(import.meta.url);

const invalidFiles = [
  'index.d.ts',
  'request/index.d.ts',
  'response/index.d.ts',
];

/**
 * @param {string} basename
 */
function fixFile(basename) {
  let pathName;
  try {
    pathName = require.resolve(`@jest-mock/express/dist/src/${basename}`);
  } catch {
    // ignore if not found
    return;
  }

  const content = readFileSync(pathName, 'utf8');
  const newContent = content.replaceAll(
    'jest.Mock',
    `import('jest-mock').Mock`,
  );
  if (newContent !== content) {
    writeFileSync(pathName, newContent);
  }
}

for (const basename of invalidFiles) {
  fixFile(basename);
}
