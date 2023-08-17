// @ts-check

// This script fixes the firestore-jest-mock package to use the correct
// Mock type from jest-mock instead of the one from @types/jest.
// This is needed because the @types/jest Mock type is not compatible with
// the jest-mock Mock type.

import {readFileSync, writeFileSync} from 'node:fs';
import {createRequire} from 'node:module';

const require = createRequire(import.meta.url);

const invalidFiles = [
  {lib: 'firestore', file: 'auth'},
  {lib: 'firestore', file: 'fieldValue'},
  {lib: 'firestore', file: 'firebase'},
  {lib: 'firestore', file: 'firestore'},
  {lib: 'firestore', file: 'query'},
  {lib: 'firestore', file: 'timestamp'},
  {lib: 'firestore', file: 'transaction'},
  {lib: 'express', file: 'index.d.ts'},
  {lib: 'express', file: 'request/index.d.ts'},
  {lib: 'express', file: 'response/index.d.ts'},
];

/**
 * @param {{lib: string, file: string}} invalidFile
 */
function fixFile(invalidFile) {
  let pathName;
  try {
    if (invalidFile.lib === 'firestore') {
      pathName = require.resolve(
        `firestore-jest-mock/mocks/${invalidFile.file}.d.ts`,
      );
    } else if (invalidFile.lib === 'express') {
      pathName = require.resolve(
        `@jest-mock/express/dist/src/${invalidFile.file}`,
      );
    }
  } catch {
    // ignore if not found
    return;
  }
  if (!pathName) return;

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
