// @ts-check

import {jestConfig} from '../shared/src/tool/jest-config.js';

/** @type {import('jest').Config} */
const config = {
  ...jestConfig,
  testMatch: ['**/?(*.)+(pg-test).[jt]s?(x)'],
  // Uncomment this to workaround the "Do not know how to serialize a BigInt" error:
  // https://github.com/jestjs/jest/issues/11617#issuecomment-1028651059
  // maxWorkers: 1,
};

export {config as default};
