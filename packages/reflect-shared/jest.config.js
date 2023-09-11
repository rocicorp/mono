// @ts-check

import {getVersion} from './tool/get-version.js';

/** @type {import('jest').Config} */
const config = {
  preset: 'ts-jest/presets/default-esm',
  transform: {
    '^.+\\.tsx?$': [
      'ts-jest',
      {
        tsconfig: 'tsconfig.json',
        useESM: true,
      },
    ],
  },
  globals: {
    ['REFLECT_VERSION']: getVersion(),
  },
  moduleNameMapper: {
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },
};

export {config as default};
