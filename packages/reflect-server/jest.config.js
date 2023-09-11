import {getVersion} from '../reflect-shared/tool/get-version.js';

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
  moduleNameMapper: {
    'shared/(.*)\\.js$': '<rootDir>/../shared/$1',
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },
  testEnvironment: 'miniflare',
  globals: {
    ['REFLECT_VERSION']: getVersion(),
  },
};

export {config as default};
