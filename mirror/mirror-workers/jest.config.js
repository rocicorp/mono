import {getVersion} from 'reflect-shared/tool/get-version.js';

export default {
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
    'reflect-shared/(.*)\\.js$': '<rootDir>/../../packages/reflect-shared/$1',
    'shared/(.*)\\.js$': '<rootDir>/../../packages/shared/$1',
    'mirror-protocol/(.*)\\.js$': '<rootDir>/../mirror-protocol/$1',
    'mirror-schema/(.*)\\.js$': '<rootDir>/../mirror-schema/$1',
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },
  globals: {
    ['REFLECT_VERSION']: getVersion(),
    ['REFLECT_CLI_NAME']: 'reflect-cli',
  },
};
