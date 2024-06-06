// @ts-check

import {readFileSync} from 'node:fs';

/**
 * @returns {string}
 */
function getReflectVersion() {
  const url = new URL('../../../reflect/package.json', import.meta.url);
  return JSON.parse(readFileSync(url, 'utf-8')).version;
}

/** @type {import('jest').Config} */
export const jestConfig = {
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
    '^cf-shared/(.*)\\.js$': '<rootDir>/../../packages/cf-shared/$1',
    '^datadog$': '<rootDir>/../../packages/datadog/src/mod.ts',
    '^datadog/(.*)\\.js$': '<rootDir>/../../packages/datadog/$1',
    '^shared/(.*)\\.js$': '<rootDir>/../../packages/shared/$1',
    '^reflect-shared/(.*)\\.js$': '<rootDir>/../../packages/reflect-shared/$1',
    '^reflect-protocol/(.*)\\.js$':
      '<rootDir>/../../packages/reflect-protocol/$1',
    '^@rocicorp/zql/(.*)\\.js$': '<rootDir>/../../packages/zql/$1',
    '^mirror-protocol/(.*)\\.js$': '<rootDir>/../../mirror/mirror-protocol/$1',
    '^mirror-schema/(.*)\\.js$': '<rootDir>/../../mirror/mirror-schema/$1',
    '^mirror-workers/(.*)\\.js$': '<rootDir>/../../mirror/mirror-workers/$1',
    '^cloudflare-api/(.*)\\.js$': '<rootDir>/../../mirror/cloudflare-api/$1',
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },
  globals: {
    ['REFLECT_VERSION']: getReflectVersion(),
    ['REFLECT_CLI_NAME']: 'reflect-cli',
    ['TESTING']: true,
  },
  // Disable prettier for inline snapshots since it is broken
  // https://github.com/jestjs/jest/issues/14305
  prettierPath: null,
};
