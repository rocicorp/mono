// @ts-check

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
    '^datadog$': '<rootDir>/../../packages/datadog/src/mod.ts',
    '^datadog/(.*)\\.js$': '<rootDir>/../../packages/datadog/$1',
    '^zql/(.*)\\.js$': '<rootDir>/../../packages/zql/$1',
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },
  globals: {
    ['TESTING']: true,
  },
  // Disable prettier for inline snapshots since it is broken
  // https://github.com/jestjs/jest/issues/14305
  prettierPath: null,
};
