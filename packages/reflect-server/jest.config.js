/** @type {import('jest').Config} */
const config = {
  preset: 'ts-jest/presets/default-esm',
  testPathIgnorePatterns: ['/node_modules/', '/out/'],
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
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },
  testEnvironment: 'miniflare',
};

export {config as default};
