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
    'shared/(.*)\\.js$': '<rootDir>/../../packages/shared/$1',
    'mirror-protocol/(.*)\\.js$': '<rootDir>/../mirror-protocol/$1',
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },
};
