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
    'mirror-schema/(.*)\\.js$': '<rootDir>/../mirror-schema/$1',
    'mirror-workers/(.*)\\.js$': '<rootDir>/../mirror-workers/$1',
    'cloudflare-api/(.*)\\.js$': '<rootDir>/../cloudflare-api/$1',
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },
};
