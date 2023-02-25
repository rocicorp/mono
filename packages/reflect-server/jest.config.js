import * as path from 'path';
import {fileURLToPath} from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

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
    '^reflect-protocol/(.*)\\.js$': path.join(
      __dirname,
      '../reflect-protocol/$1',
    ),
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },
  testEnvironment: 'miniflare',
};
