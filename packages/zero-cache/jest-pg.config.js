// @ts-check

import {jestConfig} from '../shared/src/tool/jest-config.js';

/** @type {import('jest').Config} */
const config = {
  ...jestConfig,
  testMatch: ['**/?(*.)+(pg-test).[jt]s?(x)'],
};

export {config as default};
