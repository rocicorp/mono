// @ts-check

import {jestConfig} from 'shared/src/tool/jest-config.js';

/** @type {import('jest').Config} */
const config = {
  ...jestConfig,
  testEnvironment: 'miniflare',
};

export {config as default};
