import {defineConfig} from 'vitest/config';
import {config} from '../shared/src/tool/vitest-config.js';

export default defineConfig({
  ...config,
  test: {
    ...config.test,
    // TODO: Fix this
    globals: true,
  },
});
