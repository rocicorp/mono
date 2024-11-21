import {defineWorkspace} from 'vitest/config';

export default defineWorkspace([
  {
    extends: './src/tool/vitest-config.js',
    test: {
      name: 'nodejs',
      browser: {
        enabled: false,
        name: '', // not used but required by the type system
      },
    },
  },
  {
    extends: './src/tool/vitest-config.js',
    test: {
      name: 'chromium',
      exclude: ['src/options.test.ts'],
      browser: {
        name: 'chromium',
      },
    },
  },
]);
