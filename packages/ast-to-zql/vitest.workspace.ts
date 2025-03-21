import {defineWorkspace} from 'vitest/config';

export default defineWorkspace([
  {
    extends: '../shared/src/tool/vitest-config.js',
    test: {
      name: 'nodejs',
      browser: {
        enabled: false,
        name: '', // not used but required by the type system
      },
    },
  },
  {
    extends: '../shared/src/tool/vitest-config.js',
    test: {
      name: 'chromium',
      exclude: ['src/options.test.ts'],
      browser: {
        name: 'chromium',
      },
    },
  },
  {
    extends: '../shared/src/tool/vitest-config.js',
    test: {
      name: 'webkit',
      exclude: ['src/options.test.ts'],
      browser: {
        name: 'webkit',
      },
    },
  },
]);
