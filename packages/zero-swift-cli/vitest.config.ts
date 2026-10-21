import {fileURLToPath} from 'node:url';
import {mergeConfig} from 'vitest/config';
import config from '../shared/src/tool/vitest-config.ts';

// The generators write files, so they run in Node. Apps import
// `@rocicorp/zero`, which is its sources here, as in the apps' own tests.
export default mergeConfig(config, {
  resolve: {
    alias: [
      {
        find: /^@rocicorp\/zero$/,
        replacement: fileURLToPath(
          new URL('../zero/src/zero.ts', import.meta.url),
        ),
      },
    ],
  },
  test: {
    name: 'zero-swift-cli',
    browser: {
      enabled: false,
    },
  },
});
