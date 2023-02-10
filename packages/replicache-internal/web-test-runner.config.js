// @ts-check
import {esbuildPlugin} from '@web/dev-server-esbuild';
import {playwrightLauncher} from '@web/test-runner-playwright';
import {makeDefine} from './tool/make-define.js';

const chromium = playwrightLauncher({product: 'chromium'});
const webkit = playwrightLauncher({product: 'webkit'});
const firefox = playwrightLauncher({product: 'firefox'});

/** @type {import('@web/test-runner').TestRunnerConfig} */
const config = {
  port: 3001,
  concurrentBrowsers: 3,
  nodeResolve: true,
  plugins: [
    esbuildPlugin({
      ts: true,
      target: 'esnext',
      define: await makeDefine('debug'),
    }),
  ],
  staticLogging: !!process.env.CI,
  testFramework: {
    config: {
      ui: 'tdd',
      reporter: 'html',
      timeout: 30000,
      retries: process.env.CI ? 3 : 0, // Firefox is flaky
    },
  },
  files: [
    'src/*.test.ts',
    'src/dag/*.test.ts',
    'src/db/*.test.ts',
    'src/kv/*.test.ts',
    'src/sync/*.test.ts',
    'src/migrate/*.test.ts',
    'src/btree/*.test.ts',
    'src/persist/*.test.ts',
  ],
  browsers: [firefox, chromium, webkit],
};

export {config as default};
