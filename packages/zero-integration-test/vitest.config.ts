import {defineConfig} from 'vitest/config';
import {makeDefine} from '../shared/src/build.js';

const define = {
  ...makeDefine(),
  ['TESTING']: 'true',
};

export default defineConfig({
  optimizeDeps: {
    include: ['vitest > @vitest/expect > chai'],
    exclude: ['wa-sqlite'],
  },
  define,
  esbuild: {
    define,
  },
  test: {
    onConsoleLog(log: string) {
      if (
        log.includes('Skipping license check for TEST_LICENSE_KEY.') ||
        log.includes('REPLICACHE LICENSE NOT VALID') ||
        log.includes('enableAnalytics false') ||
        log.includes('no such entity')
      ) {
        return false;
      }
      return undefined;
    },
    // Run browser tests only for files in the "src/dom" folder
    include:
      process.env.BROWSER_TESTS === 'true'
        ? ['src/zero-client/**/*.test.{js,ts,jsx,tsx}']
        : [
            'src/**/*.test.{js,ts,jsx,tsx}',
            '!src/zero-client/**/*.test.{js,ts,jsx,tsx}',
          ],
    typecheck: {
      enabled: false,
    },
    browser:
      process.env.BROWSER_TESTS === 'true'
        ? {
            enabled: true,
            provider: 'playwright',
            headless: true,
            name: 'chromium',
            screenshotFailures: false,
          }
        : undefined,
    testTimeout: 10_000,
  },
});
