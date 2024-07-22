import tsconfigPaths from 'vite-tsconfig-paths';
import {makeDefine} from '../build.js';

const define = {
  ...makeDefine(),
  ['TESTING']: 'true',
};

export const config = {
  // https://github.com/vitest-dev/vitest/issues/5332#issuecomment-1977785593
  optimizeDeps: {
    include: ['vitest > @vitest/expect > chai'],
    exclude: ['wa-sqlite'],
  },
  define,
  esbuild: {
    define,
  },

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  plugins: [tsconfigPaths()] as any[],

  test: {
    onConsoleLog(log: string) {
      if (
        log.includes('Skipping license check for TEST_LICENSE_KEY.') ||
        log.includes('REPLICACHE LICENSE NOT VALID') ||
        log.includes('enableAnalytics false') ||
        log.includes('no such entity') ||
        log.includes('TODO: addZQLSubscription') ||
        log.includes('TODO: removeZQLSubscription')
      ) {
        return false;
      }
      return undefined;
    },
    include: ['src/**/*.{test,spec}.?(c|m)[jt]s?(x)'],
    browser: {
      enabled: true,
      provider: 'playwright',
      headless: true,
      name: 'chromium',
      screenshotFailures: false,
    },
    typecheck: {
      enabled: false,
    },
    testTimeout: 10_000,
  },
} as const;
