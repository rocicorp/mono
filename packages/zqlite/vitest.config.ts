import {defineConfig} from 'vitest/config';
import {makeDefine} from '../shared/src/build.js';
import tsconfigPaths from 'vite-tsconfig-paths';

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
    typecheck: {
      enabled: false,
    },
    testTimeout: 10_000,
  },
});
