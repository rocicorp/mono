import {argv} from 'node:process';
import {makeDefine} from '../build.js';

const define = {
  ...makeDefine(),
  ['TESTING']: 'true',
};

/**
 * Find name from process.argv.
 *
 * The argv has --browser.name=chromium which
 * overrides the test.browser.name in the config but there is no way to read it
 * at this level so we have to do it manually.
 */
function findBrowserName() {
  for (const arg of argv) {
    const m = arg.match(/--browser\.name=(.+)/);
    if (m) {
      return m[1];
    }
  }
  return undefined;
}

const logSilenceMessages = [
  'Skipping license check for TEST_LICENSE_KEY.',
  'REPLICACHE LICENSE NOT VALID',
  'enableAnalytics false',
  'no such entity',
  'Zero starting up with no server URL',
  'PokeHandler clearing due to unexpected poke error',
  'Not indexing value',
  'Zero starting up with no server URL',
];
export default {
  // https://github.com/vitest-dev/vitest/issues/5332#issuecomment-1977785593
  optimizeDeps: {
    include: ['vitest > @vitest/expect > chai'],
    exclude: ['wa-sqlite'],
  },
  define,
  esbuild: {
    define,
  },

  test: {
    name: findBrowserName(),
    onConsoleLog(log: string) {
      for (const message of logSilenceMessages) {
        if (log.includes(message)) {
          return false;
        }
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
};
