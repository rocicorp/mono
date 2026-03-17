import {playwright} from '@vitest/browser-playwright';
import {defineConfig} from 'vitest/config';
import {makeDefine} from '../build.ts';

export const CI = process.env['CI'] === 'true' || process.env['CI'] === '1';
const {VITEST_BROWSER} = process.env;

function assertValidBrowser(
  browser: string | undefined,
): asserts browser is 'chromium' | 'firefox' | 'webkit' | undefined {
  switch (browser) {
    case 'chromium':
    case 'firefox':
    case 'webkit':
    case undefined:
      return;
    default:
      throw new Error(`Invalid VITEST_BROWSER value: ${browser}`);
  }
}

assertValidBrowser(VITEST_BROWSER);

const define = {
  ...makeDefine(),
  ['TESTING']: 'true',
};

const logSilenceMessages = [
  'Skipping license check for TEST_LICENSE_KEY.',
  'REPLICACHE LICENSE NOT VALID',
  'enableAnalytics false',
  'no such entity',
  'PokeHandler clearing due to unexpected poke error',
  'Not indexing value',
  'Zero starting up with no server URL',
];

export default defineConfig({
  // https://github.com/vitest-dev/vitest/issues/5332#issuecomment-1977785593
  optimizeDeps: {
    include: ['vitest > @vitest/expect > chai'],
  },
  define,

  test: {
    onConsoleLog(log: string) {
      for (const message of logSilenceMessages) {
        if (log.includes(message)) {
          return false;
        }
      }
      return undefined;
    },
    include: ['src/**/*.{test,spec}{,.node}.?(c|m)[jt]s?(x)'],
    silent: 'passed-only',
    browser: {
      enabled: true,
      provider: playwright(),
      headless: true,
      screenshotFailures: false,
      instances: VITEST_BROWSER
        ? ([{browser: VITEST_BROWSER}] as const)
        : [
            {browser: 'chromium'},
            ...(CI
              ? ([{browser: 'firefox'}, {browser: 'webkit'}] as const)
              : []),
          ],
    },
    coverage: {
      provider: 'v8',
      include: ['src/**'],
    },
    typecheck: {
      enabled: false,
    },
    testTimeout: 10_000,
  },
});
