import {makeDefine} from '../build.ts';

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

export default {
  // https://github.com/vitest-dev/vitest/issues/5332#issuecomment-1977785593
  optimizeDeps: {
    include: ['vitest > @vitest/expect > chai'],
  },
  define,
  esbuild: {
    define,
  },

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
      provider: 'playwright',
      headless: true,
      screenshotFailures: false,
      instances: [
        {browser: 'chromium'},
        {browser: 'firefox'},
        {browser: 'webkit'},
      ],
    },
    typecheck: {
      enabled: false,
    },
    testTimeout: 10_000,
  },
};
