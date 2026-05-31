import {playwright} from '@vitest/browser-playwright';
import {defineConfig} from 'vitest/config';
import type {BrowserConfigOptions, BrowserInstanceOption} from 'vitest/node';
import {makeDefine} from '../build.ts';

export const CI = process.env['CI'] === 'true' || process.env['CI'] === '1';
const browserEnv = process.env['VITEST_BROWSER'];

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

assertValidBrowser(browserEnv);

const define = {
  ...makeDefine(),
  'import.meta.env.VITEST': 'true',
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

function newBrowserConfig(): BrowserConfigOptions {
  if (browserEnv) {
    const browser = browserEnv as 'chromium' | 'firefox' | 'webkit';
    return {
      enabled: true,
      provider: playwright(),
      headless: true,
      screenshotFailures: false,
      instances: [{browser}],
    };
  }

  const instances: BrowserInstanceOption[] = [{browser: 'chromium'}];

  if (CI) {
    instances.push({browser: 'firefox'}, {browser: 'webkit'});
  }

  return {
    enabled: true,
    provider: playwright(),
    headless: true,
    screenshotFailures: false,
    instances,
  };
}

export function newConfig() {
  const browser = newBrowserConfig();

  return defineConfig({
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
      browser,

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
}

export default newConfig();

const externalizedWarningRegExp =
  /has been externalized for browser compatibility/;

export function newBenchConfig() {
  const browser = newBrowserConfig();

  return defineConfig({
    define: {
      ...define,
      'process.env.NO_COLOR': JSON.stringify(process.env.NO_COLOR ?? ''),
      'process.env.NODE_DISABLE_COLORS': JSON.stringify(
        process.env.NODE_DISABLE_COLORS ?? '',
      ),
      'process.env.BENCH_OUTPUT_FORMAT': JSON.stringify(
        process.env.BENCH_OUTPUT_FORMAT ?? '',
      ),
      'process.env.BENCH_SUMMARY': JSON.stringify(
        process.env.BENCH_SUMMARY ?? '',
      ),
    },

    test: {
      include: ['src/**/*.bench{,.node}.?(c|m)[jt]s?(x)'],
      disableConsoleIntercept: true,
      silent: false,
      onConsoleLog(str, type, _entity) {
        if (externalizedWarningRegExp.test(str)) {
          return false;
        }
        if (type === 'stderr') {
          console.error(str);
        } else {
          console.log(str);
        }
        return false;
      },
      browser,
      slowTestThreshold: 15_000,
      testTimeout: 60_000,
      hookTimeout: 60_000,
      // Run bench files sequentially to avoid memory contention between workers.
      maxWorkers: 1,
    },

    optimizeDeps: {
      exclude: ['@mitata/counters'],
    },

    server: {
      headers: {
        'Cross-Origin-Opener-Policy': 'same-origin',
        'Cross-Origin-Embedder-Policy': 'require-corp',
      },
    },
  });
}

export const benchConfig = newBenchConfig();
