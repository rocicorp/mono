import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import tsconfigPaths from 'vite-tsconfig-paths';
import {defineConfig} from 'vitest/config';

function getVersion(name: string) {
  const url = new URL(`../${name}/package.json`, import.meta.url);
  const s = readFileSync(fileURLToPath(url), 'utf-8');
  return JSON.parse(s).version;
}

export default defineConfig({
  // https://github.com/vitest-dev/vitest/issues/5332#issuecomment-1977785593
  optimizeDeps: {
    include: ['vitest > @vitest/expect > chai'],
    exclude: ['wa-sqlite'],
  },
  esbuild: {
    define: {
      ['REPLICACHE_VERSION']: JSON.stringify(getVersion('replicache')),
      ['ZERO_VERSION']: JSON.stringify(getVersion('zero-client')),
      ['TESTING']: JSON.stringify(true),
    },
  },

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  plugins: [tsconfigPaths()] as any[],

  test: {
    onConsoleLog(log) {
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
    },
    include: ['src/**/*.{test,spec}.?(c|m)[jt]s?(x)'],
    browser: {
      enabled: true,
      provider: 'playwright',
      headless: true,
      name: 'chromium',
    },
    typecheck: {
      enabled: false,
    },
  },
});
