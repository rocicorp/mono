import {defineConfig} from 'vitest/config';

export default defineConfig({
  test: {
    onConsoleLog(log) {
      if (
        log.includes('Skipping license check for TEST_LICENSE_KEY.') ||
        log.includes('REPLICACHE LICENSE NOT VALID') ||
        log.includes('enableAnalytics false') ||
        log.includes('no such entity')
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
