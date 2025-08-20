import {defineConfig, mergeConfig} from 'vitest/config';
import config from '../shared/src/tool/vitest-config.ts';

const ci = process.env['CI'] === 'true' || process.env['CI'] === '1';

function nameFromURL(url: string) {
  // importer looks like file://....../packages/NAME/... and we want the NAME
  return url.match(/\/packages\/([^/]+)/)?.[1] ?? 'unknown';
}

export function configForVersion(version: number, url: string) {
  const name = nameFromURL(url);
  return mergeConfig(config, {
    test: {
      name: `${name}/pg-${version}`,
      browser: {enabled: false},
      silent: 'passed-only',
      include: ['src/**/*.pg-test.?(c|m)[jt]s?(x)'],
      exclude: ['src/**/*.test.?(c|m)[jt]s?(x)'],
      globalSetup: [`../zero-cache/test/pg-${version}.ts`],
      coverage: {
        enabled: !ci, // Don't run coverage in continuous integration.
        reporter: [['html'], ['clover', {file: 'coverage.xml'}]],
        include: ['src/**'],
      },
      testTimeout: 20000,
      hookTimeout: 20000,
    },
  });
}

export function configForNoPg(url: string) {
  const name = nameFromURL(url);
  return mergeConfig(config, {
    test: {
      name: `${name}/no-pg`,
      include: ['src/**/*.test.?(c|m)[jt]s?(x)'],

      browser: {enabled: false},
      silent: 'passed-only',
      coverage: {
        enabled: !ci, // Don't run coverage in continuous integration.
        reporter: [['html'], ['clover', {file: 'coverage.xml'}]],
        include: ['src/**'],
      },
    },
  });
}

// To run tests against a custom Postgres instance (e.g. Aurora), specify
// the connection string in the CUSTOM_PG environment variable, and optionally
// limit the test runner to the "custom-pg" project:
//
// CUSTOM_PG=postgresql://... npm run test -- --project custom-pg
export function configForCustomPg(url: string) {
  if (process.env['CUSTOM_PG']) {
    const name = nameFromURL(url);
    return [
      mergeConfig(config, {
        test: {
          name: `${name}/custom-pg`,
          browser: {enabled: false},
          silent: 'passed-only',
          include: ['src/**/*.pg-test.?(c|m)[jt]s?(x)'],
          exclude: ['src/**/*.test.?(c|m)[jt]s?(x)'],
          provide: {
            // Referenced by ./src/test/db.ts
            pgConnectionString: process.env['CUSTOM_PG'],
          },
        },
      }),
    ];
  }
  return [];
}

export default defineConfig({
  test: {
    projects: ['vitest.config.*.ts', ...configForCustomPg(import.meta.url)],
  },
});
