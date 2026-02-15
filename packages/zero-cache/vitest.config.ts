import {defineConfig, mergeConfig} from 'vitest/config';
import config, {CI} from '../shared/src/tool/vitest-config.ts';

function nameFromURL(url: string) {
  // importer looks like file://....../packages/NAME/... and we want the NAME
  return url.match(/\/packages\/([^/]+)/)?.[1] ?? 'unknown';
}

export function configForVersion(version: number, url: string) {
  const TIMEOUT = (CI ? 2 : 1) * 20_000;
  const name = nameFromURL(url);
  const merged = mergeConfig(config, {
    test: {
      name: `${name}/pg-${version}`,
      browser: {enabled: false},
      silent: 'passed-only',
      globalSetup: [`../zero-cache/test/pg-${version}.ts`],
      coverage: {
        enabled: !CI, // Don't run coverage in continuous integration.
        reporter: [['html'], ['clover', {file: 'coverage.xml'}]],
        include: ['src/**'],
      },
      testTimeout: TIMEOUT,
      hookTimeout: TIMEOUT,
    },
  });
  // Override include to only pg tests (mergeConfig merges arrays, we want to replace)
  merged.test.include = ['src/**/*.pg.test.?(c|m)[jt]s?(x)'];
  merged.test.exclude = [];
  return merged;
}

export function configForNoPg(url: string) {
  const name = nameFromURL(url);
  return mergeConfig(config, {
    test: {
      name: `${name}/no-pg`,
      include: ['src/**/*.test.?(c|m)[jt]s?(x)'],
      exclude: ['src/**/*.pg.test.?(c|m)[jt]s?(x)'],
      browser: {enabled: false},
      silent: 'passed-only',
      coverage: {
        enabled: !CI, // Don't run coverage in continuous integration.
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
          include: ['src/**/*.pg.test.?(c|m)[jt]s?(x)'],
          exclude: [],
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
