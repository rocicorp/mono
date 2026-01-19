import {defineConfig, mergeConfig} from 'vitest/config';
import config, {CI} from '../shared/src/tool/vitest-config.ts';

function nameFromURL(url: string) {
  // importer looks like file://....../packages/NAME/... and we want the NAME
  return url.match(/\/packages\/([^/]+)/)?.[1] ?? 'unknown';
}

export function configForVersion(version: number, url: string) {
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
      testTimeout: 20000,
      hookTimeout: 20000,
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

export default defineConfig({
  test: {
    projects: ['vitest.config.*.ts'],
  },
});
