import {defineConfig, mergeConfig} from 'vitest/config';
import config from '../../packages/shared/src/tool/vitest-config.ts';

const ci = process.env['CI'] === 'true' || process.env['CI'] === '1';

function nameFromURL(url: string) {
  return url.match(/\/apps\/([^/]+)/)?.[1] ?? 'unknown';
}

export function configForVersion(version: number, url: string) {
  const name = nameFromURL(url);
  return mergeConfig(config, {
    test: {
      name: `${name}/pg-${version}`,
      browser: {enabled: false},
      silent: false,
      include: [
        'src/**/*.pg-test.?(c|m)[jt]s?(x)',
        'server/**/*.pg-test.?(c|m)[jt]s?(x)',
      ],
      exclude: [
        'src/**/*.test.?(c|m)[jt]s?(x)',
        'server/**/*.test.?(c|m)[jt]s?(x)',
      ],
      globalSetup: ['../../packages/zero-cache/test/pg-16.ts'],
      coverage: {
        enabled: !ci,
        reporter: [['html'], ['clover', {file: 'coverage.xml'}]],
        include: ['src/**', 'server/**'],
      },
    },
  });
}

export function configForNoPg(url: string) {
  const name = nameFromURL(url);
  return mergeConfig(config, {
    test: {
      name: `${name}/no-pg`,
      browser: {enabled: false},
      silent: false,
      include: [
        'src/**/*.test.?(c|m)[jt]s?(x)',
        'server/**/*.test.?(c|m)[jt]s?(x)',
      ],
      exclude: [
        'src/**/*.pg-test.?(c|m)[jt]s?(x)',
        'server/**/*.pg-test.?(c|m)[jt]s?(x)',
      ],
      coverage: {
        enabled: !ci,
        reporter: [['html'], ['clover', {file: 'coverage.xml'}]],
        include: ['src/**', 'server/**'],
      },
    },
  });
}

export default defineConfig({
  test: {
    workspace: [
      configForNoPg(import.meta.url),
      configForVersion(16, import.meta.url),
    ],
  },
});
