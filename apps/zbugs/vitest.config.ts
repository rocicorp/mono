import {dirname, resolve} from 'path';
import {fileURLToPath} from 'url';
import tsconfigPaths from 'vite-tsconfig-paths';
import {defineConfig, mergeConfig} from 'vitest/config';
import config from '../../packages/shared/src/tool/vitest-config.ts';

const ci = process.env['CI'] === 'true' || process.env['CI'] === '1';

function nameFromURL(url: string) {
  return url.match(/\/apps\/([^/]+)/)?.[1] ?? 'unknown';
}

export function configForVersion(version: number, url: string) {
  const name = nameFromURL(url);
  const rootDir = dirname(fileURLToPath(url));
  const packagesDir = resolve(rootDir, '../../packages');

  return mergeConfig(config, {
    plugins: [tsconfigPaths()],
    resolve: {
      alias: [
        {
          find: '@rocicorp/zero/test-helpers',
          replacement: resolve(packagesDir, 'zero/src/test-helpers.ts'),
        },
        {
          find: '@rocicorp/zero',
          replacement: resolve(packagesDir, 'zero/src/zero.ts'),
        },
      ],
    },
    test: {
      name: `${name}/pg-${version}`,
      browser: {enabled: false},
      silent: 'passed-only',
      include: [
        'src/**/*.pg.test.?(c|m)[jt]s?(x)',
        'server/**/*.pg.test.?(c|m)[jt]s?(x)',
        'shared/**/*.pg.test.?(c|m)[jt]s?(x)',
      ],
      exclude: [],
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
  const rootDir = dirname(fileURLToPath(url));
  const packagesDir = resolve(rootDir, '../../packages');

  return mergeConfig(config, {
    plugins: [tsconfigPaths()],
    resolve: {
      alias: [
        {
          find: '@rocicorp/zero/test-helpers',
          replacement: resolve(packagesDir, 'zero/src/test-helpers.ts'),
        },
        {
          find: '@rocicorp/zero',
          replacement: resolve(packagesDir, 'zero/src/zero.ts'),
        },
      ],
    },
    test: {
      name: `${name}/no-pg`,
      browser: {enabled: false},
      silent: 'passed-only',
      include: [
        'src/**/*.test.?(c|m)[jt]s?(x)',
        'server/**/*.test.?(c|m)[jt]s?(x)',
        'shared/**/*.test.?(c|m)[jt]s?(x)',
      ],
      exclude: [
        'src/**/*.pg.test.?(c|m)[jt]s?(x)',
        'server/**/*.pg.test.?(c|m)[jt]s?(x)',
        'shared/**/*.pg.test.?(c|m)[jt]s?(x)',
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
    projects: [
      configForNoPg(import.meta.url),
      configForVersion(16, import.meta.url),
    ],
  },
});
