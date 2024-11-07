import {defineConfig, defineWorkspace, mergeConfig} from 'vitest/config';
import {config} from '../shared/src/tool/vitest-config.js';

const {define, esbuild} = config;

const baseConfig = defineConfig({define, esbuild});

const pgConfigForVersion = (version: number) =>
  mergeConfig(baseConfig, {
    test: {
      name: `pg-${version}`,
      include: ['src/**/*.pg-test.?(c|m)[jt]s?(x)'],
      globalSetup: [`./test/pg-${version}.ts`],
    },
  });

export default defineWorkspace([
  mergeConfig(baseConfig, {
    test: {
      name: 'no-pg',
      include: ['src/**/*.test.?(c|m)[jt]s?(x)'],
    },
  }),
  pgConfigForVersion(15),
  pgConfigForVersion(16),
  pgConfigForVersion(17),
]);
