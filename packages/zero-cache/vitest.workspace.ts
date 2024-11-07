import {defineConfig, defineWorkspace, mergeConfig} from 'vitest/config';
import {config} from '../shared/src/tool/vitest-config.js';

const {define, esbuild} = config;

const baseConfig = defineConfig({
  define,
  esbuild,
  test: {
    // include: ['src/**/*.test.?(c|m)[jt]s?(x)'],
    onConsoleLog(log: string) {
      if (
        log.includes(
          'insert or update on table "fk_ref" violates foreign key constraint "fk_ref_ref_fkey"',
        )
      ) {
        return false;
      }
      return undefined;
    },
  },
});

const configFor = (pgName: string) =>
  mergeConfig(baseConfig, {
    test: {
      name: pgName,
      include: ['src/**/*.pg-test.?(c|m)[jt]s?(x)'],
      globalSetup: [`./test/${pgName}.ts`],
    },
  });

export default defineWorkspace([
  mergeConfig(baseConfig, {
    test: {
      name: 'no-pg',
      include: ['src/**/*.test.?(c|m)[jt]s?(x)'],
    },
  }),
  configFor('pg-15'),
  configFor('pg-16'),
  configFor('pg-17'),
]);
