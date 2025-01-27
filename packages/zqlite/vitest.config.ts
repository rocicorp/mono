import {configDefaults, defineConfig} from 'vitest/config';
import config from '../shared/src/tool/vitest-config.ts';

const {define, esbuild} = config;

export default defineConfig({
  define,
  esbuild,
  test: {
    fakeTimers: {
      toFake: [...(configDefaults.fakeTimers.toFake ?? []), 'performance'],
    },
  },
});
