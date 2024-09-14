import {configDefaults, defineConfig} from 'vitest/config';
import {config} from '../shared/src/tool/vitest-config.js';

const {define, plugins, esbuild} = config;

export default defineConfig({
  plugins,
  define,
  esbuild,
  test: {
    fakeTimers: {
      toFake: [...(configDefaults.fakeTimers.toFake ?? []), 'performance'],
    },
  },
});
