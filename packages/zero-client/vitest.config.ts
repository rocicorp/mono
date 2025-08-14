import codspeedPlugin from '@codspeed/vitest-plugin';
import {defineConfig} from 'vitest/config';
import {assert} from '../shared/src/asserts.ts';
import baseConfig from '../shared/src/tool/vitest-config.ts';

assert(
  !('plugins' in baseConfig),
  'If we add plugins to the base update this code',
);

export default defineConfig({
  // Start from the shared config
  ...baseConfig,
  plugins: [codspeedPlugin()],
});
