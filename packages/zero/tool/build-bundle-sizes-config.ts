import {resolve} from 'node:path';
import {defineConfig, mergeConfig} from 'vite';
import {baseConfig} from './build-base-config.ts';

// Bundle size dashboard config: single entry, no code splitting, minified
// Uses esbuild's dropLabels to strip BUNDLE_SIZE labeled code blocks
export default defineConfig(
  mergeConfig(baseConfig, {
    build: {
      minify: true,
      rollupOptions: {
        input: {
          // Resolve from package root (where vite is run from), not tool directory
          zero: resolve(import.meta.dirname, '../src/zero.ts'),
        },
        output: {
          // No code splitting for bundle size measurements
          inlineDynamicImports: true,
        },
        treeshake: {
          moduleSideEffects: false,
        },
      },
    },
    esbuild: {
      dropLabels: ['BUNDLE_SIZE'],
    },
  }),
);
