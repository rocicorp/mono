import {resolve} from 'node:path';
import {defineConfig} from 'vite';
import {define, external} from './build-common.ts';

// Bundle size dashboard config: single entry, no code splitting, minified
// Uses esbuild's dropLabels to strip BUNDLE_SIZE labeled code blocks
export default defineConfig({
  logLevel: 'warn',
  build: {
    outDir: 'out',
    emptyOutDir: false,
    sourcemap: true,
    target: 'es2022',
    ssr: true,
    reportCompressedSize: false,
    minify: true,
    rollupOptions: {
      external,
      input: {
        // Single entry point for bundle size measurement
        zero: resolve(import.meta.dirname, '../src/zero.ts'),
      },
      output: {
        format: 'es',
        entryFileNames: '[name].js',
        // No code splitting for bundle size measurements
        inlineDynamicImports: true,
      },
      treeshake: {
        moduleSideEffects: false,
      },
    },
  },
  define,
  resolve: {
    conditions: ['import', 'module', 'default'],
  },
  esbuild: {
    dropLabels: ['BUNDLE_SIZE'],
  },
});
