import {svelte} from '@sveltejs/vite-plugin-svelte';
import {defineConfig} from 'vitest/config';
import {makeDefine} from '../shared/src/build.ts';

export default defineConfig({
  plugins: [svelte({compilerOptions: {hmr: false}})],
  define: {
    ...makeDefine(),
    'import.meta.env.VITEST': 'true',
  },
  test: {
    environment: 'node',
    include: ['src/**/*.{test,spec}.?(c|m)[jt]s?(x)'],
    silent: 'passed-only',
    coverage: {
      provider: 'v8',
      include: ['src/**'],
    },
    testTimeout: 10_000,
  },
});
