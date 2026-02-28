import react from '@vitejs/plugin-react';
import {fileURLToPath, URL} from 'node:url';
import {defineConfig, type PluginOption, type ViteDevServer} from 'vite';
import svgr from 'vite-plugin-svgr';
import tsconfigPaths from 'vite-tsconfig-paths';
import {makeDefine} from '../../packages/shared/src/build.ts';
import {fastify} from './api/index.ts';

const zeroReactPath = fileURLToPath(
  new URL('../../packages/zero/src/react.ts', import.meta.url),
);
const zeroPath = fileURLToPath(
  new URL('../../packages/zero/src/zero.ts', import.meta.url),
);

async function configureServer(server: ViteDevServer) {
  await fastify.ready();
  server.middlewares.use((req, res, next) => {
    if (!req.url?.startsWith('/api')) {
      return next();
    }
    fastify.server.emit('request', req, res);
  });
}

export default defineConfig({
  // Keep a single runtime identity for `@rocicorp/zero` and `@rocicorp/zero/react`.
  // Without this + `optimizeDeps.exclude` below, Vite can prebundle
  // `@rocicorp/zero-virtual` against a different module instance, which breaks
  // Zero context/query internals checks in dev.
  resolve: {
    dedupe: ['@rocicorp/zero', '@rocicorp/zero/react'],
    alias: [
      {find: '@rocicorp/zero/react', replacement: zeroReactPath},
      {find: '@rocicorp/zero', replacement: zeroPath},
    ],
  },
  optimizeDeps: {
    exclude: [
      '@rocicorp/zero',
      '@rocicorp/zero/react',
      '@rocicorp/zero-virtual',
    ],
  },
  plugins: [
    tsconfigPaths(),
    svgr() as unknown as PluginOption,
    react() as unknown as PluginOption,
    {
      name: 'api-server',
      configureServer,
    },
  ],
  define: makeDefine(),
  build: {
    target: 'esnext',
    rollupOptions: {
      input: {
        main: '/index.html',
        debug: '/debug.html',
        roci: '/roci.html',
      },
    },
  },
});
