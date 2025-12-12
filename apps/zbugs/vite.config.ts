import react from '@vitejs/plugin-react';
import {defineConfig, type PluginOption, type ViteDevServer} from 'vite';
import svgr from 'vite-plugin-svgr';
import tsconfigPaths from 'vite-tsconfig-paths';
import {makeDefine} from '../../packages/shared/src/build.ts';
import {fastify} from './api/index.ts';

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
