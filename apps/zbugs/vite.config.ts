import react from '@vitejs/plugin-react';
import {defineConfig, type ViteDevServer} from 'vite';
import svgr from 'vite-plugin-svgr';
import {makeDefine} from '../../packages/shared/src/build.js';
import {fastify} from './api/index.js';

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
    svgr(),
    react(),
    {
      name: 'api-server',
      configureServer,
    },
  ],
  define: makeDefine(),
  build: {
    target: 'esnext',
  },
});
