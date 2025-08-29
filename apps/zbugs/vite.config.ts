import react from '@vitejs/plugin-react';
import {defineConfig, type ViteDevServer} from 'vite';
import svgr from 'vite-plugin-svgr';
import tsconfigPaths from 'vite-tsconfig-paths';
import {makeDefine} from '../../packages/shared/src/build.ts';

async function configureServer(server: ViteDevServer) {
  const {fastify} = await import('./api/index.js');
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
    rollupOptions: {
      external: ['expo-sqlite'],
    },
  },
  resolve: {
    alias: {
      // Mock expo-sqlite to prevent eager transformation by Vitest
      // Even though it's loaded via dynamic import, Vitest eagerly transforms it during dependency analysis
      'expo-sqlite': 'data:text/javascript,export default {}',
    },
  },
});
