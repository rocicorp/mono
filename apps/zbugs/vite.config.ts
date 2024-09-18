import react from '@vitejs/plugin-react';
import {makeDefine} from 'shared/src/build.js';
import {defineConfig, ViteDevServer} from 'vite';
import svgr from 'vite-plugin-svgr';
import tsconfigPaths from 'vite-tsconfig-paths';
import {app} from './api/index.js';

async function configureServer(server: ViteDevServer) {
  await app.ready();
  server.middlewares.use((req, res, next) => {
    if (!req.url.startsWith('/api')) {
      return next();
    }
    app.server.emit('request', req, res);
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
  },
});
