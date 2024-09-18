import {getRequestListener} from '@hono/node-server';
import react from '@vitejs/plugin-react';
import {makeDefine} from 'shared/src/build.js';
import {defineConfig} from 'vite';
import svgr from 'vite-plugin-svgr';
import tsconfigPaths from 'vite-tsconfig-paths';
import {app} from './api/index.js';

export default defineConfig({
  plugins: [
    tsconfigPaths(),
    svgr(),
    react(),
    {
      name: 'api-server',
      configureServer(server) {
        server.middlewares.use((req, res, next) => {
          if (!req.url.startsWith('/api')) {
            return next();
          }
          getRequestListener(async request => {
            return await app.fetch(request, {});
          })(req, res);
        });
      },
    },
  ],
  define: makeDefine(),
  build: {
    target: 'esnext',
  },
});
