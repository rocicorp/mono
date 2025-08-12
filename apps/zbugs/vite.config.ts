import react from '@vitejs/plugin-react';
import {defineConfig, loadEnv, type ViteDevServer} from 'vite';
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

export default defineConfig(({mode}) => {
  const env = loadEnv(mode, process.cwd(), '');
  return {
    plugins: [
      tsconfigPaths(),
      svgr(),
      react(),
      {
        name: 'api-server',
        configureServer,
      },
    ],
    define: {
      ...makeDefine(),
      'process.env.AWS_REGION': JSON.stringify(env.AWS_REGION),
      'process.env.AWS_ACCESS_KEY_ID': JSON.stringify(env.AWS_ACCESS_KEY_ID),
      'process.env.AWS_SECRET_ACCESS_KEY': JSON.stringify(
        env.AWS_SECRET_ACCESS_KEY,
      ),
    },
    build: {
      target: 'esnext',
    },
  };
});
