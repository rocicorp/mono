import {defineConfig, type PluginOption, type ViteDevServer} from 'vite';
import solid from 'vite-plugin-solid';
import tsconfigPaths from 'vite-tsconfig-paths';
import {makeDefine} from '../../packages/shared/src/build.ts';
import {fastify} from '../zbugs/api/index.ts';

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
    tsconfigPaths() as unknown as PluginOption,
    solid() as unknown as PluginOption,
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
